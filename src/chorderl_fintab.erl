%%%-------------------------------------------------------------------
%%% @author bruce
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 30. Jun 2016 14:48
%%%-------------------------------------------------------------------
-module(chorderl_fintab).

-include("chorderl.hrl").

-compile([{parse_transform, lager_transform}]).


%% API
-export([init_finger_table/2, notify/3, fix_fingers/2]).
-export([find_successor/4, update_successor/3, fetch_successor/1]).
-export([closest_preceding_finger/3]).
-export([get_finger_start/3]).
-export([the_condition/3]).
-export([find_predecessor_remote_continued/3]).
-export([find_predecessor_remote_continued_2/2]).


%% FingerTableList =  [
%%    {Start, Successor},
%%    {Start, Successor},
%%    {Start, Successor},
%%    ...
%% ]

%% Node N (NodeID) learns its predecessor and fingers by asking￼N' (PeerPid) to look them up
init_finger_table(NodeID, nil) ->
  io:format("~p: (init_finger_table) First node. Setting all fingers to ourself...~n", [self()]),
  FingerTableList = fill_finger_table_with_self(NodeID),
  FingerTableList;
init_finger_table(NodeID, PeerPid) ->
  io:format("~p: (init_finger_table) Finding successor...~n", [self()]),
  StartID = get_finger_start(NodeID, 1, ?M), % N.finger[1].start
  Result = chorderl:call_find_successor(PeerPid, {StartID, self()}),
  FirstFinger = { StartID, Result },
%%  chorderl:cast_find_successor(PeerPid, Qref, {StartID, self()}),
%%  Finger =
%%    receive {Qref, {Skey, Spid}} ->
%%      io:format("~p: (init_finger_table) Found successor: ~p~n", [self(), Spid]),
%%        %%io:format("~p: (init_finger_table) Found successor: ~p. Checking its predecessor...~n", [self(), Spid]),
%%        %%chorderl:cast_query_predecessor(Spid, self(), init_finger_table),
%%      { StartID, {Skey, Spid} }
%%    after ?Timeout ->
%%      io:format("~p: (init_finger_table) Time out: no response~n",[self()]),
%%      {error, timeout}
%%    end,
  FingerTableList = fill_finger_table(NodeID, PeerPid, [FirstFinger]),
  FingerTableList.

fill_finger_table(NodeID, PeerPid, FingerTableList) ->
  fill_finger_table(NodeID, PeerPid, FingerTableList, 1). %% from i = 1 to m - 1

fill_finger_table(_NodeID, _PeerPid, FingerTableList, ?M) ->
  FingerTableList;
fill_finger_table(NodeID, PeerPid, FingerTableList, Index) ->
  NextStartID = get_finger_start(NodeID, Index+1, ?M), % N.finger[i+1].start
  { _StartID, {Fkey, Fpid} } = lists:nth(Index, FingerTableList), % N.finger[i].node
  NextFinger =
    case ( chorderl_utils:is_between(NextStartID, NodeID, Fkey) or (NextStartID == NodeID) ) of
       true ->
         %%io:format("~p: (init_finger_table) Copying previous finger...~n", [self()]),
         {NextStartID, {Fkey, Fpid} };
       false ->
         Result = chorderl:call_find_successor(PeerPid, {NextStartID, self()}),
         { NextStartID, Result }
%%         chorderl:cast_find_successor(PeerPid, Qref, {NextStartID, self()}),
%%         receive
%%           {Qref, {Skey, Spid}} ->
%%             io:format("~p: (fill_finger_table) Found finger: ~p~n", [self(), Spid]),
%%             %%io:format("~p: (init_finger_table) Found successor: ~p. Checking its predecessor...~n", [self(), Spid]),
%%             %%chorderl:cast_query_predecessor(Spid, self(), init_finger_table),
%%             { NextStartID, {Skey, Spid} }
%%         after ?Timeout ->
%%           io:format("~p: (fill_finger_table) Time out: no response~n",[self()]),
%%           {error, timeout}
%%         end
      end,
  fill_finger_table(NodeID, PeerPid, FingerTableList ++ [NextFinger], Index+1).

fill_finger_table_with_self(NodeID) ->
  fill_finger_table_with_self(NodeID, [], 1).

fill_finger_table_with_self(_NodeID, List, ?M+1) ->
  List;
fill_finger_table_with_self(NodeID, List, I) ->
  StartID = get_finger_start(NodeID, I, ?M), % N.finger[i].start
  Finger = {StartID, {NodeID, self()}},
  fill_finger_table_with_self(NodeID, List ++ [Finger], I + 1).

%%fill_finger_table(NodeID, List, End, End) ->
%%  List;
%%fill_finger_table(NodeID, List, I, End) ->
%%  Finger = lists:nth(I, List), % N.finger[i]
%%  { _StartID, {Fkey, Fpid} } = Finger, % N.finger[i].node
%%  StartID1 = get_finger_start(NodeID, I + 1, ?M), % N.finger[i+1].start
%%  Finger1 = case ( chorderl_utils:is_between(StartID1, NodeID, Fkey) or StartID1 == NodeID ) of
%%               true ->
%%                 {StartID1, {Fkey, Fpid} };
%%               false ->
%%                 Qref = make_ref(),
%%                 chorderl:cast_find_successor(PeerPid, Qref, {StartID, self()}),
%%                 receive
%%                   {Qref, {Skey, Spid}} ->
%%                     io:format("~p: (init_finger_table) Found successor: ~p~n", [self(), Spid]),
%%                     %%io:format("~p: (init_finger_table) Found successor: ~p. Checking its predecessor...~n", [self(), Spid]),
%%                     %%chorderl:cast_query_predecessor(Spid, self(), init_finger_table),
%%                     FingerTableList =
%%                       [{
%%                         StartID,
%%                         {Skey, Spid}
%%                       }],
%%                     FingerTableList
%%                 after ?Timeout ->
%%                   io:format("~p: Time out: no response~n",[self()]),
%%                   {error, timeout}
%%                 end
%%              end,
%%  fill_finger_table(NodeID, List ++ [Finger1], I + 1, End).

fix_fingers(NodeID, FingerTableList) ->
  Index = random:uniform(?M),
  { StartID, OldFinger } = lists:nth(Index, FingerTableList),
  io:format("~p: (fix_fingers) Fixing index ~p: ~p ~n", [self(), Index, { StartID, OldFinger }]),
  %%Result = chorderl:call_find_successor(self(), Qref, { StartID, self()} ),
  case find_successor({ StartID, Index }, NodeID, FingerTableList, fix_fingers) of
    {ok, Result} ->
      NewFinger = { StartID, Result },
      if
        Result /= OldFinger ->
          io:format("~p: (fix_fingers) Finger change: {~p, ~p -> ~p}~n", [self(), StartID, chorderl_utils:display_node(OldFinger), chorderl_utils:display_node(Result)]);
        true -> ok
      end,
%%  chorderl:cast_find_successor(chorderl_utils:node_id_to_proc_name(NodeID), Qref, { StartID, self()} ),
%%  NewFinger =
%%    receive
%%      {Qref, {Skey, Spid}} ->
%%        io:format("~p: (fix_fingers) Found finger: ~p~n", [self(), Spid]),
%%        %%io:format("~p: (init_finger_table) Found successor: ~p. Checking its predecessor...~n", [self(), Spid]),
%%        %%chorderl:cast_query_predecessor(Spid, self(), init_finger_table),
%%        { StartID, {Skey, Spid} }
%%    after ?Timeout ->
%%      io:format("~p: (fix_fingers) Time out: no response~n",[self()]),
%%      {error, timeout}
%%    end,
      NewFingerTableList = lists:sublist(FingerTableList, Index-1) ++ [ NewFinger | lists:nthtail(Index, FingerTableList)], %% Replace Finger with NewFinger = {StartID, Result} at Index
      %%NewFingerTableList = lists:keyreplace( StartID, 1, FingerTableList, NewFinger ),
      NewFingerTableList;
    {later, _StartID} ->
      io:format("~p: (fix_fingers) Finger {~p, ~p} will be updated later~n", [self(), StartID, OldFinger]),
      FingerTableList
  end.

%% todo query_successor (reply at once) vs. find_successor (continue to find on other nodes? disrupt)?
find_successor({StartID, Index}, NodeID, FingerTableList, fix_fingers) ->
  PredResult = find_predecessor(StartID, NodeID, FingerTableList, fix_fingers), %% find Predecessor for StartID
  case PredResult of
    {NodeID, _Self} ->
      io:format("~p: (find_successor 1) Predecessor found: ~p ---> ~p ~n", [self(), PredResult, StartID]),
      Result1 = fetch_successor(FingerTableList),
      io:format("~p: (find_successor 1) Successor found: ~p ---> ~p ~n", [self(), StartID, Result1]),
      {ok, Result1};
    {_PKey, Ppid} ->
      io:format("~p: (find_successor 2) Predecessor found: ~p ---> ~p ~n", [self(), PredResult, StartID]),
      Result2 = chorderl:cast_query_successor(Ppid, self(), {{fix_fingers, find_successor}, StartID, Index}), %% todo change to async
      io:format("~p: (find_successor 2) Successor found: ~p ---> ~p ~n", [self(), StartID, Result2]),
      {later, StartID};  %% Ask Ppid
    {later, StartID} ->
      {later, StartID};
    nil ->
      io:format("~p: (find_successor 3) Predecessor found: ~p ---> ~p ~n", [self(), PredResult, StartID]),
      %% From ! {Qref, {NodeID, self()}}
      io:format("~p: (find_successor 3) Successor found: ~p ---> ~p ~n", [self(), StartID, {NodeID, self()}]),
      {ok, {NodeID, self()}}
  end;
find_successor({NewNodeID, From}, NodeID, FingerTableList, node_join) ->
  PredResult = find_predecessor(NewNodeID, NodeID, FingerTableList, node_join), %% find Predecessor for NewNodeID
  case PredResult of
    {NodeID, _Self} ->
      io:format("~p: (find_successor 1) Predecessor found: ~p ---> ~p ~n", [self(), PredResult, NewNodeID]),
      Result1 = fetch_successor(FingerTableList),
      io:format("~p: (find_successor 1) Successor found: ~p ---> ~p ~n", [self(), NewNodeID, Result1]),
      {ok, Result1};
    {_PKey, Ppid} ->
      io:format("~p: (find_successor 2) Predecessor found: ~p ---> ~p ~n", [self(), PredResult, NewNodeID]),
      Result2 = chorderl:call_query_successor(Ppid),
      io:format("~p: (find_successor 2) Successor found: ~p ---> ~p ~n", [self(), NewNodeID, Result2]),
      {ok, Result2};  %% Ask Ppid
    nil ->
      io:format("~p: (find_successor 3) Predecessor found: ~p ---> ~p ~n", [self(), PredResult, NewNodeID]),
      %% From ! {Qref, {NodeID, self()}}
      io:format("~p: (find_successor 3) Successor found: ~p ---> ~p ~n", [self(), NewNodeID, {NodeID, self()}]),
      {ok, {NodeID, self()}}
  end.

%% ask node NodeID to find NewNodeID's predecessor
%% n.find_predecessor(id)
%%  n' = n;
%%  while(id not belongsto (n', n'.successor])
%%    n' = n'.closest_preceding_finger(id);
%%  return n';
find_predecessor(NewNodeID, NodeID, FingerTableList, fix_fingers) ->
  Result = find_predecessor_local(NewNodeID, NodeID, FingerTableList),
  case Result of
    {ok, Pred} ->
      Pred;
    {continue, Finger} ->
      find_predecessor_remote(NewNodeID, Finger, fix_fingers) %% todo not remote
  end;
find_predecessor(NewNodeID, NodeID, FingerTableList, node_join) ->
  Result = find_predecessor_local(NewNodeID, NodeID, FingerTableList),
  case Result of
    {ok, Pred} ->
      Pred;
    {continue, Finger} ->
      find_predecessor_remote(NewNodeID, Finger, node_join)
  end.

find_predecessor_local(NewNodeID, NodeID, FingerTableList) ->
  io:format("~p: (find_predecessor) Finding predecessor of ~p locally~n", [self(), NewNodeID]),
  {Skey, _Spid} = fetch_successor(FingerTableList),
  case the_condition(NewNodeID, NodeID, Skey) of
    true ->
      {Fkey, Fpid} = closest_preceding_finger(NewNodeID, NodeID, FingerTableList),
      {continue, {Fkey, Fpid}};     %%  evalue id not belongsto (n', n'.successor] again
    false ->
      {ok, {NodeID, self()}}
  end.

find_predecessor_remote(NewNodeID, {Nkey, Npid}, fix_fingers) -> %% todo change to async when fix_fingers
  io:format("~p: (find_predecessor_remote) Finding predecessor of ~p remotely at ~p~n", [self(), NewNodeID, {Nkey, Npid}]),
  chorderl:cast_query_successor(
    Npid,
    self(),
    {{fix_fingers, find_predecessor_remote}, NewNodeID, {Nkey, Npid}}
  ), %% Ask Ppid
  {later, NewNodeID};
find_predecessor_remote(NewNodeID, {Nkey, Npid}, node_join) ->
  io:format("~p: (find_predecessor_remote) Finding predecessor of ~p remotely at ~p~n", [self(), NewNodeID, {Nkey, Npid}]),
  {Skey, _Spid} = chorderl:call_query_successor(Npid),  %% Ask Ppid
  case the_condition(NewNodeID, Nkey, Skey) of
    true ->
      {Fkey, Fpid} = chorderl:call_closest_preceding_finger(Npid, NewNodeID),
      find_predecessor_remote(NewNodeID, {Fkey, Fpid}, node_join); %% recursive RPC call
    false ->
      {Nkey, Npid}
  end.

find_predecessor_remote_continued(NewNodeID, {Nkey, Npid}, {Skey, _Spid}) ->
  case the_condition(NewNodeID, Nkey, Skey) of
    true ->
      chorderl:cast_closest_preceding_finger(Npid, self(), NewNodeID),
      {later, NewNodeID};
    false ->
      {Nkey, Npid}
  end.

find_predecessor_remote_continued_2(NewNodeID, {Fkey, Fpid}) ->
  find_predecessor_remote(NewNodeID, {Fkey, Fpid}, fix_fingers). %% recursive RPC call

the_condition(NewNodeID, NodeID, Skey) ->
  not(chorderl_utils:is_between(NewNodeID, NodeID, Skey) or (NewNodeID == Skey)). %% (NodeID, Skey]

closest_preceding_finger(NewNodeID, NodeID, FingerTableList) ->
  closest_preceding_finger(NewNodeID, NodeID, FingerTableList, ?M).

%% return closest finger preceding NewNodeID
%% n.closest_preceding_finger(id)
%%   for i = m downto 1
%%     if(finger[i].node belongsto (n, id) )
%%       return finger[i].node;
%%   return n;
closest_preceding_finger(_NewNodeID, NodeID, _FingerTableList, 0) ->
  {NodeID, self()};
closest_preceding_finger(NewNodeID, NodeID, FingerTableList, Index) ->
  { _StartID, {Fkey, Fpid} }  = lists:nth(Index, FingerTableList),
  case chorderl_utils:is_between(Fkey, NodeID, NewNodeID) of
    true ->
      {Fkey, Fpid};
    false ->
      closest_preceding_finger(NewNodeID, NodeID, FingerTableList, Index-1)
  end.
%%  {NodeID, self()}. %% DEBUG


%% Pred: Our successor's Predecessor
%% NodeID: Our NodeID
%% Successor: Our current Successor
%% Returns: Our new Successor
% Asks Successor for Pred's successor, and decide whether Pred should be NodeID's (our) successor instead
update_successor(Pred, NodeID, Successor) ->
  {Skey, Spid} = Successor,
  case Pred of
    nil -> % Our Successor's predecessor is nil
      io:format("~p: Notifying ~p...~n", [self(), Spid]),
      chorderl:cast_notify(Spid, {NodeID, self()}),
      Successor;
    {NodeID, _} -> % Our Successor's predecessor is us (NodeID)
      Successor;
    {Skey, _} -> % Our Successor's predecessor is itself (Skey)
      io:format("~p: Notifying ~p...~n", [self(), Spid]),
      chorderl:cast_notify(Spid, {NodeID, self()}),
      Successor;
    {Xkey, Xpid} -> % Our Successor's predecessor is some other node X (Xkey)
      case chorderl_utils:is_between(Xkey, NodeID, Skey) of
        true -> % Our Successor's predecessor (Xkey) is between us and Successor (NodeId and Skey)
          io:format("~p: Notifying ~p...~n", [self(), Xpid]),
          chorderl:cast_notify(Xpid, {NodeID, self()}),
          Pred;
        false -> % Our Successor's predecessor (Xkey) is not between us and Successor (NodeId and Skey)
          io:format("~p: Notifying ~p...~n", [self(), Spid]),
          chorderl:cast_notify(Spid, {NodeID, self()}),
          Successor
      end
  end.

%% return: new Predecessor
notify({Nkey, Npid}, NodeID, Predecessor) ->
  case Predecessor of
    nil -> % Our Predecessor is nil
      {Nkey, Npid};
    {Pkey, _Ppid} ->
      case chorderl_utils:is_between(Nkey, Pkey, NodeID) of
        true -> % New node (Nkey) is between us and our Predecessor (NodeId and Pkey)
          %%io:format("~p: (notify) New node (~p) is between us and our Predecessor~n", [self(), Npid]),
          {Nkey, Npid};
        false -> % New node (Nkey) is not between us and our Predecessor (NodeId and Pkey)
          %%io:format("~p: (notify) New node (~p) is NOT between us and our Predecessor~n", [self(), Npid]),
          Predecessor
      end
  end.

%%
%% private functions
%%

%% finger[i].start = (n + 2^(i-1) ) mod 2^m, 1 <= i <= m
%get_finger_start(NodeID, I) ->
%  ( binary:decode_unsigned(NodeID, big) +
%    trunc(math:pow(2, I-1)) )
%    rem trunc(math:pow(2, ?M)). % encoding NodeID as unsigned big endian

get_finger_start(NodeID, I, M) ->
  ( NodeID +
    trunc(math:pow(2, I-1)) )
    rem trunc(math:pow(2, M)). % encoding NodeID as unsigned big endian

%% returns: {Upper, Lower} e.g. finger[1].interval = [finger[1].start, finger[2].start)
get_finger_interval(NodeID, I, M) ->
  {get_finger_start(NodeID, I, M), get_finger_start(NodeID, I+1, M)}.

fetch_successor(FingerTableList) ->
  { _StartID, Successor } = lists:nth( 1, FingerTableList ),
  Successor.
