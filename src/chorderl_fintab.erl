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

%% API
-export([init_finger_table/2, notify/3, fix_fingers/2]).
-export([find_successor/5, find_predecessor/4, update_successor/3]).

-export([get_finger_start/3]).

%% FingerTableList =  [
%%    {Start, Successor},
%%    {Start, Successor},
%%    {Start, Successor},
%%    ...
%% ]

%% Node N (NodeID) learns its predecessor and fingers by asking￼N' (PeerPid) to look them up
init_finger_table(NodeID, PeerPid) ->
  init_finger_table(NodeID, PeerPid, 1). %% todo

init_finger_table(NodeID, nil, 1) ->
  StartID = get_finger_start(NodeID, 1, ?M), % N.finger[1].start
  io:format("~p: (init_finger_table) First node. Setting successor to ourself...~n", [self()]),
  [{
    StartID,
    {NodeID, self()}
  }];
init_finger_table(NodeID, PeerPid, 1) ->
  Qref = make_ref(),
  StartID = get_finger_start(NodeID, 1, ?M), % N.finger[1].start
  io:format("~p: (init_finger_table) Finding successor...~n", [self()]),
  chorderl:cast_find_successor(PeerPid, Qref, {StartID, self()}),
  receive
    {Qref, {Skey, Spid}} ->
      io:format("~p: (init_finger_table) Found successor: ~p. Checking its predecessor...~n", [self(), Spid]),
      chorderl:cast_query_predecessor(Spid, self(), init_finger_table),
      [{
        StartID,
        {Skey, Spid}
      }]
  after ?Timeout ->
    io:format("~p: Time out: no response~n",[self()]),
    {error, timeout}
  end;
init_finger_table(NodeID, PeerPid, Index) ->
  todo.

fix_fingers(_I, LoopData) ->
  todo.

%% todo query_successor (reply at once) vs. find_successor (continue to find on other nodes? disrupt)?
find_successor({NewNodeID, From}, NodeID, Successor, FingerTableList, Qref) ->
  Result = find_predecessor(NewNodeID, NodeID, Successor, FingerTableList), %% find Predecessor for NewNodeID from LoopData
  case Result of
    {_PKey, Ppid} ->
      chorderl:cast_query_successor(Ppid, Qref, From);  %% Ask Ppid, reply to Pid
    nil ->
      From ! {Qref, {NodeID, self()}}
  end.

%% ask node NodeID to find NewNodeID's predecessor
find_predecessor(NewNodeID, NodeID, Successor, FingerTableList) ->
  case Successor of
    nil ->
      nil;
    {Skey, Spid} ->
      case chorderl_utils:is_between(NewNodeID, NodeID, Skey) or (NewNodeID == Skey) of %% (NodeID, Skey]
        true ->
          {Skey, Spid};
        false ->
          closest_preceding_finger(NewNodeID, NodeID, FingerTableList, ?M)
      end
  end.

%% return closest finger preceding NewNodeID
closest_preceding_finger(_NewNodeID, NodeID, FingerTableList, 1) ->
  {NodeID, self()};
closest_preceding_finger(_NewNodeID, NodeID, FingerTableList, Index) ->
  {NodeID, self()}. %% todo

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

get_successor(LoopData) ->
  todo.