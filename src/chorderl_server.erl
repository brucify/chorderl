%%%-------------------------------------------------------------------
%%% @author bruce
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 12. Aug 2016 18:50
%%%-------------------------------------------------------------------
-module(chorderl_server).

-include("chorderl.hrl").

-behaviour(gen_server).

%% API
-export([init/1, terminate/2, handle_cast/2, handle_info/2, code_change/3]).


%% Callback Functions

%% 1. Get predecessor
%% 2. Get successor
%% 3. Create fingers
%% 4. Create store
init([NodeID, PeerPid]) ->
  %% {ok, Successor} = connect(NodeID, PeerPid), % Ask Peer for our Successor
  FingerTableList = chorderl_fintab:init_finger_table(NodeID, PeerPid),
  {_StartID, Successor } = lists:nth( 1, FingerTableList ),
  %%Store = chorderl_store:create(list_to_atom(integer_to_list(NodeID)), latin1),
  Store = [],
  LoopData =
    #{node_id => NodeID,
      predecessor => nil,
      successor => Successor,
      fin_tab => FingerTableList,
      store => Store},
  {ok, LoopData}.

terminate(_Reason, _LoopData) ->
  ok.

handle_cast(stop, LoopData) ->
  {stop, normal, LoopData};

% New thinks it might be our predecessor
handle_cast({notify, New}, LoopData) ->
  NodeID = maps:get(node_id, LoopData),
  Predecessor = maps:get(predecessor, LoopData),
  NewPred = chorderl_fintab:notify(New, NodeID, Predecessor), % find out if New can be our new predecessor
  if
    NewPred /= Predecessor ->
      io:format("~p: (notify) Predecessor change: ~p -> ~p~n", [self(), chorderl_utils:display_node(Predecessor), chorderl_utils:display_node(NewPred)]);
    true -> ok
  end,
  {noreply, LoopData#{predecessor := NewPred}};

% Peer needs to know our key
handle_cast({query_id, Qref, From}, LoopData) ->
  NodeID = maps:get(node_id, LoopData),
  From ! {Qref, NodeID},
  {noreply, LoopData};

% Peer needs to know our Predecessor
handle_cast({query_predecessor, From, Type}, LoopData) ->
  %%io:format("~p: Received predecessor query from ~p~n", [self(), From]),
  Predecessor = maps:get(predecessor, LoopData),
  chorderl:cast_send_predecessor(From, Predecessor, Type),
  {noreply, LoopData};

% NewNode needs to know our Successor
handle_cast({query_successor, Qref, From}, LoopData) ->
  Successor = maps:get(successor, LoopData),
  From ! {Qref, Successor},
  {noreply, LoopData};

% NewNode wants to know who its Successor is
% NewNode = {NodeID, Pid}
handle_cast({find_successor, Qref, NewNode}, LoopData) ->
  NodeID = maps:get(node_id, LoopData),
  Successor = maps:get(successor, LoopData),
  FingerTableList = maps:get(fin_tab, LoopData),
  chorderl_fintab:find_successor(NewNode,  NodeID, Successor, FingerTableList, Qref),
  {noreply, LoopData};

% Peer needs to know who its Predecessor is
handle_cast({find_predecessor, Qref, {NewNodeID, From}}, LoopData) ->
  NodeID = maps:get(node_id, LoopData),
  Successor = maps:get(successor, LoopData),
  FingerTableList = maps:get(fin_tab, LoopData),
  Pred = chorderl_fintab:find_predecessor(NewNodeID, NodeID, Successor, FingerTableList),
  From ! {Qref, Pred},
  {noreply, LoopData};

% called periodically, verifies our Successor todo: use ticker to trigger
%% todo:  external process timer triggering cast_stabilize(Pid)
handle_cast({stabilize}, LoopData) ->
  {_, Spid} = maps:get(successor, LoopData),
  %%io:format("~p: (stabilize) Current successor is ~p. Querying its predecessor...~n", [self(), Spid]),
  io:format("."),
  chorderl:cast_query_predecessor(Spid, self(), stabilize), % ask our successor about its predecessor
  {noreply, LoopData};

% Our successor tell us about its predecessor Pred
%% init_finger_table || stabilize
handle_cast({status_predecessor, Pred, init_finger_table}, LoopData) ->
  io:format("~p: (init_finger_table) The requested predecessor is ~p~n", [self(), chorderl_utils:display_node(Pred)]),
  io:format("~p: (init_finger_table) New predecessor: ~p~n", [self(), chorderl_utils:display_node(Pred)]),
  {noreply, LoopData#{predecessor := Pred}}; % update our predecessor
handle_cast({status_predecessor, Pred, stabilize}, LoopData) ->
  %%io:format("~p: (stabilize) The requested predecessor is ~p~n", [self(), chorderl_utils:display_node(Pred)]),
  NodeID = maps:get(node_id, LoopData),
  Successor = maps:get(successor, LoopData),
  NewSucc = chorderl_fintab:update_successor(Pred, NodeID, Successor), % find out if Pred can be our new successor (stabilization)
  if
    NewSucc /= Successor ->
      io:format("~p: (stabilize) Successor change: ~p -> ~p~n", [self(), chorderl_utils:display_node(Successor), chorderl_utils:display_node(NewSucc)]);
    true -> ok
  end,
  {noreply, LoopData#{successor := NewSucc}}; % update our successor todo update Succ using fing_tab

% We are informed that our successor is Succ
handle_cast({status_successor, Successor}, LoopData) ->
  {noreply, LoopData#{successor := Successor}}; %% todo update Succ using fing_tab

%% A node will take care of all the keys it is responsible for. If not responsible, forward it to our Successor
handle_cast({add, Key, Value, Qref, Client}, LoopData) ->
  NodeID = maps:get(node_id, LoopData),
  Successor = maps:get(successor, LoopData),
  Predecessor = maps:get(predecessor, LoopData),
  Store = maps:get(store, LoopData),

  Added = chorderl_store:add(Key, Value, Qref, Client,
    NodeID, Predecessor, Successor, Store),
  {noreply, LoopData#{store := Added}}; % todo change to call ???

handle_cast({lookup, Key, Qref, Client}, LoopData) ->
  NodeID = maps:get(node_id, LoopData),
  Successor = maps:get(successor, LoopData),
  Predecessor = maps:get(predecessor, LoopData),
  Store = maps:get(store, LoopData),

  chorderl_store:lookup(Key, Qref, Client,
    NodeID, Predecessor, Successor, Store),
  {noreply, LoopData}; % todo change

handle_cast(_, LoopData) ->
  {noreply, LoopData}.

%% todo handle_info(({nodeup, _Node})
handle_info(_Other, LoopData) ->
  {noreply, LoopData}.

code_change(_OldVsn, LoopData, _Extra) ->
  {ok, LoopData}.


%% Customer Services API

connect(NodeID, nil) ->
  {ok, {NodeID, self()}}; % set our Successesor to ourselves {NodeID, self()}}
connect(NodeID, PeerPid) ->
  Qref = make_ref(),
  %cast_query_id(Peer, Qref, self()),
  chorderl:cast_find_successor(PeerPid, Qref, {NodeID, self()}),
  receive
    {Qref, {Skey, Spid}} ->
      {ok, {Skey, Spid}} % set our Successor to Successor
  after ?Timeout ->
    io:format("Time out: no response~n",[]),
    {error, timeout}
  end.

