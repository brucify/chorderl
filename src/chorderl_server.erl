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

-compile([{parse_transform, lager_transform}]).

%% API
-export([init/1, terminate/2, handle_cast/2, handle_call/3, handle_info/2, code_change/3]).


%% Callback Functions

%% 1. Get predecessor
%% 2. Get successor
%% 3. Create fingers
%% 4. Create store
init([NodeID, PeerPid]) ->
  lager:start(),
  %% {ok, Successor} = connect(NodeID, PeerPid), % Ask Peer for our Successor
  FingerTableList = chorderl_fintab:init_finger_table(NodeID, PeerPid),
  Successor = chorderl_fintab:fetch_successor(FingerTableList),
  %%Store = chorderl_store:create(list_to_atom(integer_to_list(NodeID)), latin1),
  Store = [],
  LoopData =
    #{?KEY_NODE_ID => NodeID,
      ?KEY_PREDECESSOR => nil,
      ?KEY_SUCEESSOR => Successor,
      ?KEY_FINGER_TABLE => FingerTableList,
      ?KEY_STORE => Store},
  {ok, LoopData}.

terminate(_Reason, _LoopData) ->
  ok.


% NewNode needs to know our Successor
handle_call({query_successor}, _From, LoopData) ->
  FingerTableList = maps:get(?KEY_FINGER_TABLE, LoopData),
  Successor = chorderl_fintab:fetch_successor(FingerTableList),
  {reply, Successor, LoopData};

% NewNode wants to know who its Successor is
% NewNode = {NodeID, Pid}
handle_call({find_successor, NewNode}, _From, LoopData) ->
  NodeID = maps:get(?KEY_NODE_ID, LoopData),
  FingerTableList = maps:get(?KEY_FINGER_TABLE, LoopData),
  Reply = chorderl_fintab:find_successor(NewNode, NodeID, FingerTableList),
  {reply, Reply, LoopData}.

handle_cast(stop, LoopData) ->
  {stop, normal, LoopData};

% New thinks it might be our predecessor
handle_cast({notify, New}, LoopData) ->
  NodeID = maps:get(?KEY_NODE_ID, LoopData),
  Predecessor = maps:get(?KEY_PREDECESSOR, LoopData),
  NewPred = chorderl_fintab:notify(New, NodeID, Predecessor), % find out if New can be our new predecessor
  if
    NewPred /= Predecessor ->
      io:format("~p: (notify) Predecessor change: ~p -> ~p~n", [self(), chorderl_utils:display_node(Predecessor), chorderl_utils:display_node(NewPred)]);
    true -> ok
  end,
  {noreply, LoopData#{?KEY_PREDECESSOR := NewPred}};

% Peer needs to know our key
handle_cast({query_id, Qref, From}, LoopData) ->
  NodeID = maps:get(?KEY_NODE_ID, LoopData),
  From ! {Qref, NodeID},
  {noreply, LoopData};

% Peer needs to know our Predecessor
handle_cast({query_predecessor, From, Type}, LoopData) ->
  %%io:format("~p: Received predecessor query from ~p~n", [self(), From]),
  Predecessor = maps:get(?KEY_PREDECESSOR, LoopData),
  chorderl:cast_send_predecessor(From, Predecessor, Type),
  {noreply, LoopData};

handle_cast({query_fingers, Qref, From}, LoopData) ->
  FingerTableList = maps:get(?KEY_FINGER_TABLE, LoopData),
  From ! {Qref, FingerTableList},
  {noreply, LoopData};

% Peer needs to know who its Predecessor is
%%handle_cast({find_predecessor, Qref, {NewNodeID, From}}, LoopData) ->
%%  NodeID = maps:get(?KEY_NODE_ID, LoopData),
%%  Successor = maps:get(?KEY_SUCEESSOR, LoopData),
%%  FingerTableList = maps:get(?KEY_FINGER_TABLE, LoopData),
%%  Pred = chorderl_fintab:find_predecessor(NewNodeID, NodeID, Successor, FingerTableList),
%%  From ! {Qref, Pred},
%%  {noreply, LoopData};

% called periodically, verifies our Successor todo: use ticker to trigger
%% todo:  external process timer triggering cast_stabilize(Pid)
handle_cast({stabilize}, LoopData) ->
  FingerTableList = maps:get(?KEY_FINGER_TABLE, LoopData),
  Successor = chorderl_fintab:fetch_successor(FingerTableList),
  {_, Spid} = Successor,
  %%io:format("~p: (stabilize) Current successor is ~p. Querying its predecessor...~n", [self(), Spid]),
  chorderl:cast_query_predecessor(Spid, self(), stabilize), % ask our successor about its predecessor
  io:format("."),
  {noreply, LoopData};

handle_cast({fix_fingers}, LoopData) ->
  FingerTableList = maps:get(?KEY_FINGER_TABLE, LoopData),
  NodeID = maps:get(?KEY_NODE_ID, LoopData),
  %%io:format("~p: (stabilize) Current successor is ~p. Querying its predecessor...~n", [self(), Spid]),
  NewFingerTableList = chorderl_fintab:fix_fingers(NodeID, FingerTableList),
  {noreply, LoopData#{?KEY_FINGER_TABLE := NewFingerTableList}};

% Our successor tell us about its predecessor Pred
%% init_finger_table || stabilize
handle_cast({status_predecessor, Pred, init_finger_table}, LoopData) ->
  io:format("~p: (init_finger_table) The requested predecessor is ~p~n", [self(), chorderl_utils:display_node(Pred)]),
  io:format("~p: (init_finger_table) New predecessor: ~p~n", [self(), chorderl_utils:display_node(Pred)]),
  {noreply, LoopData#{?KEY_PREDECESSOR := Pred}}; % update our predecessor
handle_cast({status_predecessor, Pred, stabilize}, LoopData) ->
  %%io:format("~p: (stabilize) The requested predecessor is ~p~n", [self(), chorderl_utils:display_node(Pred)]),
  NodeID = maps:get(?KEY_NODE_ID, LoopData),
  FingerTableList = maps:get(?KEY_FINGER_TABLE, LoopData),
  Successor = chorderl_fintab:fetch_successor(FingerTableList),
  NewSucc = chorderl_fintab:update_successor(Pred, NodeID, Successor), % find out if Pred can be our new successor (stabilization)
  if
    NewSucc /= Successor ->
      io:format("~p: (stabilize) Successor change: ~p -> ~p~n", [self(), chorderl_utils:display_node(Successor), chorderl_utils:display_node(NewSucc)]);
    true -> ok
  end,
  NewFingerTableList = [ {chorderl_fintab:get_finger_start(NodeID, 1, ?M), NewSucc} | lists:nthtail( 1, FingerTableList) ], % update our successor,replace the 1st element
  {noreply, LoopData#{?KEY_FINGER_TABLE := NewFingerTableList}};

% We are informed that our successor is Succ
handle_cast({status_successor, Successor}, LoopData) ->
  FingerTableList = maps:get(?KEY_FINGER_TABLE, LoopData),
  NewFingerTableList = [ Successor | lists:nthtail( 1, FingerTableList) ], %% replace the 1st element
  {noreply, LoopData#{?KEY_FINGER_TABLE := NewFingerTableList}};

%% A node will take care of all the keys it is responsible for. If not responsible, forward it to our Successor
handle_cast({add, Key, Value, Qref, Client}, LoopData) ->
  NodeID = maps:get(?KEY_NODE_ID, LoopData),
  Successor = maps:get(?KEY_SUCEESSOR, LoopData),
  Predecessor = maps:get(?KEY_PREDECESSOR, LoopData),
  Store = maps:get(?KEY_STORE, LoopData),

  Added = chorderl_store:add(Key, Value, Qref, Client,
    NodeID, Predecessor, Successor, Store),
  {noreply, LoopData#{?KEY_STORE := Added}}; % todo change to call ???

handle_cast({lookup, Key, Qref, Client}, LoopData) ->
  NodeID = maps:get(?KEY_NODE_ID, LoopData),
  Successor = maps:get(?KEY_SUCEESSOR, LoopData),
  Predecessor = maps:get(?KEY_PREDECESSOR, LoopData),
  Store = maps:get(?KEY_STORE, LoopData),

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
  %cast_query_id(Peer, Qref, self()),
  Result = chorderl:call_find_successor(PeerPid, {NodeID, self()}),
  {ok, Result}.
%%  chorderl:cast_find_successor(PeerPid, Qref, {NodeID, self()}),
%%  receive
%%    {Qref, {Skey, Spid}} ->
%%      {ok, {Skey, Spid}} % set our Successor to Successor
%%  after ?Timeout ->
%%    io:format("Time out: no response~n",[]),
%%    {error, timeout}
%%  end.

