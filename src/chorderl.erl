%%%-------------------------------------------------------------------
%%% @author bruce
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 17. May 2016 13:26
%%%-------------------------------------------------------------------
-module(chorderl).

-behaviour(gen_server).

%% API
-export([join/1, join/2, stop/1]).
-export([fix_fingers/0]).
-export([cast_add/5, cast_notify/2, cast_stabilize/1, cast_lookup/4]).
-export([cast_query_key/3, cast_query_predecessor/2, cast_query_successor/3]).
-export([cast_send_predecessor/2, cast_find_successor/3, cast_find_predecessor/3]).
-export([registered/0]).
-export([init/1, terminate/2, handle_cast/2, handle_info/2, code_change/3]).

-define(Timeout, 1000).

%% Exported Client Functions
%% Operation & Maintenance API


join(Key) ->
  join(Key, nil).

%% 1. Ask Peer: who's my Successor?
%% 2. Wait for Peer's answer: It's X
%% 2.1 Peer checks: NewID's Predecessor is Y
%% 2.2 Peer asks Y: who's your Successor?
%% 2.3 Y says: it's X
%% 3. NewID notifies X that its new Predecessor is us
%% 4. X verifies and decides if it sets Predecessor to us
% Key: <<"the key">>
% Peer: <0.999.0>
join(Key, Peer) ->
  NodeID = chorderl_utils:generate_node_id(Key),
  Module = atom_to_binary(?MODULE, latin1),
  ProcName = binary_to_atom(<<Module/binary, "_", NodeID/binary>>, latin1), % e.g. 'chorderl_ì%KÅ\205\021Î¿#}qÆ\034\016ìâ´quX'
  gen_server:start_link(
    {local, ProcName},
    ?MODULE,
    [NodeID, Peer],
    []
  ).

stop(NodeID) ->
  gen_server:cast(NodeID, stop).

fix_fingers() ->
  ok.

%% Set new Predecessor
cast_notify(Pid, New) ->
  gen_server:cast(Pid, {notify, New}).

%% Request NodeID
cast_query_key(Pid, Qref, From) ->
  gen_server:cast(Pid, {query_key, Qref, From}).

%% Check who is Predecessor of Pid
cast_query_predecessor(Pid, From) ->
  %lager:info("gen_server casting ~p", [lager:pr({Pid, From}, ?MODULE)]),
  gen_server:cast(Pid, {query_predecessor, From}).

%% Check who is Successor of Pid. NewNode needs to know
cast_query_successor(Pid, Qref, From) ->
  gen_server:cast(Pid, {query_successor, Qref, From}).

% Request Successor of From
cast_find_successor(Pid, Qref, {NodeID, From}) ->
  gen_server:cast(Pid, {find_successor, Qref, {NodeID, From}}).

% Request Predecessor of From
cast_find_predecessor(Pid, Qref, {NodeID, From}) ->
  gen_server:cast(Pid, {find_predecessor, Qref, {NodeID, From}}).

%% Send current Predecessor
cast_send_predecessor(Pid, Pred) ->
  gen_server:cast(Pid, {status_predecessor, Pred}).

%% Start stabilization
cast_stabilize(Pid) ->
  gen_server:cast(Pid, {stabilize}).

%% Save a Key/Value pair
cast_add(Pid, Key, Value, Qref, Client) ->
  gen_server:cast(Pid, {add, Key, Value, Qref, Client}).

%% Look up a Key
cast_lookup(Pid, Key, Qref, Client) ->
  gen_server:cast(Pid, {lookup, Key, Qref, Client}).

registered() ->
  chorderl_utils:registered().

%% Callback Functions

%% 1. Get successor
%% 2. Get predecessor
%% 3. Create store
init([NodeID, Peer]) ->
  {ok, Successor} = connect(NodeID, Peer), % Ask Peer for our Successor
  Store = chorderl_store:create(binary_to_atom(NodeID, latin1)),
  LoopData =
    #{node_id => NodeID,
      predecessor => nil,
      successor => Successor,
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
  Pred = notify(New, NodeID, Predecessor), % find out if New can be our new predecessor
  {noreply, LoopData#{predecessor := Pred}};

% Peer needs to know our key
handle_cast({query_key, Qref, From}, LoopData) ->
  NodeID = maps:get(node_id, LoopData),
  From ! {Qref, NodeID},
  {noreply, LoopData};

% Peer needs to know our Predecessor
handle_cast({query_predecessor, From}, LoopData) ->
  Predecessor = maps:get(predecessor, LoopData),
  cast_send_predecessor(From, Predecessor),
  {noreply, LoopData};

% NewNode needs to know our Successor
handle_cast({query_successor, Qref, From}, LoopData) ->
  Successor = maps:get(successor, LoopData),
  From ! {Qref, Successor},
  {noreply, LoopData};

% Peer needs to know who its Successor is
% NewNode = {NodeID, Pid}
handle_cast({find_successor, Qref, NewNode}, LoopData) ->
  find_successor(NewNode, LoopData, Qref), %% todo: change LoopData to fing_tab
  {noreply, LoopData};

% Peer needs to know who its Predecessor is
handle_cast({find_predecessor, Qref, {NewNodeID, From}}, LoopData) ->
  Pred = find_predecessor(NewNodeID, LoopData), %% todo: change LoopData to fing_tab
  From ! {Qref, Pred},
  {noreply, LoopData};

% called periodically, verifies our Successor todo: use ticker to trigger
%% todo:  external process timer triggering cast_stabilize(Pid)
handle_cast({stabilize}, LoopData) ->
  {_, Spid} = maps:get(successor, LoopData),
  cast_query_predecessor(Spid, self()), % ask our successor about its predecessor
  {noreply, LoopData};

% Our successor tell us about its predecessor Pred
handle_cast({status_predecessor, Pred}, LoopData) ->
  NodeID = maps:get(node_id, LoopData),
  Successor = maps:get(successor, LoopData),
  Succ = update_successor(Pred, NodeID, Successor), % find out if Pred can be our new successor
  {noreply, LoopData#{successor := Succ}}; % update our successor

% We are informed that our successor is Succ
handle_cast({status_successor, Succ}, LoopData) ->
  {noreply, LoopData#{successor := Succ}};

%% A node will take care of all the keys it is responsible for. If not responsible, forward it to our Successor
handle_cast({add, Key, Value, Qref, Client}, LoopData) ->
  NodeID = maps:get(node_id, LoopData),
  Successor = maps:get(successor, LoopData),
  Predecessor = maps:get(predecessor, LoopData),
  Store = maps:get(store, LoopData),

  Added = add(Key, Value, Qref, Client,
    NodeID, Predecessor, Successor, Store),
  {noreply, LoopData#{store := Added}}; % todo change to call ???

handle_cast({lookup, Key, Qref, Client}, LoopData) ->
  NodeID = maps:get(node_id, LoopData),
  Successor = maps:get(successor, LoopData),
  Predecessor = maps:get(predecessor, LoopData),
  Store = maps:get(store, LoopData),

  lookup(Key, Qref, Client,
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
connect(NodeID, Peer) ->
  Qref = make_ref(),
  %cast_query_key(Peer, Qref, self()),
  cast_find_successor(Peer, Qref, {NodeID, self()}),
  receive
    {Qref, {Skey, Spid}} ->
      {ok, {Skey, Spid}} % set our Successor to Successor
  after ?Timeout ->
    io:format("Time out: no response~n",[]),
    {error, timeout}
  end.

% A basic stabilization protocol is used to keep nodes' successors up to date
% Asks Successor for Pred's successor, and decide whether Pred should be NodeID's (our) successor instead
update_successor(Pred, NodeID, Successor) ->
  {Skey, Spid} = Successor,
  case Pred of
    nil -> % Our Successor's predecessor is nil
      cast_notify(Spid, {NodeID, self()}),
      Successor;
    {NodeID, _} -> % Our Successor's predecessor is us (NodeID)
      Successor;
    {Skey, _} -> % Our Successor's predecessor is itself (Skey)
      cast_notify(Spid, {NodeID, self()}),
      Successor;
    {Xkey, Xpid} -> % Our Successor's predecessor is some other node X (Xkey)
      case chorderl_utils:is_between(Xkey, NodeID, Skey) of
        true -> % Our Successor's predecessor (Xkey) is between us and Successor (NodeId and Skey)
          cast_notify(Xpid, {NodeID, self()}),
          Pred;
        false -> % Our Successor's predecessor (Xkey) is not between us and Successor (NodeId and Skey)
          cast_notify(Spid, {NodeID, self()}),
          Successor
      end
  end.

notify({Nkey, Npid}, NodeID, Predecessor) ->
  case Predecessor of
    nil -> % Our Predecessor is nil
      {Nkey, Npid};
    {Pkey, _} ->
      case chorderl_utils:is_between(Nkey, Pkey, NodeID) of
        true -> % New node (Xkey) is between us and our Predecessor (NodeId and Pkey)
          {Nkey, Npid};
        false -> % New node (Xkey) is not between us and our Predecessor (NodeId and Pkey)
          Predecessor
      end
  end.

add(Key, Value, Qref, Client,
    NodeID, {Pkey, _}, {_, Spid}, Store) ->
  case (Pkey < Key) and (Key =< NodeID) of % Key is within (Pkey, NodeID], assuming predecessor always smaller and successor always bigger
    true -> % We (NodeID) are responsible for Key
      Client ! {Qref, ok},
      Added = chorderl_store:add(Key, Value, Store),
      Added;
    false -> % NodeID is not responsible for Key
      cast_add(Spid, Key, Value, Qref, Client),  % forward it to Spid instead
      Store
  end.

lookup(Key, Qref, Client,
    NodeID, Predecessor, {_, Spid}, Store) ->
  case Predecessor of
    nil ->
      Result = chorderl_store:lookup(Key, Store),
      Client ! {Qref, Result};
    {Pkey, _} ->
      case (Pkey < Key) and (Key =< NodeID) of
        true -> % We (NodeID) are responsible for Key
          Result = chorderl_store:lookup(Key, Store),
          Client ! {Qref, Result};
        false -> % NodeID is not responsible for Key
          cast_lookup(Spid, Key, Qref, Client) % forward the lookup message to Spid instead
      end
  end.

find_successor({NewNodeID, From}, LoopData, Qref) ->
  {_PKey, Ppid} = find_predecessor(NewNodeID, LoopData), %% find Predecessor for NewNodeID
  cast_query_successor(Ppid, Qref, From).  %% Ask Ppid, reply to Pid

%% ask node NodeID to find NewNodeID's predecessor
find_predecessor(NewNodeID, LoopData) ->
  NodeID = maps:get(node_id, LoopData),
  Successor = maps:get(successor, LoopData),
  case Successor of
    nil ->
      nil;
    {Skey, Spid} ->
      case chorderl_utils:is_between(NewNodeID, NodeID, Skey) or (NewNodeID == Skey) of %% (NodeID, Skey]
        true ->
          {Skey, Spid};
        false ->
          closest_preceding_finger(NewNodeID, LoopData)
      end
  end.

%% return closest finger preceding NewNodeID
closest_preceding_finger(_NewNodeID, LoopData) ->
  NodeID = maps:get(node_id, LoopData),
  {NodeID, self()}. %% todo