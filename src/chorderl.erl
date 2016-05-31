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
-export([start_link/1, start_link/2, stop/1]).
-export([init/1, terminate/2, handle_cast/2, handle_info/2, code_change/3]).

-define(Timeout, 1000).

%% Exported Client Functions
%% Operation & Maintenance API

start_link(Name) ->
  start_link(Name, nil).

start_link(random_id, Peer) ->
  Name = generate_node_id(),
  M = atom_to_binary(?MODULE, latin1),
  NodeID = binary_to_atom(<<M/binary, "_", Name/binary>>, latin1),
  gen_server:start_link(
    {local, NodeID},
    ?MODULE,
    [NodeID, Peer],
    []
  );
start_link(Name, Peer) ->
  M = atom_to_binary(?MODULE, latin1),
  NodeID = binary_to_atom(<<M/binary, "_", Name/binary>>, latin1),
  gen_server:start_link(
    {local, NodeID},
    ?MODULE,
    [NodeID, Peer],
    []
  ).

stop(NodeID) ->
  gen_server:cast(NodeID, stop).

%% Callback Functions

init([NodeID, Peer]) ->
  {ok, Successor} = connect(NodeID, Peer), % Ask Peer for our Successor
  Store = chorderl_store:create(NodeID),
  LoopData = #{node_id => NodeID, predecessor => nil, successor => Successor, store => Store},
  {ok, LoopData}.

terminate(_Reason, _LoopData) ->
  ok.

handle_cast(stop, LoopData) ->
  {stop, normal, LoopData};

% Peer needs to know our key
handle_cast({key, Qref, Peer}, LoopData) ->
  NodeID = maps:get(LoopData, node_id),
  Peer ! {Qref, NodeID},
  {noreply, LoopData};

% New thinks it might be our predecessor
handle_cast({notify, New}, LoopData) ->
  NodeID = maps:get(LoopData, node_id),
  Predecessor = maps:get(LoopData, predecessor),
  Pred = notify(New, NodeID, Predecessor), % find out if New can be our new predecessor
  {noreply, LoopData#{predecessor := Pred}};

% Peer needs to know our Predecessor
handle_cast({request, Peer}, LoopData) ->
  Predecessor = maps:get(LoopData, predecessor),
  request(Peer, Predecessor),
  {noreply, LoopData};

% Our successor tell us about its predecessor Pred
handle_cast({status, Pred}, LoopData) ->
  NodeID = maps:get(LoopData, node_id),
  Successor = maps:get(LoopData, successor),
  Succ = stabilize(Pred, NodeID, Successor), % find out if Pred can be our new successor
  {noreply, LoopData#{successor := Succ}}; % update our successor

% called periodically, verifies our Successor
handle_cast({stabilize}, LoopData) ->
  Successor = maps:get(LoopData, successor),
  stabilize(Successor), % ask our successor about its predecessor
  {noreply, LoopData};

%% todo
handle_cast({add, Key, Value, Qref, Client}, LoopData) ->
  NodeID = maps:get(LoopData, node_id),
  Successor = maps:get(LoopData, successor),
  Predecessor = maps:get(LoopData, predecessor),
  Store = maps:get(LoopData, store),

  Added = add(Key, Value, Qref, Client,
    NodeID, Predecessor, Successor, Store),
  {noreply, LoopData#{store := Added}};

handle_cast({lookup, Key, Qref, Client}, LoopData) ->
  NodeID = maps:get(LoopData, node_id),
  Successor = maps:get(LoopData, successor),
  Predecessor = maps:get(LoopData, predecessor),
  Store = maps:get(LoopData, store),

  lookup(Key, Qref, Client,
    NodeID, Predecessor, Successor, Store),
  {noreply, LoopData}.

handle_info(timeout, LoopData) ->
  {noreply, LoopData};
handle_info(_Other, LoopData) ->
  {noreply, LoopData}.

code_change(_OldVsn, LoopData, _Extra) ->
  {ok, LoopData}.

%% Customer Services API

generate_node_id() ->
  crypto:hash(sha, float_to_list(random:uniform())).

stabilize({_, Spid}) ->
  Spid ! {request, self()}.

% A basic stabilization protocol is used to keep nodes' successors up to date
% Asks Successor for Pred's successor, and decide whether Pred should be NodeID's successor instead
stabilize(Pred, NodeID, Successor) ->
  {Skey, Spid} = Successor,
  case Pred of
    nil -> % Our Successor's predecessor is nil
      Spid ! {notify, {NodeID, self()}},
      Successor;
    {NodeID, _} -> % Our Successor's predecessor is us (NodeID)
      Successor;
    {Skey, _} -> % Our Successor's predecessor is itself (Skey)
      Spid ! {notify, {NodeID, self()}},
      Successor;
    {Xkey, Xpid} -> % Our Successor's predecessor is some other node X (Xkey)
      case chorderl_utils:is_between(Xkey, NodeID, Skey) of
        true -> % Our Successor's predecessor (Xkey) is between us and Successor (NodeId and Skey)
          Xpid ! {notify, {NodeID, self()}},
          Pred;
        false -> % Our Successor's predecessor (Xkey) is not between us and Successor (NodeId and Skey)
          Spid ! {notify, {NodeID, self()}},
          Successor
      end
  end.

request(Peer, Predecessor) ->
  case Predecessor of
    nil ->
      Peer ! {status, nil};
    {Pkey, Ppid} ->
      Peer ! {status, {Pkey, Ppid}}
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

connect(NodeID, nil) ->
  {ok, {NodeID, self()}}; % set our Successesor to ourselves {NodeID, self()}}
connect(_NodeID, Peer) ->
  Qref = make_ref(),
  Peer ! {key, Qref, self()},
  receive
    {Qref, Skey} ->
      {ok, {Skey, Peer}} % set our Successesor to {Skey, Peer} todo: always Peer returned ???
  after ?Timeout ->
    io:format("Time out: no response~n",[]),
    {error, timeout}
  end.

add(Key, Value, Qref, Client,
    NodeID, {Pkey, _}, {_, Spid}, Store) ->
  case (Pkey < Key) and (Key =< NodeID) of % Key is within (Pkey, NodeID], assuming predecessor always smaller and successor always bigger
    true -> % We (NodeID) are responsible for Key
      Client ! {Qref, ok},
      Added = chorderl_store:add(Key, Value, Store),
      Added;
    false -> % NodeID is not responsible for Key
      Spid ! {add, Key, Value, Qref, Client}, % forward it to Spid instead
      Store
  end.

lookup(Key, Qref, Client,
    NodeID, {Pkey, _}, {_, Spid}, Store) ->
  case (Pkey < Key) and (Key =< NodeID) of
    true -> % We (NodeID) are responsible for Key
      Result = chorderl_store:lookup(Key, Store),
      Client ! {Qref, Result};
    false -> % NodeID is not responsible for Key
      Spid ! {lookup, Key, Qref, Client}
  end.
