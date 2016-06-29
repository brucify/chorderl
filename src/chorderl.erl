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
-export([fix_fingers/0, cast_add/5, cast_get_key/3, cast_notify/2, cast_request_predecessor/2, cast_send_predecessor/2, cast_stabilize/1, cast_lookup/4]).
-export([registered/0]).
-export([init/1, terminate/2, handle_cast/2, handle_info/2, code_change/3]).

-define(Timeout, 1000).

%% Exported Client Functions
%% Operation & Maintenance API


join(Key) ->
  join(Key, nil).

%% 1. Ask Peer: who's my Successor?
%% 2. Wait for Peer's answer: It's X
%% 3. Notify X that its new Predecessor is us
%% 4. X verifies and decides if it sets Predecessor to us
% Key: <<"the key">>
% Peer: <0.999.0>
join(Key, Peer) ->
  NodeID = chorderl_utils:generate_node_id(Key),
  M = atom_to_binary(?MODULE, latin1),
  ProcName = binary_to_atom(<<M/binary, "_", NodeID/binary>>, latin1), % e.g. 'chorderl_ì%KÅ\205\021Î¿#}qÆ\034\016ìâ´quX'
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

%% Request NodeID
cast_get_key(Pid, Qref, Requester) ->
  gen_server:cast(Pid, {key, Qref, Requester}).

%% Set new Predecessor
cast_notify(Pid, New) ->
  gen_server:cast(Pid, {notify, New}).

%% Request Predecessor
cast_request_predecessor(Pid, Requester) ->
  gen_server:cast(Pid, {request, Requester}).

%% Send current Predecessor
cast_send_predecessor(Pid, Pred) ->
  gen_server:cast(Pid, {status, Pred}).

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

init([NodeID, Peer]) ->
  {ok, Successor} = connect(NodeID, Peer), % Ask Peer for our Successor
  Store = chorderl_store:create(binary_to_atom(NodeID, latin1)),
  LoopData = #{node_id => NodeID, predecessor => nil, successor => Successor, store => Store},
  {ok, LoopData}.

terminate(_Reason, _LoopData) ->
  ok.

handle_cast(stop, LoopData) ->
  {stop, normal, LoopData};

% Peer needs to know our key
handle_cast({key, Qref, From}, LoopData) ->
  NodeID = maps:get(node_id, LoopData),
  From ! {Qref, NodeID},
  {noreply, LoopData};

% New thinks it might be our predecessor
handle_cast({notify, New}, LoopData) ->
  NodeID = maps:get(node_id, LoopData),
  Predecessor = maps:get(predecessor, LoopData),
  Pred = notify(New, NodeID, Predecessor), % find out if New can be our new predecessor
  {noreply, LoopData#{predecessor := Pred}};

% Peer needs to know our Predecessor
handle_cast({request, Requester}, LoopData) ->
  Predecessor = maps:get(predecessor, LoopData),
  request(Requester, Predecessor),
  {noreply, LoopData};

% Our successor tell us about its predecessor Pred
handle_cast({status, Pred}, LoopData) ->
  NodeID = maps:get(node_id, LoopData),
  Successor = maps:get(successor, LoopData),
  Succ = stabilize(Pred, NodeID, Successor), % find out if Pred can be our new successor
  {noreply, LoopData#{successor := Succ}}; % update our successor

% called periodically, verifies our Successor todo: remove
handle_cast({stabilize}, LoopData) ->
  Successor = maps:get(successor, LoopData),
  stabilize(Successor), % ask our successor about its predecessor
  {noreply, LoopData};

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

% called periodically, verifies our Successor
handle_info(timeout, LoopData) ->
  Successor = maps:get(successor, LoopData),
  stabilize(Successor), %% todo: replace with external process timer triggering cast_stabilize(Pid)
  {noreply, LoopData};
%% todo handle_info(({nodeup, _Node})
handle_info(_Other, LoopData) ->
  {noreply, LoopData}.

code_change(_OldVsn, LoopData, _Extra) ->
  {ok, LoopData}.

%% Customer Services API

stabilize({_, Spid}) ->
  cast_request_predecessor(Spid, self()).

% A basic stabilization protocol is used to keep nodes' successors up to date
% Asks Successor for Pred's successor, and decide whether Pred should be NodeID's successor instead
stabilize(Pred, NodeID, Successor) ->
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

request(Requester, Predecessor) ->
  case Predecessor of
    nil ->
      cast_send_predecessor(Requester, nil);
    {Pkey, Ppid} ->
      cast_send_predecessor(Requester, {Pkey, Ppid})
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
  cast_get_key(Peer, Qref, self()),
  receive
    {Qref, Skey} -> % what do you receive??? todo
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
