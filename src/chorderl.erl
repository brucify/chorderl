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
-export([start_link/1, stop/1]).
-export([init/1, terminate/2, handle_cast/2, handle_info/2, code_change/3]).

-define(Timeout, 1000).

%% Exported Client Functions
%% Operation & Maintenance API

start_link(random_id) ->
  Name = generate_node_id(),
  M = atom_to_binary(?MODULE, latin1),
  NodeID = binary_to_atom(<<M/binary, "_", Name/binary>>, latin1),
  gen_server:start_link(
    {local, NodeID},
    ?MODULE,
    [NodeID, nil],
    []
  );
start_link(Name) ->
  M = atom_to_binary(?MODULE, latin1),
  NodeID = binary_to_atom(<<M/binary, "_", Name/binary>>, latin1),
  gen_server:start_link(
    {local, NodeID},
    ?MODULE,
    [NodeID, nil],
    []
  ).

stop(NodeID) ->
  gen_server:cast(NodeID, stop).

%% Callback Functions

init([NodeID, Peer]) ->
  LoopData = #{node_id => NodeID, predecessor => nil, successor => nil},
  {ok, Successor} = connect(NodeID, Peer),
  {ok, LoopData#{successor := Successor}}.

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
      case is_between(Xkey, NodeID, Skey) of
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
      case is_between(Nkey, Pkey, NodeID) of
        true -> % New node (Xkey) is between us and our Predecessor (NodeId and Pkey)
          {Nkey, Npid};
        false -> % New node (Xkey) is not between us and our Predecessor (NodeId and Pkey)
          Predecessor
      end
  end.

connect(NodeID, nil) ->
  {ok, {NodeID, self()}}; % set our Successesor to ourselves
connect(_NodeID, Peer) ->
  Qref = make_ref(),
  Peer ! {key, Qref, self()},
  receive
    {Qref, Skey} ->
      {ok, {Skey, Peer}} % set our Successesor to Peer
  after ?Timeout ->
    io:format("Time out: no response~n",[]),
    {error, timeout}
  end.

is_between(KeyX, Key1, Key2) when (Key1 < KeyX) and (KeyX < Key2) ->
  true;
is_between(KeyX, Key1, Key2) when (Key2 < KeyX) and (KeyX < Key1) ->
  true;
is_between(_KeyX, _Key1, _Key2) ->
  false.


