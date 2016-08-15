%%%-------------------------------------------------------------------
%%% @author bruce
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 17. May 2016 13:26
%%%-------------------------------------------------------------------
-module(chorderl).

-include("chorderl.hrl").

%% API
-export([join/1, join/2, stop/1]).

-export([cast_add/5, cast_notify/2, cast_stabilize/1, cast_lookup/4]).
-export([cast_query_id/3, cast_query_predecessor/3, cast_query_successor/3]).
-export([cast_send_successor/2, cast_send_predecessor/3, cast_find_successor/3, cast_find_predecessor/3]).

-export([registered/0, stabilize_all/0, node_status/0, fix_fingers/0]).

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
  %%Module = atom_to_binary(?MODULE, latin1),
  %%ProcName = binary_to_atom(<<Module/binary, "_", NodeID/integer>>, latin1), % e.g. 'chorderl_ì%KÅ\205\021Î¿#}qÆ\034\016ìâ´quX'
  ProcName = chorderl_utils:node_id_to_proc_name(NodeID),
  gen_server:start_link(
    {local, ProcName},
    chorderl_server,
    [NodeID, Peer],
    []
  ).

stop(NodeID) ->
  gen_server:cast(NodeID, stop).

fix_fingers() ->
  ok.

registered() ->
  chorderl_utils:registered().

stabilize_all() ->
  chorderl_utils:stabilize_all().

node_status() ->
  chorderl_utils:node_status().

%%
%% gen_server APIs
%%

%% Set new Predecessor
cast_notify(Pid, New) ->
  gen_server:cast(Pid, {notify, New}).

%% Start stabilization
%% todo draw SSD of stablization, join, init_fing_table
cast_stabilize(Pid) ->
  gen_server:cast(Pid, {stabilize}).

%% Save a Key/Value pair
cast_add(Pid, Key, Value, Qref, Client) ->
  gen_server:cast(Pid, {add, Key, Value, Qref, Client}).

%% Look up a Key
cast_lookup(Pid, Key, Qref, Client) ->
  gen_server:cast(Pid, {lookup, Key, Qref, Client}).

%%
%% queries (async with async reply)
%%

%% Request NodeID
cast_query_id(Pid, Qref, From) ->
  gen_server:cast(Pid, {query_id, Qref, From}).

%% Check who is Predecessor of Pid
%% cast_query_predecessor can be called both during stablization AND init_finger_table
%% Type: init_finger_table || stabilize
cast_query_predecessor(Pid, From, Type) ->
  %lager:info("gen_server casting ~p", [lager:pr({Pid, From}, ?MODULE)]),
  gen_server:cast(Pid, {query_predecessor, From, Type}).

%% Check who is Successor of Pid. NewNode needs to know
cast_query_successor(Pid, Qref, From) ->
  gen_server:cast(Pid, {query_successor, Qref, From}).

%%
%% find (async with async reply)
%%

% Request Successor of From
cast_find_successor(Pid, Qref, {NodeID, From}) ->
  gen_server:cast(Pid, {find_successor, Qref, {NodeID, From}}).

% Request Predecessor of From
cast_find_predecessor(Pid, Qref, {NodeID, From}) ->
  gen_server:cast(Pid, {find_predecessor, Qref, {NodeID, From}}).

%%
%% replies (async)
%%

%% Send to Pid current Predecessor
cast_send_predecessor(Pid, Pred, Type) ->
  gen_server:cast(Pid, {status_predecessor, Pred, Type}).

% Send to Pid current Predecessor
cast_send_successor(Pid, Succ) ->
  gen_server:cast(Pid, {status_successor, Succ}).