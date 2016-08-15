%%%-------------------------------------------------------------------
%%% @author bruce
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 28. Jun 2016 11:15
%%%-------------------------------------------------------------------
-module(chorderl_tests).

-include_lib("eunit/include/eunit.hrl").

%% API
-export([]).

-define(IP1, <<"127.0.0.1">>).
-define(IP2, <<"127.0.0.2">>).
-define(IP3, <<"127.0.0.3">>).
-define(NodeID1, (chorderl_utils:generate_node_id(?IP1))).
-define(NodeID2, (chorderl_utils:generate_node_id(?IP2))).
-define(NodeID3, (chorderl_utils:generate_node_id(?IP3))).
-define(ProcName1, (chorderl_utils:ip_to_proc_name(?IP1))).
-define(ProcName2, (chorderl_utils:ip_to_proc_name(?IP2))).
-define(ProcName3, (chorderl_utils:ip_to_proc_name(?IP3))).

main_test_() ->
  NodeID1 = ?NodeID1,
  NodeID2 = ?NodeID2,
  NodeID3 = ?NodeID3,

  {inorder,
    [
      ?_assertEqual([], chorderl:registered() ),
      ?_assertMatch({ok, _Pid}, chorderl:join(?IP1)),
      ?_assertEqual(?NodeID1, cast_query_key(?ProcName1)),
      ?_assertEqual([?ProcName1], chorderl:registered()),

      ?_assertMatch({ok, _Pid}, chorderl:join(?IP2, ?ProcName1)),

      ?_assertMatch(ok, chorderl:cast_stabilize(?ProcName2)),
      ?_assertMatch(ok, chorderl:cast_stabilize(?ProcName1)),

      ?_assertMatch({NodeID2, _}, cast_query_predecessor(?ProcName1)),
      ?_assertMatch({NodeID2, _}, cast_query_successor(?ProcName1)),
      ?_assertMatch(NodeID1, cast_query_predecessor(?ProcName2)),
      ?_assertMatch({NodeID1, _}, cast_query_successor(?ProcName2)),

      ?_assertMatch({ok, _Pid}, chorderl:join(?IP3, ?ProcName2)),

      ?_assertMatch(ok, chorderl:cast_stabilize(?ProcName3)),
      ?_assertMatch(ok, chorderl:cast_stabilize(?ProcName2)),
      ?_assertMatch(ok, chorderl:cast_stabilize(?ProcName1)),

      ?_assertMatch({NodeID3, _}, cast_query_predecessor(?ProcName1)),
      ?_assertMatch({NodeID2, _}, cast_query_successor(?ProcName1)),
      ?_assertMatch({NodeID1, _}, cast_query_predecessor(?ProcName2)),
      ?_assertMatch({NodeID3, _}, cast_query_successor(?ProcName2)),
      ?_assertMatch(nil, cast_query_predecessor(?ProcName3)),
      ?_assertMatch({NodeID1, _}, cast_query_successor(?ProcName3))

    ]
  }.

cast_query_key(ProcName) ->
  Qref = make_ref(),
  chorderl:cast_query_id(ProcName, Qref, self()),
  receive
    {Qref, NodeID} ->
      NodeID
  after 1000 ->
    io:format("Time out: no response~n",[]),
    {error, timeout}
  end.

cast_query_predecessor(ProcName) ->
  chorderl:cast_query_predecessor(ProcName, self()),
  receive
    {'$gen_cast', {status_predecessor, Pred}} ->
      Pred
  after 1000 ->
    io:format("Time out: no response~n",[]),
    {error, timeout}
  end.

cast_query_successor(ProcName) ->
  Qref = make_ref(),
  chorderl:cast_query_successor(ProcName, Qref, self()),
  receive
    {Qref, Succ} ->
      Succ
  after 1000 ->
    io:format("Time out: no response~n",[]),
    {error, timeout}
  end.