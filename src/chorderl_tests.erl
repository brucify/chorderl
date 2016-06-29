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

%% Join as 127.0.0.1
join_test_() ->
  IP1 = <<"127.0.0.1">>,
  IP2 = <<"127.0.0.2">>,
  NodeID1 = chorderl_utils:generate_node_id(IP1),
  ProcName1 = binary_to_atom(<<"chorderl_", NodeID1/binary>>, latin1),

  {inorder,
    [
      ?_assertEqual([], chorderl:registered() ),
      ?_assertMatch({ok, _Pid}, chorderl:join(IP1)),
      ?_assertEqual(NodeID1, cast_get_key(ProcName1)),
      ?_assertEqual([ProcName1], chorderl:registered()),
      ?_assertMatch({ok, _Pid}, chorderl:join(IP2, ProcName1))
    ]
  }.

cast_get_key(ProcName) ->
  Qref = make_ref(),
  chorderl:cast_get_key(ProcName, Qref, self()),
  receive
    {Qref, NodeID} ->
      NodeID
  after 1000 ->
    io:format("Time out: no response~n",[]),
    {error, timeout}
  end.

join_second_node() ->
  chorderl:join(<<"127.0.0.2">>, whereis('chorderl_K\204±[ÿnåyaRIZ#\016Eã×éGÙ')).
