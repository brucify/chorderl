%%%-------------------------------------------------------------------
%%% @author bruce
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 30. May 2016 11:18
%%%-------------------------------------------------------------------
-module(chorderl_utils).
-author("bruce").

-include("chorderl.hrl").

%% API
-export([registered/0, node_status/0, sort_registered/0, display_node/1]).

-export([cast_query_id/1, cast_query_fingers/1]).

-export([stabilize_all/0, fix_fingers_all/0, stabilize_all/1, fix_fingers_all/1]).

-export([is_between/3, generate_node_id/1, ip_to_proc_name/1, node_id_to_proc_name/1]).

-export([demo/0, demo/1]).

-export([generate_node_id_3_bits/1]).

%% Check if KeyX is between Key1 and Key2
%is_between(KeyX, Key1, Key2) when (Key1 < KeyX) and (KeyX < Key2) ->
%  %io:format("chorderl_utils: comparing Nkey: ~p, Pkey ~p and NodeID: ~p~n",[KeyX, Key1, Key2]),
%  true;
%is_between(KeyX, Key1, Key2) when (Key2 < KeyX) and (KeyX < Key1) ->
%  %io:format("chorderl_utils: comparing Nkey: ~p, Pkey ~p and NodeID: ~p~n",[KeyX, Key1, Key2]),
%  true;
%is_between(KeyX, Key1, Key2) ->
%  %io:format("chorderl_utils: comparing Nkey: ~p, Pkey ~p and NodeID: ~p~n",[KeyX, Key1, Key2]),
%  false.

is_between(X, A, B) when X >= ?RING_SIZE ->
  is_between(X rem ?M, A, B);
is_between(X, A, B) when B > A ->
  B > X andalso X > A;
is_between(X, A, B) when A > B ->
  not (A >= X andalso X >= B);
is_between(X, A, B) when A == B ->
  true.

%%is_between(X, A, B) when B > A ->
%%  B > X andalso X > A;
%%is_between(X, A, B) when A > B ->
%%  not (B > X andalso X > A);
%%is_between(X, A, B) when A == B ->
%%  true.

%% todo NodeID has to be integer
%% sha1: 160-bit
%%49> chorderl_utils:generate_node_id(<<"127.0.0.1">>) < chorderl_utils:generate_node_id(<<"127.0.0.2">>).
%%true
%%50> chorderl_utils:generate_node_id(<<"127.0.0.2">>) < chorderl_utils:generate_node_id(<<"127.0.0.3">>).
%%true
%%51> chorderl_utils:generate_node_id(<<"127.0.0.3">>) < chorderl_utils:generate_node_id(<<"127.0.0.4">>).
%%false
%%52> chorderl_utils:generate_node_id(<<"127.0.0.4">>) < chorderl_utils:generate_node_id(<<"127.0.0.5">>).
%%false
%%53> chorderl_utils:generate_node_id(<<"127.0.0.5">>) < chorderl_utils:generate_node_id(<<"127.0.0.6">>).
%%true
%%54> chorderl_utils:generate_node_id(<<"127.0.0.6">>) < chorderl_utils:generate_node_id(<<"127.0.0.7">>).
%%false

generate_node_id(random_id) ->
  NodeID = crypto:hash(sha, float_to_list(random:uniform())),
  binary:decode_unsigned(NodeID, big);%%
generate_node_id(Key) ->
  NodeID = crypto:hash(sha, Key),
  binary:decode_unsigned(NodeID, big).

generate_node_id_3_bits(Key) ->
  Key.

%% List processes starting with "chorderl..." in erlang:registered()
registered() ->
  look_for_chorderl(
    lists:map(
      fun(X)->
        atom_to_list(X)
      end,
      erlang:registered()
    ),
    []
  ).

look_for_chorderl([], Res) ->
  Res;
look_for_chorderl([ [99,104,111,114,100,101,114,108 | Rest] | T], Res) -> %matching "chorderl..."
  look_for_chorderl(T, [list_to_atom([99,104,111,114,100,101,114,108 | Rest]) | Res]);
look_for_chorderl([_ | T], Res) ->
  look_for_chorderl(T, Res).

% Ticker = spawn_link(net_kernel, ticker, [self(), Ticktime]),

%ticker(Kernel, Tick) when is_integer(Tick) ->
%  process_flag(priority, max),
%  ?tckr_dbg(ticker_started),
%  ticker_loop(Kernel, Tick).

%ticker_loop(Kernel, Tick) ->
%  receive
%    {new_ticktime, NewTick} ->
%      ?tckr_dbg({ticker_changed_time, Tick, NewTick}),
%     ?MODULE:ticker_loop(Kernel, NewTick)
%  after Tick ->
%    Kernel ! tick,
%    ?MODULE:ticker_loop(Kernel, Tick)
%  end.

% IP: <<"127.0.0.3">>
ip_to_proc_name(IP) ->
  NodeID=
    case ?M of
      160 ->
        chorderl_utils:generate_node_id(IP);
      3 -> %% 0-255
        chorderl_utils:generate_node_id_3_bits(IP)
    end,
  ProcName = node_id_to_proc_name(NodeID),
  ProcName.

node_id_to_proc_name(NodeID) ->
  ProcName = list_to_atom("chorderl_" ++ binary_to_list(term_to_binary(NodeID))),
  ProcName.

node_status() ->
  Nodes = chorderl_utils:registered(),
  Fun =
    fun(ProcName) ->
%      ID = cast_query_id(ProcName),
      Succ = call_query_successor(ProcName),
      Pred = cast_query_predecessor(ProcName),
      Fingers = cast_query_fingers(ProcName),
      NodeID = cast_query_id(ProcName),

%      io:format(" Proc: ~p~n", [ProcName]),
%      io:format(" ======~p=====~n", [whereis(ProcName)]),
%      io:format("|                    |~n"),
%      io:format("|  Succ: ~p <---- ~n", [Succ]),
%      io:format("|  Pred: ~p ----> ~n", [Pred]),
%      io:format("|                    |~n"),
%      io:format(" ======~p=====~n", [whereis(ProcName)]),
%      io:format("~n~n"),

      io:format(" ===================== ~p ======================~n", [{NodeID, whereis(ProcName)}]),
      io:format("|  name: ~p~n", [ProcName]),
      io:format("|  id: ~p~n", [NodeID]),
      io:format("|                                                          |~n"),
      io:format("|  Pred: ~p --->~n", [Pred]),
      io:format("|                      ~p~n", [{NodeID, whereis(ProcName)}]),
      io:format("|                                 ---> Succ: ~p ~n", [Succ]),
      io:format("|  Last 8 Fingers:                                         |~n"),
      print_fingers_x(lists:reverse(Fingers), 8, 0),
      io:format("|  Total: ~p~n", [length(Fingers)]),
      io:format("|                                                          |~n"),
      io:format(" ==========================================================~n"),
      io:format("~n~n")

%      io:format("=====~p, ~p=====~n", [ProcName, whereis(ProcName)]),
%      io:format("=====~p, ~p=====~n", [ProcName, whereis(ProcName)]),
%      io:format("id: ~p~n", [ID]),
%      io:format("successor: ~p~n", [Succ]),
%      io:format("predecessor: ~p~n", [Pred]),
%      io:format("~n~n~n")
    end,
  lists:map(Fun, Nodes).

print_fingers_x([], _, _) ->
  ok;
print_fingers_x(_Fingers, X, X) ->
  ok;
print_fingers_x([Finger | Rest], X, I) ->
  io:format("| ~p~n", [Finger] ),
  print_fingers_x(Rest, X, I+1).

print_fingers([]) ->
  ok;
print_fingers([Finger | Rest]) ->
  io:format("| ~p~n", [Finger] ),
  print_fingers(Rest).

stabilize_all() ->
  lists:map(
    fun(A)-> chorderl:cast_stabilize(A) end,
    chorderl_utils:registered()
  ).

stabilize_all(N) ->
  lists:map(fun(_) -> chorderl:stabilize_all() end, lists:seq(1, N)).

fix_fingers_all() ->
  lists:map(
    fun(A)-> chorderl:cast_fix_fingers(A) end,
    chorderl_utils:registered()
  ).

fix_fingers_all(N) ->
  lists:map(fun(_) -> chorderl_utils:fix_fingers_all() end, lists:seq(1, N)).

cast_query_id(ProcName) ->
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
  chorderl:cast_query_predecessor(ProcName, self(), none),
  receive
    {'$gen_cast', {status_predecessor, Pred, none}} ->
      Pred
  after 1000 ->
    io:format("Time out: no response~n",[]),
    {error, timeout}
  end.

call_query_successor(ProcName) ->
  Result = chorderl:call_query_successor(ProcName),
  Result.
%%  chorderl:cast_query_successor(ProcName, Qref, self()),
%%  receive
%%    {Qref, Succ} ->
%%      case Succ of
%%        {SuccBin, SuccPid} ->
%%          %{binary_to_atom(<<SuccBin/binary>>, latin1), SuccPid};
%%          SuccPid;
%%        nil ->
%%          nil
%%      end
%%  after 1000 ->
%%    io:format("Time out: no response~n",[]),
%%    {error, timeout}
%%  end.

cast_query_fingers(ProcName) ->
  Qref = make_ref(),
  chorderl:cast_query_fingers(ProcName, Qref, self()),
  receive
    {Qref, FingerTableList} ->
      FingerTableList
  after 1000 ->
    io:format("Time out: no response~n",[]),
    {error, timeout}
  end.

display_node(Node) ->
  case Node of
    {_ID, Pid} ->
      Pid;
    Other ->
      Other
  end.

%% [{<0.95.0>,chorderl_è,
%% 409839559372054370104971456613029661185729719528},
%% ... ]
sort_registered() ->
  SortFun =
    fun( {_, _, ValA}, {_, _, ValB} ) ->
      ValA =< ValB
    end,
  lists:sort(
    SortFun,
    lists:map(
      fun(ProcName)->
        { whereis(ProcName), ProcName, chorderl_utils:cast_query_id(ProcName) }
      end,
      chorderl_utils:registered()
    )
  ).

%%demo() ->
%%  demo(6, 1).

demo(N) ->
  case ?M of
    160 ->
      demo(N, 1);
    3 ->
      demo_3_bits(N, 0)
  end.

demo(1, 1) ->
  chorderl:join(<<"127.0.0.1">>);
demo(N, 1) ->
  chorderl:join(<<"127.0.0.1">>),
  demo(N, 2);
demo(N, N) ->
  IP = list_to_binary("127.0.0." ++ integer_to_list(N)),
  chorderl:join(IP, chorderl_utils:ip_to_proc_name(<<"127.0.0.1">>));
demo(N, X) ->
  IP = list_to_binary("127.0.0." ++ integer_to_list(X)),
  chorderl:join(IP, chorderl_utils:ip_to_proc_name(<<"127.0.0.1">>)),
  demo(N, X+1).

demo_3_bits(1, 0) ->
  chorderl:join(0);
demo_3_bits(N, 0) ->
  chorderl:join(0),
  demo_3_bits(N, 1);
demo_3_bits(N, X) when X == N-1 ->
  chorderl:join(X, chorderl_utils:ip_to_proc_name(0));
demo_3_bits(N, X) ->
  chorderl:join(X, chorderl_utils:ip_to_proc_name(0)),
  demo_3_bits(N, X+1).

demo() ->
    chorderl:join(<<"127.0.0.1">>),
    chorderl:join(<<"127.0.0.2">>, chorderl_utils:ip_to_proc_name(<<"127.0.0.1">>)),
    chorderl:join(<<"127.0.0.3">>, chorderl_utils:ip_to_proc_name(<<"127.0.0.1">>)),
    chorderl:join(<<"127.0.0.4">>, chorderl_utils:ip_to_proc_name(<<"127.0.0.1">>)),
    chorderl:join(<<"127.0.0.5">>, chorderl_utils:ip_to_proc_name(<<"127.0.0.1">>)),
    chorderl:join(<<"127.0.0.6">>, chorderl_utils:ip_to_proc_name(<<"127.0.0.1">>)).

