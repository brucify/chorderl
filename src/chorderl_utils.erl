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

%% API
-export([is_between/3, generate_node_id/1, registered/0]).

is_between(KeyX, Key1, Key2) when (Key1 < KeyX) and (KeyX < Key2) ->
  true;
is_between(KeyX, Key1, Key2) when (Key2 < KeyX) and (KeyX < Key1) ->
  true;
is_between(_KeyX, _Key1, _Key2) ->
  false.

generate_node_id(random_id) ->
  crypto:hash(sha, float_to_list(random:uniform()));
generate_node_id(Key) ->
  crypto:hash(sha, Key).

%% List processes starting with "chorderl..." in erlang:registered()
registered() ->
  look_for_chorderl(lists:map(fun(X)-> atom_to_list(X) end, erlang:registered()), []).

look_for_chorderl([], Res) ->
  Res;
look_for_chorderl([ [99,104,111,114,100,101,114,108 | Rest] | T], Res) ->
  look_for_chorderl(T, [list_to_atom([99,104,111,114,100,101,114,108 | Rest]) | Res]); %matching "chorderl..."
look_for_chorderl([_ | T], Res) ->
  look_for_chorderl(T, Res).