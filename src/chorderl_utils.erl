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
-export([is_between/3]).

is_between(KeyX, Key1, Key2) when (Key1 < KeyX) and (KeyX < Key2) ->
  true;
is_between(KeyX, Key1, Key2) when (Key2 < KeyX) and (KeyX < Key1) ->
  true;
is_between(_KeyX, _Key1, _Key2) ->
  false.
