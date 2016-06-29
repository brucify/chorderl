%%%-------------------------------------------------------------------
%%% @author bruce
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 30. May 2016 11:22
%%%-------------------------------------------------------------------
-module(chorderl_store).

%% API
-export([create/1, add/3, lookup/2, split/3, merge/2]).

create(NodeID) ->
  StoreID = ets:new(NodeID, [ordered_set]),
  StoreID.

add(Key, Value, StoreID) ->
  ets:insert(StoreID, {Key, Value}).

lookup(Key, StoreID) ->
  case ets:lookup(StoreID, Key) of
    [{Key, Value}] -> Value;
    _ -> []
  end.

split(From, To, StoreID) ->
  ok.

merge(Entries, StoreID) ->
  ok.