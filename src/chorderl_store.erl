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
-export([add/8, lookup/7]).
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

add(Key, Value, Qref, Client,
    NodeID, {Pkey, _}, {_, Spid}, Store) ->
  case (Pkey < Key) and (Key =< NodeID) of % Key is within (Pkey, NodeID], assuming predecessor always smaller and successor always bigger
    true -> % We (NodeID) are responsible for Key
      Client ! {Qref, ok},
      Added = chorderl_store:add(Key, Value, Store),
      Added;
    false -> % NodeID is not responsible for Key
      chorderl:cast_add(Spid, Key, Value, Qref, Client),  % forward it to Spid instead
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
          chorderl:cast_lookup(Spid, Key, Qref, Client) % forward the lookup message to Spid instead
      end
  end.