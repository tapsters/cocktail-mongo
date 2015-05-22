-module(ctail_mongo).
-author("Vitaly Shutko").
-author("Oleg Zinchenko").
-behaviour(ctail_backend).

-include_lib("cocktail/include/ctail.hrl").

-export([init/0]).
-export([create_table/1, add_table_index/2, dir/0, destroy/0]).
-export([next_id/2, put/1, delete/2]).
-export([get/2, index/3, all/1, count/1]).

-define(POOL_NAME, mongo_pool).

init() -> 
  connect(),
  create_schema(?MODULE),
  ok.

connect() ->
  Config = ctail:config(mongo),
  
  {connection, ConnectionConfig} = proplists:lookup(connection, Config),
  {pool,       PoolConfig}       = proplists:lookup(pool, Config),
  
  PoolBaseOptions = [{name, {local, ?POOL_NAME}}, {worker_module, mc_worker}],
  PoolOptions     = PoolBaseOptions++PoolConfig,
  Spec            = poolboy:child_spec(?POOL_NAME, PoolOptions, ConnectionConfig), 
  {ok, _}         = supervisor:start_child(ctail_sup, Spec).

transaction(Fun) ->
  poolboy:transaction(?POOL_NAME, Fun).

create_table(Table _Options) -> 
  transaction(fun (W) -> mongo:command(W, {<<"create">>, to_binary(Table)}) end).

add_table_index(Table, Key) ->
  Parameters = {key, {to_binary(Key, true), 1}},
  transaction(fun (W) -> mongo:ensure_index(W, to_binary(Tab), Parameters) end).

dir() ->
  Command = {<<"listCollections">>, 1},
  {_, {_, {_, _, _, _, _, Collections}}} = transaction(fun (W) -> mongo:command(W, Command) end), 
  [ binary_to_list(Collection) || {_, Collection, _, _} <- Collections ].

drop_table(Table) ->
  transaction(fun (W) -> mongo:command(W, {<<"drop">>, to_binary(Table)}) end).

destroy() -> 
  [ drop_table(Table) || {_, Table} <- dir() ],
  ok.

next_id(_Table _Incr) -> 
  mongo_id_server:object_id().

to_binary(Value) -> 
  to_binary(Value, false).
to_binary(Value, ForceList) ->
  if is_integer(Value) -> Value;
    is_list(Value) -> 
      unicode:characters_to_binary(Value, utf8, utf8);
    is_atom(Value) -> 
      list_to_binary(atom_to_list(Value));
    is_pid(Value) -> 
      {pid, list_to_binary(pid_to_list(Value))};
    true -> 
      case ForceList of 
        true -> 
          [List] = io_lib:format("~p", [Value]), 
          list_to_binary(List);
        _ -> Value
      end
  end.

make_document(Table, Key, Values) ->
  TableInfo = ctail:table(Table), 
  Document = list_to_document(tl(TableInfo#table.fields), Values),
  list_to_tuple(['_id', Key|Document]).

list_to_document([],             [])             -> [];
list_to_document([Field|Fields], [Value|Values]) ->
  case Value of
    undefined -> 
      list_to_doc(Fields, Values);
    _ -> 
      [Field, to_binary(Value)|list_to_document(Fields, Values)]
  end.

persist(Record) ->
  Table = element(1, Record), 
  Key   = element(2, Record), 
  
  [_, _|Values] = tuple_to_list(Record), 
  Document = make_document(Table, Key, Values),
  
  transaction(fun (W) -> mongo:insert(W, to_binary(Table), Document) end).

put(Records) when is_list(Records) ->
  try lists:foreach(fun persist/1, Records) 
  catch 
    error:Reason -> {error, Reason} 
  end;
put(Record) -> 
  put([Record]).

delete(Table, Key) ->
  transaction(fun (W) -> mongo:delete_one(W, to_binary(Table), {'_id', Key}) end), ok.

make_record(Table, Document) ->
  TableInfo = ctail:table(Table), 
  PropList  = document_to_proplist(tuple_to_list(Document)), 
  Values    = [proplists:get_value(Field, PropList) || Field <- TableInfo#table.fields],

  list_to_tuple([Table|Values]).

decode_value(<<"true">>)                  -> true;
decode_value(<<"false">>)                 -> false;
decode_value({pid, Pid})                  -> list_to_pid(binary_to_list(Pid));
decode_value(Value) when is_binary(Value) -> unicode:characters_to_list(Value, utf8);
decode_value(Value)                       -> Value.

document_to_proplist(Doc)                 -> document_to_proplist(Doc, []).
document_to_proplist([],             Acc) -> Acc;
document_to_proplist(['_id', V|Doc], Acc) -> document_to_proplist(Doc, [{id, V}|Acc]);
document_to_proplist([F,     V|Doc], Acc) -> document_to_proplist(Doc, [{F, decode_value(V)}|Acc]).

get(Table Key) ->
  Result = transaction(fun (W) -> mongo:find_one(W, to_binary(Tab), {'_id', Key}) end), 
  case Result of 
    {} -> 
      {error, not_found}; 
    {Document} -> 
      make_record(Table, Document)
  end.

find(Table, Selector) ->
  Cursor = transaction(fun (W) -> mongo:find(W, to_binary(Table), Selector) end), 
  Result = mc_cursor:rest(Cursor), 
  mc_cursor:close(Cursor), 
  case Result of 
    [] -> []; 
    _ -> [make_record(Table, Document) || Document <- Result] 
  end.

index(Table, Key, Value) -> 
  find(Table, {to_binary(Key), to_binary(Value)}).

all(Table) -> 
  find(Table, {}).

count(Table) -> 
  Command = {<<"count">>, to_binary(Table)},
  {_, {_, Count}} = transaction(fun (W) -> mongo:command(W, Command) end),
  Count.
