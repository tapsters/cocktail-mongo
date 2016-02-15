-module(ctail_mongo).
-author("Vitaly Shutko").
-author("Oleg Zinchenko").
-behaviour(ctail_backend).

-include_lib("cocktail/include/ctail.hrl").

%% Custom functions
-export([exec/2]).
-export([find/3]).

%% Backend callbacks
-export([init/0]).
-export([create_table/1, add_table_index/2, dir/0, destroy/0]).
-export([next_id/2, put/1, delete/2]).
-export([get/2, index/3, all/1, count/1]).

-define(POOL_NAME, mongo_pool).

%% Custom functions

exec(Fun, Args) ->
  poolboy:transaction(?POOL_NAME, fun (W) -> apply(mongo, Fun, [W]++Args) end).

%% Backend callbacks

init() -> 
  connect(),
  ctail:create_schema(?MODULE),
  ok.

connect() ->
  Config = ctail:config(mongo),

  {connection, ConnectionConfig} = proplists:lookup(connection, Config),
  {pool,       PoolConfig}       = proplists:lookup(pool, Config),
  
  PoolBaseOptions = [{name, {local, ?POOL_NAME}}, {worker_module, mc_worker}],
  PoolOptions     = PoolBaseOptions++PoolConfig,
  Spec            = poolboy:child_spec(?POOL_NAME, PoolOptions, ConnectionConfig), 
  {ok, _}         = supervisor:start_child(ctail_sup, Spec).

create_table(Table) -> 
  exec(command, [{<<"create">>, to_binary(Table#table.name)}]).

add_table_index(Table, Key) ->
  exec(ensure_index, [to_binary(Table), {<<"key">>, {to_binary(Key, true), 1}}]).

dir() ->
  Command = {<<"listCollections">>, 1},
  {_, {_, {_, _, _, _, _, Collections}}} = exec(command, [Command]),
  [ binary_to_list(Collection) || {_, Collection, _, _} <- Collections ].

drop_table(Table) ->
  exec(command, [{<<"drop">>, to_binary(Table)}]).

destroy() -> 
  [ drop_table(Table) || Table <- dir() ],
  ok.

next_id(_Table, _Incr) -> 
  mongo_id_server:object_id().

to_binary({<<ObjectId:12/binary>>}) -> {ObjectId};
to_binary({Key, Value})             -> {Key, to_binary(Value)};
to_binary(Value)                    -> to_binary(Value, false).

to_binary(Value, ForceList) ->
  if 
    is_integer(Value) -> 
      Value;
    is_list(Value) -> 
      unicode:characters_to_binary(Value, utf8, utf8);
    is_atom(Value) ->
      atom_to_binary(Value, utf8);
    true -> 
      case ForceList of 
        true -> 
          [List] = io_lib:format("~p", [Value]), 
          list_to_binary(List);
        _ -> 
          Value
      end
  end.

make_id({<<ObjectId:12/binary>>}) -> {ObjectId};
make_id(Term)                     -> to_binary(Term, true).

make_field({Key, Value}) ->
  {Key, make_field(Value)};
make_field(Value) ->
  if 
    is_atom(Value) -> 
      case Value of
        true      -> to_binary(Value);
        false     -> to_binary(Value);
        _         -> {<<"atom">>, atom_to_binary(Value, utf8)}
      end;
    is_pid(Value) -> 
      {<<"pid">>, list_to_binary(pid_to_list(Value))};
    is_list(Value)  ->
      case io_lib:printable_unicode_list(Value) of
        false -> lists:map(fun make_field/1, Value);
        true  -> to_binary(Value)
      end;
    true ->
      to_binary(Value)
  end.

make_document(Table, Key, Values) ->
  TableInfo = ctail:table(Table), 
  Document = list_to_document(tl(TableInfo#table.fields), Values),

  list_to_tuple([<<"_id">>, make_id(Key)|Document]).

list_to_document([],             [])             -> [];
list_to_document([Field|Fields], [Value|Values]) ->
  case Value of
    undefined -> 
      list_to_document(Fields, Values);
    _ -> 
      case Field of
        feed_id -> [Field, make_id(Value)|list_to_document(Fields, Values)];
        _       -> [Field, make_field(Value)|list_to_document(Fields, Values)]
      end
  end.

persist(Record) ->
  Table         = element(1, Record), 
  Key           = element(2, Record), 
  [_, _|Values] = tuple_to_list(Record),
  Document      = make_document(Table, Key, Values),
  Selector      = {<<"_id">>, make_id(Key)},
  exec(update, [to_binary(Table), Selector, Document, true]).

put(Records) when is_list(Records) ->
  try lists:foreach(fun persist/1, Records) 
  catch 
    error:Reason -> {error, Reason} 
  end;
put(Record) -> 
  put([Record]).

delete(Table, Key) ->
  exec(delete_one, [to_binary(Table), {<<"_id">>, make_id(Key)}]),
  ok.

make_record(Table, Document) ->
  TableInfo = ctail:table(Table), 
  PropList  = document_to_proplist(tuple_to_list(Document)), 
  Values    = [proplists:get_value(atom_to_binary(Field, utf8), PropList) || Field <- TableInfo#table.fields],

  list_to_tuple([Table|Values]).

decode_field(<<"true">>)                  -> true;
decode_field(<<"false">>)                 -> false;
decode_field({<<"atom">>, Atom})          -> binary_to_atom(Atom, utf8);
decode_field({<<"pid">>, Pid})            -> list_to_pid(binary_to_list(Pid));
decode_field(B={<<"binary">>, _})         -> B;
decode_field({Key, Value})                -> {Key, decode_field(Value)};
decode_field(Value) when is_binary(Value) -> unicode:characters_to_list(Value, utf8);
decode_field(Value) when is_list(Value)   ->
  case io_lib:printable_unicode_list(Value) of
    false ->
        case is_proplist(Value) of
          false ->
            lists:map(fun decode_field/1, Value);
          true  ->
            [ begin
                Atom = try binary_to_existing_atom(Key, utf8)
                        catch error:badarg -> Key
                        end,
                {Atom, decode_field(Val)}
              end || {Key, Val} <- Value]
        end;
    true  -> Value
  end;
decode_field(Value)                       -> Value.

is_proplist(List) ->
    is_list(List) andalso
        lists:all(fun({AtomKey,_}) ->
                       Atom = try binary_to_existing_atom(AtomKey, utf8)
                       catch error:badarg -> AtomKey
                       end,
                       is_atom(Atom);
                     (_)           -> false
                  end,
                  List).

decode_id({<<ObjectId:12/binary>>}) ->
  {ObjectId};
decode_id(List) ->
  {ok, Tokens, _EndLine} = erl_scan:string(lists:append(binary_to_list(List), ".")),
  {ok, AbsForm}          = erl_parse:parse_exprs(Tokens),
  {value, Value, _Bs}    = erl_eval:exprs(AbsForm, erl_eval:new_bindings()),
  Value.

document_to_proplist(Doc) -> document_to_proplist(Doc, []).
document_to_proplist([],                     Acc) -> Acc;
document_to_proplist([<<"_id">>,     V|Doc], Acc) -> document_to_proplist(Doc, [{<<"id">>, decode_id(V)}|Acc]);
document_to_proplist([<<"feed_id">>, V|Doc], Acc) -> document_to_proplist(Doc, [{<<"feed_id">>, decode_id(V)}|Acc]);
document_to_proplist([F,             V|Doc], Acc) -> document_to_proplist(Doc, [{F, decode_field(V)}|Acc]).

get(Table, Key) ->
  Result = exec(find_one, [to_binary(Table), {<<"_id">>, make_id(Key)}]),
  case Result of 
    {} -> 
      {error, not_found}; 
    {Document} -> 
      {ok, make_record(Table, Document)}
  end.

find(Table, Selector, Limit) ->
  Cursor = exec(find, [to_binary(Table), Selector]),
  Result = mc_cursor:take(Cursor, Limit),
  mc_cursor:close(Cursor),

  case Result of
    [] -> [];
    _ -> [make_record(Table, Document) || Document <- Result]
  end.

index(Table, Key, Value) -> 
  find(Table, {to_binary(Key), to_binary(Value)}, infinity).

all(Table) -> 
  find(Table, {}, infinity).

count(Table) -> 
  {_, {_, Count}} = exec(command, [{<<"count">>, to_binary(Table)}]),
  Count.
