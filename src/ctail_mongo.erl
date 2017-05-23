-module(ctail_mongo).
-author("Vitaly Shutko").
-author("Oleg Zinchenko").
-behaviour(ctail_backend).

-include_lib("cocktail/include/ctail.hrl").

%% Custom functions
-export([exec/2]).
-export([find/3, find/4]).

%% Backend callbacks
-export([init/0]).
-export([create_table/1, add_table_index/2, dir/0, destroy/0]).
-export([next_id/2, put/1, delete/2]).
-export([get/2, index/3, all/1, count/1]).

-define(POOL_NAME, mongo_pool).

%% Custom functions

exec(Fun, Args) ->
  poolboy:transaction(?POOL_NAME, fun (W) -> apply(mc_worker_api, Fun, [W]++Args) end).

%% Backend callbacks

init() ->
  connect(),
  ctail:create_schema(?MODULE),
  ok.

connect() ->
  case ctail:config(mongo) of
    undefined -> skip;
    Config    ->
      {connection, ConnectionConfig} = proplists:lookup(connection, Config),
      {pool,       PoolConfig}       = proplists:lookup(pool, Config),

      PoolBaseOptions = [{name, {local, ?POOL_NAME}}, {worker_module, mc_worker}],
      PoolOptions     = PoolBaseOptions++PoolConfig,
      Spec            = poolboy:child_spec(?POOL_NAME, PoolOptions, ConnectionConfig),
      {ok, _Pid}      = supervisor:start_child(ctail_sup, Spec)
  end,

  case ctail:config(mongo_rs) of
    undefined -> skip;
    RsConfig  ->
      {pool, RsPoolConfig} = proplists:lookup(pool, RsConfig),
      {rs, Rs} = proplists:lookup(rs, RsConfig),

      {name, RsName} = proplists:lookup(name, Rs),
      {seed, RsSeed} = proplists:lookup(seed, Rs),
      {connection, RsConnection} = proplists:lookup(connection, Rs),

      TopologyOptions = RsPoolConfig ++ [ {name, ?POOL_NAME}, {register, ?POOL_NAME}],
      {ok, _RsPid} = mongoc:connect({ rs, RsName, RsSeed }, TopologyOptions, RsConnection)
  end.

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
        undefined -> undefined;
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

  {<<"$set">>, list_to_tuple([<<"_id">>, make_id(Key)|Document])}.

list_to_document([],             [])             -> [];
list_to_document([Field|Fields], [Value|Values]) ->
  case Field of
    feed_id -> [Field, make_id(Value)|list_to_document(Fields, Values)];
    _       -> [Field, make_field(Value)|list_to_document(Fields, Values)]
  end.

persist(Record) ->
  Table         = element(1, Record),
  Key           = element(2, Record),
  [_, _|Values] = tuple_to_list(Record),
  Document      = make_document(Table, Key, Values),
  Selector      = {<<"_id">>, make_id(Key)},
  exec(update, [to_binary(Table), Selector, Document, true, false]).

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

  PropList  = document_to_proplist(maps:to_list(Document)),
  Values    = [proplists:get_value(atom_to_binary(Field, utf8), PropList) || Field <- TableInfo#table.fields],

  list_to_tuple([Table|Values]).

decode_field(<<"true">>)                  -> true;
decode_field(<<"false">>)                 -> false;
decode_field(#{<<"atom">> := Atom})       -> binary_to_atom(Atom, utf8);
decode_field(<<>>)                        -> [];
decode_field(#{<<"pid">> := Pid})         -> list_to_pid(binary_to_list(Pid));
decode_field(Coordinates = #{<<"coordinates">> := _V}) ->
  list_to_tuple(lists:foldl(fun (Key, Acc) ->
                                [Key, maps:get(Key, Coordinates)|Acc]
                            end, [], maps:keys(Coordinates)));
decode_field(#{<<"tuple_list">> := List}) ->
  {<<"tuple_list">>, [decode_field(V) || V <- List]};
decode_field(#{<<"binary">> := V})        -> {<<"binary">>, V};
decode_field({Key, Value})                ->
  {decode_key(Key), decode_field(Value)};
decode_field(Value) when is_binary(Value) -> Value;
decode_field(Value) when is_list(Value)   ->
  case io_lib:printable_unicode_list(Value) of
    false ->
      lists:map(fun decode_field/1, Value);
    true  -> list_to_binary(Value)
  end;
decode_field(Value) when is_map(Value)    ->
  Res = [ {decode_key(K), decode_field(maps:get(K, Value))} || K <- maps:keys(Value)],
  case length(Res) > 1 of
    false -> hd(Res);
    true  -> Res
  end;

decode_field(Value)                       ->  Value.

decode_key(V) when is_atom(V) -> atom_to_binary(V, utf8);
decode_key(V) when is_list(V) ->
  case io_lib:printable_unicode_list(V) of
    true -> list_to_binary(V);
    false -> <<"unknown">>
  end;
decode_key(V) when is_binary(V) -> V;
decode_key(_V) -> <<"unknown">>.

decode_id({<<ObjectId:12/binary>>}) ->
  {ObjectId};
decode_id(List) ->
  {ok, Tokens, _EndLine} = erl_scan:string(lists:append(binary_to_list(List), ".")),
  {ok, AbsForm}          = erl_parse:parse_exprs(Tokens),
  {value, Value, _Bs}    = erl_eval:exprs(AbsForm, erl_eval:new_bindings()),
  Value.

document_to_proplist(Doc) -> document_to_proplist(Doc, []).
document_to_proplist([],                     Acc) -> Acc;
document_to_proplist([{<<"_id">>,     V}|Doc], Acc) -> document_to_proplist(Doc, [{<<"id">>, decode_id(V)}|Acc]);
document_to_proplist([{<<"feed_id">>, V}|Doc], Acc) -> document_to_proplist(Doc, [{<<"feed_id">>, decode_id(V)}|Acc]);
document_to_proplist([{F,             V}|Doc], Acc) -> document_to_proplist(Doc, [{F, decode_field(V)}|Acc]).

get(Table, Key) ->
  Result = exec(find_one, [to_binary(Table), {<<"_id">>, make_id(Key)}]),

  case Result of
    undefined -> {error, not_found};
    _         -> {ok, make_record(Table, Result)}
  end.

find(Table, Selector, infinity) ->
  find(Table, Selector, 0, 0);
find(Table, Selector, Limit) ->
  find(Table, Selector, 0, Limit).

find(Table, Selector, Skip, Limit) ->
  case exec(find, [to_binary(Table), Selector, #{skip => Skip, batchsize => Limit}]) of
    {ok, Cursor} ->
      Result = case Limit of
                 0 -> mc_cursor:rest(Cursor);
                 _ -> mc_cursor:next_batch(Cursor)
               end,
      mc_cursor:close(Cursor),
      case Result of
        [] -> [];
        _ -> [make_record(Table, Document) || Document <- Result]
      end;
    _            -> []
  end.

index(Table, Key, Value) ->
  find(Table, {to_binary(Key), to_binary(Value)}, infinity).

all(Table) ->
  find(Table, {}, infinity).

count(Table) ->
  {_, {_, Count}} = exec(command, [{<<"count">>, to_binary(Table)}]),
  Count.
