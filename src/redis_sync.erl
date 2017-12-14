%%%-------------------------------------------------------------------
%%% @author ccredrock@gmail.com
%%% @copyright 2017 redrock
%%% @doc redis sync
%%%     sync redis table to ets
%%%     put val and watch lock to put val
%%% @end
%%%-------------------------------------------------------------------
-module(redis_sync).

%% start
-export([start/0,           %% start application
         purge/0]).         %% !!! dangerous purge all table data

%% config
-export([sync_tables/0]).   %% config: sync tables

%% tables
-export([add_table/1,       %% add table to sync
         list_table/0]).    %% list all sync table

%% monitor
-export([monitor/1,        %% minitor table for self
         demonitor/1,      %% deminitor table for self
         list_monitor/0]). %% list all table minitor process

%% set val
-export([put_val/3,        %% put table key val(map)
         del_val/2,        %% del table key
         set_val/1,        %% set(put or del) key val
         get_val/2,        %% get table key
         list_val/1]).     %% list table key val

%% lock val
-export([get_lock/1,       %% get watch lock
         lock_put/4,       %% watch lock put
         lock_del/3,       %% watch lock del
         lock_set/2]).     %% watch lock set

%% callbacks
-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%%------------------------------------------------------------------------------
-include("redis_sync.hrl").

%%------------------------------------------------------------------------------
-spec start() -> {'ok', [atom()]} | {'error', any()}.
start() ->
    application:ensure_all_started(?MODULE).

-spec sync_tables() -> pos_integer().
sync_tables() ->
    application:get_env(?MODULE, sync_tables, []).

-spec purge() -> ok.
purge() ->
    {ok, List1} = eredis_cluster:q([<<"keys">>, ?REDIS_VERSION('*')]),
    {ok, List2} = eredis_cluster:q([<<"keys">>, ?REDIS_DATA('*', <<"*">>)]),
    eredis_cluster:q([<<"DEL">>] ++ List1 ++ List2).

%%------------------------------------------------------------------------------
%% @throws timeout
-spec add_table(Table::atom()) -> {ok, pid()} | {error, {already_started, pid()}} | {error, any()} | {'EXIT', any()}.
add_table(Table) ->
    gen_server:call(?MODULE, {add_table, Table}).

-spec list_table() -> [{Table::atom(), PID::pid()}].
list_table() ->
    supervisor:which_children(redis_table_sup).

%%------------------------------------------------------------------------------
%% @throws timeout
-spec monitor(Table::atom()) -> {ok, pid()} | {error, any()}.
monitor(Table) ->
    redis_table:monitor(which_child(Table)).

%% @private [{Id,Child,Type,Modules}]
which_child(Table) ->
    List = supervisor:which_children(redis_table_sup),
    {Table, PID, _, _} = lists:keyfind(Table, 1, List),
    PID.

%% @throws timeout
-spec demonitor(Table::atom()) -> ok.
demonitor(Table) ->
    redis_table:demonitor(which_child(Table)).

%% @throws badarg
-spec list_monitor() -> [{Table::atom(), [{From::atom(), PID::pid()}]}].
list_monitor() ->
    [{Table, redis_table:list_monitor(PID)} || {Table, PID, _, _} <- supervisor:which_children(redis_table_sup)].

%%------------------------------------------------------------------------------
-spec get_val(Table::atom(), Key::binary()) -> null | map().
get_val(Table, Key) ->
    case ets:lookup(?ETS_DATA(Table), Key) of
        [] -> null;
        [{_, #{} = Map}] -> Map
    end.

-spec list_val(Table::atom()) -> [{Table::atom(), Map::map()}].
list_val(Table) ->
    ets:tab2list(?ETS_DATA(Table)).

-spec put_val(Table::atom(), Key::binary(), Prop::map()) -> ok | any().
put_val(Table, Key, Map) ->
    set_val([{put, Table, Key, Map}]).

-spec del_val(Table::atom(), Key::binary()) -> ok | any().
del_val(Table, Key) ->
    set_val([{del, Table, Key}]).

-spec set_val(Evals::[{del, Table::atom(), Key::binary()}
                      | {put, Table::atom(), Key::binary(), Map::map()}]) -> ok | any().
set_val([]) -> ok;
set_val(Evals) ->
    Map = #{str => [], keys => [], args => [], key_nth => 0, arg_nth => 0},
    #{str := EvalStr, keys := Keys, args := Args} = form_evals(Evals, Map),
    case catch eredis_cluster:q([<<"eval">>, iolist_to_binary(EvalStr), length(Keys)] ++ Keys ++ Args) of
        {ok, _} -> ?DEBUG("set val success ~p", [{Evals}]), ok;
        Result -> ?DEBUG("set val fail ~p", [{Evals}]), Result
    end.

%% @private
form_evals([{put, Table, Key, MKV} | T],
           #{str := Str, keys := Keys, key_nth := KeyNth} = Acc) ->
    Str1 = Str ++ [[" redis.pcall('HMSET', KEYS[", integer_to_binary(KeyNth1 = KeyNth + 1), "]"]],
    Keys1 = Keys ++ [?REDIS_DATA(Table, Key)],
    #{str := Str2} = Acc1 = form_hashs(maps:to_list(MKV), Acc#{str => Str1, keys => Keys1, key_nth => KeyNth1}),
    form_evals(T, form_op(Table, [put, Key, MKV], Acc1#{str => Str2 ++ [")"]}));
form_evals([{del, Table, Key} | T],
           #{str := Str, keys := Keys, key_nth := KeyNth} = Acc) ->
    Str1 = Str ++ [[" redis.pcall('DEL', KEYS[", integer_to_binary(KeyNth1 = KeyNth + 1), "])"]],
    Keys1 = Keys ++ [?REDIS_DATA(Table, Key)],
    form_evals(T, form_op(Table, [del, Key], Acc#{str => Str1, keys => Keys1, key_nth => KeyNth1}));
form_evals([], Acc) -> Acc.

%% @private
form_hashs([{K, V} | T], #{str := Str, args := Args, arg_nth := Nth} = Acc) ->
    Str1 = Str ++ [[", ARGV[", integer_to_binary(Nth + 1), "], ARGV[", integer_to_binary(Nth2 = Nth + 2), "]"]],
    form_hashs(T, Acc#{str => Str1, args => Args ++ [term_to_binary(K), term_to_binary(V)], arg_nth => Nth2});
form_hashs([], Acc) -> Acc.

%% @private
form_op(Table, OPList,
        #{str := Str, keys := Keys, key_nth := KeyNth, args := Args, arg_nth := ArgNth} = Acc) ->
    BinKeyNth = integer_to_binary(KeyNth1 = KeyNth + 1),
    BinArgNth = integer_to_binary(ArgNth1 = ArgNth + 1),
    Str1 = Str ++ [" redis.pcall('RPUSH', KEYS[", BinKeyNth, "], ARGV[", BinArgNth, "])"],
    Keys1 = Keys ++ [?REDIS_VERSION(Table)],
    Args1 = Args ++ [form_op_args(OPList)],
    Acc#{str => Str1, keys => Keys1, key_nth => KeyNth1, args => Args1, arg_nth => ArgNth1}.

%% @private
form_op_args([OP, Key]) -> term_to_binary(#{op => OP, key => Key});
form_op_args([OP, Key, Map]) -> term_to_binary(#{op => OP, key => Key, prop => Map}).

%%------------------------------------------------------------------------------
-spec get_lock(Tables::[atom()]) -> map().
get_lock(Tables) ->
    maps:from_list(lists:flatten([ets:lookup(?ETS_VERSION, X) || X <- Tables])).

-spec lock_put(Locks::[binary()], Table::atom(), Key::binary(), Prop::map()) -> ok | any().
lock_put(Locks, Table, Key, Map) ->
    lock_set(Locks, [{put, Table, Key, Map}]).

-spec lock_del(Locks::[binary()], Table::atom(), Key::binary()) -> ok | any().
lock_del(Locks, Table, Key) ->
    lock_set(Locks, [{del, Table, Key}]).

-spec lock_set(Locks::[binary()],
               Evals::[{del, Table::atom(), Key::binary()}
                       | {put, Table::atom(), Key::binary(), Map::map()}]) -> ok | any().
lock_set(_Locks, []) -> ok;
lock_set([], _Evals) -> ok;
lock_set(Locks, Evals) ->
    Map = #{str => [], keys => [], args => []},
    Map1 = #{str := LockStr, nth := Nth} = form_locks(maps:to_list(Locks), true, Map#{nth => 0}),
    Map2 = Map1#{key_nth => Nth, arg_nth => Nth, str => []},
    #{str := EvalStr, keys := Keys, args := Args} = form_evals(Evals, Map2),
    Str = ["if ", LockStr, " then ", EvalStr, " return {}; else return {['err'] = 'lock_fail'} end"],
    case catch eredis_cluster:q([<<"eval">>, iolist_to_binary(Str), length(Keys)] ++ Keys ++ Args) of
        {ok, _} -> ?DEBUG("lock set success ~p", [{Locks, Evals}]), ok;
        Result -> ?DEBUG("lock set fail ~p", [{Locks, Evals, Result}]), Result
    end.

%% @private
form_locks([{Table, Lock} | T], IsFirst,
           #{str := Str, keys := Keys, args := Args, nth := Nth} = Acc) ->
    BinNth = integer_to_binary(Nth1 = Nth + 1),
    Str1 = Str ++ form_locks_str(IsFirst, BinNth),
    form_locks(T, false, Acc#{str  => Str1,
                              keys => Keys ++ [?REDIS_VERSION(Table)],
                              args => Args ++ [Lock],
                              nth  => Nth1});
form_locks([], _, Acc) -> Acc.

%% @private
form_locks_str(true, BinNth) -> [[" tonumber(ARGV[", BinNth, "]) == redis.pcall('LLEN', KEYS[", BinNth, "])"]];
form_locks_str(false, BinNth) -> [[" and tonumber(ARGV[", BinNth, "]) == redis.pcall('LLEN', KEYS[", BinNth, "])"]].

%%------------------------------------------------------------------------------
-spec start_link() -> {ok, pid()} | {error, {already_started, pid()}} | {error, any()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @hidden
init([]) ->
    process_flag(trap_exit, true),
    ets:new(?ETS_VERSION, [named_table, public, {read_concurrency, true}]),
    List = [redis_table_sup:start_child(Table) || Table <- sync_tables()],
    ?INFO("init success ~p", [{List}]),
    {ok, {state}, 0}.

%% @hidden
handle_call({add_table, Table}, _From, State) ->
    {reply, catch redis_table_sup:start_child(Table), State};
handle_call(_Call, _From, State) ->
    {reply, ok, State}.

%% @hidden
handle_cast(_Request, State) ->
    {noreply, State}.

%% @hidden
handle_info({'EXIT', PID, Reason}, State) ->
    ?DEBUG("find process exit ~p", [{PID, Reason}]),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

%% @hidden
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% @hidden
terminate(Reason, State) ->
    ?ERROR("process exit ~p", [{Reason, State}]).

