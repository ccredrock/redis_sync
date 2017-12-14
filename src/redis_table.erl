%%%-------------------------------------------------------------------
%%% @author ccredrock@gmail.com
%%% @copyright 2017 redrock
%%% @doc redis table
%%%     sync data from redis
%%%     notify update to monitor process
%%% @end
%%%-------------------------------------------------------------------
-module(redis_table).

%% config
-export([loop_time/0,      %% config: table process loop time
         alarm_len/0,      %% config: version list alarm len
         clean_len/0]).    %% config: clean clean len

%% monitor
-export([monitor/1,        %% minitor table for self
         demonitor/1,      %% deminitor table for self
         list_monitor/1]). %% list table all minitor process

%% callbacks
-export([start_link/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%%------------------------------------------------------------------------------
-include("redis_sync.hrl").

-define(LOOP_TIME,  50).
-define(ALARM_LEN,  100000).
-define(CLEAN_LEN,  70000). % error when over 35000/50ms

-record(state, {table = null, version = <<"-1">>, monitor = #{}}).

%%------------------------------------------------------------------------------
%% config
%%------------------------------------------------------------------------------
-spec loop_time() -> pos_integer().
loop_time() ->
    application:get_env(reids_sync, table_loop_time, ?LOOP_TIME).

-spec alarm_len() -> pos_integer().
alarm_len() ->
    application:get_env(reids_sync, table_alarm_len, ?ALARM_LEN).

-spec clean_len() -> pos_integer().
clean_len() ->
    application:get_env(reids_sync, table_clean_len, ?CLEAN_LEN).

%%------------------------------------------------------------------------------
%% @throws timeout
-spec monitor(PID::pid()) -> ok | {error, any()}.
monitor(PID) ->
    gen_server:call(PID, {monitor, self()}, infinity).

%% @throws timeout
-spec demonitor(PID::pid()) -> ok.
demonitor(PID) ->
    gen_server:call(PID, {demonitor, self()}, infinity).

%% @throws badarg
-spec list_monitor(PID::pid()) -> [{From::atom(), PID::pid()}].
list_monitor(PID) ->
    #state{monitor = Map} = sys:get_state(PID),
    maps:to_list(Map).

%%------------------------------------------------------------------------------
%% gen_server
%%------------------------------------------------------------------------------
-spec start_link(Table  ::binary()) ->
    {ok, pid()} | {error, {already_started, pid()}} | {error, any()}.
start_link(Table) ->
    gen_server:start_link(?MODULE, [Table], []).

%% @hidden
init([Table]) ->
    process_flag(trap_exit, true),
    {ok, init_table(#state{table = Table}), 0}.

%% @hidden
handle_call({monitor, From}, _From, State) ->
    {Result, State1} = handle_monitor(From, State),
    {reply, Result, State1};
handle_call({demonitor, From}, _From, State) ->
    handle_demonitor(From, State),
    {reply, ok, State};
handle_call(_Call, _From, State) ->
    {reply, ok, State}.

%% @hidden
handle_cast(_Request, State) ->
    {noreply, State}.

%% @hidden
handle_info(timeout, State) ->
    State1 = handle_timeout(State),
    erlang:send_after(loop_time(), self(), timeout),
    {noreply, State1};
handle_info({'DOWN', _, process, PID, _}, State) ->
    handle_demonitor(PID, State),
    {noreply, State};
handle_info({'EXIT', PID, Reason}, State) ->
    case is_pid(whereis(redis_sync)) of
        true ->
            ?DEBUG("find process exit ~p", [{PID, Reason}]),
            {noreply, State};
        false ->
            ?ERROR("master dead ~p", [{PID, Reason}]),
            {stop, master_stop, State}
    end;
handle_info(_Info, State) ->
    {noreply, State}.

%% @hidden
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% @hidden
terminate(Reason, State) ->
    ?WARN("process exit ~p", [{Reason, State}]).

%%------------------------------------------------------------------------------
%% @private
init_table(#state{table = Table} = State) ->
    ets:new(?ETS_DATA(Table), [named_table, public, {read_concurrency, true}]),
    State1 = reload_table(State),
    ?INFO("init success ~p", [{State}]),
    State1.

%% @private
reload_table(#state{table = Table} = State) ->
    {ok, Version} = eredis_cluster:q([<<"LLEN">>, ?REDIS_VERSION(Table)]),
    case catch reload_table(State, Version) of
        {'EXIT', Reason} ->
            lager:warning("reload_table fail ~p", [{Reason}]),
            timer:sleep(loop_time()),
            reload_table(State);
        Result ->
            Result
    end.

%% @private
reload_table(#state{table = Table} = State, Version) ->
    {ok, KeyList} = eredis_cluster:q([<<"KEYS">>, ?REDIS_DATA(Table, <<"*">>)]),
    {ok, PropList} = redis_transaction([[<<"HGETALL">>, X] || X <- KeyList]),
    {ok, Version} = eredis_cluster:q([<<"LLEN">>, ?REDIS_VERSION(Table)]),
    List = [{parse_key(RedisKey), parse_prop(Vals, #{})} || {RedisKey, Vals} <- lists:zip(KeyList, PropList)],
    ets:insert(?ETS_DATA(Table), List),
    ets:insert(?ETS_VERSION, {Table, Version}),
    ?INFO("reload table success ~p", [{State, Version, List}]),
    State#state{version = Version}.

%% @private
redis_transaction([]) -> {ok, []};
redis_transaction(List) -> eredis_cluster:transaction(List).

%% @private
parse_key(TableKey) ->
    [_, Left] = re:split(TableKey, <<"@">>), Left.

%% @private
parse_prop([Key, Val | T], Map) ->
    parse_prop(T, Map#{binary_to_term(Key) => binary_to_term(Val)});
parse_prop([], Map) -> Map.

%%------------------------------------------------------------------------------
%% @private
handle_timeout(#state{table = Table, version = OldBinVersion} = State) ->
    try
        {ok, BinVersion} = eredis_cluster:q([<<"LLEN">>, ?REDIS_VERSION(Table)]),
        AlarmLen = alarm_len(),
        case {binary_to_integer(BinVersion),
              binary_to_integer(OldBinVersion)} of
            {Version, Version} -> State;
            {Version, _} when Version >= AlarmLen ->
                clean_table(Table, BinVersion, AlarmLen), State;
            {Version, LastVersion} when Version < LastVersion ->
                update_table(State,
                             integer_to_binary(binary_to_integer(OldBinVersion) - clean_len()),
                             BinVersion);
            _ ->
                update_table(State, OldBinVersion, BinVersion)
        end
    catch
        _:R ->
            ?WARN("loop fail ~p", [{R, erlang:get_stacktrace(), State}]),
            State
    end.

%% @private
clean_table(Table, BinLen, AlarmLen) ->
    CleanLen = clean_len(),
    EvalStr = ["local Len = redis.pcall('LLEN', KEYS[1])
                if Len >= tonumber(ARGV[1]) then
                    local LeftLen = Len - tonumber(ARGV[2])
                    redis.pcall('LTRIM', KEYS[1], 0 - LeftLen,  -1)
                    return {Len, LeftLen}
                else
                    return Len
                end"],
    {ok, LeftLen} = eredis_cluster:q([<<"eval">>, iolist_to_binary(EvalStr), 1, ?REDIS_VERSION(Table), AlarmLen, CleanLen]),
    ?INFO("clean table success ~p", [{Table, BinLen, AlarmLen, CleanLen, LeftLen}]).

%% @private
update_table(#state{table = Table, monitor = Map} = State, LastVersion, Version) ->
    {ok, BinList} = eredis_cluster:q([<<"LRANGE">>, ?REDIS_VERSION(Table), LastVersion, binary_to_integer(Version) - 1]),
    TermList = [binary_to_term(X) || X <- BinList],
    update_table_ets(?ETS_DATA(Table), TermList = [binary_to_term(X) || X <- BinList]),
    ets:insert(?ETS_VERSION, {Table, Version}),
    [PID ! {redis_sync, Table, Version, TermList} || {PID, _} <- maps:to_list(Map)],
    ?INFO("update table success ~p", [{Table, LastVersion, Version, TermList, Map}]),
    State#state{version = Version}.

%% @private
update_table_ets(ETS, List) ->
    Fun = fun(#{op := del, key := X}) ->
                  ets:delete(ETS, X);
             (#{op := put, key := X, prop := Prop}) ->
                  case ets:lookup(ETS, X) of
                      [] -> ets:insert(ETS, {X, Prop});
                      [{_, OldProp}] -> ets:insert(ETS, {X, maps:merge(OldProp, Prop)})
                  end
          end,
    lists:foreach(Fun, List).

%% @private
handle_monitor(From, #state{version = Version, monitor = Map} = State) ->
    case maps:find(From, Map) of
        error ->
            erlang:monitor(process, From),
            ?INFO("monitor success ~p", [{State, From, Version}]),
            {ok, State#state{monitor = Map#{From => Version}}};
        {ok, _Version} ->
            {ok, State}
    end.

%% @private
handle_demonitor(From, #state{monitor = Map} = State) ->
    case maps:find(From, Map) of
        error -> ok;
        {ok, _} -> ?INFO("demonitor success ~p", [{From, State}])
    end.

