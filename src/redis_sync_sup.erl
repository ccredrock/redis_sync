%%%-------------------------------------------------------------------
%%% @author ccredrock@gmail.com
%%% @copyright 2017 redrock
%%% @doc
%%% @end
%%%-------------------------------------------------------------------
-module(redis_sync_sup).

-export([start_link/0, init/1]).

%%------------------------------------------------------------------------------
-behaviour(supervisor).

%%------------------------------------------------------------------------------
-spec start_link() -> supervisor:startlink_ret().
start_link() ->
    {ok, Sup} = supervisor:start_link({local, ?MODULE}, ?MODULE, []),
    {ok, _} = supervisor:start_child(?MODULE, {redis_table_sup,
                                               {redis_table_sup, start_link, []},
                                               permanent, infinity, supervisor,
                                               [redis_table_sup]}),
    {ok, _} = supervisor:start_child(?MODULE, {redis_sync,
                                               {redis_sync, start_link, []},
                                               transient, infinity, worker,
                                               [redis_sync]}),
    {ok, Sup}.

-spec init([]) -> supervisor:init().
init([]) ->
    {ok, {{one_for_one, 1, 60}, []}}.

