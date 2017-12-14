%%%-------------------------------------------------------------------
%%% @author ccredrock@gmail.com
%%% @copyright 2017 redrock
%%% @doc
%%% @end
%%%-------------------------------------------------------------------
-module(redis_table_sup).

-export([start_link/0, init/1, start_child/1]).

%%------------------------------------------------------------------------------
-behaviour(supervisor).

%%------------------------------------------------------------------------------
-spec start_link() -> supervisor:startlink_ret().
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec init([]) -> supervisor:init().
init([]) ->
    {ok, {{one_for_one, 1, 60}, []}}.

-spec start_child(Table::atom()) -> supervisor:startchild_ret().
start_child(Table) ->
    supervisor:start_child(?MODULE,
                           {Table,
                            {redis_table, start_link, [Table]},
                            transient, infinity, worker,
                            [redis_table]}).

