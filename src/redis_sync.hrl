%%%-------------------------------------------------------------------
%%% @author ccredrock@gmail.com
%%% @copyright 2017 redrock
%%% @doc reids_sync
%%%
%%% @end
%%%-------------------------------------------------------------------
-ifndef(REDIS_SYNC_HRL).
-define(REDIS_SYNC_HRL, true).

-define(ETS_VERSION, '$sync_version').

-define(ETS_DATA(T),
        list_to_atom("$sync_table_" ++ atom_to_list(T))).

-define(REDIS_VERSION(T),
        iolist_to_binary([<<"${sync}_version_">>, atom_to_binary(T, utf8)])).

-define(REDIS_DATA(T, K),
        iolist_to_binary([<<"${sync}_data_">>, atom_to_binary(T, utf8), <<"@">>, K])).

-define(DEBUG, lager:debug).
-define(INFO,  lager:info).
-define(WARN,  lager:warning).
-define(ERROR, lager:error).

-endif.

