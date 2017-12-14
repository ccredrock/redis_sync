
-module(redis_sync_tests).

-include_lib("eunit/include/eunit.hrl").

basic_test_() ->
    {inorder,
     {setup,
      fun() ->
              redis_sync:start(),
              application:set_env(reids_sync, table_loop_time, 50),
              application:set_env(reids_sync, table_alarm_len, 10),
              application:set_env(reids_sync, table_clean_len, 7)
      end,
      fun(_) ->
              redis_sync:purge()
      end,
      [{"table",
        fun() ->
                ?assertEqual(ok, element(1, hd(eredis_cluster:qa([<<"INFO">>])))),
                ?assertEqual(ok, element(1, redis_sync:add_table(test3))),
                ?assertEqual(3, length(redis_sync:list_table()))
        end},
       {"put_val",
        fun() ->
                ?assertEqual(ok, redis_sync:put_val(test1, <<"key1">>, #{ckey1 => cval1})),
                timer:sleep(100),
                ?assertEqual(cval1, maps:get(ckey1, redis_sync:get_val(test1, <<"key1">>))),
                ?assertEqual(ok, redis_sync:del_val(test1, <<"key1">>)),
                timer:sleep(100),
                ?assertEqual(null, redis_sync:get_val(test1, <<"key1">>))
        end},
       {"lock_val",
        fun() ->
                Locks = redis_sync:get_lock([test1]),
                ?assertEqual(ok, redis_sync:lock_put(Locks, test1, <<"key1">>, #{ckey1 => cval1})),
                timer:sleep(100),
                ?assertEqual(cval1, maps:get(ckey1, redis_sync:get_val(test1, <<"key1">>))),
                ?assertNotEqual(ok, redis_sync:lock_del(Locks, test1, <<"key1">>)),
                ?assertEqual(ok, redis_sync:lock_del(redis_sync:get_lock([test1]), test1, <<"key1">>)),
                timer:sleep(100),
                ?assertEqual(null, redis_sync:get_val(test1, <<"key1">>))
        end},
       {"monitor",
        fun() ->
                ets:new(monitor, [named_table, public]),
                ets:insert(monitor, [{update, 0}]),
                Fun = fun Fun() -> receive {redis_sync, _, _, _} -> ets:update_counter(monitor, update, 1) end, Fun() end,
                PID = spawn(fun() -> redis_sync:monitor(test1), Fun() end),
                ?assertEqual(ok, redis_sync:put_val(test1, <<"key1">>, #{ckey1 => cval1})),
                timer:sleep(100 + 50 * 5),
                ?assertEqual(ok, redis_sync:put_val(test1, <<"key1">>, #{ckey1 => cval2})),
                timer:sleep(100 + 50 * 5),
                ?assertEqual(2, element(2, hd(ets:lookup(monitor, update)))),
                ?assertEqual(ok, redis_sync:del_val(test1, <<"key1">>)),
                exit(PID, shutdown),
                ets:delete(monitor)
        end},
       {"clean",
        fun() ->
                ?assertEqual(<<"0">>, maps:get(test2, redis_sync:get_lock([test2]))),
                [ok = redis_sync:put_val(test2, <<"key1">>, #{X => X}) || X <- lists:seq(1, 11)],
                timer:sleep(500),
                ?assertEqual(<<"4">>, maps:get(test2, redis_sync:get_lock([test2])))
        end}
      ]}
    }.

