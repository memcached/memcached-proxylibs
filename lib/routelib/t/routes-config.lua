--verbose(true)
--debug(true)
local_zone("zbar")

settings{
    backend_connect_timeout = 3,
}

pools{
    foo = {
        backends = {
            "127.0.0.1:11322",
        }
    },
    bar = {
        backends = {
            "127.0.0.1:11323",
        }
    },
    baz = {
        backends = {
            "127.0.0.1:11324",
        }
    },
    set_all = {
        zfoo = {
            backends = {
                "127.0.0.1:11322",
            }
        },
        zbar = {
            backends = {
                "127.0.0.1:11323",
            }
        },
        zbaz = {
            backends = {
                "127.0.0.1:11324",
            }
        },
    }
}

routes{
    map = {
        failover = route_failover{
            children = { "foo", "bar", "baz" },
            miss = true,
            failover_count = 2,
        },
        failovernomiss = route_failover{
            children = { "foo", "bar", "baz" },
            miss = false,
            failover_count = 2,
        },
        split = route_split{
            child_a = "foo",
            child_b = "bar",
        },
        splitsub = route_split{
            child_a = route_direct{
                child = "foo"
            },
            child_b = route_direct{
                child = "bar"
            },
        },
        allfastest = route_allfastest{
            children = { "foo", "bar", "baz" }
        },
        allsync = route_allsync{
            children = { "foo", "bar", "baz" }
        },
        zfailover = route_zfailover{
            children = "set_all",
            stats = true,
            miss = true,
        },
        -- used to test if each backend has a clear pipeline
        direct_a = route_direct{
            child = "foo",
        },
        direct_b = route_direct{
            child = "bar",
        },
        direct_c = route_direct{
            child = "baz",
        },
        d_submap = cmdmap{
            mg = route_direct{ child = "foo" },
            md = route_direct{ child = "bar" },
            ma = route_direct{ child = "baz" },
        },
    },
    cmap = {
        mg = route_direct{ child = "baz" },
        md = route_direct{ child = "bar" },
        ma = route_direct{ child = "foo" },
    }
}
