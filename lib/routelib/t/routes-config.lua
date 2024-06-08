--verbose(true)
--debug(true)

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
}

routes{
    map = {
        failover = route_failover{
            children = { "foo", "bar", "baz" },
            miss = true,
            failover_count = 2,
        },
        direct_a = route_direct{
            child = "foo",
        },
        direct_b = route_direct{
            child = "bar",
        },
        direct_c = route_direct{
            child = "baz",
        },
    },
}
