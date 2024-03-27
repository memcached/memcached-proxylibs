verbose(true)
debug(true)

settings{
    backend_connect_timeout = 3,
}

pools{
    foo = {
        backends = {
            "127.0.0.1:11312",
        }
    },
    baz = {
        backends = {
            "127.0.0.1:11313",
        }
    },
    set_bar = {
        z1 = {
            backends = {
                "127.0.0.1:11312",
            }
        },
        z2 = {
            backends = {
                "127.0.0.1:11313",
            }
        }
    }
}

-- testing all the forms of defining child pools
routes{
    map = {
        foo = route_direct{
            child = "foo",
        },
        bar = route_allsync{
            children = "set_bar",
        },
        baz = route_allsync{
            children = { "foo", "baz" },
        },
        bee = route_allsync{
            children = { z1 = "foo", z2 = "baz" },
        },
        zee = route_allsync{
            children = { "set_bar_z1", "set_bar_z2" },
        },
    }
}
