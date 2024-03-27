-- use `-o proxy_config` to load your custom libraries along with routelib.
-- IE:
-- -o proxy_config=routelib.lua:customroute-lib.lua
-- you can also load it here via `require`, but reloading the proxy may not
-- load the latest version of your extensions that way.

-- get foo/a
-- SERVER_ERROR hello, world: foo

verbose(true)
debug(true)

-- pools not actually used in this example.
pools{
    foo = {
        backends = {
            "127.0.0.1:11214",
        }
    },
}

routes{
    map = {
        foo = route_myhello{
            msg = "hello, world:"
        },
    },
}
