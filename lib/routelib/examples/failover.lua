verbose(true)
debug(true)

-- we configure route_failover to track itself with a stats counter, accessable
-- via "stats proxy" under user_* fields.
pools{
    foo = {
        backends = {
            "127.0.0.1:11214",
        }
    },
    baz = {
        backends = {
            "127.0.0.1:11215",
        }
    }
}

routes{
    map = {
        bar = cmdmap{
            [mcp.CMD_GET] = route_failover{
                children = { "foo", "baz" },
                stats = true,
                miss = true,
                shuffle = true,
            },
            [mcp.CMD_SET] = route_allsync{
                children = { "foo", "baz" },
            },
        },
    },
}
