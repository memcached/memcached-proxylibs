verbose(true)
debug(true)

-- it's possible to have different route trees based on what port is being
-- accessed to memcached.
-- this is useful if you want to route to different pools by port alone, or
-- have different prefix trees for different services.
--
-- memcached must be started like:
-- -l 127.0.0.1:12051 -l tag_b_:127.0.0.1:12052 -l tag_cccc_:127.0.0.1:12053
-- this gives a default listener on 12051, "b" tag for 12052, and "cccc" tag
-- for 12053.
pools{
    foo = {
        backends = {
            "127.0.0.1:11214",
        }
    },
    bar = {
        backends = {
            { host = "127.0.0.1", port = 11216, retrytimeout = 5 }
        }
    },
}

-- no supplied tag makes this the "default" router.
routes{
    foo = route_allfastest{
        children = { "foo" },
    },
}

-- this route tree is only executed if a client is connected to port 12052
routes{
    tag = "b",
    foo = route_allfastest{
        children = { "foo" },
    },
}

-- this route tree is only executed if a client is connected to port 12053
routes{
    tag = "cccc",
    foo = route_allfastest{
        children = { "foo" },
    },
}
