verbose(true)
debug(true)

settings{
    active_request_limit = 100,
    backend_connect_timeout = 3,
    -- define global overrides to apply to all pool specific settings
    pool_options = {
        filter = "tags",
        filter_conf = "{}"
    }
}

pools{
    foo = {
        -- settings for this particular pool.
        options = { seed = "hello" },
        backends = {
            "127.0.0.1:11214",
        }
    },
}

routes{
    map = {
        foo = route_allfastest{
            children = { "foo" },
        },
    },
}
