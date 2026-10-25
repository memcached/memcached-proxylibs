pools{
    main = {
        backends = {
            "127.0.0.1:11341",
        }
    },
    side = {
        backends = {
            "127.0.0.1:11342",
        }
    },
}

routes{
    cmap = {
        -- max_pending = 0: every request waits for its children.
        touch = route_allasync{
            children = { "main" },
            max_pending = 0,
            pending_wait = 5,
        },
        -- ... but not longer than pending_wait.
        mg = route_allasync{
            children = { "main" },
            max_pending = 0,
            pending_wait = 0.2,
        },
        -- background retries on another pool.
        delete = route_allasync{
            children = { "side" },
            retry = { retry_for = 10, backoff = 0.1, backoff_max = 0.1, attempts = 4 },
        },
        -- only throttles if requests in a retry loop are counted.
        decr = route_allasync{
            children = { "main" },
            max_pending = 1,
            pending_wait = 5,
        },
        -- far below the limit.
        incr = route_allasync{
            children = { "main" },
            max_pending = 100,
            pending_wait = 5,
        },
    },
    default = route_direct{ child = "main" },
}
