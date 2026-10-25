pools{
    main = {
        backends = {
            "127.0.0.1:11331",
        }
    },
    side = {
        backends = {
            "127.0.0.1:11332",
        }
    },
}

routes{
    cmap = {
        delete = route_allasync{
            children = { "main" },
            retry = { retry_for = 10, backoff = 0.1, backoff_max = 0.1, attempts = 4 },
        },
        -- no retries once one request (on any retry route) is in the retry
        -- loop of this worker.
        touch = route_allasync{
            children = { "side" },
            retry = { retry_for = 10, backoff = 0.1, backoff_max = 0.1, attempts = 3,
                      max_inflight = 1, stats_name = "capped" },
        },
        -- route_retry on its own answers after the retries.
        mg = route_retry{
            child = "main",
            retry_for = 10, backoff = 0.1, backoff_max = 0.1, attempts = 3,
            stats_name = "direct",
        },
    },
    default = route_direct{ child = "main" },
}
