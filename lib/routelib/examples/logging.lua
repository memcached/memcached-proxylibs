settings{
    backend_options = {
        log = {
            rate = 5, -- log one out of every 5 by random chance
            errors = true, -- log all errors
            deadline = 250, -- log all results slower than 250ms
            tag = "all" -- informational tag for log
        }
    }
}

pools{
    main = {
        -- per-pool logging options.
        --[[backend_options = {
            -- if no constraints given, log everything (SLOW!)
            log = { tag = "mainpool" }
        },--]]
        backends = {
            "127.0.0.1:11214",
            "127.0.0.1:11215",
        }
    }
}

routes{
    default = route_direct{ child = "main" }
}
