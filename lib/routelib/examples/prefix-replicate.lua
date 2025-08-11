-- NOTE: See REPLICATE.md in the parent directory for a longer
-- explanation of how replicated setups work and when to use them.
--
-- In this example we logically split our cache where by default keys are
-- spread across a pool of servers to maximize available memory. Then, a small
-- set of highly accessed "hot" keys are replicated across multiple servers

-- verbosity({ verbose = true, debug = true })

pools{
    main = {
        backends = {
            "127.0.0.1:11211",
            "127.0.0.2:11211",
            "127.0.0.3:11211",
            "127.0.0.4:11211",
            "127.0.0.5:11211",
        }
    },
    -- These could even be a subset of the same servers listed above, however
    -- we have to describe it as three pools with one backend in each.
    set_hot = {
        { backends = { "127.1.0.1:11211" } },
        { backends = { "127.1.0.2:11211" } },
        { backends = { "127.1.0.3:11211" } },
    },
}

-- keys with prefix "hot/" get routed to a special handler.
routes{
    map = {
        hot = cmdmap{
            -- send all commands to all pools by default
            all = route_allfastest{
                children = "set_hot" 
            },
            -- override specific commands to fetch from just one server for
            -- performance reasons.
            get = route_failover{
                children = "set_hot",
                stats = true,
                miss = true, -- failover if miss, not just for down server
                shuffle = true, -- each proxy will have a different order
            },
            -- if gat/gets/mg/etc are used override here.
        },
    },
    -- by default, all commands map to exactly one server within the larger
    -- pool.
    default = route_direct{
        child = "main",
    },
}
