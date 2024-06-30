-- NOTE: See REPLICATE.md in the parent directory for a longer
-- explanation of how replicated setups work and when to use them.
--
-- This is an example of a very basic "cache replication" setup.
--

-- Memcached proxy thinks in terms of "pools", not individual backends. This
-- may seem wrong if your goal is to "replicate data to all memcached
-- servers". However doing this is a nonstandard antipattern: The design of
-- memcached is that adding servers _increases the available memory to cache_
-- Thus a "pool" of servers have a key hashed against a list of servers.
--
-- In some cases you may still want to replicate a subset of the cache to
-- multiple servers, or multiple pools in different racks, regions, zones,
-- datacenters, etc.
--
-- In this example we set up two pools in a set with a single backend in each,
-- and then tell the routes below to copy keys to all pools.
pools{
    set_all = {
        { backends = { "127.0.0.1:11214" } },
        { backends = { "127.0.0.1:11215" } },
    }
}

--[[
-- In this example, each pool has two backends available. All keys will will
-- have one copy in the first set of backends (127.0.*) and one copy in the
-- second set of backends (127.1.*). This doubles the available memory for
-- caching data while still providing some replication.
pools{
    set_all = {
        { backends = { "127.0.0.1:11211", "127.0.0.2:11211", } },
        { backends = { "127.1.0.1:11211", "127.1.0.2:11211", } },
    }
}
--]]

routes{
    cmap = {
        get = route_failover{
            children = "set_all",
            stats = true,
            miss = true, -- failover on miss
            shuffle = true, -- try the list in a randomized order
            failover_count = 2, -- retry at most 2 times. comment out to try all
        },
        -- if you use gets/gat/mg/etc copy the above here.
    },
    -- by default, send commands everywhere. ie; touch/set/delete
    default = route_allsync{
        children = "set_all",
    },
    --[[
    -- This will send commands to all backends, but return to the client on
    -- the first response it gets. This will give better performance but may
    -- miss errors.
    default = route_allfastest{
        children = "set_all",
    },
    --]]
}
