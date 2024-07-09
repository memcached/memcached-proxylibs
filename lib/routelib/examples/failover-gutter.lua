-- This is an example for reducing the impact of a downed cache server.
--
-- For example, we have a list of three cache servers in a pool: a, b, c
-- If cache node 'b' fails, clients will receive errors until the 'b' server
-- is replaced.
-- With this example, we use the failover and ttl adjusting routes to allow
-- temporarily rerouting cache requests while a server is failed.
--
-- We do this first by "failing over" the request from the bad server to
-- another server, with several different approaches.
--
-- Next, if a request has failed over, we adjust the TTL of any 'set' commands
-- to be lower. In this example we use five minutes. This prevents cache
-- entires that have "failed over" from staying around for a long period of
-- time, which can cause issues if server 'b' repeatedly fails.
-- The short cache time should allow a good hit rate for objects which are
-- immediately being reused (ie; a user browsing around a website, or rate
-- limit counters).

-- Questions:
-- - Do we need to copy all deletes to the gutter pool?
--   - If a sufficiently low TTL is used and servers do not typically flap,
--   no. Depending on your setup you might have to.
-- - What about touch/gat commands?
--   - This will depend on your needs. Please contact us if you have questions
--   here.
-- - How do we handle backends that are flapping (ie; sometimes timing out but
--   not competely dead?
--   - You can adjust "anti flap" parameters, which will force a backend to
--   stay down with a backoff algorithm:
-- settings{
--  backend_flap_time = 30, -- stable for 30 seconds means not flapping
--  backend_flap_backoff_ramp = 1.2, -- multipler for backoff wait time
--  backend_flap_backoff_max = 3600, -- try at least once an hour
-- }

local be_list = {
    "127.0.0.1:11214",
    "127.0.0.1:11215",
    "127.0.0.1:11216",
}

pools{
    foo = {
        backends = be_list,
    },
    -- for one example, we use a dedicated gutter pool. This server will be
    -- idle unless another server has failed or is being serviced.
    gutter = {
        backends = { "127.1.0.0.1:11311" }
    },
    -- in this example, we reuse the main set of backends, but change the key
    -- hashing seed so keys will map to the list of backends differently. This
    -- can minimize server maintenance while avoiding having idle "gutter"
    -- servers.
    -- The downside is keys may map to the same place, especially if you have
    -- a small list of backends. This can be partly mitigated by having
    -- multiple gutters with different seeds and failing several times.
    -- There may still be keys that fail but they will hopefully be few and
    -- the service can survive until the cache node is restored.
    --
    -- To use this, replace "gutter" below with "gutter_same"
    gutter_same = {
        options = { seed = "failover" },
        backends = be_list,
    },
}

routes{
    cmap = {
        set = route_failover{
            children = { "foo", route_ttl{ ttl = 300, child = "gutter" } },
            stats = true,
            miss = false, -- do not fail over on miss, only errors!
            -- do not shuffle the pool list, either!
        },
        -- repeat this for add, cas, ms, as necessary
    },
    -- we handle the rest of the commands with a default failover to gutter.
    default = route_failover{
        children = { "foo", "gutter" },
    },
}
