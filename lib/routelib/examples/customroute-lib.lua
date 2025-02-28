-- this call could also just be at the bottom of this file.
register_route_handlers({
    "myhello",
})

-- the "_conf()" function is run from the configuration thread
-- at this point you can grab stats counter ids, or otherwise query global data
-- or morph passed-in data

-- NOTE: any pools or child functions you intend to use _must_ start with
-- `child`. examples:
-- route_myhello{ child_a: "poolfoo", child_b: "poolbar" }
-- route_myhello{ children: { "foo", "bar" } }
-- route_myhello{ child_main: "foo", children_failover: { "bar", "baz" } }
--
-- anything with the "child" name is pre-processed within routelib to resolve
-- recursive routes and validate pools.
function route_myhello_conf(t, ctx)
    -- here we tag the route label onto our message.
    -- in a complex setup, we could use the route label as an index into a
    -- global structure with further data/overrides/etc.
    t.msg = t.msg .. " " .. ctx:label()
    -- if user asked for a stats counter, lets track how often this route was
    -- called.
    -- you could construct a string from the label to get a route and handler
    -- specific counter instead.
    if t.stats then
        t.stats_id = ctx:stats_get_id("myhello")
    end

    -- the result table must be a pure lua object, as it is copied between
    -- lua VM's from the configuration thread to the worker threads.
    -- this means special user objects, metatables, etc, will not transfer.
    return t
end

-- all of the rest is run from each worker thread.
-- these are run in different lua VM's, so you cannot access global data from
-- the configuration thread.

-- called once during the worker configuration phase: configures and returns a
-- generator.
function route_myhello_start(a, ctx, fgen)
    -- not adding any children for this function.

    -- configure the function generator
    fgen:ready({ a = a, n = ctx:label(), f = route_myhello_f })
end

-- called once per "query slot" needed to satisfy parallel requests.
-- returned function is reused after each request
function route_myhello_f(rctx, a)
    -- do any processing, unwrapping referenced data from deeply nested tables
    -- for speed, decide on a specialized function to return based on
    -- arguments passed, etc.
    -- here we pre-process the message to wrap it with an error message
    -- now at runtime we will never make allocations (and thus never collect
    -- garbage)
    local msg = "SERVER_ERROR " .. a.msg .. "\r\n"
    local stats_id = a.stats_id
    local s = mcp.stat
    return function(r)
        if stats_id then
            s(stats_id, 1)
        end
        return msg
    end
end
