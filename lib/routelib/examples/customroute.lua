-- get foo/a
-- SERVER_ERROR hello, world: foo

package.loaded["routelib"] = nil
local s = require("routelib")
verbose(true)
debug(true)

function config()
    -- this call could also just be at the bottom of this file.
    register_route_handlers({
        "myhello",
    })

    -- pools not actually used in this example.
    pools{
        foo = {
            backends = {
                "127.0.0.1:11214",
            }
        },
    }

    routes{
        map = {
            foo = route_myhello{
                msg = "hello, world:"
            },
        },
    }
end

-- can load custom route handlers from a different file via require, as at the
-- top of this file.

-- the "_conf()" function is run from the configuration thread
-- at this point you can grab stats counter ids, or otherwise query global data
-- or morph passed-in data
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
        t.stast_id = stats_get_id("myhello")
    end

    -- the result table must be a pure lua object, as it is copied between
    -- lua VM's from the configuration thread to the worker threads.
    -- this means special user objects, metatables, etc, will not transfer.
    return { f = "route_myhello_start", a = t }
end

-- all of the rest is run from each worker thread.
-- these are run in different lua VM's, so you cannot access global data from
-- the configuration thread.

-- called once during the worker configuration phase: configures and returns a
-- generator.
function route_myhello_start(a, ctx)
    local fgen = mcp.funcgen_new()
    -- not adding any children for this function.

    fgen:ready({ a = a, n = ctx:label(), f = route_myhello_f })

    -- make sure to return the generator we just made
    return fgen
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
    local stats_id = t.stats_id
    local s = mcp.stat
    return function(r)
        if stats_id then
            s(stats_id, 1)
        end
        return msg
    end
end
