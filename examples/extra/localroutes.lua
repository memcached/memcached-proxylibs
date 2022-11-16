-- This file shows workarounds for some missing features in the proxy.
-- 1) mcp.await() cannot call lua functions, so you can't do extra processing
-- or full logging on sub-requests
-- 2) mcp.await() cannot do fully asynchronous requests (NOTE: ignore this for
-- now, checking if still necessary)
-- 3) the proxy instance has a local memcached cache, but you cannot route to
-- it directly from proxy code.
--
-- In a future version these workarounds will not be necessary: if you use
-- them please keep an eye on the release notes.
-- This workaround uses features of the listener configuration, namely:
-- 1) proto[*]:ip:port
-- this forces a listening socket onto a specific protocol. So you can set
-- proto[ascii] to create a listener that goes directly to the bundled cache
-- instance
-- 2) tag[*]:ip:port
-- adds a 1-8 char name tag to the listener socket, which can be used from
-- inside the proxy to create dedicated routes.

-- TESTING:
-- memcached -l 0.0.0.0:11211,proto[ascii]:127.0.0.1:11213,tag[semisync]:127.0.0.1:11214,tag[async]:127.0.0.1:11215 -o proxy_config=./localroutes.lua
-- plus a second instance: memcached -l 127.1.1.1:11212,127.1.1.2:11212,127.1.1.3:11212

-- get /semisync/test
-- END
-- ts=1668554204.413679 gid=2 type=proxy_req elapsed=222 type=105 code=17
-- status=0 be=127.1.1.1:11212 detail=synchronous_req req=get /semisync/test
-- ts=1668554204.413784 gid=3 type=proxy_req elapsed=125 type=105 code=17
-- status=0 be=127.1.1.2:11212 detail=semisync_req req=get /semisync/test
-- ts=1668554204.413862 gid=4 type=proxy_req elapsed=203 type=105 code=17
-- status=0 be=127.1.1.3:11212 detail=semisync_req req=get /semisync/test
-- || note the first request is labeled "synchronous_req" and the rest
-- "semisync"

-- || testing the bundled cache route
-- | from 127.0.0.1:11211 |
-- set /internal/test3 0 0 2
-- hi
-- STORED
-- get /internal/test3   
-- VALUE /internal/test3 0 2
-- hi
-- END

-- | confirm directly from second instance 127.1.1.1:11212 |
-- get /internal/test3
-- END
-- | confirms key was set to the proxy-local cache instance

-- This is a very simplified configuration so we can focus on the mechanics
-- specific to the workaround.
function mcp_config_pools(oldss)
    local srv = mcp.backend

    -- Single backend for zones to ease testing.
    -- For purposes of this config the proxy is always "zone 1" (z1)
    local b1 = srv('b1', '127.1.1.1', 11212)
    local b2 = srv('b2', '127.1.1.2', 11212)
    local b3 = srv('b3', '127.1.1.3', 11212)

    -- dedicated pool for the "internal" or builtin cache
    local int = srv('int', '127.0.0.1', 11213)

    -- dedicated pool for talking to ourselves and issuing semisync requests
    local semisync = srv('semisync', '127.0.0.1', 11214)

    -- dedicated pool for talking to ourselves and issuing fully async
    -- requests
    local async = srv('async', '127.0.0.1', 11215)

    -- convert the backends to pools.
    -- as per a normal full config see simple.lua or t/startfile.lua
    local zones = {
        z1 = mcp.pool({ b1 }),
        z2 = mcp.pool({ b2 }),
        z3 = mcp.pool({ b3 }),
        int = mcp.pool({ int }),
        semisync = mcp.pool({ semisync }),
        async = mcp.pool({ async }),
    }

    return zones
end

-- WORKER CODE:

-- Using a very simple route handler only to allow testing the three
-- workarounds in the same configuration file.
function prefix_factory(pattern, list, default)
    local p = pattern
    local l = list
    local d = default
    local s = mcp.stat
    return function(r)
        local route = l[string.match(r:key(), p)]
        if route == nil then
            return d(r)
        end
        return route(r)
    end
end

function mcp_config_routes(zones)
    -- For this example config we do specialized work based on the key prefix.
    local prefixes = {}

    -- In the semi-sync case, we make the client wait for the local zone, then
    -- it asynchronously sends to the "far" zones. Normally this would be used
    -- with sets.
    -- You would also use a route factory like "failover_factory" from
    -- t/startfile.lua, where a "my_zone' variable is used to figure out both
    -- which zone to wait on, and which zones are handled by semisync_p below
    prefixes["semisync"] = function(r) 
        local res = mcp.await(r, { zones.z1, zones.semisync }, 1, mcp.AWAIT_FIRST)
        mcp.log_req(r, res[1], "synchronous_req")
        return res[1]
    end

    -- TODO: Ignore this example for now. it might not be necessary
    prefixes["async"] = function(r)
        local res = mcp.await(r, { zones.int, zones.async }, 1, mcp.AWAIT_FIRST)
        return "OK\r\n" -- we're not actually intending on doing any work in an async request
    end

    -- This is the full example for internal routing; useful if you want to
    -- check for a local cache first.
    -- This can also be used to create a "fast" fake request handler as per
    -- the above "async" code. The full requests are sent asynchronously, and
    -- a dummy request is sent locally and ignored.
    prefixes["internal"] = function(r)
        return zones.int(r)
    end

    local routemain = prefix_factory("^/(%a+)/", prefixes, function(r) return "SERVER_ERROR no route\r\n" end)
    mcp.attach(mcp.CMD_ANY_STORAGE, routemain)

    -- In a full config, we use "my_zone" to figure out which zones should be
    -- included here; in a semisync case the client waits on the first zone,
    -- but not the next two.
    -- Note the tag as the last argument to mcp.attach()
    local semisync_p = { zones.z2, zones.z3 }
    mcp.attach(mcp.CMD_ANY_STORAGE, function(r)
        local res
        for _, p in pairs(semisync_p) do
            res = p(r)
            mcp.log_req(r, res, "semisync_req")
        end
        return res -- doesn't really matter what we return here.
    end, "semisync")

    -- In a full asynchronous case, we're sending this data to all zones. If
    -- your asynchronous use case is to use a specific pool or do some special
    -- processing, do that instead.
    local async_p = { zones.z1, zones.z2, zones.z3 }
    mcp.attach(mcp.CMD_ANY_STORAGE, function(r)
        local res
        for _, p in pairs(async_p) do
            res = p(r)
            mcp.log_req(r, res, "async_req")
        end
        return res -- doesn't really matter what we return here.
    end, "async")

    -- nothing to attach for "internal"
end
