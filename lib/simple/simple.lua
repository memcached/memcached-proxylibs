-- WARNING! THIS LIBRARY IS DEPRECATED! It will stop working with newer
-- versions of memcached.
-- Please use routelib: https://github.com/memcached/memcached-proxylibs/blob/main/lib/routelib/README.md

local M = { c = { pools = {} } }
local STAT_FAILOVER <const> = 1

-- https://stackoverflow.com/questions/9168058/how-to-dump-a-table-to-console
-- should probably get a really nice one of these for the library instead.
local function dump(o)
   if type(o) == 'table' then
      local s = '{ '
      for k,v in pairs(o) do
         if type(k) ~= 'number' then k = '"'..k..'"' end
         s = s .. '['..k..'] = ' .. dump(v) .. ','
      end
      return s .. '} '
   else
      return tostring(o)
   end
end

local function logreq_factory(route)
    local nr = route
    return function(r)
        local res, detail = nr(r)
        mcp.log_req(r, res, detail)
        return res
    end
end

-- NOTE: this function is culling key prefixes. it is an error to use it
-- without a left anchored (^) pattern.
local function prefix_factory(pattern, list, default, do_trim)
    -- tag the start anchor so users don't have to remember.
    -- might want to test if it's there first? :)
    local p = "^" .. pattern
    local l = list
    local d = default
    if do_trim then
        return function(r)
            local i, j, match = string.find(r:key(), p)
            local route = nil
            if match ~= nil then
                -- remove the key prefix so we don't waste storage.
                r:ltrimkey(j)
                route = l[match]
            end
            if route == nil then
                return d(r)
            else
                return route(r)
            end
        end
   else
        return function(r)
            local i, j, match = string.find(r:key(), p)
            local route = nil
            if match ~= nil then
                route = l[match]
            end
            if route == nil then
                return d(r)
            else
                return route(r)
            end
        end
   end
end

local function command_factory(map, default)
    local m = map
    local d = default
    return function(r)
        local f = map[r:command()]
        if f == nil then
            return d(r)
        end
        return f(r)
    end
end

-- TODO: is the return value the average? anything special?
-- walks a list of selectors and repeats the request.
local function walkall_factory(pool)
    local p = {}
    -- TODO: a shuffle could be useful here.
    for _, v in pairs(pool) do
        table.insert(p, v)
    end
    local x = #p
    return function(r)
        local restable = mcp.await(r, p)
        -- walk results and return "best" result
        for _, res in pairs(restable) do
            if res:ok() then
                return res
            end
        end
        -- else we return the first result.
        return restable[1]
    end
end

local function failover_factory(zones, local_zone)
    local near_zone = zones[local_zone]
    local far_zones = {}
    -- NOTE: could shuffle/sort to re-order zone retry order
    -- or use 'next(far_zones, idx)' via a stored upvalue here
    for k, v in pairs(zones) do
        if k ~= local_zone then
            far_zones[k] = v
        end
    end
    local s = mcp.stat
    return function(r)
        local res = near_zone(r)
        if res:hit() == false then
            s(STAT_FAILOVER, 1)
            -- got a local miss. attempt all replicas, return the first HIT
            -- seen, if no hits return the first result.
            local restable = mcp.await(r, far_zones, 1)
            for _, res in pairs(restable) do
                if res:hit() then
                    return res, "failover_hit"
                end
            end

            return restable[1], "failover_failure"
        end
        return res, "primary_hit" -- send result back to client
    end
end

--
-- User interface functions
--

function pool(a)
    -- print(dump(a))
    M.c.pools[a.name] = a
end
function router(a)
    -- print(dump(a))
    M.c.router = a
end
function my_zone(zone)
    -- print(zone)
    M.c.my_zone = zone
end
function verbose(opt)
    M.is_verbose = opt
end
function say(...)
    if M.is_verbose then
        print(...)
    end
end

--
-- Loader functions
--

local function make_backend(host)
    say("making backend for... " .. host)

    local ip, port, name = string.match(host, "^(.+):(%d+)%s+(%a+)")
    if ip ~= nil then
        return mcp.backend(name, ip, port)
    end

    local ip, port = string.match(host, "^(.+):(%d+)")
    if ip ~= nil then
        return mcp.backend(host, ip, port)
    end

    error(host .. " is an invalid backend string")
end

local function make_pool(conf)
    local p = {}

    for _, be in pairs(conf.backends) do
        table.insert(p, make_backend(be))
    end

    return mcp.pool(p, conf.distributor)
end

local function make_zoned_pool(conf, zname, backends)
    local p = {}

    for _, be in pairs(backends) do
        table.insert(p, make_backend(be))
    end

    local dist = conf.distributor
    local zdist = conf.zone_distributors
    if zdist ~= nil then
        if zdist[zname] ~= nil then
            say("using overridden distributor for zone " .. zname)
            dist = zdist[zname]
        end
    end

    return mcp.pool(p, dist)
end

-- place/replace the global function
function mcp_config_pools(old)
    mcp.add_stat(STAT_FAILOVER, "simple_failovers")
    local c = M.c
	local r = {
        router_type = "keyprefix",
        match_prefix = "/(%a+)/",
        prefix_trim = true,
    }

    -- merge in any missing defaults.
    if M.c["router"] ~= nil then
        for k, v in pairs(r) do
            if M.c.router[k] ~= nil then
                say("router: overriding default for", k)
            else
                M.c.router[k] = v
            end
        end
        r = M.c.router
    end

    --print("read:\n")
    --print(dump(c), dump(r))
	-- convert config into backend and pool objects.
	local o = { pools = {} }

    if r.router_type == "flat" then
        if c.pools["default"] == nil then
            error("router_type: flat requires pool with name 'default'")
        end
        local conf = c.pools["default"]
        if c.my_zone == nil then
            -- no zone configured, manage a single pool.
            o.pool = make_pool(conf)
        else
            if conf.zones[c.my_zone] == nil then
                error("pool: default missing local zone: " .. c.my_zone)
            end
            local z = {}
            for zname, backends in pairs(conf.zones) do
                z[zname] = make_zoned_pool(conf, zname, backends)
            end
            o.pool = z
        end
    elseif r.router_type == "keyprefix" then
        -- TODO: figure out inherited defaults to use for the mcp.pool arguments
        for name, conf in pairs(c.pools) do
            local z = {}
            if c.my_zone == nil or conf.zones == nil then
                -- no zone configured, build pool from 'backends'
                z = make_pool(conf)
            else
                if conf.zones[c.my_zone] == nil then
                    error("pool: " .. conf.name .. " missing local zone: " .. c.my_zone)
                end
                for zname, backends in pairs(conf.zones) do
                    z[zname] = make_zoned_pool(conf, zname, backends)
                end
            end
            o.pools[name] = z
        end
    else
        error("unknown router type: " .. r.router_type)
    end

    o.my_zone = c.my_zone

    o.r = r
    -- reset the module's configuration so reload will work.
    M.c = { pools = {} }
    return o
end

-- also intentionally creating a global.
function mcp_config_routes(c)
    -- print(dump(c))
    local default = c.r["default_pool"]
    if c.r["default_pool"] == nil then
        default = function(r) return "SERVER_ERROR no route\r\n" end
    end

    if c.r.router_type == "flat" then
        if c["my_zone"] == nil then
            say("setting up a zoneless flat pool")
            local top
            if c.r.log ~= nil then
                top = logreq_factory(o.pool)
            else
                top = function(r) return o.pool(r) end
            end
            mcp.attach(mcp.CMD_ANY_STORAGE, top)
        else
            local myz = c.my_zone
            say("setting up flat replicated zone. local: " .. myz)

            local pools = {}
            local zones = c.pool
            local failover = failover_factory(zones, myz)
            local all = walkall_factory(zones)
            local map = {
                [mcp.CMD_ADD] = all,
                [mcp.CMD_SET] = all,
                [mcp.CMD_DELETE] = all,
                [mcp.CMD_APPEND] = all,
                [mcp.CMD_PREPEND] = all,
                [mcp.CMD_INCR] = all,
                [mcp.CMD_DECR] = all,
                [mcp.CMD_MS] = all,
                [mcp.CMD_MD] = all,
            }
            local top
            if c.r.log ~= nil then
                top = logreq_factory(command_factory(map, failover))
            else
                top = command_factory(map, failover)
            end
            mcp.attach(mcp.CMD_ANY_STORAGE, top)
        end
    else
        -- with a non-zoned configuration we can run with a completely flat config
        if c["my_zone"] == nil then
            say("setting up a zoneless route")
            local top = prefix_factory(c.r.match_prefix, c.pools, c.pools[default], c.r.prefix_trim)
            if c.r.log ~= nil then
                top = logreq_factory(top)
            end
            mcp.attach(mcp.CMD_ANY_STORAGE, top)
        else
            -- else we have a more complex setup.
            local myz = c.my_zone
            say("setting up a zoned route. local: " .. myz)

            -- process each pool to have replicated zones.
            local pools = {}
            for name, zones in pairs(c.pools) do
                if type(zones) == "userdata" then
                    -- flat pool.
                    pools[name] = zones
                else
                    local failover = failover_factory(zones, myz)
                    local all = walkall_factory(zones)
                    -- TODO: flesh this out more; append/prepend/replace/etc?
                    -- think a bit about good defaults?
                    local map = {
                        [mcp.CMD_ADD] = all,
                        [mcp.CMD_SET] = all,
                        [mcp.CMD_DELETE] = all,
                        [mcp.CMD_APPEND] = all,
                        [mcp.CMD_PREPEND] = all,
                        [mcp.CMD_INCR] = all,
                        [mcp.CMD_DECR] = all,
                        [mcp.CMD_MS] = all,
                        [mcp.CMD_MD] = all,
                    }
                    pools[name] = command_factory(map, failover)
                end
            end
            print(dump(pools))

            local top = prefix_factory(c.r.match_prefix, pools, pools[default], c.r.prefix_trim)
            if c.r.log ~= nil then
                top = logreq_factory(top)
            end
            mcp.attach(mcp.CMD_ANY_STORAGE, top)
        end
    end
end

return M
