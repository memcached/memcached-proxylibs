local function module_defaults()
    return {
        c_in = {
            pools = {},
            routes = {},
        }
    }
end
M = module_defaults()

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

--
-- User interface functions
--

function settings(a)
    if M.is_debug then
        print("settings:")
        print(dump(a))
    end
    M.c_in.settings = a
end

function pools(a)
    if M.is_debug then
        print("pools config:")
        print(dump(a))
    end
    M.c_in.pools = a
end

function routes(a)
    dsay("routes:")
    dsay(dump(a))
    if a["conf"] == nil then
        a["conf"] = {}
    end

    if a.tag then
        M.c_in.routes[a.tag] = a
    else
        M.c_in.routes["default"] = a
    end
end

-- TODO: match regexp list against set of pools
function find_pools(m)
    error("unimplemented")
end

function local_zone(zone)
    dsay(zone)
    M.c.local_zone = zone
end

function verbose(opt)
    M.is_verbose = opt
    say("verbosity set to:", opt)
end

function debug(opt)
    M.is_debug = opt
    if M.is_debug or M.is_verbose then
        print("debug set to:", opt)
    end
end

function say(...)
    if M.is_verbose then
        print(...)
    end
end

function dsay(...)
    if M.is_debug then
        print(...)
    end
end

--
-- User/Pool configuration thread functions
--

-- TODO: remember values and add to verbose print if changed on reload
local function settings_parse(a)
    local setters = {
        ["backend_connect_timeout"] = function(v)
            mcp.backend_connect_timeout(v)
        end,
        ["backend_retry_waittime"] = function(v)
            mcp.backend_retry_waittime(v)
        end,
        ["backend_read_timeout"] = function(v)
            mcp.backend_read_timeout(v)
        end,
        ["backend_failure_limit"] = function(v)
            mcp.backend_failure_limit(v)
        end,
        ["tcp_keepalive"] = function(v)
            mcp.tcp_keepalive(v)
        end,
        ["active_request_limit"] = function(v)
            mcp.active_req_limit(v)
        end,
        ["buffer_memory_limit"] = function(v)
            mcp.buffer_memory_limit(v)
        end,
        ["backend_flap_time"] = function(v)
            mcp.backend_flap_time(v)
        end,
        ["backend_flap_backoff_ramp"] = function(v)
            mcp.backend_flap_backoff_ramp(v)
        end,
        ["backend_flap_backoff_max"] = function(v)
            mcp.backend_flap_backoff_max(v)
        end,
    }

    -- TODO: throw error if setting unknown
    for setting, value in pairs(a) do
        local func = setters[setting]
        if func ~= nil then
            say("changing global setting:", setting, "to:", value)
            func(value)
        elseif setting == "poolopts" then
            error("unimplemented")
        end
    end
end

local function make_backend(name, host, o)
    local b = {}
    -- override per-backend options if requested
    if o ~= nil then
        for k, v in pairs(o) do
            b[k] = v
        end
    end

    if type(host) == "table" then
        for k, v in pairs(host) do
            b[k] = v
        end

        if b.host == nil then
            error("host missing from server entry")
        end
    else
        say("making backend for... " .. host)

        if string.match(host, "_down_$") then
            b.down = true
        end

        local ip, port, name = string.match(host, "^(.+):(%d+)%s+(%a+)")
        if ip ~= nil then
            b.host = ip
            b.port = port
            b.label = name
        else
            local ip, port = string.match(host, "^(.+):(%d+)")
            if ip ~= nil then
                b.host = ip
                b.port = port
            end
        end

        if b.host == nil then
            error(host .. " is an invalid backend string")
        end
    end

    -- create a default label out of the host:port if none directly supplied
    if b.label == nil then
        b.label = b.host .. ":" .. b.port
    end

    return mcp.backend(b)
end

-- converts a table describing pool objects into a new table of real pool
-- objects.
local function pools_parse(a)
    local pools = {}
    for name, conf in pairs(a) do
        local popts = conf.options
        local sopts = conf.server_options
        local s = {}
        -- TODO: some convenience functions for asserting?
        -- die more gracefully if server list missing
        for _, server in pairs(conf.servers) do
            table.insert(s, make_backend(name, server, sopts))
        end

        dsay("making pool:", name, "\n", dump(popts))
        pools[name] = mcp.pool(s, popts)
    end

    return pools
end

--
-- Worker thread configuration functions
--

-- TODO: allow a way to override which attach() happens for a router.
-- by default we just do CMD_ANY_STORAGE
-- NOTE: this function should be used for vadliating/preparsing the router
-- config and routes sections, but right now it doesn't do anything.
local function routes_parse(routes, pools)
    return { r = routes, p = pools }
end

-- re-wrap the arguments to create the function generator within a worker
-- thread.
local function make_route(arg, ctx)
    dsay("generating a route:", ctx:label(), ctx:cmd())
    -- resolve the named function to a real function from global
    local f = _G[arg.f]
    -- create and return the funcgen object
    return f(arg.a, ctx)
end

-- create and return a full router object.
-- 1) walk the input route set, copying into a new map
-- 2) execute any route handlers found
-- 3) route handler resolves children into (pool/fgen) and returns fgen
-- 4) create the root route handler and return
-- NOTE: we do an unfortunate duck typing of a map entry, as metatables can't
-- cross the cross-VM barrier so we can't type the route handlers. Map entries
-- can have sub-maps and they'll both just look like tables.
-- TODO: support top level command only maps
local function make_router(set, pools)
    local map = {}
    dsay("making a new router")

    local ctx = {
        label = function(self)
            return self._label
        end,
        cmd = function(self)
            return self._cmd
        end,
        get_child = function(self, child)
            if type(child) == "string" then
                return pools[child]
            elseif type(child) == "table" then
                return make_route(child, self)
            else
                error("invalid child given to route handler")
            end
        end
    }

    -- create a new map with route entries resolved.
    for mk, mv in pairs(set.map) do
        -- duck type: if this map is a route or a command map
        if mv["f"] == nil then
            dsay("command map:", mk)
            -- command map
            local cmap = {}
            for cmk, cmv in pairs(mv) do
                ctx._label = mk
                ctx._cmd = cmk
                local fgen = make_route(cmv, ctx)
                if fgen == nil then
                    error("route start handler did not return a generator")
                end
                cmap[cmk] = fgen
            end
            map[mk] = cmap
        else
            dsay("route:", mk)
            -- route function
            ctx._label = mk
            ctx._cmd = mcp.CMD_ANY_STORAGE
            local fgen = make_route(mv, ctx)
            if fgen == nil then
                error("route start handler did not return a generator")
            end
            map[mk] = fgen
        end
    end

    -- NOTE: we're directly passing the router configuration from the user
    -- into the function, but we could use indirection here to create
    -- convenience functions, default sets, etc.
    local conf = set.conf
    if set.default then
        ctx._label = "default"
        ctx._cmd = mcp.CMD_ANY_STORAGE
        conf.default = make_route(set.default, ctx)
    end

    conf.map = map
    return mcp.router_new(conf)
end

--
-- Loader functions
--

function mcp_config_pools()
    dsay("mcp_config_pools: start")
    config()
    -- create all necessary pool objects and prepare the configuration for
    -- passing on to workers
    -- Step 0) update global settings if requested
    if M.c_in.settings then
        settings_parse(M.c_in.settings)
    end

    -- Step 1) create pool objects
    local pools = pools_parse(M.c_in.pools)
    -- Step 2) prepare router descriptions
    local conf = routes_parse(M.c_in.routes, pools)
    -- Step 3) Reset global configuration
    dsay("mcp_config_pools: done")
    M = module_defaults()

    return conf
end

-- TODO: need a method to nil out a tag/route if unspecified. I think this
-- doesn't work from the API level.
function mcp_config_routes(c)
    local routes = c.r
    local pools = c.p
    dsay("mcp_config_routes: start")

    -- for each tagged route tree, swap pool names for pool objects, create
    -- the function generators, and the top level router object.
    for tag, set in pairs(routes) do
        dsay("building root for tag:", tag)
        local root = make_router(set, pools)
        if tag == "default" then
            dsay("attaching to proxy default tag")
            mcp.attach(mcp.CMD_ANY_STORAGE, root)
        else
            dsay("attaching to proxy for tag:", tag)
            mcp.attach(mcp.CMD_ANY_STORAGE, root, tag)
        end
    end
    dsay("mcp_config_routes: done")
end

--
-- Configuration level route handler definitions
--

-- route handlers on the configuration level are descriptors.
-- actual functions need to be generated later, once passed to workers
-- 1) validate arguments if possible
-- 2) return table with construction information:
--    - function name (can't use func references since we're crossing VMs!)
--    - config settings
function route_allfastest(t)
    -- TODO: validate arguments here.
    -- probably good to at least check that all children exist in the pools
    -- list.
    return { f = "route_allfastest_start", a = t }
end

function route_latest(t)
    return { f = "route_latest_start", a = t }
end

function route_split(t)
    return { f = "route_split_start", a = t }
end

--
-- Worker level Route handlers
--

-- route process:
-- 1) create funcgen object
-- 2) replace pool names with pool objects
-- 3) return function generator
-- the label and, if known, specific sub-command are passed in so they can be
-- used for log and stats functions
-- can possibly return a command-specific optimized function

-- so many layers of generation :(
local function route_allfastest_f(rctx, arg)
    local mode = mcp.WAIT_ANY
    dsay("generating an allfastest function")
    return function(r)
        rctx:enqueue(r, arg)
        local done = rctx:wait_cond(1, mode)
        for x=1, #arg do
            local res = rctx:res_any(arg[x])
            if res ~= nil then
                return res
            end
        end
    end
end

-- copy request to all children, but return first response
function route_allfastest_start(a, ctx)
    local fgen = mcp.funcgen_new()
    dsay("starting an allfastest handler")
    local o = {}
    for _, child in pairs(a.children) do
        local c = ctx:get_child(child)
        table.insert(o, fgen:new_handle(c))
    end

    fgen:ready({ a = o, n = ctx:label(), f = route_allfastest_f })
    return fgen
end

local function route_latest_f(rctx, arg)
    local limit = arg.limit
    local count = arg.count
    local t = arg.t

    return function(r)
        for i=1, limit do
            local r = rctx:enqueue_and_wait(r, t[i])
            if r:ok() then
                return r
            end
        end
    end
end

-- randomize the pool list
-- walk one at a time
function route_latest_start(a, ctx)
    local fgen = mcp.funcgen_new()
    local o = { t = {}, c = 0 }
    -- NOTE: if given a limit, we don't actually need handles for every pool.
    -- would be a nice small optimization to shuffle the list of children then
    -- only grab N entries.
    -- Not doing this _right now_ because I'm not confident children is an
    -- array or not.
    for _, child in pairs(children) do
        local c = ctx:get_child(child)
        table.insert(o.t, fgen:new_handle(c))
        o.c = o.c + 1
    end

    -- shuffle the handle list
    -- TODO: utils section with a shuffle func
    for i=#o.t, 2, -1 do
        local j = math.random(i)
        o.t[i], o.t[j] = o.t[j], o.t[i]
    end

    o.limit = a.failover_count

    fgen:ready({ a = o, n = ctx:label(), f = route_latest_f })
    return fgen
end

local function route_split_f(rctx, arg)
    local a = arg.child_a
    local b = arg.child_b
    dsay("generating a split function")

    return function(r)
        rctx:enqueue(r, b)
        return rctx:enqueue_and_wait(r, a)
    end
end

-- split route
-- mostly exists just to test recursive functions
-- should add options to do things like "copy N% of traffic from A to B"
function route_split_start(a, ctx)
    local fgen = mcp.funcgen_new()
    local o = {}
    dsay("starting a split route handler")
    o.child_a = fgen:new_handle(ctx:get_child(a.child_a))
    o.child_b = fgen:new_handle(ctx:get_child(a.child_b))
    fgen:ready({ a = o, n = ctx:label(), f = route_split_f })

    return fgen
end

-- TODO: failover route

