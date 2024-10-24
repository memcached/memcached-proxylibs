-- This library should originate from:
-- https://github.com/memcached/memcached-proxylibs/tree/main/lib/routelib

local STATS_MAX <const> = 1024

local CMD_ID_MAP <const> = {
    mg = mcp.CMD_MG,
    ms = mcp.CMD_MS,
    md = mcp.CMD_MD,
    mn = mcp.CMD_MN,
    ma = mcp.CMD_MA,
    me = mcp.CMD_ME,
    get = mcp.CMD_GET,
    gat = mcp.CMD_GAT,
    set = mcp.CMD_SET,
    add = mcp.CMD_ADD,
    cas = mcp.CMD_CAS,
    gets = mcp.CMD_GETS,
    gats = mcp.CMD_GATS,
    incr = mcp.CMD_INCR,
    decr = mcp.CMD_DECR,
    touch = mcp.CMD_TOUCH,
    append = mcp.CMD_APPEND,
    delete = mcp.CMD_DELETE,
    replace = mcp.CMD_REPLACE,
    prepend = mcp.CMD_PREPEND,
    all = mcp.CMD_ANY_STORAGE,
}

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

-- classes for typing.
-- NOTE: metatable classes do not cross VM's, so they may only be used in the
-- same VM they were assigned (pools or routes)
local CommandMap = {}
local RouteConf = {}
local BuiltPoolSet = {} -- processed pool set.

-- TODO: this can/should be seeded only during config_pools
local function module_defaults(old)
    local stats = {
        map = {},
        freelist = {},
        next_id = 1,
    }
    if old and old.stats then
        stats = old.stats
        stats.map = {} -- but reset the main map
    end
    return {
        c_in = {
            pools = {},
            routes = {},
        },
        stats = stats,
    }
end
__M = module_defaults(__M)
local M = __M

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
    -- merge the list so pools{} can be called multiple times
    local p = M.c_in.pools
    for k, v in pairs(a) do
        p[k] = v
    end
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
    M.c_in.local_zone = zone
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

function cmdmap(t)
    local remap = {}
    for k, v in pairs(t) do
        local nk = k
        if type(k) == "string" then
            nk = CMD_ID_MAP[k]
            if nk == nil then
                error("unknown command in cmdmap: " .. k)
            end
        elseif type(k) ~= "number" then
            error("cmdmap keys must all be strings or id numbers")
        end
        remap[nk] = v
    end
    return setmetatable(remap, CommandMap)
end

--
-- User/Pool configuration thread functions
--

-- TODO: this wrapper func will allow for loading json vs lua or special error
-- handling. Presently it does little.
local function load_userconfig(file)
    if file == nil then
        error("must provide config file via -o proxy_arg")
    end
    dofile(file)
end

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
            -- alias for convenience.
            mcp.active_req_limit(v)
        end,
        ["active_req_limit"] = function(v)
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
        ["backend_use_tls"] = function(v)
            mcp.backend_use_tls(v)
            mcp.init_tls()
        end,
    }

    -- TODO: throw error if setting unknown
    for setting, value in pairs(a) do
        local func = setters[setting]
        if func ~= nil then
            say("changing global setting:", setting, "to:", value)
            func(value)
        elseif setting == "pool_options" then
            M.pool_options = value
        end
    end
end

local function make_backend(name, host, o)
    local b = {port = "11211"}
    -- override per-backend options if requested
    if o ~= nil then
        for k, v in pairs(o) do
            dsay("backend using override: ", k, v)
            b[k] = v
        end
    end

    if type(host) == "table" then
        for k, v in pairs(host) do
            b[k] = v
        end

        if b.host == nil then
            error("host missing from backend entry")
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
            error(host .. " is an invalid backend string. must be at least host:port")
        end
    end

    -- create a default label out of the host:port if none directly supplied
    if b.label == nil then
        b.label = b.host .. ":" .. b.port
    end

    if b.tls then
        -- okay to call repeatedly
        mcp.init_tls()
    end

    return mcp.backend(b)
end

local function pools_make(conf)
    local popts = {}
    -- seed global overrides
    if M.pool_options then
        for k, v in pairs(M.pool_options) do
            dsay("pool using global override:", k, v)
            popts[k] = v
        end
    end
    -- apply local overrides
    if conf.options then
        for k, v in pairs(conf.options) do
            dsay("pool using local override:", k, v)
            popts[k] = v
        end
    end

    local bopts = conf.backend_options
    local s = {}
    -- TODO: some convenience functions for asserting?
    -- die more gracefully if backend list missing
    for _, backend in pairs(conf.backends) do
        table.insert(s, make_backend(name, backend, bopts))
    end

    return mcp.pool(s, popts)
end

-- converts a table describing pool objects into a new table of real pool
-- objects.
local function pools_parse(a)
    local pools = {}
    for name, conf in pairs(a) do
        -- Check if conf is a pool set or single pool.
        -- Add result to top level pools[name] either way.
        if string.find(name, "^set_") ~= nil then
            dsay("parsing a PoolSet")
            local pset = {}
            for sname, sconf in pairs(conf) do
                dsay("making pool:", sname, "\n", dump(sconf))
                pset[sname] = pools_make(sconf)
            end
            pools[name] = setmetatable(pset, BuiltPoolSet)
        else
            dsay("making pool:", name, "\n", dump(conf))
            pools[name] = pools_make(conf)
        end
    end

    return pools
end

-- 1) walk keys looking for child*, recurse if RouteConfs are found
-- 2) resolve and call route.f
-- 3) return the response directly. the _conf() function should handle
-- anything that needs global context in the mcp_config_pools stage.
local function configure_route(r, ctx)
    local route = r.a

    -- TODO: think we can take the built pools list and actually validate here
    -- that the children all index properly.
    -- this would ensure errors happen during the config stage instead of the
    -- worker load stage.

    -- first, recurse and resolve any children.
    for k, v in pairs(route) do
        -- try to not accidentally coerce a non-string key into a string!
        if type(k) == "string" and string.find(k, "^child") ~= nil then
            dsay("Checking child for route:", k)
            if type(v) == "table" then
                if (getmetatable(v) == RouteConf) then
                    route[k] = configure_route(v, ctx)
                else
                    -- it is a table of children.
                    for ck, cv in pairs(v) do
                        if type(cv) == "table" and (getmetatable(cv) == RouteConf) then
                            v[ck] = configure_route(cv, ctx)
                        end
                    end
                end
            end -- if "table"
        end -- if "child"
    end -- for(route)

    local f = _G["route_" .. r.f .. "_conf"]
    local ret = f(route, ctx)
    if ret == nil then
        error("route configure function failed to return anything: " .. r.f)
    end
    -- decorate the response with something that can be duck-typed cross-VM.
    return { f = r.f, _rlib_route = true, a = ret }
end

-- route*() config functions can call this to get ids to use for stats counters
local function stats_get_id(name)
    local st = M.stats

    -- already have an ID for this stat name.
    if st.map[name] then
        return st.map[name]
    end

    -- same name was seen previously, refresh it for the new map and return.
    if st.old_map and st.old_map[name] then
        local id = st.old_map[name]
        st.map[name] = id
        return id
    end

    -- pop a free id from the list
    if #st.freelist ~= 0 then
        local id = table.remove(st.freelist)
        dsay("stats reusing freelisted id:", name, id)
        mcp.add_stat(id, name)
        st.map[name] = id
        return id
    end

    -- iterate the ID's
    if st.next_id < STATS_MAX then
        local id = st.next_id
        st.next_id = id + 1
        mcp.add_stat(id, name)
        st.map[name] = id
        return id
    end

    if #st.freelist == 0 then
        error("max number of stat counters reached:", STATS_MAX)
    end
end

-- 1) walk the tree
-- 2) for each entrypoint, look for child_* keys
--  - check if metatable is RouteConf or not
-- 3) for each child entry with a table, run configure_route()
-- 4) resolve and run the .f function
-- 5) take the result of that and pack it:
--    { f = "route_name_start", a = t }
-- routes that have stats should assign stats or global overrides in this conf
-- stage.
-- NOTE: we are editing the entries in-place
local function configure_router(set, pools, c_in)
    -- create ctx object to hold label + command
    local ctx = {
        label = function(self)
            return self._label
        end,
        cmd = function(self)
            return self._cmd
        end,
        pool = function(self, name)
            return pools[name]
        end,
        local_zone = function(self)
            return c_in.local_zone
        end,
        get_stats_id = function(self, name)
            local id = stats_get_id(name)
            dsay("stats get id:", name, id)
            return id
        end
    }

    if set.map then
        -- a prefix map
        for mk, mv in pairs(set.map) do
            dsay("examining route map entry:", mk)
            ctx._label = mk
            ctx._cmd = mcp.CMD_ANY_STORAGE
            if (getmetatable(mv) == CommandMap) then
                dsay("parsing a CommandMap entry")
                for cmk, cmv in pairs(mv) do
                    if (getmetatable(cmv) ~= RouteConf) then
                        error("bad entry in route table map")
                    end
                    -- replace the table entry
                    ctx._cmd = cmk
                    mv[cmk] = configure_route(cmv, ctx)
                end
            elseif (getmetatable(mv) == RouteConf) then
                dsay("parsing a RouteConf entry")
                set.map[mk] = configure_route(mv, ctx)
            else
                error("unknown route map type")
            end
        end
    end

    if set.cmap then
        local cmap_new = {}
        -- a command map
        ctx._label = "cmdmap"
        for cmk, cmv in pairs(set.cmap) do
            local nk = cmk
            if type(cmk) == "string" then
                nk = CMD_ID_MAP[cmk]
            end
            if nk == nil then
                error("unknown command in cmdmap: " .. cmk)
            end
            ctx._cmd = nk
            cmap_new[nk] = configure_route(cmv, ctx)
        end
        set.cmap = cmap_new
    end

    if set.default then
        ctx._label = "default"
        ctx._cmd = mcp.CMD_ANY_STORAGE
        set.default = configure_route(set.default, ctx)
    end
end

-- TODO: allow a way to override which attach() happens for a router.
-- by default we just do CMD_ANY_STORAGE
-- NOTE: this function should be used for vadliating/preparsing the router
-- config and routes sections.
local function routes_parse(c_in, pools)
    local routes = c_in.routes

    local found = false
    for tag, set in pairs(routes) do
        if set.map == nil and set.cmap == nil then
            if set.default then
                -- FIXME: need an upstream fix to allow default-only routers
                set.cmap = {}
            else
                error("must pass map or cmap to a router")
            end
        end

        configure_router(set, pools, c_in)
        found = true
    end

    if not found then
        error("missing routes{} section")
    end

    return { r = routes, p = pools }
end

-- _after_ pre-processing all route handlers, check if any existing stats
-- counters are unused.
-- if, across reloads, some stats counters disappear, we can reuse the IDs
-- NOTE that this is a race, we cannot reuse an ID that was freed during the
-- current load as they might be used by in-flight requests.
-- We assume:
-- 1) there's going to be some amount of time between reloads, usually more
-- than enough time for in-flight requests to finish.
-- 2) if this is ever not true, the harm is both rare and minimal.
-- 3) this is probably improvable in the future once all the pre-API2 code is
-- dropped and we can attach stats ID's to rctx's and do this
-- recycling work internally in the proxy.
local function stats_turnover()
    local st = M.stats

    if st.old_map then
        for k, v in pairs(st.old_map) do
            --dsay("checking old stats map entry:", k, v)
            if st.map[k] == nil then
                dsay("stats entry no longer used, freelisting id:", k, st.old_map[k])
                -- key from old map not in new map, freelist the ID
                table.insert(st.freelist, st.old_map[k])
                -- blank the name to remove it from stats output
                mcp.add_stat(st.old_map[k], "")
            end
        end
    end

    st.old_map = st.map
end

--
-- Worker thread configuration functions
--

-- re-wrap the arguments to create the function generator within a worker
-- thread.
local function make_route(arg, ctx)
    dsay("generating a route:", ctx:label(), ctx:cmd())
    -- walk the passed arg table for any children to process.
    for k, v in pairs(arg.a) do
        if type(k) == "string" and string.find(k, "^child") ~= nil then
            if type(v) == "table" then
                if v._rlib_route then
                    arg.a[k] = make_route(v, ctx)
                else
                    -- child = { route{}, "bar"}
                    -- child = { foo = bar, baz = route{} }
                    -- walk _this_ table looking for routes
                    for ck, cv in pairs(v) do
                        -- TODO: else if table throw error?
                        -- should limit child recursion.
                        if type(cv) == "table" and cv._rlib_route then
                            v[ck] = make_route(cv, ctx)
                        elseif type(cv) == "string" then
                            v[ck] = ctx:get_child(cv)
                        end
                    end -- for child table do
                end -- if _rlib_route
            elseif type(v) == "string" then
                arg.a[k] = ctx:get_child(v)
            end -- if "table"
        end
    end

    -- resolve the named function to a real function from global
    local f = _G["route_" .. arg.f .. "_start"]
    -- create and return the funcgen object
    local fgen = mcp.funcgen_new()
    f(arg.a, ctx, fgen)
    -- FIXME: can we check if an fgen is marked as ready?
    return fgen
end

-- create and return a full router object.
-- 1) walk the input route set, copying into a new map
-- 2) execute any route handlers found
-- 3) route handler resolves children into (pool/fgen) and returns fgen
-- 4) create the root route handler and return
-- NOTE: we do an unfortunate duck typing of a map entry, as metatables can't
-- cross the cross-VM barrier so we can't type the route handlers. Map entries
-- can have sub-maps and they'll both just look like tables.
local function make_router(set, pools)
    local map = {}
    local cmap = {}
    dsay("making a new router")

    local ctx = {
        label = function(self)
            return self._label
        end,
        cmd = function(self)
            return self._cmd
        end,
        -- TODO: while we check and throw errors here, we should instead
        -- prefer to check this stuff on the mcp_config_pools side to avoid
        -- throwing unrecoverable errors.
        get_child = function(self, child)
            if type(child) ~= "string" then
                error("invalid child given to route handler: " .. type(child))
            end

            -- if "^set_words_etc" then special parse
            -- if "^set_" then special parse
            -- else match against pools directly
            local set, name = string.match(child, "^(set_%w+)_(.+)")
            if set and name then
                if pools[set] == nil then
                    error("no pool set matching: " .. set)
                end
                if pools[set][name] == nil then
                    error("no pool set zone matching: " .. child)
                end
                -- querying within a pool set
                return pools[set][name]
            end

            -- still starts with set_, so we want to copy out that table.
            if string.find(child, "^set_") ~= nil then
                if pools[child] == nil then
                    error("no pool set matching: " .. child)
                end
                local t = {}
                for k, v in pairs(pools[child]) do
                    t[k] = v
                end
                return t
            end

            -- else we're directly querying a pool.
            if pools[child] == nil then
                error("no pool matching: " .. child)
            end
            return pools[child]
        end
    }

    -- NOTE: we're directly passing the router configuration from the user
    -- into the function, but we could use indirection here to create
    -- convenience functions, default sets, etc.
    local conf = set.conf
    if set.default then
        ctx._label = "default"
        ctx._cmd = mcp.CMD_ANY_STORAGE
        conf.default = make_route(set.default, ctx)
    end

    if set.map then
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
        conf.map = map
    end

    if set.cmap then
        -- we're only routing based on the command, no prefix strings
        ctx._label = "cmdmap"
        for cmk, cmv in pairs(set.cmap) do
            ctx._cmd = cmk
            local fgen = make_route(cmv, ctx)
            if fgen == nil then
                error("route start handler did not return a generator")
            end
            cmap[cmk] = fgen
        end
        conf.cmap = cmap
    end

    return mcp.router_new(conf)
end

--
-- ----------------
-- Loader functions
-- ----------------
--
-- Contains the top level mcp_config_pools() and mcp_config_routes() handlers
-- mcp_config_pools executes from the configuration thread
-- mcp_config_routes executes from each worker thread

function mcp_config_pools()
    load_userconfig(mcp.start_arg)
    dsay("=== mcp_config_pools: start ===")
    -- create all necessary pool objects and prepare the configuration for
    -- passing on to workers
    -- Step 0) update global settings if requested
    if M.c_in.settings then
        settings_parse(M.c_in.settings)
    end

    -- Step 1) create pool objects
    local pools = pools_parse(M.c_in.pools)
    -- Step 2) prepare router descriptions
    local conf = routes_parse(M.c_in, pools)
    -- Step 3) Reset global configuration
    stats_turnover()
    dsay("=== mcp_config_pools: done ===")

    -- let say/dsay work in the worker reload stage
    conf.is_verbose = M.is_verbose
    conf.is_debug = M.is_debug

    M = module_defaults(M)

    return conf
end

-- TODO: need a method to nil out a tag/route if unspecified. I think this
-- doesn't work from the API level.
-- NOTES:
-- 1) the "router" object is prefix/key matching only. for command maps we
-- directly set the handlers against mcp.attach()
-- 2) the system does technically support:
--    command map -> prefix map -> route|command map
--    ... but we don't allow this with this library, not right now.
-- 3) it's possible to support 2 by doing even more pre-processing, so if
-- there's demand I don't believe anything here would make it impossible.
function mcp_config_routes(c)
    local routes = c.r
    local pools = c.p
    M.is_verbose = c.is_verbose
    M.is_debug = c.is_debug

    dsay("=== mcp_config_routes: start ===")

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
    dsay("=== mcp_config_routes: done ===")
end

--
-- ---------------------------------------------
-- Configuration level route handler definitions
-- ---------------------------------------------
--
-- Functions run from the configuration thread which pre-parse the provided
-- route objects and do any required decoration or recursion

-- route handlers on the configuration level are descriptors.
-- actual functions need to be generated later, once passed to workers
-- 1) validate arguments if possible
-- 2) return table with construction information:
--    - function name (can't use func references since we're crossing VMs!)
--    - config settings


-- register global wrapper functions for collecting user input
-- give the configuration a type so we can easily pick them out while walking
-- the route map later.
function register_route_handlers(t)
    for _, name in pairs(t) do
        _G["route_" .. name] = function(t)
            return setmetatable({ f = name, a = t }, RouteConf)
        end
    end
end

--
-- ---------------------------
-- Route handler definitions
-- ---------------------------
--
-- _conf() functions executed in config thread
-- _start() functions executed in worker threads

-- route process:
-- 1) create funcgen object
-- 2) replace pool names with pool objects
-- 3) return function generator
-- the label and, if known, specific sub-command are passed in so they can be
-- used for log and stats functions
-- can possibly return a command-specific optimized function

--
-- route_allfastest start
--

function route_allfastest_conf(t)
    return t
end

-- so many layers of generation :(
local function route_allfastest_f(rctx, arg)
    local mode = mcp.WAIT_OK
    dsay("generating an allfastest function")
    return function(r)
        rctx:enqueue(r, arg)
        local done = rctx:wait_cond(1, mode)
        local final = nil
        -- return first non-error.
        for x=1, #arg do
            local res, mode = rctx:result(arg[x])
            if mode == mcp.RES_OK or mode == mcp.RES_GOOD then
                return res
            else
                final = res
            end
        end
        -- return an error of the final result
        return final
    end
end

-- copy request to all children, but return first response
function route_allfastest_start(a, ctx, fgen)
    dsay("starting an allfastest handler")
    local o = {}
    for _, child in pairs(a.children) do
        table.insert(o, fgen:new_handle(child))
    end

    fgen:ready({ a = o, n = ctx:label(), f = route_allfastest_f })
end

--
-- route_allfastest end
--

--
-- route_failover start
--

function route_failover_conf(t, ctx)
    if t.stats then
        local name = ctx:label() .. "_retries"
        if t.stats_name then
            name = t.stats_name .. "_retries"
        end
        t.stats_id = ctx:get_stats_id(name)
    end
    return t
end

local function route_failover_f(rctx, arg)
    local limit = arg.limit
    local t = arg.t
    local miss = arg.miss
    local s = nil
    local s_id = 0
    if arg.stats_id then
        s = mcp.stat
        s_id = arg.stats_id
    end

    return function(r)
        local res = nil
        for i=1, limit do
            res = rctx:enqueue_and_wait(r, t[i])
            if i > 1 and s then
                -- increment the retries counter
                s(s_id, 1)
            end
            if (miss == true and res:hit()) or (miss == false and res:ok()) then
                return res
            end
        end

        -- didn't get what we want, return the final one.
        return res
    end
end

function route_failover_start(a, ctx, fgen)
    local o = { t = {}, c = 0 }
    -- NOTE: if given a limit, we don't actually need handles for every pool.
    -- would be a nice small optimization to shuffle the list of children then
    -- only grab N entries.
    -- Not doing this _right now_ because I'm not confident children is an
    -- array or not.
    for _, child in pairs(a.children) do
        table.insert(o.t, fgen:new_handle(child))
        o.c = o.c + 1
    end

    if a.shuffle then
        -- shuffle the handle list
        for i=#o.t, 2, -1 do
            local j = math.random(i)
            o.t[i], o.t[j] = o.t[j], o.t[i]
        end
    end

    o.miss = a.miss
    local hcount = #(o["t"])
    if a.failover_count then
        if a.failover_count > hcount then
            o.limit = hcount
        else
            o.limit = a.failover_count
        end
    else
        o.limit = hcount
    end
    o.stats_id = a.stats_id

    fgen:ready({ a = o, n = ctx:label(), f = route_failover_f })
end

--
-- route_failover end
--

--
-- route_split start
--

function route_split_conf(t)
    return t
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
function route_split_start(a, ctx, fgen)
    local o = {}
    dsay("starting a split route handler")
    o.child_a = fgen:new_handle(a.child_a)
    o.child_b = fgen:new_handle(a.child_b)
    fgen:ready({ a = o, n = ctx:label(), f = route_split_f })
end

--
-- route_split end
--

--
-- route_direct start
--

function route_direct_conf(t)
    return t
end

local function route_direct_f(rctx, handle)
    return function(r)
        return rctx:enqueue_and_wait(r, handle)
    end
end

function route_direct_start(a, ctx, fgen)
    local handle = fgen:new_handle(a.child)
    fgen:ready({ a = handle, n = ctx:label(), f = route_direct_f })
end

--
-- route_direct end
--

--
-- route_allsync start
--

function route_allsync_conf(t)
    return t
end

local function route_allsync_f(rctx, arg)
    -- just an alias for clarity.
    local handles = arg

    return function(r)
        rctx:enqueue(r, handles)
        rctx:wait_cond(#handles, mcp.WAIT_ANY)
        local final = nil
        for x=1, #handles do
            local res, mode = rctx:result(handles[x])
            -- got something not okay or good
            if mode == mcp.RES_ANY then
                final = res
                break
            else
                final = res
            end
        end
        -- return an error or last result.
        return final
    end
end

function route_allsync_start(a, ctx, fgen)
    dsay("starting an allsync route handler")
    local o = {}
    for _, v in pairs(a.children) do
        table.insert(o, fgen:new_handle(v))
    end

    fgen:ready({
        a = o,
        n = ctx:label(),
        f = route_allsync_f,
    })
end

--
-- route_allsync end
--

--
-- route_zfailover start
--

function route_zfailover_conf(t, ctx)
    if t.stats then
        local name = ctx:label() .. "_retries"
        if t.stats_name then
            name = t.stats_name .. "_retries"
        end
        t.stats_id = ctx:get_stats_id(name)
    end
    if t.failover_count == nil then
        t.failover_count = #t.children
    end

    -- Since we're a "zone minded" route handler, we check for a globally
    -- configured zone.
    if t.local_zone == nil then
        t.local_zone = ctx:local_zone()
    end

    if t.local_zone == nil then
        error("route_zfailover must have a local_zone defined")
    end

    return t
end

local function route_zfailover_f(rctx, arg)
    local limit = arg.limit
    local t = arg.t
    local miss = arg.miss
    local s_id = arg.stats_id
    local s = mcp.stat

    -- first, find out local child
    local near = t[arg.local_zone]
    -- now, gather our non-local-zones
    local far = {}
    local farcount = 0
    for k, v in pairs(t) do
        if k ~= arg.local_zone then
            table.insert(far, v)
            farcount = farcount + 1
        end
    end
    local mode = mcp.WAIT_FASTGOOD

    return function(r)
        local res = rctx:enqueue_and_wait(r, near)
        if res:hit() or (miss == false and res:ok()) then
            return res
        end

        if stat then
            s(s_id, 1)
        end
        -- didn't get what we want to begin with, fan out.
        rctx:enqueue(r, far)
        rctx:wait_cond(farcount, mode)

        -- look for a good result, else any OK, else any result.
        local final = nil
        for x=1, #far do
            local res, tag = rctx:result(far[x])
            if tag == mcp.RES_GOOD then
                return res
            elseif tag == mcp.RES_OK then
                final = res
            elseif final ~= nil then
                final = res
            end
        end
        return final
    end
end

function route_zfailover_start(a, ctx, fgen)
    local o = { t = {}, c = 0 }

    -- our list of children is actually a map, so we build this differently
    -- than the failover route.
    for k, child in pairs(a.children) do
        o.t[k] = fgen:new_handle(child)
        o.c = o.c + 1
    end

    o.miss = a.miss
    o.limit = a.failover_count
    o.stats_id = a.stats_id
    o.local_zone = a.local_zone

    fgen:ready({ a = o, n = ctx:label(), f = route_zfailover_f })
end

--
-- route_zfailover end
--

--
-- route_ttl start
--

function route_ttl_conf(t, ctx)
    -- just a TTL arg?
    return t
end

-- NOTE: this could be written more directly by duplicating the function code:
-- - with a specific command, directly return the function instead of
-- generating both and dropping one
-- - in the "unknown command" section, just use if's in the main code for the
-- flag/token command and a single rctx:enqueue call for return.
--
-- We do this as a programming exercise. If the functions were much larger it
-- would make more sense.
local function route_ttl_f(rctx, arg)
    local ttl = arg.ttl
    local cmd = arg.cmd
    local h = arg.handle

    -- meta set has TTL in a flag
    local cmd_ms = function(r)
        r:flag_set('T', ttl)
        return rctx:enqueue_and_wait(r, h)
    end
    -- SET/ADD/CAS have TTL in the 4th token
    local cmd_txt = function(r)
        r:token(4, ttl)
        return rctx:enqueue_and_wait(r, h)
    end

    -- command known ahead of time, return specialized function
    if cmd then
        if cmd == mcp.CMD_MS then
            return cmd_ms
        elseif cmd == mcp.CMD_SET then
            return cmd_txt
        elseif cmd == mcp.CMD_ADD then
            return cmd_txt
        elseif cmd == mcp.CMD_CAS then
            return cmd_txt
        else
            error("invalid command for route_ttl")
        end
    end

    -- command isn't known ahead of time, find the function at runtime.
    return function(r)
        local cmd = r:command()
        -- for a small number of options should be more efficient than a table
        if cmd == mcp.CMD_MS then
            return cmd_ms(r)
        elseif cmd == mcp.CMD_SET then
            return cmd_txt(r)
        elseif cmd == mcp.CMD_ADD then
            return cmd_txt(r)
        elseif cmd == mcp.CMD_CAS then
            return cmd_txt(r)
        else
            return "SERVER_ERROR invalid command for route_ttl\r\n"
        end
    end
end

function route_ttl_start(a, ctx, fgen)
    -- if ctx:cmd() == mcp.CMD_ANY_STORAGE do etc else etc
    local o = { ttl = a.ttl }
    o.handle = fgen:new_handle(a.child)
    if ctx:cmd() ~= mcp.CMD_ANY_STORAGE then
        o.cmd = ctx:cmd()
    end

    fgen:ready({
        a = o,
        n = ctx:label(),
        f = route_ttl_f,
    })
end

--
-- route_ttl end
--


register_route_handlers({
    "failover",
    "allfastest",
    "allsync",
    "split",
    "direct",
    "zfailover",
    "ttl",
})
