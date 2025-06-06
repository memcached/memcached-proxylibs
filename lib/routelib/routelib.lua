-- This library should originate from:
-- https://github.com/memcached/memcached-proxylibs/tree/main/lib/routelib
--
-- GUIDE:
-- - functions near the top section under "User interface" are called from the
-- user config file
-- - functions prefixed with main_* are routelib internal functions called in
-- the configuration thread
-- - functions prefixed with worker_* are routelib internal functions called
-- on worker threads during the route config phase.

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

local function dump_pretty(o, indent)
    indent = indent or ""
    if type(o) == 'table' then
        local s = '{\n'
        local next_indent = indent .. "    "
        for k,v in pairs(o) do
            if type(k) ~= 'number' then k = '"'..k..'"' end
            s = s .. next_indent .. '['..k..'] = ' .. dump_pretty(v, next_indent) .. ',\n'
        end
        return s .. indent .. '}'
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
local function main_module_defaults(old)
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
__M = main_module_defaults(__M)
local M = __M

--
-- User interface functions
--
-- These are only called from the user config file.
--

function settings(a)
    if M.is_debug then
        print("settings:")
        print(dump(a))
    end
    M.c_in.settings = a
end

function pools(a)
    -- merge the list so pools{} can be called multiple times
    local p = M.c_in.pools
    for k, v in pairs(a) do
        p[k] = v
    end
end

function routes(a)
    if a["conf"] == nil then
        a["conf"] = {}
    end

    if a.tag then
        M.c_in.routes[a.tag] = a
    else
        M.c_in.routes["default"] = a
    end
end

function local_zone(zone)
    if zone then
        dsay("=== local zone: ", zone)
        M.c_in.local_zone = zone
    else
        return M.c_in.local_zone
    end
end

function local_zone_from_env(envname)
    local zone = os.getenv(envname)
    if zone == nil then
        error("failed to get local zone from environment variable: " .. envname)
    end
    dsay("=== local zone: ", zone)
    M.c_in.local_zone = zone
end

function local_zone_from_file(filename)
    local f, err, errno = io.open(filename, "r")
    if f == nil then
        error("failure while opening local zone file: " .. filename .. " " .. err)
    end
    local zone = f:read("l")
    f:close()
    if zone == nil then
        error("failed to read local zone from file: " .. filename)
    end
    dsay("=== local zone: ", zone)
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
-- These functions, prefixed with main_ execute centrally in the configuration
-- thread.
--

-- TODO: this wrapper func will allow for loading json vs lua or special error
-- handling. Presently it does little.
local function main_load_userconfig(file)
    if file == nil then
        error("must provide config file via -o proxy_arg")
    end
    dofile(file)
end

-- TODO: remember values and add to verbose print if changed on reload
local function main_settings_parse(a)
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
        elseif setting == "backend_options" then
            M.backend_options = value
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

local function main_pools_make(conf)
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

    local bopts = {}
    -- seed global overrides
    if M.backend_options then
        for k, v in pairs(M.backend_options) do
            dsay("backend option using global override:", k, v)
            bopts[k] = v
        end
    end
    if conf.backend_options then
        for k, v in pairs(conf.backend_options) do
            dsay("backend option using local override:", k, v)
            bopts[k] = v
        end
    end

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
local function main_pools_parse(a)
    local pools = {}
    for name, conf in pairs(a) do
        if name == "internal" then
            error("pool name 'internal' is reserved, please use another name")
        end
        -- Check if conf is a pool set or single pool.
        -- Add result to top level pools[name] either way.
        if string.find(name, "^set_") ~= nil then
            dsay("parsing a PoolSet")
            local pset = {}
            for sname, sconf in pairs(conf) do
                dsay("making pool:", sname, "\n", dump(sconf))
                pset[sname] = main_pools_make(sconf)
            end
            pools[name] = setmetatable(pset, BuiltPoolSet)
        else
            dsay("making pool:", name, "\n", dump(conf))
            pools[name] = main_pools_make(conf)
        end
    end

    return pools
end

-- Magic function for turning a route handler into a set of route handlers,
-- based on a pool set or list of route handlers.
local function configure_route_wrap(r, ctx)
    local route = r.a
    -- wrap the child data into a new table
    local children = route.child_wrap
    -- recursion case: another child needs to first transform into children
    if (getmetatable(children) == RouteConf) then
        dsay("running possible recursive child_wrap child")
        children = configure_route(children, ctx)
        if (children._rlib_route) then
            error("child_wrap argument must be a list of children, not a single route")
        end
    end

    -- IF "set_" then expand to array of "set_name_key" strings
    -- set child to textual name and continue
    children = ctx:expand_children(children)
    local t = {}
    for k, v in pairs(children) do
        local rn = {}
        -- shallow copy of the route handler.
        for k, v in pairs(r) do
            rn[k] = v
        end
        rn.a = {}
        -- then shallow copy of the argument list
        for k, v in pairs(r.a) do
            rn.a[k] = v
        end

        -- reuse the same route config, but once for each child.
        rn.a.child = v
        rn.a.child_wrap = nil
        t[k] = configure_route(rn, ctx)
    end

    return t
end

-- 1) walk keys looking for child*, recurse if RouteConfs are found
-- 2) resolve and call route.f
-- 3) return the response directly. the _conf() function should handle
-- anything that needs global context in the mcp_config_pools stage.
-- FIXME: non-local func because configure_route_wrap can't find it
function configure_route(r, ctx)
    local route = r.a

    -- first, recurse and resolve any children.
    -- also check them for validity once they should be pools
    for k, v in pairs(route) do
        -- try to not accidentally coerce a non-string key into a string!
        if type(k) == "string" and string.find(k, "^child") ~= nil then
            if k == "child_wrap" then
                dsay("Wrapping route around multiple children:", k)
                return configure_route_wrap(r, ctx)
            end
            dsay("Checking child for route:", k)
            if type(v) == "table" then
                if (getmetatable(v) == RouteConf) then
                    route[k] = configure_route(v, ctx)
                elseif type(v) == "table" and v._rlib_route then
                    -- already processed
                else
                    -- it is a table of children.
                    for ck, cv in pairs(v) do
                        if type(cv) == "table" and (getmetatable(cv) == RouteConf) then
                            v[ck] = configure_route(cv, ctx)
                        else
                            ctx:check_child(cv)
                        end
                    end
                end
            else -- if "table"
                ctx:check_child(v)
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
local function main_configure_router(set, pools, c_in)
    -- create ctx object to hold label + command
    local ctx = {
        label = function(self)
            return self._label
        end,
        cmd = function(self)
            return self._cmd
        end,
        check_child = function(self, child)
            -- ensure this child is a string of a valid pool
            if type(child) ~= "string" then
                error("checking child: invalid child given to route handler: " .. type(child))
            end

            -- shortcut for the magic internal pool.
            if child == "internal" then
                return true
            end

            local set, name = string.match(child, "^(set_%w+)_(.+)")
            if set and name then
                if pools[set] ~= nil then
                    if pools[set][name] ~= nil then
                        -- querying within a pool set
                        return true
                    end
                end
            end

            -- still starts with set_, so we want to copy out that table.
            if string.find(child, "^set_") ~= nil then
                if pools[child] == nil then
                    error("checking child: no pool set matching: " .. child)
                end
                return true
            end

            -- else we're directly querying a pool.
            if pools[child] == nil then
                error("checking child: no pool matching: " .. child)
            end

            return true
        end,
        expand_children = function(self, children)
            if type(children) == "table" then
                -- nothing to do.
                return children
            elseif type(children) == "string" then
                if string.find(children, "^set_") ~= nil then
                    if pools[children] == nil then
                        error("checking child: no pool set matching: " .. children)
                    else
                        local t = {}
                        for k, v in pairs(pools[children]) do
                            t[k] = children .. "_" .. k
                        end
                        return t
                    end
                else
                    error("child_wrap expected a pool set but received single pool: " .. children)
                end
            end
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
local function main_routes_parse(c_in, pools)
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

        main_configure_router(set, pools, c_in)
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
local function main_stats_turnover()
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
-- These functions only run in the worker threads.
--

-- re-wrap the arguments to create the function generator within a worker
-- thread.
local function worker_make_route(arg, ctx)
    dsay("generating a route:", ctx:label(), ctx:cmd())
    -- walk the passed arg table for any children to process.
    for k, v in pairs(arg.a) do
        if type(k) == "string" and string.find(k, "^child") ~= nil then
            if type(v) == "table" then
                if v._rlib_route then
                    arg.a[k] = worker_make_route(v, ctx)
                else
                    -- child = { route{}, "bar"}
                    -- child = { foo = bar, baz = route{} }
                    -- walk _this_ table looking for routes
                    for ck, cv in pairs(v) do
                        -- TODO: else if table throw error?
                        -- should limit child recursion.
                        if type(cv) == "table" and cv._rlib_route then
                            v[ck] = worker_make_route(cv, ctx)
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
local function worker_make_router(set, pools)
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
        -- NOTE: while we check and throw errors here, this is after checking
        -- for the same errors during the pools config phase. Thus if these
        -- errors fire something has gone seriously wrong with routelib
        -- itself.
        get_child = function(self, child)
            if type(child) ~= "string" then
                error("invalid child given to route handler: " .. type(child))
            end

            -- shortcut for the process-internal cache.
            if child == "internal" then
                return mcp.internal_handler
            end

            -- if "^set_words_etc" then special parse
            -- if "^set_" then special parse
            -- else match against pools directly
            local set, name = string.match(child, "^(set_%w+)_(.+)")
            if set and name then
                if pools[set] ~= nil then
                    if pools[set][name] ~= nil then
                        -- querying within a pool set
                        return pools[set][name]
                    end
                end
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
        conf.default = worker_make_route(set.default, ctx)
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
                    local fgen = worker_make_route(cmv, ctx)
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
                local fgen = worker_make_route(mv, ctx)
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
            local fgen = worker_make_route(cmv, ctx)
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

function main_config_dump_state(c)
    if M.is_debug then
        dsay("======== GLOBAL SETTINGS CONFIG ========")
        dsay(dump_pretty(c.settings))

        dsay("======== POOLS CONFIG ========")
        dsay(dump_pretty(c.pools))

        dsay("======== ROUTES CONFIG ========")
        dsay(dump_pretty(c.routes))
    end
end

function mcp_config_pools()
    main_load_userconfig(mcp.start_arg)
    dsay("=== mcp_config_pools: start ===")
    -- create all necessary pool objects and prepare the configuration for
    -- passing on to workers
    -- Step 0) update global settings if requested
    if M.c_in.settings then
        main_settings_parse(M.c_in.settings)
    end
    main_config_dump_state(M.c_in)

    -- Step 1) create pool objects
    local pools = main_pools_parse(M.c_in.pools)
    -- Step 2) prepare router descriptions
    local conf = main_routes_parse(M.c_in, pools)
    -- Step 3) Reset global configuration
    main_stats_turnover()
    dsay("=== mcp_config_pools: done ===")

    -- let say/dsay work in the worker reload stage
    conf.is_verbose = M.is_verbose
    conf.is_debug = M.is_debug

    M = main_module_defaults(M)

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
        local root = worker_make_router(set, pools)
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

local function route_allfastest_f(rctx, arg)
    local mode = mcp.WAIT_OK -- wait until first non-error
    if arg.miss then
        mode = mcp.WAIT_GOOD -- wait until first good or until all children return
    end
    local wait = arg.wait

    dsay("generating an allfastest function")
    return function(r)
        rctx:enqueue(r, arg)
        if wait then
            -- return best result received after 'wait' time elapsed.
            local done = rctx:wait_cond(1, mode, wait)
        else
            local done = rctx:wait_cond(1, mode)
        end
        local final = nil
        -- return first good result, or non-error (if nothing good returned), or last error
        -- TODO: convert to rctx:best_result(arg)
        for x=1, #arg do
            local res, tag = rctx:result(arg[x])
            if tag == mcp.RES_GOOD then
                return res
            elseif tag == mcp.RES_OK then
                final = res
            elseif final == nil or not final:ok() then
                final = res
            end
        end
        -- return an error of the final result
        return final
    end
end

-- copy request to all children, but return first response
-- FIXME: fix the table in here:
-- - using o as both an array and hash table and iterating over it in the main
-- function, can be confusing for users and potentially harmful on editing
-- later. Instead put the child handles into a sub-table.
function route_allfastest_start(a, ctx, fgen)
    dsay("starting an allfastest handler")
    local o = {}
    for _, child in pairs(a.children) do
        table.insert(o, fgen:new_handle(child))
    end

    o.miss = a.miss
    o.wait = a.wait
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

    if t.local_zone ~= nil then
        zone = ctx:local_zone()
    end

    return t
end

-- TODO: if final res is due to a timeout, we could return a more descriptive
-- SERVER_ERROR
-- .. also add another stats counter for timeout
local function route_failover_f(rctx, arg)
    local limit = arg.limit
    local t = arg.t
    local miss = arg.miss
    local wait = arg.wait
    local s = nil
    local s_id = 0
    if arg.stats_id then
        s = mcp.stat
        s_id = arg.stats_id
    end

    return function(r)
        local res = nil
        local rmiss = nil -- flag if any children returned a miss
        for i=1, limit do
            if wait then
                -- can return a nil res
                -- lua has no 'continue'
                res = rctx:enqueue_and_wait(r, t[i], wait)
            else
                res = rctx:enqueue_and_wait(r, t[i])
            end

            if i > 1 and s then
                -- increment the retries counter
                s(s_id, 1)
            end

            -- process result
            if res == nil then
                -- do nothing.
            elseif res:hit() then
                return res
            elseif res:ok() then
                if miss == true then
                    -- save the ok/miss result and continue looping (treat miss as a failure)
                    rmiss = res
                else
                    -- return ok/miss (treat miss as a good result)
                    return res
                end
            end
        end

        -- didn't get what we want, return either miss (if any) or last error
        if rmiss then
            return rmiss
        end
        return res
    end
end

function route_failover_start(a, ctx, fgen)
    local o = { t = {}, c = 0 }

    -- We have a local zone defined _and_ the children table seems to have
    -- this zone defined. We must try this zone first.
    local zone = a.local_zone
    if zone ~= nil and a.children[zone] ~= nil then
        table.insert(o.t, fgen:new_handle(a.children[zone]))
        o.c = o.c + 1
        for name, child in pairs(a.children) do
            if zone ~= name then
                table.insert(o.t, fgen:new_handle(child))
                o.c = o.c + 1
            end
        end
        o.zone = zone
    else
        for _, child in pairs(a.children) do
            table.insert(o.t, fgen:new_handle(child))
            o.c = o.c + 1
        end
    end

    -- TODO: should zone cause shuffle to be ignored?
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
    o.wait = a.wait

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
-- route_allasync start
--

function route_allasync_conf(t)
    return t
end

local function route_allasync_f(rctx, arg)
    local handles = arg.h
    local mut = arg.m
    local nres = rctx:response_new()

    return function(r)
        rctx:enqueue(r, handles)
        rctx:wait_cond(0) -- do not wait
        mut(nres, r) -- return a "NULL" response
        return nres
    end
end

function route_allasync_start(a, ctx, fgen)
    dsay("starting an allasync route handler")
    local o = {}
    for _, v in pairs(a.children) do
        table.insert(o, fgen:new_handle(v))
    end
    local mut = mcp.res_mutator_new(
        { t = "resnull", idx = 1 }
    )

    fgen:ready({
        a = { h = o, m = mut },
        u = 1,
        n = ctx:label(),
        f = route_allasync_f,
    })
end

--
-- route_allasync end
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

--
-- route_null start
--

function route_null_conf(t)
    return t
end

function route_null_start(a, ctx, fgen)
    local mut = mcp.res_mutator_new(
        { t = "resnull", idx = 1 }
    )

    fgen:ready({
        n = ctx:label(),
        u = 1,
        f = function(rctx)
            local nres = rctx:response_new()
            return function(r)
                mut(nres, r)
                return nres
            end
        end
    })
end

--
-- route_null end
--

--
-- route_ratelim start
-- NOTE: EXPERIMENTAL API. MAY CHANGE.
--

-- TODO:
-- arg to use bytes of req/res instead of request rate.
function route_ratelim_conf(t, arg)
    if t.stats then
        local name = ctx:label() .. "_limits"
        if t.stats_name then
            name = t.stats_name .. "_limits"
        end
        t.stats_id = ctx:get_stats_id(name)
    end

    if t.tickrate == nil then
        t.tickrate = 1000
    end

    if t.limit == nil then
        error("must specify limit to route_ratelim")
    end
    if t.fillrate == nil then
        error("must specify fillrate to route_ratelim")
    end

    if t.global then
        local tbf_global = mcp.ratelim_global_tbf({
            limit = t.limit,
            fillrate = t.fillrate,
            tickrate = t.tickrate,
        })
        t.tbf_global = tbf_global
    end
    return t
end

local function route_ratelim_f(rctx, o)
    local rlim = o.rlim
    local h = o.handle
    local null = o.null
    local nres = rctx:response_new()

    if o.fail_until_limit then
        -- invert the rate limiter: disallow requests up until this rate.
        return function(r)
            if rlim(1) then
                null(nres, r)
                return nres
            else
                return rctx:enqueue_and_wait(r, h)
            end
        end
    else
        return function(r)
            if rlim(1) then
                return rctx:enqueue_and_wait(r, h)
            else
                -- TODO: allow specifying a "SERVER_ERROR" instead?
                return null(r)
            end
        end
    end
end

function route_ratelim_start(a, ctx, fgen)
    local o = {}

    o.handle = fgen:new_handle(a.child)
    if a.global then
        o.rlim = a.tbf_global
    else
        o.rlim = mcp.ratelim_tbf({
            limit = a.limit,
            fillrate = a.fillrate,
            tickrate = a.tickrate,
        })
    end
    o.null = mcp.res_mutator_new(
        { t = "resnull", idx = 1 }
    )
    o.fail_until_limit = a.fail_until_limit

    fgen:ready({
        a = o,
        u = 1,
        n = ctx:label(),
        f = route_ratelim_f
    })
end

--
-- route_ratelim end
--

--
-- route_random start
--

function route_random_conf(t)
    return t
end

function route_random_start(a, ctx, fgen)
    local handles = {}
    local count = 0

    for k, child in pairs(a.children) do
        table.insert(handles, fgen:new_handle(child))
        count = count + 1
    end

    fgen:ready({
        n = ctx:label(),
        f = function(rctx)
            return function(r)
                local pool = handles[math.random(count)]
                return rctx:enqueue_and_wait(r, pool)
            end
        end
    })
end

--
-- route_random end
--

register_route_handlers({
    "failover",
    "allfastest",
    "allsync",
    "split",
    "direct",
    "zfailover",
    "ttl",
    "null",
    "ratelim",
    "random"
})
