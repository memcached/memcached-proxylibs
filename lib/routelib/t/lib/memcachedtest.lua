-- TODO: rename ps to un
local ps = require('posix.unistd')
local sk = require('posix.sys.socket')
local wa = require('posix.sys.wait')
local sg = require('posix.signal')
local ti = require('posix.time')
local poll = require('posix.poll')

local READ_SIZE <const> = 16384
local READ_TIMEOUT <const> = 1000 -- 1s is plenty

local M = {}
local unixcount = 0

local function _poll_read_one(fd, timeout)
    local fds = {
        [fd] = {events={IN=true}},
    }
    local ready = poll.poll(fds, timeout)

    if ready and fds[fd].revents and fds[fd].revents.IN then
        return true
    else
        return false
    end
end

-- connection object
local S = {}
S.__index = S

function S:__gc()
    self:close()
end

function S:new(fd)
    return setmetatable({ sock = fd }, S)
end

function S:close()
    ps.close(self.sock)
end

function S:getfd()
    return self.sock
end

function S:send(msg)
    local res, err, errno = sk.send(self.sock, msg)
    if res == nil then
        error("failed to write to socket: " .. err)
    end
    return res
end

-- wait up to timeout milliseconds for socket to become readable
function S:poll_read(timeout)
    return _poll_read_one(self:getfd(), timeout)
end

-- NOTE: tests have a general limitation of only being able to deal with
-- non-binary values. this has always been true for memcached tests. This is
-- fine since values are opaque and thus we don't need to test them.
--
-- Implement a basic buffer.
-- has to support:
-- - returning up to and including a \n
-- - holding the remaining bytes
-- - using the remaining bytes on the next call
function S:read(msg)
    while true do
        local i = nil
        -- do a plain search for a little speed.
        if self.buf then
            i = string.find(self.buf, "\n", 1, true)
        end
        -- a table version could be faster, but this is for a test suite that
        -- typically runs small commands so it might not be worth making this
        -- more complex.
        if not i then
            local readable = self:poll_read(READ_TIMEOUT)
            if not readable then
                error("failed to read from socket: timeout")
            end
            local bytes, err, errno = sk.recv(self.sock, READ_SIZE)
            if bytes == nil then
                error("failed to read from socket: " .. err)
            end

            if string.len(bytes) == 0 then
                error("remote socket is closed")
            end

            if self.buf then
                self.buf = self.buf .. bytes
            else
                self.buf = bytes
            end
        else
            local rbuf
            local slen = string.len(self.buf)

            -- "\n" is at end of buffer
            if slen == i then
                rbuf = self.buf
                self.buf = nil
            else
                -- cut off the rest for the next read
                rbuf = string.sub(self.buf, 1, i)
                self.buf = string.sub(self.buf, i+1, slen)
            end

            return rbuf
        end
    end
end

function S:mem_stats(cmd)
    if cmd then
        self:send("stats " .. cmd .. "\r\n")
    else
        self:send("stats\r\n")
    end

    local s = {}

    -- lua can't do assignments in loop evaluations.
    -- we can do a "generic for" instead but that's going to be more work to
    -- set up.
    while true do
        local line = self:read()
        if string.find(line, "END", 1, true) == 1 then
            break
        end
        local mode, name, val = string.match(line, "^(%a+) (%S+)%s+([^\r\n]+)")
        if mode then
            -- lua also doesn't autoconvert strings to numbers, so we do the
            -- test here. This _should_ work with all cases for stats outputs.
            local n = tonumber(val)
            if n then
                s[name] = n
            else
                s[name] = val
            end
        end
    end

    return s
end

-- memcached serer handler object
local H = {}
H.__index = H

function H:__gc()
    self:cleanup()
end

function H:new(o)
    -- wrap the fd with a conn object
    o.conn = S:new(o.conn)
    return setmetatable(o, H)
end

function H:stop()
    sg.kill(self.pid, sg.SIGINT)
end

function H:reload()
    sg.kill(self.pid, sg.SIGUSR1)
end

function H:cleanup()
    if self.domainsocket then
        os.remove(self.domainsocket)
    end

    sg.kill(self.pid, sg.SIGINT)

    -- ensure the daemon is stopped
    local stopped = false
    for x=1, 50, 1 do
        local res, state = wa.wait(self.pid, wa.WNOHANG)
        if res == self.pid then
            stopped = true
            break
        elseif res == nil then
            error("failed to stop memcached: " .. tostring(self.pid))
        end
        ti.nanosleep({tv_sec = 0, tv_nsec = 200000000})
    end

    if not stopped then
        print("WARNING: memcached did not stop. sending SIGKILL and giving up")
        sg.kill(self.pid, sg.SIGKILL)
    end
end

function H:sock()
    return self.conn
end

function H:new_sock()
    local fd = sk.socket(sk.AF_UNIX, sk.SOCK_STREAM, 0)

    local res, err, errno = sk.connect(fd, {family = sk.AF_UNIX, path = self.domainsocket})

    if res == nil then
        error("failed to connect to memcached: " .. self.domainsocket .. " :" .. err)
    end
    return S:new(fd)
end

-- TODO: use the timedrun binary from the build tree so we can guarantee
-- memcached stops after a failed run?
-- TODO:
-- TLS clients (needs LuaSec/something... no features used right now)
-- run as root
-- network sockets or supplied unix sockets
-- keeping it as simple as possible for the first pass, since we're just doing
-- this for route library tests and not "core memcached intregation tests"
M.new_memcached = function(args)
    local use_external = os.getenv("TEST_EXTERNAL")
    local mc_path = os.getenv("MC_PATH")
    local pid = ps.getpid()

    local sockfile = "/tmp/memcachetest." .. pid .. "." .. unixcount
    unixcount = unixcount + 1

    args = args .. " -s " .. sockfile

    -- If use_external:
    -- Don't fork, but print the command and start trying to connect to
    -- sockfile in a loop.
    local childpid = false
    if use_external then
        print("EXTERNAL memcached requested. Start arguments:\n",
            mc_path .. "/memcached-debug " .. args)
    else
        -- TODO: try to find memcached binary in more ways
        childpid = ps.fork()

        if childpid == 0 then
            -- child
            local a = {}
            --print("ARGS: " .. args)
            for tok in string.gmatch(args, "%S+") do
                table.insert(a, tok)
            end
            -- TODO: check error
            ps.exec(mc_path .. "/memcached-debug", a)

        end
    end

    if childpid or use_external then
        -- parent
        local fd = sk.socket(sk.AF_UNIX, sk.SOCK_STREAM, 0)
        local connected = false
        -- was originally a 0.25s loop for 99 tries.
        -- instead doing a backoff so we can start with faster first try.
        local nsec_per_ms <const> = 1000000
        for x=nsec_per_ms * 10, nsec_per_ms * 800, nsec_per_ms * 10 do
            -- TODO: restructure to not eat connect error
            if sk.connect(fd, {family = sk.AF_UNIX, path = sockfile}) then
                connected = true
                break
            end
            ti.nanosleep({tv_sec = 0, tv_nsec = x})
        end

        if not connected then
            error("failed to connect to memcached daemon")
        end

        -- return server handle object
        return H:new({ pid = childpid, domainsocket = sockfile, conn = fd })
    end
end

-- proxytest object
local PT = {}
PT.__index = PT

local function _mock_server(port)
    local fd = sk.socket(sk.AF_INET, sk.SOCK_STREAM, 0)

    -- so we can re-run tests quickly.
    local ok, err = sk.setsockopt(fd, sk.SOL_SOCKET, sk.SO_REUSEADDR, 1)

    local res, err, errno = sk.bind(fd,
        {family = sk.AF_INET, addr = "127.0.0.1", port = port})
    if res == nil then
        error("failed to bind to listener for mock server: " .. err)
    end

    sk.listen(fd, 1024)

    return fd
end

function PT:__gc()
    self:cleanup()
end

function PT:cleanup()
    -- close servers first, so proxy can't reconnect
    if self._srv then
        for _, srv in pairs(self._srv) do
            ps.close(srv)
        end
        self._srv = nil
    end

    -- be objects have their own GC, but we can close them here.
    if self._be then
        for _, be in pairs(self._be) do
            be:close()
        end
        self._be = nil
    end
end

function PT:new(o)
    -- check o.servers
    assert(o.servers ~= nil, "must pass list of servers")
    assert(type(o.servers) == "table", "must pass list of servers")
    assert(o.lu ~= nil, "must pass in luaunit instance")

    -- create mock servers
    o._srv = {}
    for _, v in pairs(o.servers) do
        local srv = _mock_server(v)
        assert(srv ~= nil, "failed to create mock server for: " .. v)
        table.insert(o._srv, srv)
    end
    return setmetatable(o, PT)
end

local function _accept_backend(l)
    local ready = _poll_read_one(l, READ_TIMEOUT)
    if not ready then
        error("Failed to accept new connection on backend socket: timeout")
    end
    local fd, err, errno = sk.accept(l)
    if fd == nil then
        error("Failed to accept new connection on backend socket: " .. err)
    end

    -- wrap fd with a sock object
    local sock = S:new(fd)

    -- issue version check
    local cmd = sock:read()
    if cmd ~= "version\r\n" then
        error("did not receive version command from proxy: " .. cmd)
    end
    sock:send("VERSION 1.0.0-mock\r\n")

    -- return wrapped fd
    return sock
end

function PT:accept_backends()
    -- TODO: if _be, walk and close conns after establishing new ones
    self._be = {}
    for _, srv in pairs(self._srv) do
        local be = _accept_backend(srv)
        table.insert(self._be, be)
    end
end

function PT:accept_backend(idx)
    -- TODO: if _be slot exists, close/remove it
    self._be[idx] = _accept_backend(self._srv[idx])
end

function PT:set_c(sock)
    self._c = sock
end

function PT:check_c()
    local c = self._c
    c:send("version\r\n")
    if string.find(c:read(), "VERSION") == 1 then
        -- all good.
    else
        error("VERSION not received from server to client")
    end
end

-- wait up to timeout milliseconds for the client to become readable.
function PT:wait_c(timeout)
    return self._c:poll_read(timeout)
end

-- Remember the last thing sent from a client to the proxy
-- so we can more easily check if it arrived at a backend
function PT:c_send(cmd)
    local c = self._c
    c:send(cmd)
    self._cmd = cmd
end

-- backends can be a bare number, a table of numbers in a specific order, or
-- "all" to run against all backends.
function PT:_be_list(list)
    local l = {}
    if type(list) == "number" then
        table.insert(l, self._be[list])
    elseif type(list) == "string" and list == "all" then
        for k, v in ipairs(self._be) do
            l[k] = v
        end
    elseif type(list) == "table" then
        for k, v in ipairs(list) do
            table.insert(l, self._be[v])
        end
    end

    return l
end

-- Check that the last command sent from a client arrives at this list of
-- backends
function PT:be_recv_c(list, detail)
    detail = detail or 'be received data'
    if self._cmd == nil then
        error("must issue a command with :c_send before calling :be_recv_c")
    end

    local l = self:_be_list(list)
    local cmd = self._cmd
    for k, v in ipairs(l) do
        local res = v:read()
        self.lu.assertEquals(res, cmd, detail)
    end
end

-- TODO: be_recv_c_like

-- check that a specific string was received at a backend socket
function PT:be_recv(list, cmd, detail)
    if cmd == nil then
        error("must provide a command to check in :be_recv")
    end
    detail = detail or 'be received data'

    local l = self:_be_list(list)
    for k, v in ipairs(l) do
        local res = v:read()
        self.lu.assertEquals(res, cmd, detail)
    end
end

-- send specific command to a backend socket back towards the proxy
-- remember the last command sent.
function PT:be_send(list, cmd)
    self._becmd = cmd
    local l = self:_be_list(list)
    for k, v in ipairs(l) do
        v:send(cmd)
    end
end

-- Check that client receives the last command sent to any backend. This is a
-- very common case so it reduces test size.
function PT:c_recv_be(detail)
    detail = detail or 'client received a response'

    local cmd = self._becmd
    local c = self._c
    local res = c:read()
    self.lu.assertEquals(res, cmd, detail)
end

-- Check that client receives an arbitrary string.
function PT:c_recv(cmd, detail)
    detail = detail or 'client received response'
    local c = self._c
    local res = c:read()
    self.lu.assertEquals(res, cmd, detail)
end

-- Clear out any remembered commands and check the client pipe is clear.
function PT:clear()
    self._becmd = nil
    self._cmd = nil
    self:check_c()
end

M.new_proxytest = function(o)
    return PT:new(o)
end

return M
