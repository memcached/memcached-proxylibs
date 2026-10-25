-- tests for route_allasync back-pressure (max_pending). Needs a memcached
-- with mcp.active_requests().

local lu = require('luaunit')
local t = require("t/lib/memcachedtest")
local ti = require('posix.time')
local sg = require('posix.signal')

local srv
local sock
local stsock
local p

function runTests()
    p = t.new_proxytest({ lu = lu, servers = {11341, 11342} })

    srv = t.new_memcached("-o proxy_config=./routelib.lua,proxy_arg=./t/allasync-config.lua -t 1")
    sock = srv:sock()
    p:set_c(sock)
    p:accept_backends()
    stsock = srv:new_sock()

    local res = lu.LuaUnit.run()
    os.exit(res, true)
end

-- NOTE: sockets close themselves when collected; don't close them here.
local function stat(name)
    return stsock:mem_stats("proxy")["user_" .. name] or 0
end

local function wait_stat(name, want)
    local v
    for _ = 1, 50 do
        v = stat(name)
        if v == want then
            return v
        end
        ti.nanosleep({ tv_sec = 0, tv_nsec = 100000000 })
    end
    return v
end

TestAllAsyncPending = {}

function TestAllAsyncPending:tearDown()
    lu.assertEquals(wait_stat("retry_inflight", 0), 0, "no request left in the retry loop")
    p:clear()
end

function TestAllAsyncPending:testWaitsForChildren()
    local before = stat("allasync_throttled")
    p:c_send("touch foo 10\r\n")
    p:be_recv(1, "touch foo 10\r\n")
    lu.assertEquals(p:wait_c(300), false, "client not answered while the backend is pending")
    p:be_send(1, "TOUCHED\r\n")
    lu.assertEquals(p:wait_c(2000), true, "client answered after the backend")
    p:c_recv("NOT_FOUND\r\n")
    lu.assertEquals(stat("allasync_throttled"), before + 1, "throttled counter")
end

function TestAllAsyncPending:testPendingWait()
    p:c_send("mg foo v\r\n")
    p:be_recv(1, "mg foo v\r\n")
    lu.assertEquals(p:wait_c(1000), true, "answered after pending_wait without the backend")
    p:c_recv("EN\r\n")
    p:be_send(1, "EN\r\n")
end

function TestAllAsyncPending:testBelowLimit()
    local before = stat("allasync_throttled")
    p:c_send("incr cnt 1\r\n")
    lu.assertEquals(p:wait_c(1000), true, "answered without waiting")
    p:c_recv("NOT_FOUND\r\n")
    p:be_recv(1, "incr cnt 1\r\n")
    p:be_send(1, "NOT_FOUND\r\n")
    lu.assertEquals(stat("allasync_throttled"), before, "not throttled")
end

-- park a delete in the retry loop on the side pool: attempt 2 in flight.
local function park(key)
    p:c_send("delete " .. key .. "\r\n")
    p:c_recv("NOT_FOUND\r\n")
    p:be_recv(2, "delete " .. key .. "\r\n", "attempt 1")
    p:be_send(2, "SERVER_ERROR boom\r\n")
    p:be_recv(2, "delete " .. key .. "\r\n", "attempt 2 in flight")
    lu.assertEquals(stat("retry_inflight"), 1, "one request in the retry loop")
end

-- 2 active (the retrying delete and this one), 1 of them in a retry loop:
-- not over max_pending = 1.
local function check_not_throttled()
    local before = stat("allasync_throttled")
    p:c_send("decr cnt 1\r\n")
    lu.assertEquals(p:wait_c(1000), true, "answered without waiting")
    p:c_recv("NOT_FOUND\r\n")
    p:be_recv(1, "decr cnt 1\r\n")
    p:be_send(1, "NOT_FOUND\r\n")
    lu.assertEquals(stat("allasync_throttled"), before, "retrying request not counted")
end

function TestAllAsyncPending:testRetryingNotCounted()
    park("a")
    check_not_throttled()
    p:be_send(2, "DELETED\r\n")
end

function TestAllAsyncPending:testRetryingNotCountedReload()
    park("b")

    local w = srv:new_sock()
    w:send("watch proxyevents\r\n")
    lu.assertEquals(w:read(), "OK\r\n")
    sg.kill(srv.pid, sg.SIGHUP)
    lu.assertStrContains(w:read(), "status=start")
    lu.assertStrContains(w:read(), "status=done")

    check_not_throttled()
    p:be_send(2, "DELETED\r\n")
end

runTests()
