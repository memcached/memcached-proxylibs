-- tests for route_retry and the retry option of route_allasync.

local lu = require('luaunit')
local t = require("t/lib/memcachedtest")
local ti = require('posix.time')
local sg = require('posix.signal')

local srv
local sock
local stsock
local p

function runTests()
    p = t.new_proxytest({ lu = lu, servers = {11331, 11332} })

    srv = t.new_memcached("-o proxy_config=./routelib.lua,proxy_arg=./t/retry-config.lua -t 1")
    sock = srv:sock()
    p:set_c(sock)
    p:accept_backends()
    stsock = srv:new_sock()

    local res = lu.LuaUnit.run()
    os.exit(res, true)
end

-- NOTE: sockets close themselves when collected; don't close them here.
local function stats()
    return stsock:mem_stats("proxy")
end

local function stat(st, name)
    return st["user_" .. name] or 0
end

-- background retries finish after the client was answered; poll for them.
local function wait_stat(name, want)
    local v
    for _ = 1, 50 do
        v = stat(stats(), name)
        if v == want then
            return v
        end
        ti.nanosleep({ tv_sec = 0, tv_nsec = 100000000 })
    end
    return v
end

TestRetry = {}

function TestRetry:tearDown()
    lu.assertEquals(wait_stat("retry_inflight", 0), 0, "no request left in the retry loop")
    p:clear()
end

function TestRetry:testRecovers()
    local before = stats()
    p:c_send("delete foo\r\n")
    p:c_recv("NOT_FOUND\r\n", "client answered before the backend")
    p:be_recv(1, "delete foo\r\n", "attempt 1")
    p:be_send(1, "SERVER_ERROR boom\r\n")
    p:be_recv(1, "delete foo\r\n", "attempt 2")
    p:be_send(1, "SERVER_ERROR boom\r\n")
    p:be_recv(1, "delete foo\r\n", "attempt 3")
    p:be_send(1, "DELETED\r\n")
    local want = stat(before, "retry_main_recovered") + 1
    lu.assertEquals(wait_stat("retry_main_recovered", want), want, "recovered")
    local after = stats()
    lu.assertEquals(stat(after, "retry_main_retries") - stat(before, "retry_main_retries"), 2, "two retries")
    lu.assertEquals(stat(after, "retry_main_dropped"), stat(before, "retry_main_dropped"), "nothing dropped")
end

function TestRetry:testGivesUp()
    local before = stats()
    p:c_send("delete bar\r\n")
    p:c_recv("NOT_FOUND\r\n")
    for n = 1, 4 do
        p:be_recv(1, "delete bar\r\n", "attempt " .. n)
        p:be_send(1, "SERVER_ERROR boom\r\n")
    end
    local want = stat(before, "retry_main_dropped") + 1
    lu.assertEquals(wait_stat("retry_main_dropped", want), want, "dropped after the last attempt")
    lu.assertEquals(p:be_wait_one(1, 300), false, "no fifth attempt")
end

function TestRetry:testFinalNotRetried()
    local before = stats()
    p:c_send("delete baz\r\n")
    p:c_recv("NOT_FOUND\r\n")
    p:be_recv(1, "delete baz\r\n")
    p:be_send(1, "NOT_FOUND\r\n")
    lu.assertEquals(p:be_wait_one(1, 300), false, "not retried")
    lu.assertEquals(stat(stats(), "retry_main_retries"), stat(before, "retry_main_retries"))
end

function TestRetry:testDirect()
    p:c_send("mg foo v\r\n")
    p:be_recv(1, "mg foo v\r\n", "attempt 1")
    lu.assertEquals(p:wait_c(200), false, "client waits for the retries")
    p:be_send(1, "SERVER_ERROR boom\r\n")
    p:be_recv(1, "mg foo v\r\n", "attempt 2")
    p:be_send(1, "VA 2\r\nhi\r\n")
    p:c_recv("VA 2\r\n", "client gets the result of the retry")
    p:c_recv("hi\r\n")
end

-- park one request in the retry loop: attempt 2 stays in flight.
local function park(key)
    p:c_send("delete " .. key .. "\r\n")
    p:c_recv("NOT_FOUND\r\n")
    p:be_recv(1, "delete " .. key .. "\r\n", "attempt 1")
    p:be_send(1, "SERVER_ERROR boom\r\n")
    p:be_recv(1, "delete " .. key .. "\r\n", "attempt 2 in flight")
    lu.assertEquals(stat(stats(), "retry_inflight"), 1, "one request in the retry loop")
end

-- with the retry loop full, a failure on the capped route is dropped right
-- away instead of retried.
local function check_capped(before)
    p:c_send("touch other 10\r\n")
    p:c_recv("NOT_FOUND\r\n")
    p:be_recv(2, "touch other 10\r\n", "attempt 1")
    p:be_send(2, "SERVER_ERROR boom\r\n")
    local want = stat(before, "capped_side_dropped") + 1
    lu.assertEquals(wait_stat("capped_side_dropped", want), want, "dropped: retry loop is full")
    lu.assertEquals(p:be_wait_one(2, 300), false, "dropped request not retried")
end

function TestRetry:testMaxInflight()
    local before = stats()
    park("a")
    check_capped(before)
    p:be_send(1, "DELETED\r\n")
end

-- the retry state is kept across reloads, so the cap still counts the
-- request that was parked before the reload.
function TestRetry:testMaxInflightReload()
    local before = stats()
    park("b")

    local w = srv:new_sock()
    w:send("watch proxyevents\r\n")
    lu.assertEquals(w:read(), "OK\r\n")
    sg.kill(srv.pid, sg.SIGHUP)
    lu.assertStrContains(w:read(), "status=start")
    lu.assertStrContains(w:read(), "status=done")

    check_capped(before)
    p:be_send(1, "DELETED\r\n")
end

runTests()
