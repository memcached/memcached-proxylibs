-- The intent of this test file is _not_ to test routelib, but to stand as an
-- example for how to use t/lib/memcachedtest, and a file for "testing the
-- test framework" without routelib complicating things.

local lu = require('luaunit')
local t = require("t/lib/memcachedtest")

local srv
local sock
local p

function runTests()
    -- proxy test object
    -- create this first so memcached will queue connections to it when it
    -- starts
    p = t.new_proxytest({ lu = lu, servers = {11512, 11513} })

    srv = t.new_memcached("-o proxy_config=./t/basic-config.lua -t 1")
    -- example of starting the daemon with -l overridden.
    -- instead of trying to magically parse what to connect to, you also pass
    -- in the host and port in case of multiple -l's being used.
    --srv = t.new_memcached("-o proxy_config=./t/basic-config.lua -t 1 -l 127.0.0.1:11211", "127.0.0.1", 11211)
    -- this creates a wrapped connection to a specific port. used when testing
    -- memcached from different ports.
    -- local cli = t.new_handle("127.0.0.1", "11211")
    sock = srv:sock()
    p:set_c(sock)
    p:accept_backends()

    local res = lu.LuaUnit.run()
    -- The second 'true' here is important. It lets the test harness clean up
    -- before exiting.
    os.exit(res, true)
end

TestBasics = {}

-- untested API:
-- local bes = p:be_wait(list, timeout)
-- if bes == nil then
--   -- none became readable
-- end
-- else #bes is the number of readable backends
-- loop through bes and compare via p:be_is(index, value) to see if a backend
-- was originally in index 1, 2, etc. ie; reader or writer or copy.
-- might change this to return the original indexes directly?

function TestBasics:testBasics()
    p:c_send("mg one/foo\r\n")
    -- example: waiting if a specific backend becomes available or not
    --lu.assertEquals(p:be_wait_one(1, 500), true)
    p:be_recv_c(1, "backend one received mg")
    p:be_send(1, "EN\r\n")
    p:c_recv_be("client received response from backend")
end

function TestBasics:testStats()
    local stats = sock:mem_stats()
    lu.assertEquals(stats["read_buf_oom"], 0, 'expected stat exists')

    stats = sock:mem_stats("proxy")
    lu.assertNotNil(stats["cmd_mg"], 'stats proxy also works')
end

function TestBasics:testMS()
    p:c_send("ms one/boo 2\r\nhi\r\n")
    -- this also works
    --p:c_send("ms one/boo 2\r\n")
    --p:c_send("hi\r\n")
    p:be_recv(1, "ms one/boo 2\r\n")
    p:be_recv(1, "hi\r\n")
    p:be_send(1, "HD\r\n")
    p:c_recv_be()
end

runTests()
