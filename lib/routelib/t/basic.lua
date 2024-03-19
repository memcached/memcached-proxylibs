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
    sock = srv:sock()
    p:set_c(sock)
    p:accept_backends()

    local res = lu.LuaUnit.run()
    -- The second 'true' here is important. It lets the test harness clean up
    -- before exiting.
    os.exit(res, true)
end

TestBasics = {}

function TestBasics:testBasics()
    p:c_send("mg one/foo\r\n")
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
