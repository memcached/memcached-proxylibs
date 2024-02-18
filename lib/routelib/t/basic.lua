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
    srv:cleanup()
    os.exit(res)
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

runTests()
