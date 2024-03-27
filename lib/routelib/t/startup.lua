local lu = require('luaunit')
local t = require("t/lib/memcachedtest")

local srv
local sock
local p

function runTests()
    p = t.new_proxytest({ lu = lu, servers = {11312, 11313} })

    srv = t.new_memcached("-o proxy_config=./routelib.lua,proxy_arg=./t/startup-config.lua -t 1")
    sock = srv:sock()
    p:set_c(sock)
    p:accept_backends()

    local res = lu.LuaUnit.run()
    os.exit(res, true)
end

TestStartup = {}

-- TODO: mixed pool sets and sub-routes

-- series of very basic end to end tests to ensure child pools were parsed
-- properly.
function TestStartup:testDirectChild()
    p:c_send("mg foo/test\r\n")
    p:be_recv_c(1, "direct route worked")
    p:be_send(1, "EN\r\n")
    p:c_recv_be("client received miss")
    p:clear()
end

function TestStartup:testSetChildren()
    p:c_send("mg bar/test\r\n")
    p:be_recv_c({1,2})
    p:be_send({1,2}, "EN\r\n")
    p:c_recv_be()
    p:clear()
end

function TestStartup:testListChildren()
    p:c_send("mg baz/test\r\n")
    p:be_recv_c({1,2})
    p:be_send({1,2}, "EN\r\n")
    p:c_recv_be()
    p:clear()
end

function TestStartup:testHashChildren()
    p:c_send("mg bee/test\r\n")
    p:be_recv_c({1,2})
    p:be_send({1,2}, "EN\r\n")
    p:c_recv_be()
    p:clear()
end

function TestStartup:testSetSubChildren()
    p:c_send("mg zee/test\r\n")
    p:be_recv_c({1,2})
    p:be_send({1,2}, "EN\r\n")
    p:c_recv_be()
    p:clear()
end

runTests()
