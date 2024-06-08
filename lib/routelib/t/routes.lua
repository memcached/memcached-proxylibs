-- big mess of basic route handler tests

local lu = require('luaunit')
local t = require("t/lib/memcachedtest")

local srv
local sock
local p

function runTests()
    p = t.new_proxytest({ lu = lu, servers = {11322, 11323, 11324} })

    srv = t.new_memcached("-o proxy_config=./routelib.lua,proxy_arg=./t/routes-config.lua -t 1")
    sock = srv:sock()
    p:set_c(sock)
    p:accept_backends()

    local res = lu.LuaUnit.run()
    os.exit(res, true)
end

-- ensure the client and backend pipes are clear.
function clearAll(p)
    -- ensure client pipeline is clear
    p:clear()
    p:c_send("mg direct_a/clear t\r\n")
    p:be_recv_c(1, "a received")
    p:be_send(1, "HD t1\r\n")
    p:c_recv_be("client received a response")

    p:c_send("mg direct_b/clear t\r\n")
    p:be_recv_c(2, "b received")
    p:be_send(2, "HD t1\r\n")
    p:c_recv_be("client received b response")

    p:c_send("mg direct_c/clear t\r\n")
    p:be_recv_c(3, "c received")
    p:be_send(3, "HD t1\r\n")
    p:c_recv_be("client received c response")

    -- double check client is clear.
    p:clear()
end

-- we have three pools configured but set failover limit to 2
TestMissFailover = {}

function TestMissFailover:testHit()
    p:c_send("mg failover/a t\r\n")
    p:be_recv_c(1, "first be")
    p:be_send(1, "HD t1\r\n")
    p:c_recv_be("client received hit")
    clearAll(p)
end

function TestMissFailover:testMiss()
    p:c_send("mg failover/a t\r\n")
    p:be_recv_c(1, "first be")
    p:be_send(1, "EN\r\n")
    p:be_recv_c(2, "second be got failover req")
    p:be_send(2, "HD t3\r\n")
    p:c_recv_be("client received second hit")
    clearAll(p)
end

function TestMissFailover:testAllMiss()
    p:c_send("mg failover/a t\r\n")
    for x=1, 2 do
        p:be_recv_c(x, "be received req")
        p:be_send(x, "EN\r\n")
    end
    p:c_recv_be("client received a miss")
    clearAll(p)
end

function TestMissFailover:testFailure()
    p:c_send("mg failover/a t\r\n")
    p:be_recv_c(1, "first be")
    p:be_send(1, "SERVER_ERROR cracked an egg\r\n")
    p:be_recv_c(2, "second be")
    p:be_send(2, "HD t4\r\n")
    p:c_recv_be("client received hit")
    clearAll(p)
end

-- TODO: failovernomiss?

-- split
-- direct
-- allsync
-- allfastest
-- zfailover

runTests()
