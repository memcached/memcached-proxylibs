-- tests for routelib's builtin route handlers.

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

TestNoMissFailover = {}

function TestNoMissFailover:testHit()
    p:c_send("mg failovernomiss/a t\r\n")
    p:be_recv_c(1, "first be")
    p:be_send(1, "HD t1\r\n")
    p:c_recv_be("client received hit")
    clearAll(p)
end

function TestNoMissFailover:testMiss()
    p:c_send("mg failovernomiss/a t\r\n")
    p:be_recv_c(1, "first be")
    p:be_send(1, "EN\r\n")
    p:c_recv_be("client received a miss")
    clearAll(p)
end

function TestNoMissFailover:testError()
    p:c_send("mg failovernomiss/a t\r\n")
    p:be_recv_c(1, "first be")
    p:be_send(1, "SERVER_ERROR big scary error\r\n")
    p:be_recv_c(2, "second be got failover req")
    p:be_send(2, "HD t4\r\n")
    p:c_recv_be("client received second hit")
    clearAll(p)
end

function TestNoMissFailover:testAllError()
    p:c_send("mg failovernomiss/a t\r\n")
    for x=1, 2 do
        p:be_recv_c(x, "be received req")
        p:be_send(x, "SERVER_ERROR boo\r\n")
    end
    p:c_recv_be("client received an error")
    clearAll(p)
end

-- TODO: add tests with some errors. I think it's fine though?
TestSplit = {}

function TestSplit:testSet()
    p:c_send("ms split/a 2 c\r\nhi\r\n")
    for x=1, 2 do
        p:be_recv(x, "ms split/a 2 c\r\n")
        p:be_recv(x, "hi\r\n")
    end
    p:be_send(1, "HD c1\r\n")
    p:be_send(2, "HD c2\r\n")
    -- only see response from child a
    p:c_recv("HD c1\r\n")
    clearAll(p)
end

function TestSplit:testGet()
    p:c_send("mg split/a t\r\n")
    p:be_recv({1, 2}, "mg split/a t\r\n")
    p:be_send(1, "HD t1\r\n")
    p:be_send(2, "HD t2\r\n")
    -- only see response from child a
    p:c_recv("HD t1\r\n")
    clearAll(p)
end

function TestSplit:testGet2()
    p:c_send("mg split/a t\r\n")
    p:be_recv({1, 2}, "mg split/a t\r\n")
    -- flip order
    p:be_send(2, "HD t2\r\n")
    p:be_send(1, "HD t1\r\n")
    -- only see response from child a
    p:c_recv("HD t1\r\n")
    clearAll(p)
end

function TestSplit:testGetLate()
    p:c_send("mg split/a t\r\n")
    p:be_recv({1, 2}, "mg split/a t\r\n")
    p:be_send(1, "HD t1\r\n")
    -- client works without b response
    p:c_recv("HD t1\r\n")
    p:be_send(2, "HD t2\r\n")
    clearAll(p)
end

-- using sub-routes as children
TestSplitSub = {}

function TestSplitSub:testSet()
    p:c_send("ms splitsub/a 2 c\r\nhi\r\n")
    for x=1, 2 do
        p:be_recv(x, "ms splitsub/a 2 c\r\n")
        p:be_recv(x, "hi\r\n")
    end
    p:be_send(1, "HD c1\r\n")
    p:be_send(2, "HD c2\r\n")
    -- only see response from child a
    p:c_recv("HD c1\r\n")
    clearAll(p)
end

function TestSplitSub:testGet()
    p:c_send("mg splitsub/a t\r\n")
    p:be_recv({1, 2}, "mg splitsub/a t\r\n")
    p:be_send(1, "HD t1\r\n")
    p:be_send(2, "HD t2\r\n")
    -- only see response from child a
    p:c_recv("HD t1\r\n")
    clearAll(p)
end

function TestSplitSub:testGet2()
    p:c_send("mg splitsub/a t\r\n")
    p:be_recv({1, 2}, "mg splitsub/a t\r\n")
    -- flip order
    p:be_send(2, "HD t2\r\n")
    p:be_send(1, "HD t1\r\n")
    -- only see response from child a
    p:c_recv("HD t1\r\n")
    clearAll(p)
end

function TestSplitSub:testGetLate()
    p:c_send("mg splitsub/a t\r\n")
    p:be_recv({1, 2}, "mg splitsub/a t\r\n")
    p:be_send(1, "HD t1\r\n")
    -- client works without b response
    p:c_recv("HD t1\r\n")
    p:be_send(2, "HD t2\r\n")
    clearAll(p)
end

TestAllFastest = {}

-- Honestly not sure what else to test here?
-- this route returns the result regardless of what it is. as long as it
-- returns after the first one it's good.
function TestAllFastest:testBasic()
    p:c_send("mg allfastest/a t\r\n")
    p:be_recv_c({1, 2, 3}, "all three got request")
    p:be_send(2, "HD t2\r\n")
    p:c_recv_be("first response")
    p:be_send({1, 3}, "HD t4\r\n")
    clearAll(p)
end

-- supposed to respond with the "worst" reply (an error/etc)
TestAllSync = {}

function TestAllSync:testBasic()
    p:c_send("mg allsync/a t\r\n")
    p:be_recv_c({1, 2, 3}, "all three got request")
    p:be_send(1, "HD t1\r\n")
    -- ensure client doesn't wake up
    lu.assertEquals(sock:poll_read(200), false, "client not readable")
    p:be_send(2, "HD t2\r\n")
    p:be_send(3, "HD t3\r\n")
    p:c_recv("HD t3\r\n", "got last response since all good")
    clearAll(p)
end

-- FIXME: should this return EN? optionally?
-- IE: an option to return error or normal resp, plus option to return
-- miss/ns/nf/etc depending on command
-- plus option for returning quorum?
function TestAllSync:testMiss()
    p:c_send("mg allsync/a t\r\n")
    p:be_recv_c({1, 2, 3}, "all three got request")
    p:be_send(1, "EN\r\n")
    p:be_send(2, "HD t2\r\n")
    p:be_send(3, "HD t3\r\n")
    p:c_recv("HD t3\r\n", "got last response since all good")
    clearAll(p)
end

function TestAllSync:testError()
    p:c_send("mg allsync/a t\r\n")
    p:be_recv_c({1, 2, 3}, "all three got request")
    p:be_send(1, "HD t1\r\n")
    p:be_send(2, "SERVER_ERROR botched\r\n")
    p:be_send(3, "HD t3\r\n")
    p:c_recv("SERVER_ERROR botched\r\n", "got last response since all good")
    clearAll(p)
end

TestZFailover = {}

-- local zone is set to 2 to ensure we don't just send to the
-- first/second/third in the list.
local LZ = 2
local FZ = {1, 3} -- far zones
function TestZFailover:testHit()
    p:c_send("mg zfailover/a t\r\n")
    p:be_recv_c(LZ, "local zone got first attempt")
    p:be_send(LZ, "HD t7\r\n")
    p:c_recv_be("got resp from first zone")
    clearAll(p)
end

function TestZFailover:testMiss()
    p:c_send("mg zfailover/a t\r\n")
    p:be_recv_c(LZ, "local zone got first attempt")
    p:be_send(LZ, "EN\r\n")
    p:be_recv_c(FZ, "far zones both got requests")
    p:be_send(FZ, "HD t9\r\n")
    p:c_recv_be("got resp from far zones")
    clearAll(p)
end

runTests()
