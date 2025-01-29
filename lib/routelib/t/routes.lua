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

TestNULL = {}

function TestNULL:testBasic()
    -- TODO: fill all commands.
    local expect = {
        { "get null/foo\r\n", "END\r\n" },
        { "set null/foo 0 0 2\r\nhi\r\n", "NOT_STORED\r\n" },
        { "delete null/foo\r\n", "NOT_FOUND\r\n" },
        { "mg null/foo\r\n", "EN\r\n" },
        { "ms null/foo 2\r\nhi\r\n", "NS\r\n" },
        { "md null/foo\r\n", "NF\r\n" },
        { "ma null/foo\r\n", "NF\r\n" },
    }

    for _, e in ipairs(expect) do
        p:c_send(e[1])
        p:c_recv(e[2])
    end

    clearAll(p)
end

TestTTL = {}

-- test twice in here: once for the submap and once for the non-map
function TestTTL:testMS()
    local keys = { "ttl/a", "ttl_submap/a" }

    for _, key in ipairs(keys) do
        local pfx = "ms " .. key
        p:c_send(pfx .. " 2 T999\r\nhi\r\n")
        p:be_recv(1, pfx .. " 2 T45\r\n")
        p:be_recv(1, "hi\r\n")
        p:be_send(1, "HD\r\n")
        p:c_recv_be()

        -- no existing flag (ie: TTL0)
        p:c_send(pfx .. " 2\r\nhi\r\n")
        p:be_recv(1, pfx .. " 2 T45\r\n")
        p:be_recv(1, "hi\r\n")
        p:be_send(1, "HD\r\n")
        p:c_recv_be()

        -- unrelated flag makes it through
        p:c_send(pfx .. " 2 F50\r\nhi\r\n")
        p:be_recv(1, pfx .. " 2 F50 T45\r\n")
        p:be_recv(1, "hi\r\n")
        p:be_send(1, "HD\r\n")
        p:c_recv_be()
    end

    clearAll(p)
end

function TestTTL:testSET()
    local keys = { "ttl/a", "ttl_submap/a" }

    for _, key in ipairs(keys) do
        local pfx = "set " .. key
        p:c_send(pfx .. " 0 999 2\r\nhi\r\n")
        p:be_recv(1, pfx .. " 0 45 2\r\n")
        p:be_recv(1, "hi\r\n")
        p:be_send(1, "STORED\r\n")
        p:c_recv_be()
    end

    clearAll(p)
end

function TestTTL:testADD()
    local keys = { "ttl/a", "ttl_submap/a" }

    for _, key in ipairs(keys) do
        local pfx = "add " .. key
        p:c_send(pfx .. " 0 999 2\r\nhi\r\n")
        p:be_recv(1, pfx .. " 0 45 2\r\n")
        p:be_recv(1, "hi\r\n")
        p:be_send(1, "STORED\r\n")
        p:c_recv_be()
    end

    clearAll(p)
end

function TestTTL:testCAS()
    local keys = { "ttl/a", "ttl_submap/a" }

    for _, key in ipairs(keys) do
        local pfx = "cas " .. key
        p:c_send(pfx .. " 0 999 2 333\r\nhi\r\n")
        p:be_recv(1, pfx .. " 0 45 2 333\r\n")
        p:be_recv(1, "hi\r\n")
        p:be_send(1, "STORED\r\n")
        p:c_recv_be()
    end

    clearAll(p)
end

TestCmaps = {}

function TestCmaps:testSub()
    p:c_send("mg d_submap/a t\r\n")
    p:be_recv_c(1, "mg to first be")
    p:be_send(1, "HD t31\r\n")
    p:c_recv_be()

    p:c_send("md d_submap/a\r\n")
    p:be_recv_c(2, "md to second be")
    p:be_send(2, "HD\r\n")
    p:c_recv_be()

    p:c_send("ma d_submap/a\r\n")
    p:be_recv_c(3, "ma to third be")
    p:be_send(3, "HD\r\n")
    p:c_recv_be()

    clearAll(p)
end

-- "top level" route cmap as fallback for unknown map entry
function TestCmaps:testTop()
    p:c_send("mg badroute/a t\r\n")
    p:be_recv_c(3, "mg to third be")
    p:be_send(3, "HD t32\r\n")
    p:c_recv_be()

    p:c_send("md badroute/a\r\n")
    p:be_recv_c(2, "md to second be")
    p:be_send(2, "HD\r\n")
    p:c_recv_be()

    p:c_send("ma badroute/a\r\n")
    p:be_recv_c(1, "ma to first be")
    p:be_send(1, "HD\r\n")
    p:c_recv_be()

    clearAll(p)
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

function TestMissFailover:testBestResult()
    p:c_send("mg failover/a t\r\n")
    p:be_recv_c(1, "first be got miss")
    p:be_send(1, "EN\r\n")
    p:be_recv_c(2, "second be got error")
    p:be_send(2, "SERVER_ERROR last error\r\n")
    p:c_recv("EN\r\n") -- The client should receive "best result" out of two, which is miss-response from the first be
    clearAll(p)
end

function TestMissFailover:testLastError()
    p:c_send("mg failover/a t\r\n")
    p:be_recv_c(1, "first be got error-1")
    p:be_send(1, "SERVER_ERROR error-1\r\n")
    p:be_recv_c(2, "second be got error-2")
    p:be_send(2, "SERVER_ERROR error-2\r\n")
    p:c_recv("SERVER_ERROR error-2\r\n") -- The client should receive the last error if all bes return errors
    clearAll(p)
end

-- we have three pools configured but set failover limit to 2
TestMissFailoverPSET = {}

function TestMissFailoverPSET:testHit()
    p:c_send("mg failoverpset/b t\r\n")
    p:be_recv_c(1, "first be")
    p:be_send(1, "HD t1\r\n")
    p:c_recv_be("client received hit")
    clearAll(p)
end

function TestMissFailoverPSET:testMiss()
    p:c_send("mg failoverpset/b t\r\n")
    p:be_recv_c(1, "first be")
    p:be_send(1, "EN\r\n")
    p:be_recv_c(2, "second be got failover req")
    p:be_send(2, "HD t3\r\n")
    p:c_recv_be("client received second hit")
    clearAll(p)
end

function TestMissFailoverPSET:testAllMiss()
    p:c_send("mg failoverpset/b t\r\n")
    for x=1, 2 do
        p:be_recv_c(x, "be received req")
        p:be_send(x, "EN\r\n")
    end
    p:c_recv_be("client received a miss")
    clearAll(p)
end

function TestMissFailoverPSET:testFailure()
    p:c_send("mg failoverpset/b t\r\n")
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

TestNoCountFailover = {}

function TestNoCountFailover:testAll()
    -- Ensure we don't walk off the end of the handles array if not explicitly
    -- given a failover count.
    p:c_send("mg failovernocount/a t\r\n")
    for x=1, 3 do
        p:be_recv_c(x, "be received req")
        p:be_send(x, "EN\r\n")
    end
    p:c_recv_be("EN\r\n")
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

-- allfastest should return the first non-error response. Else the final
-- error.
function TestAllFastest:testBasic()
    p:c_send("mg allfastest/a t\r\n")
    p:be_recv_c({1, 2, 3}, "all three got request")
    p:be_send(2, "HD t2\r\n")
    p:c_recv_be("first response")
    p:be_send({1, 3}, "HD t4\r\n")
    clearAll(p)
end

function TestAllFastest:testIgnoreMissAndErr()
    p:c_send("mg allfastest/b t\r\n")
    p:be_recv_c({1, 2, 3}, "all three got request")
    p:be_send(1, "SERVER_ERROR too many potatos\r\n")
    p:be_send(2, "EN\r\n") -- should ignore "miss" response when miss==true flag is set
    p:be_send(3, "HD t3\r\n")
    p:c_recv("HD t3\r\n")
    clearAll(p)
end

function TestAllFastest:testReturnMiss()
    p:c_send("mg allfastestnomiss/b t\r\n")
    p:be_recv_c({1, 2, 3}, "all three got request")
    p:be_send(1, "SERVER_ERROR too many potatos\r\n")
    p:be_send(2, "EN\r\n") -- should return "miss" response when miss==false
    p:be_send(3, "HD t3\r\n")
    p:c_recv("EN\r\n")
    clearAll(p)
end

function TestAllFastest:testLastErr()
    p:c_send("mg allfastest/c t\r\n")
    p:be_recv_c({1, 2, 3}, "all three got request")
    p:be_send(1, "SERVER_ERROR one\r\n")
    p:be_send(2, "SERVER_ERROR two\r\n")
    p:be_send(3, "SERVER_ERROR three\r\n")
    p:c_recv("SERVER_ERROR three\r\n")
    clearAll(p)
end

function TestAllFastest:testMiddleHit()
    p:c_send("mg allfastest/d t\r\n")
    p:be_recv_c({1, 2, 3}, "all three got request")
    p:be_send(1, "SERVER_ERROR one\r\n")
    p:be_send(2, "HD t5\r\n")
    p:be_send(3, "EN\r\n")
    p:c_recv("HD t5\r\n")
    clearAll(p)
end

function TestAllFastest:testMiddleMiss()
    p:c_send("mg allfastest/d t\r\n")
    p:be_recv_c({1, 2, 3}, "all three got request")
    p:be_send(1, "SERVER_ERROR one\r\n")
    p:be_send(2, "EN\r\n")
    p:be_send(3, "SERVER_ERROR final\r\n")
    p:c_recv("EN\r\n")
    clearAll(p)
end

function TestAllFastest:testNoMissIgnoreLastHit()
    p:c_send("mg allfastestnomiss/d t\r\n")
    p:be_recv_c({1, 2, 3}, "all three got request")
    p:be_send(1, "SERVER_ERROR one\r\n")
    p:be_send(2, "EN\r\n") -- if miss==false, the first ok is returned ignoring subsequent hits
    p:be_send(3, "HD t5\r\n")
    p:c_recv("EN\r\n")
    clearAll(p)
end

function TestAllFastest:testResume()
    p:c_send("mg allfastest/d t\r\n")
    p:be_recv_c({1, 2, 3}, "all three got request")
    p:be_send(3, "HD t6\r\n")
    p:c_recv_be("got response from 3")
    p:be_send(1, "SERVER_ERROR one\r\n")
    p:be_send(2, "SERVER_ERROR two\r\n")
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

TestFailoverZone = {}
-- local zone is set to 2 to ensure we don't just send to the
-- first/second/third in the list.
local LZ = 2
local FZ = {1, 3} -- far zones

function TestFailoverZone:testHit()
    p:c_send("mg failoverzone/a t\r\n")
    p:be_recv_c(LZ, "local zone got first attempt")
    p:be_send(LZ, "HD t3\r\n")
    p:c_recv_be("got resp from first zone")
    clearAll(p)
end

-- FIXME: p:be_wait sucks. fucking fix it.
function TestFailoverZone:testErr()
    p:c_send("mg failoverzone/b t\r\n")
    p:be_recv_c(LZ, "local zone got first attempt")
    p:be_send(LZ, "SERVER_ERROR skip local\r\n")
    local mFZ = {1, 3}
    local seen = false
    for x=1, 2 do
        local bes = p:be_wait(mFZ, 1)
        for _, v in pairs(bes) do
            local be = mFZ[v]
            p:be_recv_c(be, "far zone got request")
            if seen then
                p:be_send(be, "HD t3\r\n")
            else
                p:be_send(be, "SERVER_ERROR skip first far\r\n")
                seen = true
            end
        end
    end

    p:c_recv("HD t3\r\n", "got resp from far zone")
    clearAll(p)
end

TestZFailover = {}

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
