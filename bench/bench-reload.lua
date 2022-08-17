-- basic test for benchmarking/fiddling with the reload system
-- Change the number of pools or backends per pool and watch the timing.
-- To simulate a single backend change, adjust the "extra_port" value.
-- Extra work can be added to the functions to simulate 

local pool_count = 50
local backends = 200
local backend_prefix = "xrv"
-- change this port value to simulate the change of a single backend
local extra_port = 11215

function mcp_config_pools(old)
    local be = mcp.backend
    local pools = {}
    for x=1, pool_count, 1 do
        local p = {}
        for y=1, backends, 1 do
            table.insert(p, be(backend_prefix .. ":" .. x .. ":" .. y, "127.0.0.1", 11212))
        end
        pools["main:" .. x] = mcp.pool(p)
    end

    pools["extra"] = mcp.pool({be("extra", "127.0.0.1", extra_port)})

    return pools
end

function mcp_config_routes(conf)
    local z = {}
    -- do a little useless work just to add time here.
    -- can get more adventurous to simluate even more work I guess?
    for zname, pool in pairs(conf) do
        z[zname] = pool
    end
    mcp.attach(mcp.CMD_ANY_STORAGE, function(r) return "ERROR no_route\r\n" end)
end
