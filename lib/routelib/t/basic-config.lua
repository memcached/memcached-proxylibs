function mcp_config_pools()
    local srv = mcp.backend

    local b1 = srv('b1', '127.0.0.1', 11512)
    local b2 = srv('b2', '127.0.0.1', 11513)
    local b1z = mcp.pool({b1})
    local b2z = mcp.pool({b2})

    local p = {b1z, b2z}

    return p
end

function new_direct(arg)
    local fgen = mcp.funcgen_new()
    local h = fgen:new_handle(arg)
    fgen:ready({ f = function(rctx)
        return function(r)
            return rctx:enqueue_and_wait(r, h)
        end
    end})
    return fgen
end

function mcp_config_routes(c)
    local map = {
        ["one"] = new_direct(c[1]),
        ["two"] = new_direct(c[2]),
    }

    local pfx = mcp.router_new({ map = map })

    mcp.attach(mcp.CMD_ANY_STORAGE, pfx)
end
