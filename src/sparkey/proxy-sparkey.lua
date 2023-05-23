function mcp_config_pools()
    local sparkey = require("sparkey")
    sparkey.poolopen()
    return {}
end

function mcp_config_routes(pool)
    local sparkey = require("sparkey")
    sparkey.routeopen("toast.spi", "toast.spl")
    mcp.attach(mcp.CMD_MG, function(r)
        local value = sparkey.get(r:key())
        if value ~= nil then
            return "VA " .. string.len(value) .. "\r\n" .. value .. "\r\n"
        else
            return "EN\r\n"
        end
    end)
end
