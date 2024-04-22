-- basic test.
local statsd = require("statsd")

function mcp_config_pools()
    local sd = statsd.new({
        host = "127.0.0.1",
        port = 8125,
        autoflush = false,
        namespace = "extra.prefix.thing",
    })

    sd:count("foo", 1)
    sd:count("bar", 5, "foo:bar,baz:quux")
    sd:flush()
    sd:gauge("moar", 7)
    sd:flush()
end

function mcp_config_routes()
    -- n/a
end
