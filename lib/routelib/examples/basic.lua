package.loaded["routelib"] = nil
local s = require("routelib")
verbose(true)
debug(true)

-- place configuration within this function, which will be called at load
-- time once.
-- see https://github.com/memcached/memcached/wiki/Proxy for full detail
function config()
    -- override proxy settings
    settings{
        active_request_limit = 100,
        backend_connect_timeout = 3,
    }

    -- define all of your pools here.
    -- you can override backend or pool specific options here as well.
    pools{
        foo = {
            backend_options = { connecttimeout = 5, retrytimeout = 1 },
            backends = {
                "127.0.0.1:11214",
                "127.0.0.1:11215 _down_",
            }
        },
        bar = {
            backends = {
                "127.0.0.1:11216",
            }
        },
    }

    -- define a root route tree.
    -- configure the router, build a map of route handlers, and set a default
    -- handler if desired.
    routes{
        conf = {
            mode = "prefix",
            stop = "/"
        },
        map = {
            -- route handler for path "foo/*"
            foo = route_allfastest{
                children = { "foo" },
            },
            bar = cmdmap{
                -- only handle SET commands for path "bar/*"
                [mcp.CMD_SET] = route_allfastest{
                    children = { "bar" },
                },
            },
        },
        default = route_allfastest{
            children = { "bar" }
        },
    }
end

