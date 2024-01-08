package.loaded["routelib"] = nil
local s = require("routelib")
verbose(true)
debug(true)

function config()
    settings{
        active_request_limit = 100,
        backend_connect_timeout = 3,
    }

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

    routes{
        conf = {
            mode = "prefix",
            stop = "/"
        },
        map = {
            foo = route_allfastest{
                children = { "foo" },
            },
        },
        default = route_allfastest{
            children = { "bar" }
        },
    }
end

