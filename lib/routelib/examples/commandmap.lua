package.loaded["routelib"] = nil
local s = require("routelib")
verbose(true)
debug(true)

function config()
    pools{
        foo = {
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
            [mcp.CMD_GET] = route_allfastest{
                children = { "foo" },
            },
        },
        default = route_allfastest{
            children = { "bar" }
        },
    }
end

