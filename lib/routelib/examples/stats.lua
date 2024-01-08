package.loaded["routelib"] = nil
local s = require("routelib")
verbose(true)
debug(true)

-- we configure route_latest to track itself with a stats counter, accessable
-- via "stats proxy" under user_* fields.
function config()
    pools{
        foo = {
            backends = {
                "127.0.0.1:11214",
            }
        },
        baz = {
            backends = {
                "127.0.0.1:11215",
            }
        }
    }

    routes{
        map = {
            bar = route_latest{
                children = { "foo", "baz" },
                stats = true,
            },
        },
    }
end

