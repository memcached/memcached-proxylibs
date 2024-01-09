# Route library for memcached proxy

This is a basic, extensible library for common use cases with the memcached
proxy.

For more information on the proxy: https://github.com/memcached/memcached/wiki/Proxy

## Quick Start

Make sure memcached is built with the proxy. As of this writing version 1.6.23
or newer is required.

```sh
./configure --enable-proxy
make
make test
```

See the example configurations in: https://github.com/memcached/memcached-proxylibs/tree/main/lib/routelib/examples

Save as `example.lua`:
```lua
local rlib = require("routelib")

function config()
    pools{
        foo = {
            backends = {
                "127.0.0.1:11212",
            }
        }
    }

    routes{
        bar = route_direct{
            child = { "foo" },
        },
    }
end
```

In this configuration requests that start with "bar/" will use the backends
described in pool "foo".

NOTE: the proxy is simply a router. It does not handle starting/stopping of
backend memcached instances. For this example you need a memcached listening
on localhost port 11212

You may need to adjust the require line to give the full path to where `routelib.lua` was downloaded.

Now, start the proxy: `memcached -o proxy_config=./example.lua`

You may also specify the path to routelib.lua with environment variables: `LUA_PATH="/path/to/routelib/?.lua" memcached -o proxy_config=./example.lua`

## Main config Reference

Top level functions:

- `verbose(boolean)`: adds some print logging while loading the configuration
- `debug(boolean)`: adds prints useful when developing routelib itself
- `say(...)`: print if verbose is set
- `dsay(...)`: print if debug is set

For configuration description, see comments inline below. NOTE: we will
frequently refer to:
https://github.com/memcached/memcached/wiki/Proxy#api-documentation for
descriptions of the full options available. This route library is a thin
wrapper around the main proxy API.

See below for a full reference on route handlers provided by the library.

```lua
function config()
    -- Overrides proxy-wide global settings
    settings{
        -- settings are as described in the wiki
        active_request_limit = 100,
        -- if supplied, this overrides the defaults for pool settings.
        -- see the `mcp.pool()` docs in the wiki.
        pool_options = {
            filter = "tags",
            filter_conf = "{}"
        }
    }

    -- define all of your pools here.
    -- you can override backend or pool specific options here as well.
    pools{
        foo = {
            -- override settings related to backend servers (timeouts/etc)
            -- see `mcp.backend({})` docs in the wiki.
            backend_options = { connecttimeout = 5, retrytimeout = 1 },
            backends = {
                -- backends may be described as a simple host:port string
                "127.0.0.1:11214",
                -- adding "_down_" to the end of the string forces a backend
                -- to be unavailable.
                "127.0.0.1:11215 _down_",
                -- it's also possible to give a label to a backend. this will
                -- create TCP connections that can be shared between pools if they
                -- share the same label.
                "127.0.0.1:11200 label",
            }
        },
        bar = {
            backends = {
                -- it's also possible to directly describe backends with a
                -- table, and individually override traits
                -- see `mcp.backend({})` docs in the wiki.
                { host = "127.0.0.1", port = 11216, retrytimeout = 5 }
            }
        },
    }

    -- define a root route tree.
    -- configure the router, build a map of route handlers, and set a default
    -- handler if desired.
    routes{
        -- describe how the top level router should distribute keys among the
        -- different map entries. By default this looks like: "foo/restofkey"
        -- Other modes allow: "/foo/restofkey", "foo-+-restofkey",
        -- "___foo/restofkey" etc
        -- See wiki page for full detail.
        conf = {
            mode = "prefix",
            stop = "/"
        },
        map = {
            -- route handler for path "foo/*"
            foo = route_allfastest{
                children = { "foo" },
            },
            -- within a prefix (`bar/restofkey`) in this case, we can also
            -- assign route handlers by what specific command was used to get here.
            bar = cmdmap{
                -- only handle SET commands for path "bar/*"
                [mcp.CMD_SET] = route_allfastest{
                    children = { "bar" },
                },
            },
        },
        -- if key does not match anything in the map, use this instead.
        default = route_allfastest{
            children = { "bar" }
        },
    }

    -- Multiple route trees can be defined at once if using "tagged mode". See
    -- `examples/tagged.lua` for a complete example
    routes{
        tag = "baz",
        map = { etc },
    }
end
```
---

## Writing custom route handlers

See: https://github.com/memcached/memcached-proxylibs/blob/main/lib/routelib/examples/customroute.lua

For detail on how routes are executed and what objects are available, see the
main wiki: https://github.com/memcached/memcached/wiki/Proxy

## Route handler reference

TODO