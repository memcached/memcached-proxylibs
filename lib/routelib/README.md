# Route library for memcached proxy

This is a basic, extensible library for common use cases with the memcached
proxy.

For more information on the proxy: https://docs.memcached.org/features/proxy/

Please use the main documentation to learn about how to use the proxy. This
README file exists as minimalist documentation.

## Quick Start

See the example configurations in: https://github.com/memcached/memcached-proxylibs/tree/main/lib/routelib/examples

Save as `example.lua`:
```lua
pools{
    foo = {
        backends = {
            "127.0.0.1:11212",
        }
    },
    fallback = {
        backends = {
            "127.0.0.1:11213",
        }
    }
}

routes{
    map = {
        bar = route_direct{
            child = "foo",
        },
    },
    default = route_direct{ child = "fallback" }
}
```

In this configuration requests that start with "bar/" will use the backends
described in pool "foo". Any key that does not start with "bar/" will instead
be fetched from the "fallback" pool, which we added as the default route.

Now, start the proxy: `memcached -o proxy_config=routelib.lua,proxy_arg=example.lua`

You may also specify the path to lua libraries with environment variables: `LUA_PATH="/path/to/etc/?.lua" memcached -o proxy_config=routelib.lua,proxy_arg=example.lua``

## Main config Reference

Top level functions:

- `verbose(boolean)`: adds some print logging while loading the configuration
- `debug(boolean)`: adds prints useful when developing routelib itself
- `say(...)`: print if verbose is set
- `dsay(...)`: print if debug is set

For configuration description, see comments inline below. NOTE: we will
frequently refer to:
https://docs.memcached.org/features/proxy/api-reference/ for
descriptions of the full options available. This route library is a thin
wrapper around the main proxy API.

See below for a full reference on route handlers provided by the library.

```lua
-- Overrides proxy-wide global settings
settings{
    -- settings are as described in the wiki
    active_req_limit = 100,
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
            set = route_allfastest{
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
```
---

## Zones and pool sets

Some route handlers use a high level concept of "zones", which can be combined
with "pool sets" to make copies of cache data that may span multiple racks,
availability zones, regions, etc.

The router is first told what its "local zone" name is. Zone aware route
handlers will use this to try to first get data from the nearest zone, or
ensure copies are in all zones, and so on.

Each zone will have its own individual pool of backends, which act as one big
logical pool with one copy of a key in each zone.

These pool sets are defined with a special syntax.

```lua
local_zone("west")

pools{
    set_main = {
        west   = { backends = { etc } },
        south  = { backends = { etc } },
        east   = { backends = { etc } },
    }
}

routes{
    map = {
        main = route_zfailover{
            -- routelib will do the work of resolving the set here.
            children = "set_main",
        }
    }
}
```

## Pool sets without zones

Pool sets (as described above) can also be used as shorthand for non-zoned
routes. For example, instead of listing all of the pools for an 'allsync'
route, we define a pool set.

This example defines three pools of backends, then collects them using array-style syntax.
The allsync route will make three copies of all requests.

```lua
pools{
    set_all = {
        { backends = { etc } },
        { backends = { etc } },
        { backends = { etc } },
    }
}

routes{
    map = {
        main = route_allsync{
            children = "set_all"
        }
    }
}
```

## Writing custom route handlers

See: https://github.com/memcached/memcached-proxylibs/blob/main/lib/routelib/examples/customroute.lua

For detail on how routes are executed and what objects are available, see the
main wiki: https://github.com/memcached/memcached/wiki/Proxy

## Route handler reference

### `route_direct`

Routes to a single pool and returns the result without any other logic.

```lua
route_direct{
    child = "poolname"
}
```

### `route_allsync`

Routes a request to all supplied pools in parallel. Waits for all responses to
complete and returns the first error seen. If no errors, returns the last
result.

```lua
route_allsync{
    children = { "poola", "poolb", "poolc" }
}
```

### `route_allfastest`

Routes a request to all supplied pools in parallel. Returns the first response
obtained.

```lua
route_allfastest{
    children = { "poola", "poolb", "poolc" }
}
```

### `route_split`

Routes a request to child_a and child_b, but only waits for the response from
child_a.

```lua
route_split{
    child_a = "poola",
    child_b = "poolb"
}
```

### `route_ttl`

Replaces the TTL in a set, add, cas, or ms request.

```lua
route_ttl{
  ttl = 45,
  child = "pool"
}
```

### `route_failover`

Takes a list of pools and attempts to run a command in order. Behavior can be
adjusted by arguments listed below.

```lua
route_failover{
    children = { "poola", "poolb", "poolc" },
    -- fail over at most this many times
    failover_count = number,
    -- randomize the children list once on startup
    shuffle = true,
    -- failover if a fetch request was a miss.
    -- by default we only fail over on error conditions
    miss = true,
}
```

### `route_zfailover`

For usage first see `Zones and pool sets` above.

Operates similarly to `route_failover`, except it prefers the local route
first. If the first response does not have the desired result, _all_ far
routes are then checked in parallel.

```lua
route_zfailover{
    children = "set_all",
    -- add user stats counter for failovers
    stats = true,
    -- fail over on miss instead of only errors.
    miss = true
}
```
