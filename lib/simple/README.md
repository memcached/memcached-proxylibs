# Simple route library for memcached proxy

This is a basic library for common use cases of the memcached proxy service.

For more information on the proxy: https://github.com/memcached/memcached/wiki/Proxy

This document is a reference to the options provided by the 'simple' library.
If you feel like anything is missing or find a bug, please feel free to open
an issue or pull request!

Backwards compatibility is not guaranteed at this time, but a best effort will
be made regardless. If significant changes are planned they will be
implemented in a new library.

## Quick Start

Make sure memcached is built with the proxy. As of this writing version 1.6.17
or newer is required.

```sh
./configure --enable-proxy
make
make test
```

See the example configurations in: https://github.com/memcached/memcached-proxylibs/tree/main/examples

Adjust as needed. Function and option documentation are below

## API

This `simple` library works by defining a set of global lua functions that
allow the user to build a configuration. The library wraps the proxy's
internal `mcp_config_pools` and `mcp_config_routes` functions. The user only
has to set some options for the "routing" and define their pools. The library
assembles these into routes for the proxy.

- `router{}`: defines overrides for how requests are routed to pool. Accepts
  arguments:
  - `router_type`: "flat" or "keyprefix". See below for detail.
  - `match_prefix`: defaults to `/(%a+)/`, defines the lua regular expression
    for matching the key prefix.
  - `default_pool`: Name of default pool to use for non-flat routes when the prefix
    does not match.

- `pool{}`: defines a pool of backend memcached instances. This pool may have
  multiple sub-pools defined as zones. This is useful for replicated
instances, backup caches, and so on.
  - `name`: the name of this pool.
  - `distributor`: Overrides the key hash function for this pool or all of its
    zones. See `examples/poolargs.lua`
  - `backends{"ip:port name", "ip:port name2"}`: defines the backends in the
    pool if multiple zones are not being used.
  - `zones{zone1 = {}, zone2 = {}}`: Defines the sub-pools for multi-zone
    configurations. Each zone must have a set of backends listed, ie:
`{ip:port name, ip:port name2}`
  - `zone_distributors{}`: Allows overriding the key hash function on a
    per-zone basis. See `examples/zonedpoolargs.lua`

- `verbose(bool)`: whether to add extra STDOUT prints while building configuration
- `my_zone(string)`: when using "zoned" configurations, this is the zone
  "local" to this router instance, where it will route requests by default
- `say(string)`: will print the string to STDOUT if verbose(true)

## "flat" routing

Flat routing is the simplest form the proxy can take. It allows you to manage
a single pool of memcached instances, abstracting them from your clients.

If zones are defined (see `examples/basic-replicate.lua`) all writes to the
proxy are copies to all defined zones. If an item fails to fetch from the
local zone, it will be fetched instead from the far zone. This allows some
basic redundancy; but it is not strictly consistent and only best-effort.

## "keyprefix" routing

Prefix routing allows you to manage multiple pools of memcached instances.
This can happen as you scale out: certain features need their own cache pools
to avoid trampling the hit rate of everyone else. In this case the front of
the key will be checked and routed to the specific pool: ie
`/foo/original_key` will route to the "foo" named pool. Keys which do not have
a matching prefix (or a prefix at all) will route to `default_pool` if one is
defined.

If zones are defined they work the same as how "flat" routing handles
replication.
