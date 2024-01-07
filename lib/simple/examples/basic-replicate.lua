-- EXAMPLE:
-- Single flat set of servers. Writes replicated to each server.
-- If no sharding is desired, list a single backend in each zone.

-- this extra package.loaded line ensures the 'simple' library gets reloaded when the proxy
-- reloads its configuration. Optional if you never plan on reloading the
-- library.
package.loaded["simple"] = nil
local s = require("simple")

verbose(true)
-- need to set which zone is preferred for reads.
-- if none, it will use the first listed zone
my_zone("z1")

router{
    router_type = "flat",
    log = true,
}

-- set/del/etc replicated to all zones.
-- get starts on local zone, expands on miss
pool{
    name = "default",
    zones = {
      z1 = {
        "127.0.0.1:11212",
        "127.0.0.1:11213",
      },
      z2 = {
        "127.0.0.1:11214",
        "127.0.0.1:11215",
      },
    }
}
