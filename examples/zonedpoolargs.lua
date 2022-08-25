local s = require("simple")

verbose(true)
my_zone("z1")

-- here we override the distributor for each zone.
-- setting the hash "seed" value differently per zone means a failed server in
-- z1 will cause misses to reroute across all of z2, instead of a specific
-- machine in z2.
pool{
    name = "foo",
    zones = {
      z1 = {
        "127.0.0.1:11212 z1foo1",
        "127.0.0.1:11213 z1foo2",
      },
      z2 = {
        "127.0.0.1:11214 z2foo1",
        "127.0.0.1:11215 z2foo1",
      },
    },
    zone_distributors = {
        z1 = { dist = mcp.dist_jump_hash, seed = "z1" },
        z2 = { dist = mcp.dist_jump_hash, seed = "z2" },
    }
}
