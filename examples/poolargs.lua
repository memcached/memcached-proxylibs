package.loaded["simple"] = nil
local s = require("simple")

-- shorthand for quick defaults
pool{
    name = "foo",
    distributor = { dist = mcp.dist_ring_hash, hash = mcp.dist_ring_hash.hash },
    backends = {"127.0.0.1:11212", "127.0.0.1:11213"},
}

-- Full override of the distributor.
-- possible to load a fully custom module in this file and pass along here.
-- sets a hash seed to change the key distribution of this pool
-- also sets hash filter to only hash parts of the key between { and }
-- characters
pool{
    name = "bar",
    distributor = { dist = mcp.dist_jump_hash, seed = "baz", filter = "tags", filter_conf = "{}" },
    backends = {"127.0.0.1:11214", "127.0.0.1:11215"},
}
