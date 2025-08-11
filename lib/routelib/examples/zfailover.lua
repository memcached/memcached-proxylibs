-- TODO: example of pulling from envvar
-- set our local zone for the file.
local_zone("foo")
--local_zone_from_env("MCZONE")
--local_zone_from_file("./zonefile.txt")

-- "z" versions of route handlers are "zone" aware. If given a set of pools
-- where one is "near" to the router and others are "far", it will attempt to
-- query the "near" one first.
-- If "near" fails all "far" pools are queried in parallel in hopes of quickly
-- finding a good result.
pools{
    -- define our zoned pools as a special "pool set"
    set_ztest = {
        foo = {
            backends = {
                "127.0.0.1:11213",
            }
        },
        baz = {
            backends = {
                "127.0.0.1:11214",
            }
        }
    }
}

routes{
    map = {
        bar = cmdmap{
            get = route_zfailover{
                -- references to a poolset are magically replaced with a
                -- table of pool objects
                children = "set_ztest",
                stats = true,
                miss = true,
                -- it's possible to override local_zone per route.
                -- local_zone = "baz"
            },
            -- route_allsync isn't programmed to understand pool sets, but
            -- can still use it: it's accepting a table of pools and just
            -- doesn't care about the keys.
            set = route_allsync{
                children = "set_ztest"
            },
        },
    },
}
