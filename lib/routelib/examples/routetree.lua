verbose(true)
debug(true)

-- A route handler can accept another route handler for any child type entry.
-- This lets you freely compose complex behaviors from simpler route handlers.
pools{
    one = {
        backends = {
            "127.0.0.1:11214",
            "127.0.0.1:11215",
        }
    },
    two = {
        backends = {
            "127.0.0.1:11216",
        }
    },
    three = {
        backends = {
            "127.0.0.1:11217",
        }
    },
}

routes{
    map = {
        foo = route_split{
            child_a = "foo",
            child_b = route_allfastest{
                children = { "two", "three" }
            },
        },
    },
}
