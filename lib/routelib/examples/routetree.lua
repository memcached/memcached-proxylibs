verbose(true)
debug(true)

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
    map = {
        foo = route_split{
            child_a = "foo",
            child_b = route_allfastest{
                children = { "bar" }
            },
        },
    },
}
