verbose(true)

-- if you don't want to route based on prefix, but instead just based on the
-- command used, replace map with cmap when building routes{}
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
    cmap = {
        [mcp.CMD_GET] = route_allfastest{
            children = { "foo" },
        },
    },
    default = route_allfastest{
        children = { "bar" }
    },
}
