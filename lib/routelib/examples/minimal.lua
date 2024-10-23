-- Minimal configuration.
-- No high level routing: just hash keys against the pool's list of backends.
-- This should be equivalent to a standard memcached client.

pools{
    main = {
        backends = {
            "127.0.0.1:11214",
            "127.0.0.1:11215",
        }
    }
}

routes{
    default = route_direct{ child = "main" }
}
