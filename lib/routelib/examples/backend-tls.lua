-- The proxy supports TLS between itself and memcached nodes.
-- For information on TLS in memcached see: https://docs.memcached.org/features/tls/
--
-- To enable support for backend TLS, the proxy _must_ be built with the
-- `--enable-proxy-tls` configure argument.

-- The proxy can be used as a TLS gateway if your native client does not
-- support it.
-- For example: you can run the proxy locally on an application server, having
-- your application connect to it without TLS over localhost.
-- Then the proxy connects over the network to your memcached nodes using TLS.
verbose(true)

settings{
    -- This enables TLS by default for all backends
    backend_use_tls = true
}

pools{
    main = {
        -- If you want only a specific pool to use backend TLS, remove the
        -- above 'backend_use_tls' from settings{} and uncomment the line
        -- below instead.
        --backend_options = { tls = true },
        backends = {
            { host = "127.0.0.1", port = 11212 },
            -- TLS can be enabled on a specific backend by setting
            -- 'tls = true' in the backend definition. This is useful
            -- if you want to test the impact of enabling TLS without enabling
            -- it on the entire pool at once.
            { host = "127.0.0.2", port = 11213, tls = true },
        }
    }
}

-- See other example files for route handling features.
routes{
    cmap = {
        get = route_direct{
            child = "main"
        },
    },
}
