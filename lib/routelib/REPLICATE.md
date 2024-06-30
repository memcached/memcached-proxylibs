# Cache replication with memcached proxy

First, lets discuss why you typically shouldn't be replicating a memcached
cache. Then we will get into the scenarios of replication and their tradeoffs.

## Memcached without replication

For a very gentle explanation about why memcached is not typically replicated,
please [read this story](https://github.com/memcached/memcached/wiki/TutorialCachingStory)

Memcached's designed strength is that adding cache servers to a pool
_increases the total amount of memory available_. Typically the more data you
can cache in memory, the higher your hit rate, the less load on your datastore
or database (or API or etc). If one of those servers breaks, you replace it
with another one and let its cache refill in time. While the server is broken,
you may experience a lowered hit rate.

Typically this means it's important to have enough memcached servers so that
losing any one won't break your service.

However, sometimes replicating a cache can be useful. Or, maybe you want _part
of your cache_ to be replicated. Or, you just want a method of minimizing the
impact of a dead cache server but without replicating the whole cache.

In these cases the memcached proxy can help you.

## Pools vs Backends in the proxy

The proxy thinks in terms of _pools of servers_. In a pool any one key is
hashed and mapped to a specific server in the list of backends (see the above
linked tutorial story). This is useful for many scenarios with larger setups,
for example with pools spanning multiple availability zones in a cloud: Each
zone may have a pool of 3 servers and you wish to make a copy of the cache
available in the remote zone.

If you just have a list of 3 servers and want to copy data to all of them, we
simply "wrap" each server in its own pool. Routelib makes this simple, and if
you ever want to expand the total size of the pools later, you can just add
servers to each pool and the rest of the configuration stays the same.

## What replication means

See the [examples directory](https://github.com/memcached/memcached-proxylibs/tree/main/lib/routelib/examples) in routelib for example files on limited replication methods. The files are named `something-replicate.lua`

In the case of the proxy, replication means that we _blindly copy sets and
deletes to multiple pools_ - This is not a replication system which strongly
synchronizes data between memcached servers, this is a best effort type of
system. This has a lot of benefits and many drawbacks: it's possible for data
to get out of sync, allowing out of date cache entries to be returned. While
it should be very rare, it can happen when servers do fail, so please keep
this in mind.

It's possible to build on top of this system and improve the consistency, or
at least reduce the likelihood of things becoming inconsistent. This document
may expand in the future with more discussion of how this is possible.

In most cases we've seen with replicated caches, people have very simple
needs, so what we've described above will fit well.

## For more complex setups

If you have a large, complex system and want to know what's possible or are
looking for help, memcached has enterprise support available [via
cacheforge](https://www.cacheforge.com) - please let us know what you're
looking for.
