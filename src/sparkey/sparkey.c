/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
* Copyright (c) 2023, Cache Forge LLC, All rights reserved.
* Alan Kasindorf <alan@cacheforge.com>
*
*  Use and distribution licensed under the BSD license.  See
*  the LICENSE file for full text.
*
*/

#include <stdlib.h>
#include <string.h>
#include <lua.h>
#include <lualib.h>
#include <lauxlib.h>

#include <sparkey.h>

// TODO: if partitions can never go over 256, could use a pearson perfect hash instead
// of the full xxhash lib.
//#define XXH_INLINE_ALL
//#include "xxhash.h"

#define LOGFILE "toast.spl"
#define HASHFILE "toast.spi"
#define VALUEMAX 512

struct lsparkey_s {
    sparkey_hashreader *reader;
    sparkey_logreader *logreader;
};

int luaopen_sparkey(lua_State *L);

static int lsparkey_poolopen(lua_State *L) {
    // TODO:
    // - need decisions on how to manage open handles and push them to route
    // handlers.
    // - probably best to use mutexed global structs and have the route
    // handles use reference counters on the file handles.
    // example: sparkey mmap is opened in poolopen
#ifdef __DISABLED_CODE
    if (lua_getglobal(L, "__sparkey") == LUA_TNIL) {
        lua_pop(L, 1);
        lua_newtable(L);
        lua_pushvalue(L, -1); // duplicate table reference.
        lua_setglobal(L, "__sparkey");
    }

    luaL_checktype(L, 1, LUA_TTABLE);
    // stack: 1: arg table. 2: filehandle table
    lua_pushnil(L); // start iterator
    while (lua_next(L, 1) != 0) {
        // key is -2, val is -1.
        const char *key = lua_tostring(L, -2);
        const char *option = lua_tostring(L, -1);
        struct lsparkey_s *s = NULL;

        lua_pushvalue(L, -2); // copy key to top.
        lua_gettable(L, 2); // pops key, value is now -1.
        if (lua_isnil(L, -1)) {
            fprintf(stderr, "Got nil from cache lookup\n");
        } else {
            s = lua_touserdata(L, -1);
        }
        lua_pop(L, 1);

        // options:
        // - read (open in read mode)
        // - write (open in write mode)
        // - close (close out)
        if (strcmp(option, "read") == 0) {
            if (s != NULL) {
                // nothing to do.
            } else {
                s = lua_newuserdatauv(L, sizeof(struct lsparkey_s), 0);
                // TODO: need to merge key into .spi and .spl for hashfile and
                // logfile.
            }
        } else if (strcmp(option, "write") == 0) {
            // TODO: ignored for now. need shared data pointer for global
            // locking.
        } else if (strcmp(option, "close") == 0) {
            if (s != NULL) {
                sparkey_hash_close(&s->reader);
                // TODO: delete from cache.
            }
        }
    }
#endif

    return 0;
}

// Does not support reloads. Only initializes the data once.
static int lsparkey_routeopen(lua_State *L) {
    const char *hashname = lua_tostring(L, 1);
    const char *hashlog = lua_tostring(L, 2);

    if (lua_getglobal(L, "__sparkey") == LUA_TNIL) {
        struct lsparkey_s *s = calloc(1, sizeof(*s));
        if (sparkey_hash_open(&s->reader, hashname, hashlog) != 0) {
            lua_pushstring(L, "sparkey: failed to open file");
            lua_error(L);
        }
        s->logreader = sparkey_hash_getreader(s->reader);
        lua_pushlightuserdata(L, s);
        lua_setglobal(L, "__sparkey");
    }
    return 0;
}

static int lsparkey_get(lua_State *L) {
    if (lua_getglobal(L, "__sparkey") == LUA_TNIL) {
        lua_pushstring(L, "sparkey: no opened sparkey file to read");
        lua_error(L);
    }
    struct lsparkey_s *s = lua_touserdata(L, -1);
    lua_pop(L, 1);

    sparkey_logiter *iter;
    sparkey_logiter_create(&iter, s->logreader); // NEED PER REQUEST

    uint64_t keylen = 0;
    const char *key = lua_tolstring(L, -1, &keylen);

    sparkey_hash_get(s->reader, (uint8_t *)key, keylen, iter);

    if (sparkey_logiter_state(iter) != SPARKEY_ITER_ACTIVE) {
        lua_pushnil(L);
    } else {
        luaL_Buffer b;
        uint64_t vlen = sparkey_logiter_valuelen(iter);
        luaL_buffinitsize(L, &b, vlen);
        uint8_t *res; // non-modifiable, temporary characters
        while (1) {
            uint64_t read;
            sparkey_logiter_valuechunk(iter, s->logreader, VALUEMAX, &res, &read);
            if (read > 0) {
                luaL_addlstring(&b, (char *)res, read);
            } else {
                break;
            }
        }
        luaL_pushresult(&b);
    }

    sparkey_logiter_close(&iter); // NEED PER REQUEST
    return 1;
}

int luaopen_sparkey(lua_State *L) {
    const struct luaL_Reg sparkey_f[] = {
        {"poolopen", lsparkey_poolopen},
        {"routeopen", lsparkey_routeopen},
        {"get", lsparkey_get},
        {NULL, NULL},
    };

    luaL_newlib(L, sparkey_f);

    return 1;
}
