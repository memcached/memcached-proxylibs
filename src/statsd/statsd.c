/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
* Copyright (c) 2024, Cache Forge LLC, All rights reserved.
* Alan Kasindorf <alan@cacheforge.com>
*
*  Use and distribution licensed under the BSD license.  See
*  the LICENSE file for full text.
*
*/

/* TODO:
 * - namespace prefix
 * - sample rate
 * - some missing error handling
 * - variable max payload (for jumbo or localhost frames)
 * - take second table of dynamic tags
 */

#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <stdbool.h>

#include <lua.h>
#include <lualib.h>
#include <lauxlib.h>

// FIXME: don't hardcode this. 8kb might work if jumbo frames are allowed?
// localhost mtu is like 64k, could be even more huge.
#define UDP_MAX_PAYLOAD 1400
#define METATABLE_NAME "mod.statsd"

struct _statsd_s {
    int sock;
    int used;
    bool autoflush;
    char *ns; // namespace
    size_t nslen;
    char pkt[UDP_MAX_PAYLOAD];
};

int luaopen_statsd(lua_State *L);

// Does not return.
// Feels like I'm missing the good builtin for this... but those funcs all
// refer to a function arg instead of an index and I don't feel like digging
// into the source to see if there's a real difference right now.
static void l_err(lua_State *L, const char *s) {
    lua_pushstring(L, s);
    lua_error(L);
}

// TODO: return code for errors.
static void _statsd_connect(struct _statsd_s *sd, const char *host, const char *port) {
    struct addrinfo hints;
    struct addrinfo *ai;
    struct addrinfo *next;

    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_DGRAM;

    int res = getaddrinfo(host, port, &hints, &ai);
    if (res != 0) {
        hints.ai_family = AF_INET6;
        res = getaddrinfo(host, port, &hints, &ai);
        if (res != 0) {
            // TODO: error code
            return;
        }
    }

    int sock = -1;
    for (next = ai; next != NULL; next = next->ai_next) {
        sock = socket(next->ai_family, next->ai_socktype, next->ai_protocol);
        if (sock == -1)
            continue;

        // lets connect the UDP socket so we can call send or sendmsg
        if (connect(sock, next->ai_addr, next->ai_addrlen) == 0) {
            break;
        }
        close(sock);
    }

    if (ai) {
        freeaddrinfo(ai);
    }

    // unable to bind socket.
    if (next == NULL) {
        // TODO: error code
        return;
    }

    sd->sock = sock;
}

// takes a table, accepts:
// { host, port, namespace, autoflush }
static int statsd_new(lua_State *L) {
    luaL_checktype(L, 1, LUA_TTABLE);
    const char *host = NULL;
    const char *port = NULL;
    int type = 0;
    struct _statsd_s *sd = NULL;
    int autoflush = 0;
    
    if (lua_getfield(L, -1, "host") == LUA_TSTRING) {
        host = lua_tostring(L, -1);
    } else {
        l_err(L, "statsd client requires 'host' argument");
    }
    lua_pop(L, 1);

    if (lua_getfield(L, -1, "port") != LUA_TNIL) {
        port = lua_tostring(L, -1);
    } else {
        // FIXME: statsd default port?
        l_err(L, "statsd client requires 'port' argument");
    }
    lua_pop(L, 1);

    size_t nslen = 0;
    const char *ns = NULL;
    type = lua_getfield(L, -1, "namespace");
    if (type != LUA_TNIL) {
        if (type == LUA_TSTRING) {
            ns = lua_tolstring(L, -1, &nslen);
        } else {
            l_err(L, "stats client arg 'namespace' must be a string");
        }
    }
    lua_pop(L, 1);

    type = lua_getfield(L, -1, "autoflush");
    if (type != LUA_TNIL) {
        if (type == LUA_TBOOLEAN) {
            autoflush = lua_toboolean(L, -1);
        } else {
            l_err(L, "stats client arg 'autoflush' must be a boolean");
        }
    }
    lua_pop(L, 1);

    sd = lua_newuserdatauv(L, sizeof(*sd), 0);
    memset(sd, 0, sizeof(*sd));
    if (autoflush) {
        sd->autoflush = true;
    }
    if (ns && nslen > 0) {
        int need_dot = 0;
        if (ns[nslen-1] != '.') {
            need_dot = 1;
        }

        sd->ns = calloc(1, nslen+need_dot);
        if (sd->ns == NULL) {
            l_err(L, "stats client failed to allocate namespace memory");
        }
        memcpy(sd->ns, ns, nslen);
        if (need_dot) {
            sd->ns[nslen] = '.';
        }
        sd->nslen = nslen+need_dot;
    }

    luaL_setmetatable(L, METATABLE_NAME);
    _statsd_connect(sd, host, port);

    return 1;
}

// TODO: haven't used UDP in a while... need to refresh on possible errors for
// connect()'ed UDP and then change the functions to bubble up errors.
static void _statsd_flush(struct _statsd_s *sd) {
    // we're connected and using a linear buffer. standard send should work.
    int sent = send(sd->sock, sd->pkt, sd->used, 0);
    // TODO: check for error or underrun.
    sd->used = 0;
}

// simplifying the tag code a little. we avoid double copying most of the
// stat, but will double copy the "extra dyanmic tags"
#define TAG_EXTRA_MAX 200

static void _statsd_stat(lua_State *L, const char *type) {
    struct _statsd_s *sd = lua_touserdata(L, 1);
    size_t klen = 0;
    const char *key = luaL_checklstring(L, 2, &klen);
    lua_Integer vali = 0;
    lua_Number valf = 0;
    bool integer = true;
    if (lua_isinteger(L, 3)) {
        vali = lua_tointeger(L, 3);
    } else {
        // floats should be less common for us, if they exist at all.
        // so don't mind the extra processing.
        int isnum = 0;
        valf = lua_tonumberx(L, 3, &isnum);
        if (!isnum) {
            l_err(L, "must pass integer or float as metric value");
        }
        integer = false;
    }

    size_t tlen = 0;
    const char *tlist = luaL_optlstring(L, 4, NULL, &tlen);

#ifdef _UNFINISHED
    char tagextra[TAG_EXTRA_MAX];
    char *tagend = tagextra + TAG_EXTRA_MAX;
    // TODO: Need to decide on if this is an array table or key/val table
    // would be much simpler/faster to stick to an array but caller would have
    // to be careful to not generate 'k:v' garbage on every request.
    if (lua_istable(L, 5)) {
        lua_pushnil(L); // start iterating
        while (lua_next(L, 5) != 0) {
            size_t tglen = 0;
            const char *tgname = lua_tolstring(L, -1, &tglen);

            lua_pop(L, 1); // drop value, keep key
        }
    }
#endif

    // 20 -> max value length as string + 10 extra chars for control
    // characters
    if (klen + 30 + tlen + sd->nslen > (UDP_MAX_PAYLOAD - sd->used)) {
        _statsd_flush(sd);
    }

    char *pkt_start;
    char *pkt = pkt_start = sd->pkt;
    if (sd->used) {
        pkt += sd->used+1;
        // already something in the buffer, make a separator.
        *pkt = '\n';
        pkt++;
    }

    // <METRIC_NAME>:<VALUE>|<TYPE>|@<SAMPLE_RATE>|#<TAG_KEY_1>:<TAG_VALUE_1>,<TAG_2>
    // pass-thru type for now.
    // sample rate not supported for now.
    if (sd->ns) {
        // using a namespace
        memcpy(pkt, sd->ns, sd->nslen);
        pkt += sd->nslen;
    }

    memcpy(pkt, key, klen);
    pkt += klen;
    *pkt = ':';
    pkt++;

    // TODO; import the itoa_ljust lib for faster integer counters.
    if (integer) {
        pkt += snprintf(pkt, 20, "%lld", vali);
    } else {
        pkt += snprintf(pkt, 20, "%.5f", valf);
    }

    *pkt = '|';
    pkt++;
    // TODO: lets turn the type into a map with the size pre-calced?
    size_t typelen = strlen(type);
    memcpy(pkt, type, typelen);
    pkt += typelen;

    // add pre-fab tag list.
    if (tlen > 0) {
        *pkt = '|';
        *(pkt+1) = '#';
        pkt += 2;
        memcpy(pkt, tlist, tlen);
        pkt += tlen;
    }

    // advance the buffer.
    sd->used += pkt - pkt_start;

    if (sd->autoflush) {
        _statsd_flush(sd);
    }
}

static int statsd_gc(lua_State *L) {
    struct _statsd_s *sd = lua_touserdata(L, 1);
    if (sd->ns) {
        free(sd->ns);
        sd->ns = NULL;
    }
    close(sd->sock);
    return 0;
}

static int statsd_gauge(lua_State *L) {
    _statsd_stat(L, "g");
    return 0;
}

static int statsd_count(lua_State *L) {
    _statsd_stat(L, "c");
    return 0;
}

static int statsd_flush(lua_State *L) {
    struct _statsd_s *sd = lua_touserdata(L, 1);
    if (sd->used > 0) {
        _statsd_flush(sd);
    } 
    return 0;
}

int luaopen_statsd(lua_State *L) {
    const struct luaL_Reg statsd_f[] = {
        {"new", statsd_new},
        {NULL, NULL},
    };
    const struct luaL_Reg statsd_m[] = {
        {"__gc", statsd_gc},
        {"gauge", statsd_gauge},
        {"count", statsd_count},
        {"flush", statsd_flush},
        {NULL, NULL},
    };

    // create metatable and set meta methods
    luaL_newmetatable(L, METATABLE_NAME);
    lua_pushvalue(L, -1);
    lua_setfield(L, -2, "__index");
    luaL_setfuncs(L, statsd_m, 0);
    lua_pop(L, 1);

    luaL_newlib(L, statsd_f);

    return 1;
}
