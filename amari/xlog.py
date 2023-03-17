# -*- coding: utf-8 -*-
# Copyright (C) 2022-2023  Nexedi SA and Contributors.
#                          Kirill Smelkov <kirr@nexedi.com>
#
# This program is free software: you can Use, Study, Modify and Redistribute
# it under the terms of the GNU General Public License version 3, or (at your
# option) any later version, as published by the Free Software Foundation.
#
# You can also Link and Combine this program with other software covered by
# the terms of any of the Free Software licenses or any of the Open Source
# Initiative approved licenses and Convey the resulting work. Corresponding
# source of such a combination shall include the source code for all other
# software used.
#
# This program is distributed WITHOUT ANY WARRANTY; without even the implied
# warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
# See COPYING file for full licensing terms.
# See https://www.nexedi.com/licensing for rationale and options.
"""Package xlog provides additional extra logging facilities to Amarisoft LTE stack.

- use xlog and LogSpec to organize logging of information available via WebSocket access(*).
  The information is logged in JSON Lines format. See 'xamari help xlog' for details.
- use Reader to read logged information from xlog.


(*) for example result of stats, ue_get, erab_get and synthetic queries.
"""

# XLog protocol
#
# XLog contains entries of 2 kinds:
#
#   1. events related to xlog operation, and
#   2. results of the queries.
#
# Events come as                    {"meta": {"event": "<EVENT-NAME>", ...}.
# Results of the queries come as    {"message": "<QUERY-NAME>", ...}
#
# Queries are specific to monitored LTE service.
# Events  are specific to xlog itself and can be as follows:
#
#   - "start"                       when xlog starts
#   - "service attach"              when xlog successfully connects to monitored LTE service
#   - "service detach"              when xlog disconnects from monitored LTE service
#   - "service connect failure"     when xlog tries to connect to monitored LTE service
#                                   with unsuccessful result.
#   - "sync"                        emitted periodically with current state of
#                                   connection to LTE service and xlog setup
#   - "xlog failure"                on internal xlog error


# TODO log file + rotate
#
# Rejected alternative: automatic rotation by an external tool, e.g. log_proxy:
#      see https://github.com/metwork-framework/log_proxy
#      and https://superuser.com/questions/291368/log-rotation-of-stdout
#
#      reason for rejection: on every rotation we want to emit "end of file"
#                            entries to old file + header to new file.


from xlte import amari
from xlte.amari import drb

import json
import traceback
from golang import func, defer, chan, select
from golang import context, sync, time
from golang.gcompat import qq

import logging; log = logging.getLogger('xlte.amari.xlog')


# LogSpec represents one specification of what to log.
# For example stats[rf]/10s.
class LogSpec:
    # .query        e.g. 'stats'
    # .optv         [] with flags to send with query
    # .period       how often to issue the query (seconds)

    DEFAULT_PERIOD = 60

    def __init__(spec, query, optv, period):
        spec.query  = query
        spec.optv   = optv
        spec.period = period

    def __str__(spec):
        return "%s[%s]/%ss" % (spec.query, ','.join(spec.optv), spec.period)

    # LogSpec.parse parses text into LogSpec.
    @staticmethod
    def parse(text):
        def bad(reason):
            raise ValueError("invalid logspec %s: %s" % (qq(text), reason))

        optv = []
        period = LogSpec.DEFAULT_PERIOD
        query = text
        _ = query.rfind('/')
        if _ != -1:
            tail  = query[_+1:]
            query = query[:_]
            if not tail.endswith('s'):
                bad("invalid period")
            try:
                period = float(tail[:-1])
            except ValueError:
                bad("invalid period")

        _ = query.find('[')
        if _ != -1:
            tail  = query[_:]
            query = query[:_]
            _ = tail.find(']')
            if _ == -1:
                bad("missing closing ]")
            optv = tail[1:_].split(',')
            tail = tail[_+1:]

        for c in '[]/ ':
            if c in query:
                bad("invalid query")

        return LogSpec(query, optv, period)


# xlog queries service @wsuri periodically according to queries specified by
# logspecv and logs the result.
def xlog(ctx, wsuri, logspecv):
    # make sure we always have meta.sync - either the caller specifies it
    # explicitly, or we add it automatically to come first with default
    # 10x·longest periodicity. Do the same about config_get - by default we
    # want it to be present after every sync.
    lsync       = None
    isync       = None
    lconfig_get = None
    pmax = 1
    for (i,l) in enumerate(logspecv):
        pmax = max(pmax, l.period)
        if l.query == "meta.sync":
            isync = i
            lsync = l
        if l.query == "config_get":
            lconfig_get = l
    logspecv = logspecv[:]  # keep caller's intact
    if lsync is None:
        isync = 0
        lsync = LogSpec("meta.sync", [], pmax*10)
        logspecv.insert(0, lsync)
    if lconfig_get is None:
        logspecv.insert(isync+1, LogSpec("config_get", [], lsync.period))


    xl = _XLogger(wsuri, logspecv, lsync.period)

    slogspecv = ' '.join(['%s' % _ for _ in logspecv])
    xl.jemit("start", {"generator": "xlog %s %s" % (wsuri, slogspecv)})

    while 1:
        try:
            xl.xlog1(ctx)
        except Exception as ex:
            if ctx.err() is not None:
                raise
            if not isinstance(ex, amari.ConnError):
                log.exception('xlog failure:')
                try:
                    xl.jemit("xlog failure", {"traceback": traceback.format_exc()})
                except:
                    # e.g. disk full in xl.jemit itself
                    log.exception('xlog failure (second level):')

        δt_reconnect = min(3, lsync.period)
        _, _rx = select(
            ctx.done().recv,                # 0
            time.after(δt_reconnect).recv,  # 1
        )
        if _ == 0:
            raise ctx.err()

# _XLogger serves xlog implementation.
class _XLogger:
    def __init__(xl, wsuri, logspecv, δt_sync):
        xl.wsuri    = wsuri
        xl.logspecv = logspecv
        xl.δt_sync  = δt_sync     # = logspecv.get("meta.sync").period
        xl.tsync    = time.now()  # first `start` serves as sync

    # emit saves line to the log.
    def emit(xl, line):
        assert isinstance(line, str)
        assert '\n' not in line, line
        print(line, flush=True)

    # jemit emits line corresponding to event to the log.
    def jemit(xl, event, args_dict):
        d = {"event": event, "time": time.now()}  # seconds since epoch
        d.update(args_dict)
        d = {"meta": d}
        xl.emit(json.dumps(d))

    # jemit_sync emits line with sync event to the log.
    # TODO logrotate at this point
    def jemit_sync(xl, state, args_dict):
        tnow = time.now()
        d = {"state":   state,
             "generator": "xlog %s %s" % (xl.wsuri, ' '.join(['%s' % _ for _ in xl.logspecv]))}
        d.update(args_dict)
        xl.jemit("sync", d)
        xl.tsync = tnow

    # xlog1 performs one cycle of attach/log,log,log.../detach.
    @func
    def xlog1(xl, ctx):
        # emit sync periodically even in detached state
        # this is useful to still know e.g. intended logspec if the service is stopped for a long time
        if time.now() - xl.tsync  >=  xl.δt_sync:
            xl.jemit_sync("detached", {})

        # connect to the service
        try:
            conn = amari.connect(ctx, xl.wsuri)
        except Exception as ex:
            xl.jemit("service connect failure", {"reason": str(ex)})
            if not isinstance(ex, amari.ConnError):
                raise
            return
        defer(conn.close)

        # emit "service attach"/"service detach"
        srv_info = {"srv_name": conn.srv_name,
                    "srv_type": conn.srv_type,
                    "srv_version": conn.srv_version}
        xl.jemit("service attach", srv_info)
        def _():
            try:
                raise
            except Exception as ex:
                d = srv_info.copy()
                d['reason'] = str(ex)
                xl.jemit("service detach", d)
                if not isinstance(ex, amari.ConnError):
                    raise
        defer(_)

        wg = sync.WorkGroup(ctx)
        defer(wg.wait)

        # spawn servers to handle queries with synthetic messages
        xmsgsrv_dict = {}
        for l in xl.logspecv:
            if l.query in _xmsg_registry:
                xsrv = _XMsgServer(l.query, _xmsg_registry[l.query])
                xmsgsrv_dict[l.query] = xsrv
                xsrv_ready = chan() # wait for xmsg._runCtx to be initialized
                wg.go(xsrv.run, conn, xsrv_ready)
                xsrv_ready.recv()

        # spawn main logger
        wg.go(xl._xlog1, conn, xmsgsrv_dict, srv_info)


    def _xlog1(xl, ctx, conn, xmsgsrv_dict, srv_info):
        # req_ queries either amari service directly, or an extra message service.
        def req_(ctx, query, opts):  # -> resp_raw
            if query in xmsgsrv_dict:
                query_xsrv = xmsgsrv_dict[query]
                _, resp_raw = query_xsrv.req_(ctx, opts)
            else:
                _, resp_raw = conn.req_(ctx, query, opts)
            return resp_raw

        # loop emitting requested logspecs
        t0 = time.now()
        tnextv = [0]*len(xl.logspecv)   # [i] - next time to arm for logspecv[i] relative to t0

        while 1:
            # go through all logspecs in the order they were given
            # pick logspec with soonest arm time
            # execute corresponding query
            #
            # by going logspecs in the order they were given, we execute queries in
            # stable order, e.g. for `stats/10s ue_get/10` - always stats first,
            # then ue_get next.
            #
            # XXX linear search instead of heapq, but len(logspecv) is usually very small.
            tmin = float('inf')
            imin = None
            for i, t in enumerate(tnextv):
                if t < tmin:
                    tmin = t
                    imin = i

            logspec = xl.logspecv[imin]
            tnextv[imin] += logspec.period

            opts = {}
            for opt in logspec.optv:
                opts[opt] = True

            # issue queries with planned schedule
            # TODO detect time overruns and correct schedule correspondingly
            tnow = time.now()
            tarm = t0 + tmin
            δtsleep = tarm - tnow
            if δtsleep > 0:
                _, _rx = select(
                    ctx.done().recv,            # 0
                    time.after(δtsleep).recv,   # 1
                )
                if _ == 0:
                    raise ctx.err()

            if logspec.query == 'meta.sync':
                xl.jemit_sync("attached", srv_info)
            else:
                resp_raw = req_(ctx, logspec.query, opts)
                xl.emit(resp_raw)


# _XMsgServer represents a server for handling particular synthetic requests.
#
# for example the server for synthetic x.drb_stats query.
class _XMsgServer:
    def __init__(xsrv, name, f):
        xsrv.name = name        # str               message name, e.g. "x.drb_stats"
        xsrv._func = f          # func(ctx, conn)   to run the service
        xsrv._reqch = chan()    # chan<respch>      to send requests to the service
        xsrv._runCtx = None     # context           not done while .run is running

    # run runs the extra server on amari service attached to via conn.
    @func
    def run(xsrv, ctx, conn: amari.Conn, ready: chan):
        xsrv._runCtx, cancel = context.with_cancel(ctx)
        defer(cancel)
        ready.close()
        # establish dedicated conn2 so that server does not semantically
        # affect requests issued by main logger. For example if we do not and
        # main logger queries stats, and x.drb_stats server also queries stats
        # internally, then data received by main logger will cover only small
        # random period of time instead of full wanted period.
        conn2 = amari.connect(ctx, conn.wsuri)
        defer(conn2.close)
        xsrv._func(ctx, xsrv._reqch, conn2)

    # req queries the server and returns its response.
    @func
    def req_(xsrv, ctx, opts):  # -> resp, resp_raw
        origCtx = ctx
        ctx, cancel = context.merge(ctx, xsrv._runCtx)  # need only merge_cancel
        defer(cancel)

        respch = chan(1)
        _, _rx = select(
            ctx.done().recv,                        # 0
            (xsrv._reqch.send, (opts, respch)),     # 1
        )
        if _ == 0:
            if xsrv._runCtx.err()  and  not origCtx.err():
                raise RuntimeError("%s server is down" % xsrv.name)
            raise ctx.err()

        _, _rx = select(
            ctx.done().recv,    # 0
            respch.recv,        # 1
        )
        if _ == 0:
            if xsrv._runCtx.err()  and  not origCtx.err():
                raise RuntimeError("%s server is down" % xsrv.name)
            raise ctx.err()
        resp = _rx

        r = {'message': xsrv.name}  # place 'message' first
        r.update(resp)
        resp = r

        resp_raw = json.dumps(resp,
                              separators=(',', ':'),  # most compact, like Amari does
                              ensure_ascii=False)     # so that e.g. δt comes as is
        return resp, resp_raw


# @_xmsg registers func f to provide server for extra messages with specified name.
_xmsg_registry = {} # name -> xsrv_func(ctx, reqch, conn)
def _xmsg(name, f, doc1):
    assert name not in _xmsg_registry
    f.xlog_doc1 = doc1
    _xmsg_registry[name] = f

_xmsg("x.drb_stats", drb._x_stats_srv, "retrieve statistics about data radio bearers")



# ----------------------------------------

# Reader wraps IO reader to read information generated by xlog.
#
# Use .read() to retrieve xlog entries.
# Use .close() when done.
#
# The reader must provide .readline() method.
# The ownership of wrapped reader is transferred to the Reader.
class ParseError(RuntimeError): pass
class Reader:
    # ._r        underlying IO reader
    # ._lineno   current line number
    pass

# xdict represents dict loaded from xlog entry.
#
# Besides usual dict properties it also has information about file position of
# the entry, and the path to the dict - e.g. /message/stats/counters.
class xdict(dict):
    # .pos      (ioname, lineno)
    # .path     ()
    pass

# Event represents one event in xlog.
class Event(xdict):
    # .event
    # .timestamp    seconds since epoch
    pass

# Message represents result of one query in xlog.
class Message(xdict):
    # .message
    # .timestamp    seconds since epoch
    pass


# Reader(r) creates new reader that will read xlog data from r.
@func(Reader)
def __init__(xr, r):
    xr._r = r
    xr._lineno = 0

    # parse header
    try:
        head = xr._jread1()
        if head is None:
            raise xr._err("header: unexpected EOF")
        meta = head.get1("meta", dict)
        ev0, t0 = xr._parse_metahead(meta)
        if ev0 != "start":
            raise xr._err("header: starts with meta.event=%s  ; expected `start`" % ev0)

        gen = meta.get1("generator", str)
        # TODO parse generator -> ._xlogspecv

    except:
        xr._r.close()
        raise

# close release resources associated with the Reader.
@func(Reader)
def close(xr):
    xr._r.close()

# read returns next xlog entry or None at EOF.
@func(Reader)
def read(xr): # -> Event|Message|None
    x = xr._jread1()
    if x is None:
        return None

    if "meta" in x:
        x.__class__ = Event
        meta = x.get1("meta", dict)
        x.event, x.timestamp = xr._parse_metahead(meta)
        return x

    if "message" in x:
        x.__class__ = Message
        x.message   = x.get1("message", str)
        # NOTE .time is internal eNB time using clock originating at eNB startup.
        #      .utc is seconds since epoch counted using OS clock.
        #      .utc field was added in 2022-12-01 - see https://support.amarisoft.com/issues/21934
        x.timestamp = x.get1("utc", (float,int))
        return x

    raise xr._err("invalid xlog entry")


# _err returns ParseError with lineno prefix.
@func(Reader)
def _err(xr, text):
    return ParseError("%s:%d : %s" % (_ioname(xr._r), xr._lineno, text))

# _jread1 reads next line and JSON-decodes it.
# None is returned at EOF.
@func(Reader)
def _jread1(xr): # -> xdict|None
    xr._lineno += 1
    try:
        l = xr._r.readline()
    except Exception as e:
        raise xr._err("read") from e

    if len(l) == 0:
        return None # EOF

    try:
        d = json.loads(l)
    except Exception as e:
        raise xr._err("invalid json: %s" % e) from None

    if not isinstance(d, dict):
        raise xr._err("got %s instead of dict" % type(d))

    xd = xdict(d)
    xd.pos  = (_ioname(xr._r), xr._lineno)
    xd.path = ()
    return xd

# _parse_metahead extracts and validates event/time from "meta" entry.
@func(Reader)
def _parse_metahead(xr, meta): # -> event, t
    event = meta.get1("event", str)
    t     = meta.get1("time",  (float,int))
    return event, t


# get1 retrieves d[key] and verifies it is instance of typeok.
@func(xdict)
def get1(xd, key, typeok):
    if key not in xd:
        raise ParseError("%s:%d/%s no `%s`" %
                            (xd.pos[0], xd.pos[1], '/'.join(xd.path), key))
    val = xd[key]
    if not isinstance(val, typeok):
        raise ParseError("%s:%d/%s : got %s  ; expected `%s`" %
                            (xd.pos[0], xd.pos[1], '/'.join(xd.path + (key,)), type(val), typeok))
    if type(val) is dict:
        val = xdict(val)
        val.pos   = xd.pos
        val.path  = xd.path + (key,)
    return val


# _ioname returns name of a file-like f.
def _ioname(f):
    if hasattr(f, 'name'):
        return f.name
    else:
        return ''


# ----------------------------------------
import sys, getopt

summary = "maintain extra log for a service"

def usage(out):
    print("""\
Usage: xamari xlog [OPTIONS] <wsuri> <logspec>+
Maintain extra log for a service.

The service is queried periodically according to logspec and results are saved
in JSON format to a file (see 'xamari help jsonlog').

<wsuri> is URI (see 'xamari help websock') of an Amarisoft-service.
<logspec> is specification of what to log. It has the following parts:

    <query>[<options>]/<period>

The query specifies a message, that should be used to query service. For
example "stats", "ue_get", "erab_get", ... Query part is mandatory.

Options specifies additional flags for the query. Options part can be omitted.

Period specifies periodicity of how often the service should be queried.
Period is optional and defaults to %d seconds.

Example for <logspec>+:

    stats[samples,rf]/30s  ue_get[stats]  erab_get/10s  qos_flow_get

Besides queries supported by Amarisoft LTE stack natively, support for the
following synthetic queries is also provided:

%s

Additionally the following queries are used to control xlog itself:

    meta.sync      specify how often synchronization events are emitted
                   default is 10x the longest period

Options:

    -h  --help            show this help
""" % (LogSpec.DEFAULT_PERIOD,
       '\n'.join("    %-14s %s" % (q, f.xlog_doc1)
                for q, f in sorted(_xmsg_registry.items()))),
file=out)


def main(ctx, argv):
    try:
        optv, argv = getopt.getopt(argv[1:], "h", ["help"])
    except getopt.GetoptError as e:
        print(e, file=sys.stderr)
        usage(sys.stderr)
        sys.exit(2)


    for opt, arg in optv:
        if opt in ("-h", "--help"):
            usage(sys.stdout)
            sys.exit(0)

    if len(argv) < 2:
        usage(sys.stderr)
        sys.exit(2)

    wsuri = argv[0]
    logspecv = []
    for arg in argv[1:]:
        logspecv.append( LogSpec.parse(arg) )

    xlog(ctx, wsuri, logspecv)
