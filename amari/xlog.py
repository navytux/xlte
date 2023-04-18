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

from __future__ import print_function, division, absolute_import

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
#   - "service attach"              when xlog successfully connects to monitored LTE service
#   - "service detach"              when xlog disconnects from monitored LTE service
#   - "service connect failure"     when xlog tries to connect to monitored LTE service
#                                   with unsuccessful result.
#   - "sync"                        emitted periodically and when xlogs starts,
#                                   stops and rotates logs. Comes with current state of
#                                   connection to LTE service and xlog setup
#   - "xlog failure"                on internal xlog error
#
# it is guaranteed that valid xlog stream has a sync event at least every LOS_window entries.
LOS_window = 1000


# Note about log rotation: we rotate output ourselves at sync points.
#
# Rejected alternative: automatic rotation by an external tool, e.g. log_proxy:
#      see https://github.com/metwork-framework/log_proxy
#      and https://superuser.com/questions/291368/log-rotation-of-stdout
#
#      reason for rejection: on every rotation we want to emit "pre-logrotate"
#                            sync to old file + "post-logrotate" sync to new file.


from xlte import amari
from xlte.amari import drb

import json
import traceback
import io
import re
from golang import func, defer, chan, select
from golang import context, sync, time
from golang.gcompat import qq

import logging
import logging.handlers
log = logging.getLogger('xlte.amari.xlog')


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


# IWriter represents output to where xlog writes its data.
# it is created by _openwriter.
class IWriter:
    def writeline(line: str):   "writeline emits and flushes line to destination"
    def need_rotate() -> bool:  "need_rotate returns True when it is time to rotate"
    def rotate():               "rotate performs rotation of destination"
    rotatespec =                "rotatespec indicates rotate specification of the writer"


# xlog queries service @wsuri periodically according to queries specified by
# logspecv and logs the result.
@func
def xlog(ctx, wsuri, w: IWriter, logspecv):
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

    # verify that sync will come at least every LOS_window records
    ns = 0
    for l in logspecv:
        ns += (lsync.period / l.period)
    if ns > LOS_window:
        raise ValueError("meta.sync asked to come ~ every %d entries, "
            "which is > LOS_window (%d)" % (ns, LOS_window))

    # ready to start logging
    xl = _XLogger(wsuri, w, logspecv, lsync.period)

    # emit sync at start/stop
    xl.jemit_sync("detached", "start", {})
    def _():
        xl.jemit_sync("detached", "stop", {})
    defer(_)

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
    def __init__(xl, wsuri, w, logspecv, δt_sync):
        xl.wsuri    = wsuri
        xl.w        = w
        xl.logspecv = logspecv
        xl.δt_sync  = δt_sync       # = logspecv.get("meta.sync").period
        xl.tsync    = float('-inf') # never yet

    # emit saves line to the log.
    def emit(xl, line):
        assert isinstance(line, str)
        assert '\n' not in line, line
        xl.w.writeline(line)

    # jemit emits line corresponding to event to the log.
    def jemit(xl, event, args_dict):
        d = {"event": event, "time": time.now()}  # seconds since epoch
        d.update(args_dict)
        d = {"meta": d}
        xl.emit(json.dumps(d))

    # jemit_sync emits line with sync event to the log.
    # the output is rotated at sync point if it is time to rotate.
    def jemit_sync(xl, state, reason, args_dict):
        tnow = time.now()
        d = {"state":   state,
             "reason":  reason,
             "flags":   "",
             "generator": "xlog %s%s %s" % (
                            '--rotate %s ' % xl.w.rotatespec  if xl.w.rotatespec  else '',
                            xl.wsuri,
                            ' '.join(['%s' % _ for _ in xl.logspecv]))}
        d.update(args_dict)
        rotate = xl.w.need_rotate()
        if rotate:
            d["flags"] = "pre-logrotate"
        xl.jemit("sync", d)
        xl.tsync = tnow
        if rotate:
            xl.w.rotate()
            # emit "post-logrotate" sync right after rotation so that new log
            # chunk starts afresh with sync.
            d["flags"] = "post-logrotate"
            xl.jemit("sync", d)

    # xlog1 performs one cycle of attach/log,log,log.../detach.
    @func
    def xlog1(xl, ctx):
        # emit sync periodically even in detached state
        # this is useful to still know e.g. intended logspec if the service is stopped for a long time
        if time.now() - xl.tsync  >=  xl.δt_sync:
            xl.jemit_sync("detached", "periodic", {})

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
        #
        # on attach, besides name/type/version, also emit everything present
        # in the first ready message from the service. This should include
        # "time" and optionally "utc" for releases ≥ 2022-12-01.
        srv_info = {"srv_name": conn.srv_name,
                    "srv_type": conn.srv_type,
                    "srv_version": conn.srv_version}
        srv_iattach = srv_info.copy()
        for k, v in conn.srv_ready_msg.items():
            if k in {"message", "type", "name", "version"}:
                continue
            srv_iattach["srv_"+k] = v
        xl.jemit("service attach", srv_iattach)
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
        def req_(ctx, query, opts):  # -> (t_rx, resp, resp_raw)
            if query in xmsgsrv_dict:
                query_xsrv = xmsgsrv_dict[query]
                resp, resp_raw = query_xsrv.req_(ctx, opts)
            else:
                resp, resp_raw = conn.req_(ctx, query, opts)
            return (time.now(), resp, resp_raw)

        # loop emitting requested logspecs
        t0 = time.now()
        tnextv = [0]*len(xl.logspecv)   # [i] - next time to arm for logspecv[i] relative to t0

        t_rx     = conn.t_srv_ready_msg           # time  of last received message
        srv_time = conn.srv_ready_msg["time"]     # .time in ----//----
        srv_utc  = conn.srv_ready_msg.get("utc")  # .utc  in ----//----  (present ≥ 2022-12-01)

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
                # emit sync with srv_time and srv_utc approximated from last
                # rx'ed message and local clock run since that reception
                tnow = time.now()
                isync = srv_info.copy()
                isync["srv_time"] = srv_time + (tnow - t_rx)
                if srv_utc is not None:
                    isync["srv_utc"]  = srv_utc + (tnow - t_rx)
                xl.jemit_sync("attached", "periodic", isync)

            else:
                t_rx, resp, resp_raw = req_(ctx, logspec.query, opts)
                srv_time = resp["time"]
                srv_utc  = resp.get("utc")
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


# _openwriter opens destination log file for writing.
# the file is configured to be logrotated according to rotatespec.
def _openwriter(path: str, rotatespec) -> IWriter:
    if rotatespec is None:
        return _PlainWriter(path)

    # parse rotatespec
    # <X>(KB|MB|GB|sec|min|hour|day)[.nbackup]
    m = re.match(r"(?P<X>[0-9]+)((?P<size>[KMG]B)|(?P<time>(sec|min|hour|day)))"
                 r"\.(?P<nbackup>[0-9]+)$", rotatespec)
    if m is None:
        raise ValueError("invalid rotatespec %s" % qq(rotatespec))

    x       = int(m.group("X"))
    nbackup = int(m.group("nbackup"))
    size    = m.group("size")
    time    = m.group("time")

    kw = {}
    kw["backupCount"] = nbackup
    if size is not None:
        kw["maxBytes"] = x * {'KB':1<<10, 'MB':1<<20, 'GB':1<<30}[size]
        logh = logging.handlers.RotatingFileHandler(path, **kw)
    else:
        assert time is not None
        kw["interval"] = x
        kw["when"] = {'sec':'S', 'min':'M', 'hour':'H', 'day':'D'}[time]
        logh = logging.handlers.TimedRotatingFileHandler(path, utc=True, **kw)

    return _RotatingWriter(logh, rotatespec)

# _PlainWriter implements writer that emits data to plain file without rotation.
class _PlainWriter(IWriter):
    def __init__(w, path):
        w.f = open(path, "w")

    def writeline(w, line: str):
        w.f.write(line+'\n')
        w.f.flush()

    def need_rotate(w): return False
    def rotate(w):      pass
    rotatespec =        None

# _RotatingWriter implements writer on top logging's RotatingFileHandler or TimedRotatingFileHandler.
class _RotatingWriter(IWriter):
    def __init__(w, logh: logging.handlers.BaseRotatingHandler, rotatespec: str):
        w.logh = logh
        w.rotatespec = rotatespec
        logh.format = lambda line: line  # tune logging not to add its headers

    def writeline(w, line: str):
        # go directly to underlying FileHandler.emit to skip automatic rollover
        # in BaseRotatingHandler.emit . Note: emit adds '\n' and does flush.
        logging.FileHandler.emit(w.logh, line)

    def need_rotate(w): return w.logh.shouldRollover('')
    def rotate(w):      w.logh.doRollover()


# ----------------------------------------

# Reader wraps IO reader to read information generated by xlog.
#
# Use .read() to retrieve xlog entries.
# Use .close() when done.
#
# The reader must provide .readline() method.
# The ownership of wrapped reader is transferred to the Reader.
class ParseError(RuntimeError): pass    # an entry could not be parsed
class LOSError(RuntimeError): pass      # loss of synchronization
class Reader:
    # ._r        underlying IO reader
    # ._lineno   current line number
    # ._sync     sync(attached) covering current message(s) | None
    #            for a message M sync S covering it can come in the log both before and after M
    #            S covers M if there is no other event/error E in between S and M
    # ._n_nosync for how long we have not seen a sync
    # ._emsgq    [](Message|Event|Exception)
    #            queue for messages/events/... while we are reading ahead to look for sync
    #            non-message could be only at tail
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

# SyncEvent specializes Event and represents "sync" event in xlog.
class SyncEvent(Event):
    # .state
    # .reason
    # .generator
    # .srv_time     | None if not present
    pass


# Reader(r) creates new reader that will read xlog data from r.
#
# if reverse=True xlog entries are read in reverse order from end to start.
@func(Reader)
def __init__(xr, r, reverse=False):
    if reverse:
        r = _ReverseLineReader(r)
    xr._r = r
    xr._lineno = 0
    xr._sync = None
    xr._n_nosync = 0
    xr._emsgq = []

# close release resources associated with the Reader.
@func(Reader)
def close(xr):
    xr._r.close()

# read returns next xlog entry or None at EOF.
@func(Reader)
def read(xr): # -> Event|Message|None
    while 1:
        # flush what we queued during readahead
        if len(xr._emsgq) > 0:
            x = xr._emsgq.pop(0)

            # event/error
            if not isinstance(x, Message):
                for _ in xr._emsgq:  # non-message could be only at tail
                    assert not isinstance(_, Message), _
                if isinstance(x, SyncEvent) and x.state == "attached":
                    assert xr._sync is x  # readahead should have set it
                else:
                    # attach/detach/sync(detached)/error separate sync from other messages
                    xr._sync = None
                if isinstance(x, Exception):
                    raise x
                return x

            # message
            assert isinstance(x, Message)

            # provide timestamps for xlog messages generated with eNB < 2022-12-01
            # there messages come without .utc field and have only .time
            # we estimate the timestamp from .time and from δ(utc,time) taken from covering sync
            if x.timestamp is None:
                if xr._sync is not None  and  xr._sync.srv_time is not None:
                    # srv_utc' = srv_time' + (time - srv_time)
                    srv_time_ = x.get1("time", (float,int))  # ParseError if not present
                    x.timestamp = srv_time_ + (xr._sync.timestamp - xr._sync.srv_time)
                if x.timestamp is None:
                    raise ParseError("%s:%d/%s no `utc` and cannot compute "
                            "timestamp with sync" % (x.pos[0], x.pos[1], '/'.join(x.path)))

            # TODO verify messages we get/got against their schedule in covering sync.
            #      Raise LOSError (loss of synchronization) if what we actually see
            #      does not match what sync says it should be.

            return x
        assert len(xr._emsgq) == 0

        # read next message/event/... potentially reading ahead while looking for covering sync
        while 1:
            try:
                x = xr._read1()
            except Exception as e:
                x = e

            # if we see EOF - we return it to outside only if the queue is empty
            # otherwise it might be that readahead reaches EOF early, but at
            # the time when queue flush would want to yield it to the user, the
            # stream might have more data.
            if x is None:
                if len(xr._emsgq) == 0:
                    return None
                else:
                    break # flush the queue

            xr._emsgq.append(x)

            # if we see sync(attached) - it will cover future messages till next
            # event, and messages that are already queued
            if isinstance(x, SyncEvent):
                xr._n_nosync = 0
                if x.state == "attached":
                    xr._sync = x
            else:
                xr._n_nosync += 1
                if xr._n_nosync > LOS_window:
                    xr._emsgq.append(LOSError("no sync for %d entries" % xr._n_nosync))

            if isinstance(x, Message):
                if xr._sync is None:    # have message and no sync -
                    continue            # - continue to read ahead to find it

            # message with sync or any event - flush the queue
            break

# _read1 serves read by reading one next raw entry from the log.
# it does not detect loss of synchronization.
@func(Reader)
def _read1(xr):
    x = xr._jread1()
    if x is None:
        return None

    if "meta" in x:
        x.__class__ = Event
        meta = x.get1("meta", dict)
        x.event, x.timestamp = xr._parse_metahead(meta)
        if x.event in {"sync", "start"}:  # for backward compatibility with old logs meta:start
            x.__class__ = SyncEvent       # is reported to users as sync(start) event
            x.generator = meta.get1("generator", str)
            if x.event == "start":
                x.state  = "detached"
                x.reason = "start"
            else:
                x.state  = meta.get1("state",  str)
                x.reason = meta.get1("reason", str)
            x.srv_time = None
            if "srv_time" in meta:
                x.srv_time = meta.get1("srv_time", (float,int))
            # TODO parse generator -> .logspecv
        return x

    if "message" in x:
        x.__class__ = Message
        x.message   = x.get1("message", str)
        # NOTE .time is internal eNB time using clock originating at eNB startup.
        #      .utc is seconds since epoch counted using OS clock.
        #      .utc field was added in 2022-12-01 - see https://support.amarisoft.com/issues/21934
        # if there is no .utc - we defer computing .timestamp to ^^^ read
        # where it might estimate it from .time and sync
        if "utc" in x:
            x.timestamp = x.get1("utc", (float,int))
        else:
            x.timestamp = None
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
    xr._lineno +=  1  if not isinstance(xr._r, _ReverseLineReader) else \
                  -1
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


# _ReverseLineReader serves xlog.Reader by wrapping an IO reader and reading
# lines from the underlying reader in reverse order.
#
# Use .readline() to retrieve lines from end to start.
# Use .close() when done.
#
# The original reader is required to provide both .readline() and .seek() so
# that backward reading could be done efficiently.
#
# Original reader can be opened in either binary or text mode -
# _ReverseLineReader will provide read data in the same mode as original.
#
# The ownership of wrapped reader is transferred to _ReverseLineReader.
class _ReverseLineReader:
    # ._r           underlying IO reader
    # ._bufsize     data is read in so sized chunks
    # ._buf         current buffer
    # ._bufpos      ._buf corresponds to ._r[_bufpos:...]

    def __init__(rr, r, bufsize=None):
        rr._r = r
        if bufsize is None:
            bufsize = 8192
        rr._bufsize = bufsize

        r.seek(0, io.SEEK_END)
        rr._bufpos = r.tell()
        if hasattr(r, 'encoding'):  # text
            rr._buf   =  ''
            rr._lfchr =  '\n'
            rr._str0  =  ''
        else:                       # binary
            rr._buf   = b''
            rr._lfchr = b'\n'
            rr._str0  = b''

        if hasattr(r, 'name'):
            rr.name = r.name

    # close releases resources associated with the reader.
    def close(rr):
        rr._r.close()

    # readline reads next line from underlying stream.
    # the lines are read backwards from end to start.
    def readline(rr):  # -> line | ø at EOF
        chunkv = []
        while 1:
            # time to load next buffer
            if len(rr._buf) == 0:
                bufpos  = max(0, rr._bufpos - rr._bufsize)
                bufsize = rr._bufpos - bufpos
                if bufsize == 0:
                    break
                rr._r.seek(bufpos, io.SEEK_SET)
                rr._buf = _ioreadn(rr._r, bufsize)
                rr._bufpos = bufpos

            assert len(rr._buf) > 0

            # let's scan to the left where \n is
            lf = rr._buf.rfind(rr._lfchr)
            if lf == -1:  # no \n - queue whole buf
                chunkv.insert(0, rr._buf)
                rr._buf = rr._buf[:0]
                continue

            if len(chunkv) == 0  and  lf+1 == len(rr._buf):  # started reading from ending \n
                chunkv.insert(0, rr._buf[lf:])
                rr._buf = rr._buf[:lf]
                continue

            chunkv.insert(0, rr._buf[lf+1:])  # \n of previous line found - we are done
            rr._buf = rr._buf[:lf+1]
            break

        return rr._str0.join(chunkv)


# _ioreadn reads exactly n elements from f.
def _ioreadn(f, n):
    l = n
    data =  ''  if hasattr(f, 'encoding')  else \
           b''
    while len(data) < n:
        chunk = f.read(l)
        data += chunk
        l -= len(chunk)
    return data[:n]  # slice in case it overreads

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
Usage: xamari xlog [OPTIONS] <wsuri> <output> <logspec>+
Maintain extra log for a service.

The service is queried periodically according to logspec and results are saved
in JSON format to output file (see 'xamari help jsonlog').

<wsuri> is URI (see 'xamari help websock') of an Amarisoft-service.
<output> is path to output file.
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

        --rotate <rotatespec>   rotate output approximately according to rotatespec
                                rotatespec is <X>(KB|MB|GB|sec|min|hour|day)[.nbackup]
    -h  --help                  show this help
""" % (LogSpec.DEFAULT_PERIOD,
       '\n'.join("    %-14s %s" % (q, f.xlog_doc1)
                for q, f in sorted(_xmsg_registry.items()))),
file=out)


def main(ctx, argv):
    try:
        optv, argv = getopt.getopt(argv[1:], "h", ["rotate=", "help"])
    except getopt.GetoptError as e:
        print(e, file=sys.stderr)
        usage(sys.stderr)
        sys.exit(2)

    rotatespec = None
    for opt, arg in optv:
        if opt in (      "--rotate"):
            rotatespec = arg

        if opt in ("-h", "--help"):
            usage(sys.stdout)
            sys.exit(0)

    if len(argv) < 3:
        usage(sys.stderr)
        sys.exit(2)

    wsuri  = argv[0]
    output = argv[1]
    logspecv = []
    for arg in argv[2:]:
        logspecv.append( LogSpec.parse(arg) )

    w = _openwriter(output, rotatespec)
    xlog(ctx, wsuri, w, logspecv)
