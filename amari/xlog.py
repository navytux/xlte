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


(*) for example result of stats, ue_get and erab_get queries.
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

import json
import traceback
from golang import func, defer
from golang import time
from golang.gcompat import qq

import logging; log = logging.getLogger('xlte.amari.xlog')


# LogSpec represents one specification of what to log.
# For example stats[rf]/10s.
class LogSpec:
    # .query        e.g. 'stats'
    # .optv         [] with flags to send with query
    # .period       how often to issue the query (seconds)

    DEFAULT_PERIOD = 60

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

        spec = LogSpec()
        spec.query  = query
        spec.optv   = optv
        spec.period = period
        return spec


# xlog queries service @wsuri periodically according to queries specified by
# logspecv and logs the result.
def xlog(wsuri, logspecv):
    xl = _XLogger(wsuri, logspecv)

    slogspecv = ' '.join(['%s' % _ for _ in logspecv])
    xl.jemit("start", {"generator": "xlog %s %s" % (wsuri, slogspecv)})

    while 1:
        try:
            xl.xlog1()
        except Exception as ex:
            if not isinstance(ex, amari.ConnError):
                log.exception('xlog failure:')
                try:
                    xl.jemit("xlog failure", {"traceback": traceback.format_exc()})
                except:
                    # e.g. disk full in xl.jemit itself
                    log.exception('xlog failure (second level):')

        time.sleep(3)


class _XLogger:
    def __init__(xl, wsuri, logspecv):
        xl.wsuri    = wsuri
        xl.logspecv = logspecv

    # emit saves line to the log.
    def emit(xl, line):
        assert '\n' not in line, line
        print(line, flush=True)

    # jemit emits line corresponding to event to the log.
    def jemit(xl, event, args_dict):
        d = {"event": event, "time": time.now()}  # seconds since epoch
        d.update(args_dict)
        d = {"meta": d}
        xl.emit(json.dumps(d))

    # xlog1 performs one cycle of attach/log,log,log.../detach.
    @func
    def xlog1(xl):
        # connect to the service
        try:
            conn = amari.connect(xl.wsuri)
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

        xl._xlog1(conn)


    def _xlog1(xl, conn):
        # emit config_get after attach
        _, cfg_raw = conn.req_('config_get', {})
        xl.emit(cfg_raw)


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
                time.sleep(δtsleep)

            _, resp_raw = conn.req_(logspec.query, opts)
            xl.emit(resp_raw)


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


Options:

    -h  --help            show this help
""" % LogSpec.DEFAULT_PERIOD, file=out)


def main(argv):
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

    xlog(wsuri, logspecv)
