# -*- coding: utf-8 -*-
# Copyright (C) 2022  Nexedi SA and Contributors.
#                     Kirill Smelkov <kirr@nexedi.com>
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


(*) for example result of stats, ue_get and erab_get queries.
"""

# TODO log file + rotate
# TODO log loading -> DataFrame


from xlte import amari

import json
import traceback
from email.utils import formatdate
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
            optv = tail[1:_-1].split(',')
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
        print(line)

    # jemit emits line corresponding to event to the log.
    def jemit(xl, event, args_dict):
        d = {"event": event, "time": formatdate(time.now())}  # RFC 822 / UTC
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
        try:
            xl._xlog1(conn)
        except Exception as ex:
            d = srv_info.copy()
            d['reason'] = str(ex)
            xl.jemit("service detach", d)
            if not isinstance(ex, amari.ConnError):
                raise


    def _xlog1(xl, conn):
        # emit config_get after attach
        _, cfg_raw = conn.req_('config_get', {})
        xl.emit(cfg_raw)


        # loop emitting requested logspecs
        t0 = time.now()
        tnextv = []     # [i] - next time to arm for logspecv[i] relative to t0
        for l in xl.logspecv:
            tnextv.append(l.period)

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
