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

from xlte.amari.kpi import LogMeasure, LogError, _trace as trace
from xlte.kpi import Measurement
from golang import func, defer, b
import io, json

from pytest import raises


# tLogMeasure provides LogMeasure testing environment.
#
# It organizes IO streams for enb.xlog and (TODO) enb.log, and connects those
# streams to LogMeasure. It then allows to verify Measurements read from LogMeasure.
#
# Use .xlog() to append a line to enb.xlog.
# Use .expect*() to assert that the next read Measurement should have particular values.
# Use .read() to read next measurement and to verify it to match previous expect* calls.
#
# tLogMeasure must be explicitly closed once no longer used.
class tLogMeasure:
    # ._fxlog   IO stream with enb.xlog data
    # ._logm    LogMeasure(._fxlog)
    # ._mok     None|Measurement built via expect* calls

    def __init__(t):
        t._fxlog = io.BytesIO(b"""\
{"meta": {"event": "start", "time": 0.01, "generator": "xlog ws://localhost:9001 stats[]/30.0s"}}
{"meta": {"event": "service attach", "time": 0.02, "srv_name": "ENB", "srv_type": "ENB", "srv_version": "2022-12-01"}}
""")
        t._logm = LogMeasure(t._fxlog, ionone())
        t._mok = None

    # close performs last scheduled checks and makes sure further reading from
    # LogMeasure results in EOF. It then closes it.
    @func
    def close(t):
        defer(t._logm.close)
        t.read()            # verify last expects, if any
        for i in range(10): # now should get EOF
            assert t._mok == None
            t.read()

    # xlog appends one line to enb.xlog.
    def xlog(t, line):
        line = b(line)
        assert b'\n' not in line
        pos = t._fxlog.tell()
        t._fxlog.seek(0, io.SEEK_END)
        t._fxlog.write(b'%s\n' % line)
        t._fxlog.seek(pos, io.SEEK_SET)

    # _mok_init reinitializes ._mok with Measurement defaults.
    def _mok_init(t):
        t._mok = Measurement()
        # init fields handled by amari.kpi to 0
        # this will be default values to verify against
        for field in (
            'RRC.ConnEstabAtt.sum',
            'RRC.ConnEstabSucc.sum',
            'S1SIG.ConnEstabAtt',
            'S1SIG.ConnEstabSucc',
            'ERAB.EstabInitAttNbr.sum',
            'ERAB.EstabInitSuccNbr.sum',
            'ERAB.EstabAddAttNbr.sum',
            'ERAB.EstabAddSuccNbr.sum',
        ):
            t._mok[field] = 0

    # expect1 requests to verify one field to have expected value.
    # the verification itself will happen on next read() call.
    def expect1(t, field, vok):
        if t._mok is None:
            t._mok_init()
        t._mok[field] = vok

    # expect_nodata requests to verify all fields besides timestamp-related to be NA.
    def expect_nodata(t):
        if t._mok is None:
            t._mok = Measurement()
        tstart = t._mok['X.Tstart']
        δt     = t._mok['X.δT']
        t._mok = Measurement() # reinit with all NA
        t._mok['X.Tstart']  = tstart
        t._mok['X.δT']      = δt

    # read retrieves next measurement from LogMeasure and verifies it to be as expected.
    def read(t): # -> Measurement
        try:
            m = t._logm.read()
            assert type(m) is type(t._mok)
            assert m == t._mok
            return m
        finally:
            t._mok = None


# verify LogMeasure works ok on normal input.
@func
def test_LogMeasure():
    t = tLogMeasure()
    defer(t.close)
    _ = t.expect1

    # empty stats after first attach
    t.xlog( jstats(0.7, {}) )
    _('X.Tstart',                   0.02)
    _('X.δT',                       0.7-0.02)
    t.expect_nodata()
    t.read()

    # further empty stats
    t.xlog( jstats(1.0, {}) )
    _('X.Tstart',                   0.7)
    _('X.δT',                       1-0.7)
    _('RRC.ConnEstabAtt.sum',       0)
    _('RRC.ConnEstabSucc.sum',      0)
    _('S1SIG.ConnEstabAtt',         0)
    _('S1SIG.ConnEstabSucc',        0)
    _('ERAB.EstabInitAttNbr.sum',   0)
    _('ERAB.EstabInitSuccNbr.sum',  0)
    _('ERAB.EstabAddAttNbr.sum',    0)
    _('ERAB.EstabAddSuccNbr.sum',   0)

    # tstats is the verb to check handling of stats message.
    #
    # it xlogs next stats(counters) and reads back new measurement via t.read().
    #
    # NOTE t.read goes 2 steps behind corresponding t.xlog call. This is on
    # purpose to sync emitting xlog entries with corresponding checks in test
    # code, because, as illustrated on the following figure, Measurement₁ is
    # emitted only after xlog₃ entry becomes available:
    #
    #   xlog₁   xlog₂   xlog₃
    #   ──|───────|───────|─────
    #     |Measure|Measure|
    #     | ment₁ | ment₂ |
    #
    # As the result it allows to write testing code as:
    #
    #   tstats(counters)
    #   _(...)  # verify effect on Measurements returned with period
    #   _(...)  # ending by timestamp of the above stats call.
    #   _(...)  # i.e. Measurement₁ if tstats call corresponds to xlog₂.
    τ_xlog = 1          # timestamp of last emitted xlog entry
    τ_logm = τ_xlog-2+1 # timestamp of next measurement to be read from logm
    counters_prev = {}
    def tstats(counters):
        nonlocal τ_xlog, τ_logm, counters_prev
        trace('\n>>> tstats τ_xlog: %s  τ_logm: %s' % (τ_xlog, τ_logm))
        t.xlog( jstats(τ_xlog+1, counters) )  # xlog τ+1
        t.read()                              # read+assert M for τ-1
        _('X.Tstart',   τ_logm+1)             # start preparing next expected M at τ
        _('X.δT',       1)
        τ_xlog += 1
        τ_logm += 1
        counters_prev = counters

    # tδstats is like tstats but takes δ for counters.
    def tδstats(δcounters):
        counters = counters_prev.copy()
        for k,δv in δcounters.items():
            counters[k] = counters.get(k,0) + δv
        tstats(counters)

    # tevent is the verb to verify handling of events.
    # its logic is similar to tstats.
    def tevent(event):
        nonlocal τ_xlog, τ_logm, counters_prev
        trace('\n>>> tstats τ_xlog: %s  τ_logm: %s' % (τ_xlog, τ_logm))
        t.xlog(json.dumps({"meta": {"event": event, "time": τ_xlog+1}}))
        t.read()
        _('X.Tstart',   τ_logm+1)
        _('X.δT',       1)
        τ_xlog += 1
        τ_logm += 1
        counters_prev = {} # reset


    # RRC.ConnEstab
    #
    # For init/fini correction LogMeasure accounts termination events in the
    # period of corresponding initiation event. We check this in detail for
    # RRC.ConnEstab, but only lightly for other measurements. This should be ok
    # since LogMeasure internally uses the same m_initfini function for
    # init/fini correcting all values.
    #
    #            ₁  p1  ₂ p2  ₃ p3  ₄ p4  ₅ p5  ₆
    #          ──|──────|─────|─────|─────|─────|─────
    # init           0     3     2     5     0
    # fini    ø ←─── 2     1←─── 2←─── 4←─── 1
    # fini'          0     3 ²   2 ²   3 ¹   0
    tstats({'rrc_connection_request':           0,
            'rrc_connection_setup_complete':    2}) # completions for previous uncovered period
    _('RRC.ConnEstabAtt.sum',       0)
    _('RRC.ConnEstabSucc.sum',      0)  # not 2
    # p2
    tstats({'rrc_connection_request':           0 +3,  # 3 new initiations
            'rrc_connection_setup_complete':    2 +1}) # 1 new completion
    _('RRC.ConnEstabAtt.sum',       3)
    _('RRC.ConnEstabSucc.sum',      3)  # not 1
    # p3
    tstats({'rrc_connection_request':           0+3 +2,  # 2 new initiations
            'rrc_connection_setup_complete':    2+1 +2}) # 2 completions for p2
    _('RRC.ConnEstabAtt.sum',       2)
    _('RRC.ConnEstabSucc.sum',      2)  # 2, but it is 2 - 2(for_p2) + 2(from_p4)
    # p4
    tstats({'rrc_connection_request':           0+3+2 +5,  # 5 new initiations
            'rrc_connection_setup_complete':    2+1+2 +4}) # 2 completions for p3 + 2 new
    _('RRC.ConnEstabAtt.sum',       5)
    _('RRC.ConnEstabSucc.sum',      3)
    # p5
    tstats({'rrc_connection_request':           0+3+2+5 +0,  # no new initiations
            'rrc_connection_setup_complete':    2+1+2+4 +1}) # 1 completion for p4
    _('RRC.ConnEstabAtt.sum',       0)
    _('RRC.ConnEstabSucc.sum',      0)


    # S1SIG.ConnEstab,  ERAB.InitEstab
    tδstats({'s1_initial_context_setup_request':    +3,
             's1_initial_context_setup_response':   +2})
    _('S1SIG.ConnEstabAtt',         3)
    _('S1SIG.ConnEstabSucc',        3) # 2 + 1(from_next)
    _('ERAB.EstabInitAttNbr.sum',   3) # currently same as S1SIG.ConnEstab
    _('ERAB.EstabInitSuccNbr.sum',  3) # ----//----

    tδstats({'s1_initial_context_setup_request':    +4,
             's1_initial_context_setup_response':   +3})
    _('S1SIG.ConnEstabAtt',         4)
    _('S1SIG.ConnEstabSucc',        2) # 3 - 1(to_prev)
    _('ERAB.EstabInitAttNbr.sum',   4) # currently same as S1SIG.ConnEstab
    _('ERAB.EstabInitSuccNbr.sum',  2) # ----//----


    # ERAB.EstabAdd
    tδstats({'s1_erab_setup_request':       +1,
             's1_erab_setup_response':      +1})
    _('ERAB.EstabAddAttNbr.sum',    1)
    _('ERAB.EstabAddSuccNbr.sum',   1)

    tδstats({'s1_erab_setup_request':       +3,
             's1_erab_setup_response':      +2})
    _('ERAB.EstabAddAttNbr.sum',    3)
    _('ERAB.EstabAddSuccNbr.sum',   2)


    # service detach/attach, connect failure, xlog failure
    tδstats({}) # untie from previous history
    i, f = 'rrc_connection_request', 'rrc_connection_setup_complete'
    I, F = 'RRC.ConnEstabAtt.sum',   'RRC.ConnEstabSucc.sum'

    tδstats({i:2, f:1})
    _(I, 2)
    _(F, 2) # +1(from_next)

    tδstats({i:2, f:2})
    _(I, 2)
    _(F, 1) # -1(to_prev)

    tevent("service detach")
    t.expect_nodata()

    t.read()                    # LogMeasure flushes its queue on "service detach".
    _('X.Tstart',   τ_logm+1)   # After the flush t.read will need to go only 1 step behind
    _('X.δT',       1)          # corresponding t.xlog call instead of previously going 2 steps beyond.
    t.expect_nodata()           # Do one t.read step manually to catch up.
    τ_logm += 1

    tevent("service connect failure")
    t.expect_nodata()
    tevent("service connect failure")
    t.expect_nodata()

    tevent("xlog failure")
    t.expect_nodata()
    tevent("xlog failure")
    t.expect_nodata()

    tevent("service attach")
    t.expect_nodata()

    t.xlog( jstats(τ_xlog+1, {i:1000, f:1000}) ) # LogMeasure restarts the queue after data starts to
    τ_xlog += 1                                  # come in again. Do one t.xlog step manually to
                                                 # increase t.read - t.xlog distance back to 2.
    tstats({i:1000+2, f:1000+2})
    _(I, 2) # no "extra" events even if counters start with jumped values after reattach
    _(F, 2) # and no fini correction going back through detach

    tevent("service detach")    # detach right after attach
    t.expect_nodata()
    tevent("service attach")
    t.expect_nodata()
    tevent("service detach")
    t.expect_nodata()


# verify that only stats with single cell and expected structure are accepted.
@func
def test_LogMeasure_badinput():
    t = tLogMeasure()
    defer(t.close)
    _ = t.expect1

    cc = 'rrc_connection_request'
    CC = 'RRC.ConnEstabAtt.sum'

    # initial ok entries
    t.xlog( jstats(1, {}) )
    t.xlog( jstats(2, {cc: 2}) )
    t.xlog( jstats(3, {cc: 2+3}) )
    # bad: not single cell
    t.xlog('{"message":"stats", "utc":11, "cells": {}}')
    t.xlog('{"message":"stats", "utc":12, "cells": {}}')
    t.xlog('{"message":"stats", "utc":13, "cells": {"a": {}, "b": {}}}')
    t.xlog('{"message":"stats", "utc":14, "cells": {"a": {}, "b": {}, "c": {}}}')
    # bad: no counters
    t.xlog('{"message":"stats", "utc":21, "counters": {"messages": {}}, "cells": {"1": {}}}')
    t.xlog('{"message":"stats", "utc":22, "counters": {"messages": {}}, "cells": {"1": {"counters": {}}}}')
    t.xlog('{"message":"stats", "utc":23, "cells": {"1": {"counters": {"messages": {}}}}}')
    t.xlog('{"message":"stats", "utc":24, "counters": {}, "cells": {"1": {"counters": {"messages": {}}}}}')
    # follow-up ok entries
    t.xlog( jstats(31, {cc: 30+4}) )
    t.xlog( jstats(32, {cc: 30+4+5}) )
    # badline 1
    t.xlog( "zzzqqqrrr" )
    # more ok entries
    t.xlog( jstats(41, {cc: 40+6}) )
    t.xlog( jstats(42, {cc: 40+6+7}) )
    # badline 2 + followup event
    t.xlog( "hello world" )
    t.xlog( '{"meta": {"event": "service attach", "time": 50}}' )
    # more ok entries
    t.xlog( jstats(51, {cc: 50+8}) )
    t.xlog( jstats(52, {cc: 50+8+9}) )

    def readok(τ, CC_value):
        _('X.Tstart',   τ)
        _('X.δT',       1)
        _(CC,           CC_value)
        t.read()

    def read_nodata(τ, δτ=1):
        _('X.Tstart',   τ)
        _('X.δT',       δτ)
        t.expect_nodata()
        t.read()

    read_nodata(0.02, 0.98) # attach-1
    readok(1, 2)            # 1-2
    readok(2, 3)            # 2-3
    read_nodata(3, 8)       # 3-11

    def tbadcell(τ, ncell):
        with raises(LogError, match="t%s: stats describes %d cells;" % (τ, ncell) +
                    "  but only single-cell configurations are supported"):
            t.read()
    tbadcell(11, 0)
    tbadcell(12, 0)
    tbadcell(13, 2)
    tbadcell(14, 3)

    def tbadstats(τ, error):
        with raises(LogError, match="t%s: stats: %s" % (τ, error)):
            t.read()
    tbadstats(21, ":10/cells/1 no `counters`")
    tbadstats(22, ":11/cells/1/counters no `messages`")
    tbadstats(23, ":12/ no `counters`")
    tbadstats(24, ":13/counters no `messages`")

    readok(31, 5)           # 31-32
    def tbadline():
        with raises(LogError, match="t?: invalid json"):
            t.read()
    tbadline()              # badline 1
    readok(41, 7)           # 41-42
    tbadline()              # badline 2
    read_nodata(50)         # 50-51
    readok(51, 9)           # 51-52


# verify that counter wrap-arounds are reported as errors.
@func
def test_LogMeasure_cc_wraparound():
    t = tLogMeasure()
    defer(t.close)
    _ = t.expect1

    cc = 'rrc_connection_request'
    CC = 'RRC.ConnEstabAtt.sum'

    t.xlog( jstats(1, {}) )
    t.xlog( jstats(2, {cc: 13}) )
    t.xlog( jstats(3, {cc: 12}) )   # cc↓   - should be reported
    t.xlog( jstats(4, {cc: 140}) )  # cc↑↑  - should should start afresh
    t.xlog( jstats(5, {cc: 150}) )

    def readok(τ, CC_value):
        _('X.Tstart',   τ)
        _('X.δT',       1)
        _(CC,           CC_value)
        t.read()

    _('X.Tstart',   0.02)   # attach-1
    _('X.δT',       0.98)
    t.expect_nodata()
    t.read()

    readok(1, 13)           # 1-2
    with raises(LogError, match=r"t3: cc %s↓  \(13 → 12\)" % cc):
        t.read()            # 2-3
    readok(4, 10)           # 4-5


# jstats returns json-encoded stats message corresponding to counters dict.
# τ goes directly to stats['utc'] as is.
def jstats(τ, counters):  # -> str
    g_cc    = {}  # global
    cell_cc = {}  # per-cell

    for cc, value in counters.items():
        if cc.startswith("rrc_"):
            cell_cc[cc] = value
        else:
            g_cc[cc] = value

    s = {
        "message":  "stats",
        "utc":      τ,
        "cells":    {"1": {"counters": {"messages": cell_cc}}},
        "counters": {"messages": g_cc},
    }

    return json.dumps(s)

def test_jstats():
    assert jstats(0, {}) == '{"message": "stats", "utc": 0, "cells": {"1": {"counters": {"messages": {}}}}, "counters": {"messages": {}}}'
    assert jstats(123.4, {"rrc_x": 1, "s1_y": 2, "rrc_z": 3, "x2_zz": 4}) == \
            '{"message": "stats", "utc": 123.4, "cells": {"1": {"counters": {"messages": {"rrc_x": 1, "rrc_z": 3}}}}, "counters": {"messages": {"s1_y": 2, "x2_zz": 4}}}'


# ionone returns empty data source.
def ionone():
    return io.BytesIO(b'')
