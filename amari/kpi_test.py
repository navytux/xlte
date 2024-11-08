# -*- coding: utf-8 -*-
# Copyright (C) 2022-2024  Nexedi SA and Contributors.
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

from __future__ import print_function, division, absolute_import

from xlte.amari.kpi import LogMeasure, LogError, _trace as trace
from xlte.kpi import Measurement, isNA
from golang import func, defer, b
import io, json, re

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
        trace('xlog += %s' % line)
        line = b(line)
        assert b'\n' not in line
        pos = t._fxlog.tell()
        t._fxlog.seek(0, io.SEEK_END)
        t._fxlog.write(b'%s\n' % line)
        t._fxlog.seek(pos, io.SEEK_SET)

    # _mok_init reinitializes ._mok with Measurement defaults.
    def _mok_init(t):
        t._mok = Measurement()
        # init fields extracted by amari.kpi from stats to 0
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

        # if a particular X.QCI[qci] is expected - default all other qcis to 0
        _ = re.match(r"^(.*)\.([0-9]+)$", field)
        if _ is not None:
            farr = "%s.QCI" % _.group(1)
            if isNA(t._mok[farr]).all():
                t._mok[farr][:] = 0

            # also automatically initialize XXX.DRB.IPTimeX_err to 0.01 upon seeing DRB.IPTimeX
            # ( in tests we use precise values for tx_time and tx_time_notailtti
            #   with δ=0.02 - see drb_trx and jdrb_stats)
            n = _.group(1)
            if n.startswith('DRB.IPTime'):
                ferr = "XXX.%s_err" % n
                if isNA(t._mok[ferr+'.QCI']).all():
                    t._mok[ferr+'.QCI'][:] = 0
                t._mok["%s.%s" % (ferr, _.group(2))] = ((vok + 0.01) - (vok - 0.01)) / 2  # ≈ 0.01

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
    t.xlog( jstats(1, {}) )
    _('X.Tstart',                   0.02)
    _('X.δT',                       1-0.02)
    t.expect_nodata()
    # note: no t.read() - see tstats


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

    # tdrb_stats is the verb to verify handling of x.drb_stats message.
    #
    # it xlogs drb stats with given δτ relative to either previous (δτ > 0) or
    # next (δτ < 0) stats or event.
    def tdrb_stats(δτ, qci_trx):
        if δτ >= 0:
            τ = τ_xlog   + δτ   # after previous stats or event
        else:
            τ = τ_xlog+1 + δτ   # before next stats or event
        trace('\n>>> tdrb_stats τ: %s  τ_xlog: %s  τ_logm: %s' % (τ, τ_xlog, τ_logm))
        t.xlog( jdrb_stats(τ, qci_trx) )



    # further empty stats
    tstats({})
    _('X.Tstart',                   1)
    _('X.δT',                       1)
    _('RRC.ConnEstabAtt.sum',       0)
    _('RRC.ConnEstabSucc.sum',      0)
    _('S1SIG.ConnEstabAtt',         0)
    _('S1SIG.ConnEstabSucc',        0)
    _('ERAB.EstabInitAttNbr.sum',   0)
    _('ERAB.EstabInitSuccNbr.sum',  0)
    _('ERAB.EstabAddAttNbr.sum',    0)
    _('ERAB.EstabAddSuccNbr.sum',   0)


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
    tstats({'C1.rrc_connection_request':        0,
            'C1.rrc_connection_setup_complete': 2}) # completions for previous uncovered period
    _('RRC.ConnEstabAtt.sum',       0)
    _('RRC.ConnEstabSucc.sum',      0)  # not 2
    # p2
    tstats({'C1.rrc_connection_request':        0 +3,  # 3 new initiations
            'C1.rrc_connection_setup_complete': 2 +1}) # 1 new completion
    _('RRC.ConnEstabAtt.sum',       3)
    _('RRC.ConnEstabSucc.sum',      3)  # not 1
    # p3
    tstats({'C1.rrc_connection_request':        0+3 +2,  # 2 new initiations
            'C1.rrc_connection_setup_complete': 2+1 +2}) # 2 completions for p2
    _('RRC.ConnEstabAtt.sum',       2)
    _('RRC.ConnEstabSucc.sum',      2)  # 2, but it is 2 - 2(for_p2) + 2(from_p4)
    # p4
    tstats({'C1.rrc_connection_request':        0+3+2 +5,  # 5 new initiations
            'C1.rrc_connection_setup_complete': 2+1+2 +4}) # 2 completions for p3 + 2 new
    _('RRC.ConnEstabAtt.sum',       5)
    _('RRC.ConnEstabSucc.sum',      3)
    # p5
    tstats({'C1.rrc_connection_request':        0+3+2+5 +0,  # no new initiations
            'C1.rrc_connection_setup_complete': 2+1+2+4 +1}) # 1 completion for p4
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


    # DRB.IPVol / DRB.IPTime  (testing all variants of stats/x.drb_stats interaction)
    tδstats({})
    tδstats({})                                             # ──S₁·d₁─────S₂·d₂─────S₃·d₃──
    tdrb_stats(+0.1, {1:  drb_trx(1.1,10,   1.2,20),
                      11: drb_trx(1.3,30,   1.4,40)})
    # nothing here - d₁ comes as the first drb_stats
    tδstats({})                                             # S₂
    tdrb_stats(+0.1, {2:  drb_trx(2.1,100,  2.2,200),       # d₂ is included into S₁-S₂
                      22: drb_trx(2.3,300,  2.4,400)})
    _('DRB.IPTimeDl.2',  2.1);  _('DRB.IPVolDl.2',  8*100)
    _('DRB.IPTimeUl.2',  2.2);  _('DRB.IPVolUl.2',  8*200)
    _('DRB.IPTimeDl.22', 2.3);  _('DRB.IPVolDl.22', 8*300)
    _('DRB.IPTimeUl.22', 2.4);  _('DRB.IPVolUl.22', 8*400)

    tδstats({})                                             # S₃
    tdrb_stats(+0.1, {3:  drb_trx(3.1,1000, 3.2,2000),      # d₃ is included int S₂-S₃
                      33: drb_trx(3.3,3000, 3.4,4000)})
    _('DRB.IPTimeDl.3',  3.1);  _('DRB.IPVolDl.3',  8*1000)
    _('DRB.IPTimeUl.3',  3.2);  _('DRB.IPVolUl.3',  8*2000)
    _('DRB.IPTimeDl.33', 3.3);  _('DRB.IPVolDl.33', 8*3000)
    _('DRB.IPTimeUl.33', 3.4);  _('DRB.IPVolUl.33', 8*4000)


    tdrb_stats(-0.1, {1: drb_trx(1.1,11,    1.2,12)})       # ──S·d─────d·S─────d·S──
    tδstats({})                                             #       cont↑
    _('DRB.IPTimeDl.1',  1.1);  _('DRB.IPVolDl.1',  8*11)
    _('DRB.IPTimeUl.1',  1.2);  _('DRB.IPVolUl.1',  8*12)
    tdrb_stats(-0.1, {2: drb_trx(2.1,21,    2.2,22)})
    tδstats({})
    _('DRB.IPTimeDl.2',  2.1);  _('DRB.IPVolDl.2',  8*21)
    _('DRB.IPTimeUl.2',  2.2);  _('DRB.IPVolUl.2',  8*22)

    tdrb_stats(-0.1, {3: drb_trx(3.1,31,    3.2,32)})       # ──d·S─────d·S─────d·S·d──
    tδstats({})                                             #       cont↑
    _('DRB.IPTimeDl.3',  3.1);  _('DRB.IPVolDl.3',  8*31)
    _('DRB.IPTimeUl.3',  3.2);  _('DRB.IPVolUl.3',  8*32)
    tdrb_stats(-0.1, {4: drb_trx(4.1,41,    4.2,42)})
    tδstats({})
    tdrb_stats(+0.1, {5: drb_trx(5.1,51,    5.2,52)})
    _('DRB.IPTimeDl.4',  4.1);  _('DRB.IPVolDl.4',  8*41)
    _('DRB.IPTimeUl.4',  4.2);  _('DRB.IPVolUl.4',  8*42)
    _('DRB.IPTimeDl.5',  5.1);  _('DRB.IPVolDl.5',  8*51)
    _('DRB.IPTimeUl.5',  5.2);  _('DRB.IPVolUl.5',  8*52)

    tdrb_stats(+0.5, {6: drb_trx(6.1,61,    6.2,62)})       # ──d·S·d──d──S───d──S──
    tδstats({})                                             #      cont↑
    _('DRB.IPTimeDl.6',  6.1);  _('DRB.IPVolDl.6',  8*61)
    _('DRB.IPTimeUl.6',  6.2);  _('DRB.IPVolUl.6',  8*62)
    tdrb_stats(+0.51,{7: drb_trx(7.1,71,    7.2,72)})
    tδstats({})
    _('DRB.IPTimeDl.7',  7.1);  _('DRB.IPVolDl.7',  8*71)
    _('DRB.IPTimeUl.7',  7.2);  _('DRB.IPVolUl.7',  8*72)

    tdrb_stats(-0.1, {8: drb_trx(8.1,81,    8.2,82)})       # combined d + S with nonzero counters
    tδstats({'s1_initial_context_setup_request':    +3,     # d──S────d·S──
             's1_initial_context_setup_response':   +2})    #     cont↑
    _('DRB.IPTimeDl.8',  8.1);  _('DRB.IPVolDl.8',  8*81)
    _('DRB.IPTimeUl.8',  8.2);  _('DRB.IPVolUl.8',  8*82)
    _('S1SIG.ConnEstabAtt',         3)
    _('S1SIG.ConnEstabSucc',        2)
    _('ERAB.EstabInitAttNbr.sum',   3) # currently same as S1SIG.ConnEstab
    _('ERAB.EstabInitSuccNbr.sum',  2) # ----//----


    # service detach/attach, connect failure, xlog failure
    tδstats({}) # untie from previous history
    i, f = 'C1.rrc_connection_request', 'C1.rrc_connection_setup_complete'
    I, F = 'RRC.ConnEstabAtt.sum',      'RRC.ConnEstabSucc.sum'

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


    # multiple cells
    # TODO emit per-cell measurements instead of accumulating all cells
    tstats({})
    t.expect_nodata()
    tstats({})
    _('RRC.ConnEstabAtt.sum',       0)
    _('RRC.ConnEstabSucc.sum',      0)
    #  C1 appears
    tstats({'C1.rrc_connection_request':    12,     'C1.rrc_connection_setup_complete': 11})
    _('RRC.ConnEstabAtt.sum',       12)
    _('RRC.ConnEstabSucc.sum',      11+1)
    #  C2 appears
    tstats({'C1.rrc_connection_request':    12+3,   'C1.rrc_connection_setup_complete': 11+3,
            'C2.rrc_connection_request':    22,     'C2.rrc_connection_setup_complete': 21})
    _('RRC.ConnEstabAtt.sum',       3+22)
    _('RRC.ConnEstabSucc.sum',      -1+3+21+2)
    #  C1 and C2 stays
    tstats({'C1.rrc_connection_request':    12+3+3, 'C1.rrc_connection_setup_complete': 11+3+3,
            'C2.rrc_connection_request':    22+4,   'C2.rrc_connection_setup_complete': 21+4})
    _('RRC.ConnEstabAtt.sum',       3+4)
    _('RRC.ConnEstabSucc.sum',      -2+3+4+2)
    #  C1 disappears
    tstats({'C2.rrc_connection_request':    22+4+4, 'C2.rrc_connection_setup_complete': 21+4+4})
    _('RRC.ConnEstabAtt.sum',       4)
    _('RRC.ConnEstabSucc.sum',      4-2)
    #  C2 disappears
    tstats({})
    _('RRC.ConnEstabAtt.sum',       0)
    _('RRC.ConnEstabSucc.sum',      0)

    tevent("service detach")
    t.expect_nodata()


# verify that only stats with expected structure are accepted.
@func
def test_LogMeasure_badinput():
    t = tLogMeasure()
    defer(t.close)
    _ = t.expect1

    cc = 'C1.rrc_connection_request'
    CC = 'RRC.ConnEstabAtt.sum'

    # initial ok entries
    t.xlog( jstats(1, {}) )
    t.xlog( jstats(2, {cc: 2}) )
    t.xlog( jstats(3, {cc: 2+3}) )
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
    read_nodata(3, 18)      # 3-21

    def tbadstats(τ, error):
        with raises(LogError, match="t%s: stats: %s" % (τ, error)):
            t.read()
    tbadstats(21, ":6/cells/1 no `counters`")
    read_nodata(21, 1)
    tbadstats(22, ":7/cells/1/counters no `messages`")
    read_nodata(22, 1)
    tbadstats(23, ":8/ no `counters`")
    read_nodata(23, 1)
    tbadstats(24, ":9/counters no `messages`")
    read_nodata(24, 7)

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

    cc = 'C1.rrc_connection_request'
    CC = 'RRC.ConnEstabAtt.sum'

    t.xlog( jstats(1, {}) )
    t.xlog( jstats(2, {cc: 13}) )
    t.xlog( jstats(3, {cc: 12}) )   # cc↓   - should be reported
    t.xlog( jstats(4, {cc: 140}) )  # cc↑↑  - should start afresh
    t.xlog( jstats(5, {cc: 150}) )

    def readok(τ, CC_value):
        _('X.Tstart',   τ)
        _('X.δT',       int(τ+1)-τ)
        if CC_value is not None:
            _(CC,       CC_value)
        else:
            t.expect_nodata()
        t.read()

    readok(0.02, None)      # attach-1
    readok(1, 13)           # 1-2
    readok(2, None)         # 2-3  M(ø)
    with raises(LogError, match=r"t3: cc %s↓  \(13 → 12\)" % cc):
        t.read()            # 2-3  raise
    readok(3, None)         # 3-4  M(ø)
    readok(4, 10)           # 4-5


# verify that LogMeasure ignores syncs in xlog stream.
@func
def test_LogMeasure_sync():
    t = tLogMeasure()
    defer(t.close)
    _ = t.expect1

    cc = 'C1.rrc_connection_request'
    CC = 'RRC.ConnEstabAtt.sum'

    t.xlog( jstats(1, {}) )
    t.xlog( jstats(2, {cc: 4}) )
    t.xlog( '{"meta": {"event": "sync", "time": 2.5, "state": "attached", "reason": "periodic", "generator": "xlog ws://localhost:9001 stats[]/30.0s"}}' )
    t.xlog( jstats(3, {cc: 7}) )

    def readok(τ, CC_value):
        _('X.Tstart',   τ)
        _('X.δT',       int(τ+1)-τ)
        if CC_value is not None:
            _(CC,       CC_value)
        else:
            t.expect_nodata()
        t.read()

    readok(0.02, None)      # attach-1
    readok(1, 4)            # 1-2
    readok(2, 3)            # 2-3  jumping over sync


# jstats returns json-encoded stats message corresponding to counters dict.
#
# if a counter goes as "Cxxx.yyy" it is emitted as counter yyy of cell xxx in the output.
# τ goes directly to stats['utc'] as is.
def jstats(τ, counters):  # -> str
    g_cc    = {}  # global cumulative counters
    cells   = {}  # .cells

    for cc, value in counters.items():
        _ = re.match(r"^C([^.]+)\.(.+)$", cc)
        if _ is not None:
            cell = _.group(1)
            cc   = _.group(2)
            cells.setdefault(cell, {})          \
                 .setdefault("counters", {})    \
                 .setdefault("messages", {})    \
                 [cc] = value
        else:
            g_cc[cc] = value

    s = {
        "message":  "stats",
        "utc":      τ,
        "cells":    cells,
        "counters": {"messages": g_cc},
    }

    return json.dumps(s)

def test_jstats():
    assert jstats(0, {}) == '{"message": "stats", "utc": 0, "cells": {}, "counters": {"messages": {}}}'
    assert jstats(123.4, {"C1.rrc_x": 1, "s1_y": 2, "C1.rrc_z": 3, "x2_zz": 4}) == \
            '{"message": "stats", "utc": 123.4, "cells": {"1": {"counters": {"messages": {"rrc_x": 1, "rrc_z": 3}}}}, "counters": {"messages": {"s1_y": 2, "x2_zz": 4}}}'

    # multiple cells
    assert jstats(432.1, {"C1.rrc_x": 11, "C2.rrc_y": 22, "C3.xyz": 33, "C1.abc": 111, "xyz": 44}) == \
            '{"message": "stats", "utc": 432.1, "cells": {'                 + \
            '"1": {"counters": {"messages": {"rrc_x": 11, "abc": 111}}}, '  + \
            '"2": {"counters": {"messages": {"rrc_y": 22}}}, '              + \
            '"3": {"counters": {"messages": {"xyz": 33}}}}, '               + \
            '"counters": {"messages": {"xyz": 44}}}'


# jdrb_stats, similarly to jstats, returns json-encoded x.drb_stats message
# corresponding to per-QCI dl/ul tx_time/tx_bytes.
def jdrb_stats(τ, qci_dlul):  # -> str
    qci_dlul = qci_dlul.copy()
    for qci, dlul in qci_dlul.items():
        assert isinstance(dlul, dict)
        assert set(dlul.keys()) == {"dl_tx_bytes", "dl_tx_time", "dl_tx_time_notailtti",
                                    "ul_tx_bytes", "ul_tx_time", "ul_tx_time_notailtti"}
        dlul["dl_tx_time_err"] = 0              # original time is simulated to be
        dlul["ul_tx_time_err"] = 0              # measured precisely in tess.
        dlul["dl_tx_time_notailtti_err"] = 0    # ----//----
        dlul["ul_tx_time_notailtti_err"] = 0    #

    s = {
        "message":  "x.drb_stats",
        "utc":      τ,
        "qci_dict": qci_dlul,
    }

    return json.dumps(s)

def test_jdrb_stats():
    # NOTE json encodes 5 and 9 keys are strings, not integers
    x = 0.01
    assert jdrb_stats(100, {5: drb_trx(0.1,1234, 0.2,4321),
                            9: drb_trx(1.1,7777, 1.2,8888)}) == ( \
        '{"message": "x.drb_stats", "utc": 100, "qci_dict":' + \
        ' {"5": {"dl_tx_bytes": 1234, "dl_tx_time": %(0.1+x)r, "dl_tx_time_notailtti": %(0.1-x)r,' + \
        ' "ul_tx_bytes": 4321, "ul_tx_time": %(0.2+x)r, "ul_tx_time_notailtti": %(0.2-x)r,' + \
        ' "dl_tx_time_err": 0, "ul_tx_time_err": 0, "dl_tx_time_notailtti_err": 0, "ul_tx_time_notailtti_err": 0},' + \
        ' "9": {"dl_tx_bytes": 7777, "dl_tx_time": 1.11, "dl_tx_time_notailtti": 1.09,' + \
        ' "ul_tx_bytes": 8888, "ul_tx_time": 1.21, "ul_tx_time_notailtti": 1.19,' + \
        ' "dl_tx_time_err": 0, "ul_tx_time_err": 0, "dl_tx_time_notailtti_err": 0, "ul_tx_time_notailtti_err": 0}' + \
        '}}') % {
            '0.1-x': 0.1-x, '0.1+x': 0.1+x,  # working-around float impreciseness
            '0.2-x': 0.2-x, '0.2+x': 0.2+x,
        }


# drb_trx returns dict describing dl/ul transmissions of a data radio bearer.
# such dict is used as per-QCI entry in x.drb_stats
def drb_trx(dl_tx_time, dl_tx_bytes, ul_tx_time, ul_tx_bytes):
    return {"dl_tx_bytes": dl_tx_bytes, "dl_tx_time": dl_tx_time + 0.01, "dl_tx_time_notailtti": dl_tx_time - 0.01,
            "ul_tx_bytes": ul_tx_bytes, "ul_tx_time": ul_tx_time + 0.01, "ul_tx_time_notailtti": ul_tx_time - 0.01}


# ionone returns empty data source.
def ionone():
    return io.BytesIO(b'')
