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
"""Package amari.kpi provides driver for KPI-related measurements for Amarisoft LTE stack.

Use LogMeasure to convert enb.xlog (TODO and enb.log) to Measurements.
The KPIs themselves can be computed from Measurements via package xlte.kpi .
"""

from xlte import kpi
from xlte.amari import xlog
from golang import func


# LogMeasure takes enb.xlog (TODO and enb.log) as input, and produces kpi.Measurements on output.
#
#     enb.xlog     ─────────
#     ─────────>  │   Log   │
#                 │         │ ────> []kpi.Measurement
#     ─────────>  │ Measure │
#     enb.log      ─────────
#
# Use LogMeasure(rxlog, rlog) to create it.
# Use .read() to retrieve Measurements.
# Use .close() when done.
class LogError(RuntimeError):
    # .timestamp    s | None for invalid input
    pass
class LogMeasure:
    # ._rxlog       IO reader for enb.xlog
    # ._rlog        IO reader for enb.log
    #
    # ._estats      \/ last xlog.Message with read stats result
    #               \/ last xlog.Event\sync | LogError
    #               \/ None
    # ._m           kpi.Measurement being prepared covering [_estats_prev, _estats) | None
    # ._m_next      kpi.Measurement being prepared covering [_estats, _estats_next) | None
    #
    # ._drb_stats   last xlog.Message with x.drb_stats | None   ; reset on error|event
    pass


# LogMeasure(rxlog, rlog) creates new LogMeasure object that will read
# enb.xlog and enb.log data from IO readers rxlog and rlog.
#
# The readers must provide .readline() and .read() methods.
# The ownership of rxlog and rlog is transferred to LogMeasure.
@func(LogMeasure)
def __init__(logm, rxlog, rlog):
    logm._rxlog = xlog.Reader(rxlog)
    logm._rlog  = rlog
    logm._estats = None
    logm._m = None
    logm._m_next = None
    logm._drb_stats = None

# close releases resources associated with LogMeasure and closes underlying readers.
@func(LogMeasure)
def close(logm):
    logm._rxlog.close()
    logm._rlog .close()


# read retrieves and returns next Measurement or None at EOF.
#
# It reads data from enb.xlog (TODO and enb.log) as needed.
@func(LogMeasure)
def read(logm):  # -> kpi.Measurement | None
    _trace('\n\n  LogMeasure.read')
    m = logm._read()
    _trace('  <-', m)
    return m

@func(LogMeasure)
def _read(logm):
    # read log data organizing periods around stats queries.
    #
    # we emit measurement X after reading stats X+2 - i.e. we emit measurement
    # for a period after reading data covering _next_ period. It is organized
    # this way to account for init/fini correction(*):
    #
    #              fini adjust
    #             -------------
    #            '             '
    #      Sx    v     Sx+1    '   Sx+2
    #   ────|───────────|───────────|────
    #        Measurement Measurement
    #             X          X+1
    #
    #
    # (*) see kpi.Measurement documentation for more details about init/fini correction.
    m = None  # kpi.Measurement to return
    while 1:
        _trace()
        _trace('m:       \t', m)
        _trace('._m:     \t', logm._m)
        _trace('._estats:\t', logm._estats)
        _trace('._m_next:\t', logm._m_next)
        _trace('._drb_stats:\t', logm._drb_stats)

        if m is not None:
            return m

        # flush the queue at an error or an event, e.g. at "service detach".
        estats = logm._estats
        if isinstance(estats, (xlog.Event, LogError)):
            # <- M for [estats_prev, estats)
            m = logm._m
            if m is not None:
                logm._m = None
                return m
            # note ._m_next is not flushed:
            # if ._m_next != None - it remains initialized with X.Tstart = estats.timestamp

            # <- error|EOF
            if isinstance(estats, LogError):
                logm._estats = None
                if estats is LogError.EOF:
                    return None
                raise estats

            # queue should be flushed now till including estats with
            # event remaining non-none, e.g. "service detach", but not an error
            assert logm._m is None
            assert isinstance(logm._estats, xlog.Event)
            assert isinstance(logm._m_next, kpi.Measurement)
            assert logm._m_next['X.Tstart'] == logm._estats.timestamp


        # fetch next entry from xlog
        try:
            x = logm._rxlog.read()
        except Exception as e:
            x = LogError(None, str(e)) # e.g. it was xlog.ParseError(...)
        _trace('  xlog:', x)

        if x is None:
            x = LogError.EOF # represent EOF as LogError

        # ignore sync events
        if isinstance(x, xlog.Event)  and  x.event == "sync":
            continue

        # handle messages that update current Measurement
        if isinstance(x, xlog.Message):
            if x.message == "x.drb_stats":
                logm._handle_drb_stats(x)
                continue
            if x.message != "stats":
                continue    # ignore other messages


        # it is an error, event\sync or stats.
        # if it is an event or stats -> finalize timestamp for _m_next.
        # start building next _m_next covering [x, x_next).
        # shift m <- ._m <- ._m_next <- (new Measurement | None for LogError)
        # a LogError throws away preceding Measurement and does not start a new one after it
        if logm._m_next is not None:
            if not isinstance(x, LogError):
                logm._m_next['X.δT'] = x.timestamp - logm._m_next['X.Tstart']
            else:
                logm._m_next = None # throw it away on seeing e.g. "stats, error"
        m = logm._m
        logm._m = logm._m_next
        if not isinstance(x, LogError):
            logm._m_next = kpi.Measurement()
            logm._m_next['X.Tstart'] = x.timestamp # note X.δT remains NA until next stats|event
        else:
            logm._m_next = None

        if isinstance(x, (xlog.Event, LogError)):
            logm._estats = x # it is ok to forget previous event after e.g. bad line with ParseError
            logm._drb_stats = None # reset ._drb_stats at an error or event
            continue         # flush the queue

        assert isinstance(x, xlog.Message)
        assert x.message == "stats"
        logm._handle_stats(x, m)
        # NOTE _handle_stats indicates logic error in x by setting ._estats to
        # LogError instead of stats. However those LogErrors come with
        # timestamp and are thus treated similarly to events: we do not throw
        # away neither ._m, nor ._m_next like we do with LogErrors that
        # represent errors at the log parsing level.
        continue


# _handle_stats handles next stats xlog entry upon _read request.
@func(LogMeasure)
def _handle_stats(logm, stats: xlog.Message, m_prev: kpi.Measurement):
    # build Measurement from stats' counters.
    #
    # we take δ(stats_prev, stat) and process it mapping Amarisoft counters to
    # 3GPP ones specified by kpi.Measurement. This approach has following limitations:
    #
    # - for most of the counters there is no direct mapping in between
    #   Amarisoft and 3GPP. For example we currently use s1_erab_setup_request for
    #   ERAB.EstabAddAtt.sum, but this mapping is not strictly correct and will
    #   break if corresponding S1 E-RAB SETUP REQUEST message contains multiple
    #   ERABs. The code has corresponding FIXME marks where such approximations
    #   are used.
    #
    # - it is not possible to implement init/fini correction precisely. From
    #   aggregated statistics we only get total amount for a fini value for a
    #   period - without knowing which part of it corresponds to init events
    #   from previous period, and which part to init events from current one.
    #   With that it is only possible to make a reasonable guess and try to
    #   preserve statistical properties, but not more. See m_initfini below for
    #   details.
    #
    # - it is possible to handle eNB with single cell only. This limitation
    #   comes from the fact that in Amarisoft LTE stack S1-related counters
    #   come as "globals" ones, while e.g. RRC-related counters are "per-cell".
    #   It is thus not possible to see how much S1 connection establishments
    #   are associated with one particular cell if there are several of them.
    #
    # TODO also parse enb.log to fix those issues.

    # check if new stats follows required structure.
    # handle it as an error event if it is not.
    try:
        _stats_check(stats)
    except LogError as e:
        logm._estats = e  # stays M(ø) for [estats_prev, bad_stats)
        return

    # stats is pre-checked to be good. push it to the queue.
    estats_prev = logm._estats
    logm._estats = stats

    # first stats after e.g. service attach -> stays M(ø) for [event_prev, stats)
    if estats_prev is None:
        return
    if isinstance(estats_prev, (xlog.Event, LogError)):
        return

    assert isinstance(estats_prev, xlog.Message)
    assert estats_prev.message == "stats"
    stats_prev = estats_prev

    # we have 2 adjacent stats. Adjust corresponding Measurement from their δ.
    # do init/fini correction if there was also third preceding stats message.
    m = logm._m.copy() # [stats_prev, stats)

    # δcc(counter) tells how specified cumulative counter changed since last stats result.
    def δcc(counter):
        old = _stats_cc(stats_prev, counter)
        new = _stats_cc(stats,      counter)
        if new < old:
            raise LogError(stats.timestamp, "cc %s↓  (%s → %s)" % (counter, old, new))
        return new - old

    # m_initfini populates m[init] and m[fini] from vinit and vfini values.
    # copy of previous ._m[fini] is correspondingly adjusted for init/fini correction.
    p = None
    if m_prev is not None:
        p = m_prev.copy()
    def m_initfini(init, vinit, fini, vfini):
        m[init] = vinit
        m[fini] = vfini
        # take as much as possible from current fini to populate prev fini.
        # this way we expose moved fini events as appearing in previous
        # period, and, with correct values coming from xlog, will have to
        # throw-away (see below for "too much" case) as minimum as possible
        # fini events. And even though we don't know exactly how many moved fini
        # was from previous period, and how much was actually from current
        # period, tossing fini values in between those periods should not change
        # overall statistics if it is computed taking both periods into account.
        if p is not None:
            if p[fini] < p[init]:
                δ = min(p[init]-p[fini], m[fini])
                p[fini] += δ
                m[fini] -= δ
        # if we still have too much fini - throw it away pretending that it
        # came from even older uncovered period
        if m[fini] > m[init]:
            m[fini] = m[init]

    # compute δ for counters.
    # any logic error in data will be reported via LogError.
    try:
        # RRC: connection establishment
        m_initfini(
            'RRC.ConnEstabAtt.sum',         δcc('rrc_connection_request'),
            'RRC.ConnEstabSucc.sum',        δcc('rrc_connection_setup_complete'))

        # S1: connection establishment
        m_initfini(
            'S1SIG.ConnEstabAtt',           δcc('s1_initial_context_setup_request'),
            'S1SIG.ConnEstabSucc',          δcc('s1_initial_context_setup_response'))

        # ERAB: Initial establishment
        # FIXME not correct if multiple ERABs are present in one message
        m_initfini(
            'ERAB.EstabInitAttNbr.sum',     δcc('s1_initial_context_setup_request'),
            'ERAB.EstabInitSuccNbr.sum',    δcc('s1_initial_context_setup_response'))

        # ERAB: Additional establishment
        # FIXME not correct if multiple ERABs are present in one message
        m_initfini(
            'ERAB.EstabAddAttNbr.sum',      δcc('s1_erab_setup_request'),
            'ERAB.EstabAddSuccNbr.sum',     δcc('s1_erab_setup_response'))

    except Exception as e:
        if not isinstance(e, LogError):
            _ = e
            e = LogError(stats.timestamp, "internal failure")
            e.__cause__ = _
        logm._estats = e
        return

    # all adjustments and checks are over.
    logm._m = m             # we can now remember our Measurement adjustments for current stats,
    if m_prev is not None:  # and commit adjustments to previous measurement, if it was there.
        m_prev.put((0,), p) # copy m_prev <- p
    return


# _stats_check verifies stats message to have required structure.
#
# only configurations with one single cell are supported.
# ( because else it would not be clear to which cell to associate e.g. global
#   counters for S1 messages )
def _stats_check(stats: xlog.Message):
    cells = stats['cells']
    if len(cells) != 1:
        raise LogError(stats.timestamp, "stats describes %d cells;  but only single-cell configurations are supported" % len(cells))
    cellname = list(cells.keys())[0]

    try:
        stats.get1("counters", dict).get1("messages", dict)
        stats.get1("cells", dict).get1(cellname, dict).get1("counters", dict).get1("messages", dict)
    except Exception as e:
        raise LogError(stats.timestamp, "stats: %s" % e)  from None
    return

# _stats_cc returns specified cumulative counter from stats result.
#
# counter may be both "global" or "per-cell".
# stats is assumed to be already verified by _stats_check.
def _stats_cc(stats: xlog.Message, counter: str):
    cells = stats['cells']
    cell = list(cells.values())[0]

    if counter.startswith("rrc_"):
        cc_dict = cell ['counters']
    else:
        cc_dict = stats['counters']

    return cc_dict['messages'].get(counter, 0)


# _handle_drb_stats handles next x.drb_stats xlog entry upon _read request.
@func(LogMeasure)
def _handle_drb_stats(logm, drb_stats: xlog.Message):
    # TODO precheck for correct message structure similarly to _stats_check

    drb_stats_prev = logm._drb_stats
    logm._drb_stats = drb_stats

    # first drb_stats after an event - we don't know which time period it covers
    if drb_stats_prev is None:
        return

    assert isinstance(drb_stats_prev, xlog.Message)
    assert drb_stats_prev.message == "x.drb_stats"

    # time coverage for current drb_stats
    τ_lo = drb_stats_prev.timestamp
    τ_hi = drb_stats.timestamp
    δτ = τ_hi - τ_lo

    # see with which ._m or ._m_next, if any, drb_stats overlaps with ≥ 50% of
    # time first, and update that measurement correspondingly.
    if not (δτ > 0):
        return

    if logm._m is not None:
        m_lo = logm._m['X.Tstart']
        m_hi = m_lo + logm._m['X.δT']

        d = max(0, min(τ_hi, m_hi) -
                   max(τ_lo, m_lo))
        if d >= δτ/2:  # NOTE ≥ 50%, not > 50% not to skip drb_stats if fill is exactly 50%
            _drb_update(logm._m, drb_stats)
            return

    if logm._m_next is not None:
        n_lo = logm._m_next['X.Tstart']
        # n_hi - don't know as _m_next['X.δT'] is ø yet

        d = max(0,     τ_hi        -
                   max(τ_lo, n_lo))
        if d >= δτ/2:
            _drb_update(logm._m_next, drb_stats)
            return

# _drb_update updates Measurement from dl/ul DRB statistics related to measurement's time coverage.
def _drb_update(m: kpi.Measurement, drb_stats: xlog.Message):
    # TODO Exception -> LogError("internal failure") similarly to _handle_stats
    qci_trx = drb_stats.get1("qci_dict", dict)

    for dir in ('dl', 'ul'):
        qvol      = m['DRB.IPVol%s.QCI'          % dir.capitalize()]
        qtime     = m['DRB.IPTime%s.QCI'         % dir.capitalize()]
        qtime_err = m['XXX.DRB.IPTime%s_err.QCI' % dir.capitalize()]

        # qci_dict carries entries only for qci's with non-zero values, but if
        # we see drb_stats we know we have information for all qcis.
        # -> pre-initialize to zero everything
        if kpi.isNA(qvol).all():        qvol[:]      = 0
        if kpi.isNA(qtime).all():       qtime[:]     = 0
        if kpi.isNA(qtime_err).all():   qtime_err[:] = 0

        for qci_str, trx in qci_trx.items():
            qci = int(qci_str)

            # DRB.IPVol and DRB.IPTime are collected to compute throughput.
            #
            # thp = ΣB*/ΣT*  where B* is tx'ed bytes in the sample without taking last tti into account
            #                and   T* is time of tx also without taking that sample's tail tti.
            #
            # we only know ΣB (whole amount of tx), ΣT and ΣT* with some error.
            #
            # -> thp can be estimated to be inside the following interval:
            #
            #          ΣB            ΣB
            #         ───── ≤ thp ≤ ─────           (1)
            #         ΣT_hi         ΣT*_lo
            #
            # the upper layer in xlte.kpi will use the following formula for
            # final throughput calculation:
            #
            #               DRB.IPVol
            #         thp = ──────────              (2)
            #               DRB.IPTime
            #
            # -> set DRB.IPTime and its error to mean and δ of ΣT_hi and ΣT*_lo
            # so that (2) becomes (1).

            # FIXME we account whole PDCP instead of only IP traffic
            ΣB      = trx['%s_tx_bytes' % dir]
            ΣT      = trx['%s_tx_time'  % dir]
            ΣT_err  = trx['%s_tx_time_err'  % dir]
            ΣTT     = trx['%s_tx_time_notailtti' % dir]
            ΣTT_err = trx['%s_tx_time_notailtti_err' % dir]

            ΣT_hi   = ΣT + ΣT_err
            ΣTT_lo  = ΣTT - ΣTT_err

            qvol[qci]      = 8*ΣB   # in bits
            qtime[qci]     = (ΣT_hi + ΣTT_lo) / 2
            qtime_err[qci] = (ΣT_hi - ΣTT_lo) / 2


# LogError(timestamp|None, *argv).
@func(LogError)
def __init__(e, τ, *argv):
    e.timestamp = τ
    super(LogError, e).__init__(*argv)

# __str__ returns human-readable form.
@func(LogError)
def __str__(e):
    t = "?"
    if e.timestamp is not None:
        t = "%s" % e.timestamp
    return "t%s: %s" % (t, super(LogError, e).__str__())

# LogError.EOF is special LogError value to represent EOF event.
LogError.EOF = LogError(None, "EOF")


# ----------------------------------------

_debug = False
def _trace(*argv):
    if _debug:
        print(*argv)
