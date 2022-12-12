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
    # ._event  currently handled xlog.Event | LogError | None
    # ._stats  currently handled xlog.Message with last read stats result | None
    # ._m      kpi.Measurement being prepared covering [_stats_prev, _stats) | None
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
    logm._event = None
    logm._stats = None
    logm._m = None

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
    while 1:
        _trace()
        _trace('._event:\t', logm._event)
        _trace('._stats:\t', logm._stats)
        _trace('._m:    \t', logm._m)

        # flush the queue fully at an error or an event, e.g. at "service detach".
        event = logm._event
        if event is not None:
            # <- M for [stats_prev, stats)
            m = logm._m
            if m is not None:
                logm._m = None
                return m
            # <- M(ø) for [stats, event)
            stats = logm._stats
            if stats is not None:
                logm._stats = None
                if event.timestamp is not None:
                    m = kpi.Measurement()
                    m['X.Tstart'] = stats.timestamp
                    m['X.δT']     = event.timestamp - stats.timestamp
                    return m
            # <- error|EOF
            if isinstance(event, LogError):
                logm._event = None
                if event is LogError.EOF:
                    return None
                raise event

            # queue should be fully flushed now
            assert logm._stats  is None
            assert logm._m      is None
            # event might remain non-none, e.g. "service detach", but not an error
            assert isinstance(event, xlog.Event)


        # fetch next entry from xlog
        try:
            x = logm._rxlog.read()
        except Exception as e:
            x = LogError(None, str(e)) # e.g. it was xlog.ParseError(...)
        _trace('  xlog:', x)

        if x is None:
            x = LogError.EOF # represent EOF as LogError
        if isinstance(x, LogError):
            logm._event = x # it is ok to forget previous event after e.g. bad line with ParseError
            continue        # flush the queue

        elif isinstance(x, xlog.Event):
            event_prev = logm._event
            logm._event = x
            if event_prev is None:
                continue    # flush

            # <- M(ø) for [event_prev, event)
            assert event_prev.timestamp is not None # LogErrors are raised after queue flush
            m = kpi.Measurement()
            m['X.Tstart'] = event_prev.timestamp
            m['X.δT']     = x.timestamp - event_prev.timestamp
            return m

        assert isinstance(x, xlog.Message)
        if x.message != "stats":
            continue

        m = logm._read_stats(x)
        if m is not None:
            return m
        continue

# _read_stats handles next stats xlog entry upon _read request.
@func(LogMeasure)
def _read_stats(logm, stats: xlog.Message):  # -> kpi.Measurement|None(to retry)
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
        event_prev = logm._event
        logm._event = e
        if event_prev is not None:
            # <- M(ø) for [event, bad_stats)
            m = kpi.Measurement()
            m['X.Tstart'] = event_prev.timestamp
            m['X.δT']     = stats.timestamp - event_prev.timestamp
            return m
        return None # flush

    # stats is pre-checked to be good. push it to the queue.
    stats_prev = logm._stats
    logm._stats = stats

    # first stats after service attach -> M(ø)
    if stats_prev is None:
        event_prev = logm._event
        if event_prev is not None:
            # <- M(ø) for [event, stats)
            logm._event = None
            m = kpi.Measurement()
            m['X.Tstart'] = event_prev.timestamp
            m['X.δT']     = stats.timestamp - event_prev.timestamp
            return m
        return None

    # we have 2 adjacent stats. Start building new Measurement from their δ.
    # do init/fini correction if there was also third preceding stats message.
    m = kpi.Measurement() # [stats_prev, stats)
    m['X.Tstart'] = stats_prev.timestamp
    m['X.δT']     = stats.timestamp - stats_prev.timestamp

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
    if logm._m is not None:
        p = logm._m.copy()
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
        logm._stats = None
        logm._event = e
        return None

    # all adjustments and checks are over.
    logm._m = m # we can now remember pre-built Measurement for current stats,
    return p    # and return adjusted previous measurement, if it was there.


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
