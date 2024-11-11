# -*- coding: utf-8 -*-
# Copyright (C) 2023-2024  Nexedi SA and Contributors.
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
"""Package drb provides infrastructure to process flows on data radio bearers.

- Sampler converts information about data flows obtained via ue_get[stats] into
  Samples that represent bursts of continuous transmissions.

- _x_stats_srv uses Sampler to process data flows at 100Hz rate and aggregates
  results into information needed to compute E-UTRAN IP Throughput KPI. The
  information is emitted in the form of synthetic x.drb_stats message whose
  generation is integrated into amari.xlog package.

Please see amari.kpi and xlte.kpi packages that turn x.drb_stats data into
final E-UTRAN IP Throughput KPI value.

See also the following related 3GPP standards references:

    - TS 32.450 6.3.1 "E-UTRAN IP Throughput"
    - TS 32.425 4.4.6 "IP Throughput measurements"
"""

from __future__ import print_function, division, absolute_import

from xlte import amari

from golang import chan, select, default, nilchan, func, defer
from golang import sync, time

import math
import sys

tti = 1*time.millisecond  # = 1·subframe    Ts       =    1/(2048·15000)·s  ≈ 32.6 ns
                          #                 Tsymb    =              2048·Ts ≈ 66.7 μs
                          #                 Slot     = 7.5·Tsymb = 15350·Ts =  0.5 ms
                          #                 Subframe = 2·Slot               =  1 ms
                          #                 Frame    = 10·Subframe          = 10 ms


# Sampler collects information about DRB usage and converts that to per-QCI UE Samples.
#
# - use .add to append ue_stats/stats as input information and get finalized Samples.
# - use .finish to wrap-up and retrieve rest of the Samples and reset the sampler.
class Sampler:
    __slots__ = (
        '_dl_sampler',  #  _Sampler('dl')
        '_ul_sampler',  #  _Sampler('ul')
    )

# Sample represents one burst of continuous transmission to/from particular UE on particular QCI.
#
# A transmission is continuous if during its time corresponding transmission
# buffer is not empty. For example a transmission where something is sent
# during 5 consecutive TTIs is continuous. As well as if something is sent not
# every TTI, but the buffer is not empty during pauses and the pauses are e.g.
# due to congestion - it is also said to be continuous transmission:
#
#       | |x|x|x|x|x| |
#
#       | |x|x| |x| | |x|x| |
#              ↑   ↑ ↑
#           buffer is not empty - the transmission sample continues
class Sample:
    __slots__ = (
        'tx_bytes',     # amount of bytes transmitted
        'tx_time',      # time interval during which sample transmission was made
        'tx_time_err',  # accuracy of tx_time measurement
    )


# _Sampler serves Sampler for one of 'dl' or 'ul' direction.
class _Sampler:
    __slots__ = (
        'dir',          # 'dl' or 'ul'
        'use_bitsync',  # whether to use _BitSync
        'use_ri',       # whether to pay attention to rank indicator
        't',            # timestamp of last ue_stats
        'ues',          # {} ue -> _UE      current state of all tracked UEs
    )

# _UE represents tracking of data transmission of particular UE.
class _UE:
    __slots__ = (
        'erab_flows',   # {} erab_id -> _ERAB_Flow      current state of all erabs related to UE
        'qci_flows',    # {} qci     -> _QCI_Flow       in-progress collection of UE-related samples
        'bitsync',      # None | _BitSync               to synchronize δtx_bytes with #tx on updates
    )

# _ERAB_Flow tracks data transmission on particular ERAB of particular UE.
class _ERAB_Flow:
    __slots__ = (
        'qci',              # qci as last reported by ue_get
        'tx_total_bytes',   # total amount transmitted as last reported by ue_get
    )

# _QCI_Flow represents in-progress collection to make up a Sample.
#
# .update(δt, tx_bytes, #tx, ...) updates flow with information about next
#               transmission period and potentially yields some finalized Samples.
# .finish() completes Sample collection.
class _QCI_Flow:
    __slots__ = (
        'tx_bytes',     # already accumulated bytes
        'tx_time',      # already accumulated time
        'tx_time_err',  # accuracy of ^^^
    )

# _BitSync helps _Sampler to match δtx_bytes and #tx in transmission updates.
#
# For example for DL a block is transmitted via PDCCH+PDSCH during one TTI, and
# then the base station awaits HARQ ACK/NACK. That ACK/NACK comes later via
# PUCCH or PUSCH. The time window in between original transmission and
# reception of the ACK/NACK is 4 TTIs for FDD and 4-13 TTIs for TDD(*).
# And Amarisoft LTEENB updates counters for dl_total_bytes and dl_tx at
# different times:
#
#   ue.erab.dl_total_bytes      - right after sending data on  PDCCH+PDSCH
#   ue.cell.{dl_tx,dl_retx}     - after receiving ACK/NACK via PUCCH|PUSCH
#
# this way an update to dl_total_bytes might be seen in one frame (= 10·TTI),
# while corresponding update to dl_tx/dl_retx might be seen in either same, or
# next, or next-next frame.
#
# What _BitSync does is that it processes stream of tx_bytes/#tx and emits
# adjusted stream with #tx corresponding to tx_bytes coming together
# synchronized in time.
#
#   .next(δt, tx_bytes, #tx)  ->  [](δt', tx_bytes', #tx')
#   .finish()                 ->  [](δt', tx_bytes', #tx')
#
# (*) see e.g. Figure 8.1 in "An introduction to LTE, 2nd ed."
class _BitSync:
    __slots__ = (
        'txq',          # [](δt,tx_bytes,_Utx)      not-yet fully processed tail of whole txv
        'i_txq',        # txq represents txv[i_txq:]
        'i_lshift',     # next left shift will be done on txv[i_lshift] <- txv[i_lshift+1]
    )


# Sampler() creates new sampler that will start sampling from ue_stats0/stats0 state.
@func(Sampler)
def __init__(s, ue_stats0, stats0):
    s._dl_sampler = _Sampler('dl', ue_stats0, stats0, use_bitsync=True,  use_ri=True)
    s._ul_sampler = _Sampler('ul', ue_stats0, stats0,
            use_bitsync=False,  # for ul tx_bytes and #tx come, it seems, synchronized out of the box
            use_ri=False)       # no rank indication for ul - assume siso
                                # TODO also use upcoming ul_rank+ul_n_layer

@func(_Sampler)
def __init__(s, dir, ue_stats0, stats0, use_bitsync, use_ri):
    s.dir = dir
    s.t = -1 # so that add(t=0, init) works
    s.use_bitsync = use_bitsync
    s.use_ri = use_ri
    s.ues = {}
    _ = s.add(ue_stats0, stats0, init=True)
    assert _ == {}
    for ue in s.ues.values():
        assert ue.qci_flows == {}

# _UE() creates new empty UE-tracking entry.
@func(_UE)
def __init__(ue, use_bitsync):
    ue.erab_flows = {}
    ue.qci_flows = {}
    ue.bitsync = _BitSync()  if use_bitsync else  None

# finish wraps up all in-progress flows.
#
# and returns all remaining samples.
# The sampler is reset after retrieval.
@func(Sampler)
def finish(s): # dl/ul samples    ; dl/ul = {} qci -> []Sample
    dl = s._dl_sampler.finish()
    ul = s._ul_sampler.finish()
    return (dl, ul)

@func(_Sampler)
def finish(s):
    qci_samples = {}
    for ue in s.ues.values():
        # wrap-up in-progress bitsync
        if ue.bitsync is not None:
            bitnext = ue.bitsync.finish()
            ue._update_qci_flows(bitnext, qci_samples)

        # wrap-up all in-progress flows
        for qci, flow in ue.qci_flows.items():
            _ = flow.finish()
            for sample in _:
                qci_samples.setdefault(qci, []).append(sample)
        ue.qci_flows = {}

        # preserve .erab_flows as if we were initialized with corresponding ue_stats0.

    return qci_samples


# add feeds next ue_get[stats] + stats reports to the sampler.
#
# and returns samples that become finalized during this addition.
@func(Sampler)
def add(s, ue_stats, stats):  # -> dl/ul samples    ; dl/ul = {} qci -> []Sample
    dl = s._dl_sampler.add(ue_stats, stats)
    ul = s._ul_sampler.add(ue_stats, stats)
    return dl, ul

class _Utx:  # UE transmission state
    __slots__ = (
        'qtx_bytes',
        'cutx',         # {} cell -> _UCtx
    )

class _UCtx: # UE transmission state on particular cell
    __slots__ = (
        'tx',
        'retx',
        'bitrate',
        'rank',
        'xl_use_avg',
    )

@func(_Sampler)
def add(s, ue_stats, stats, init=False):
    t = ue_stats['utc']
    δt = t - s.t
    s.t = t
    assert δt > 0

    qci_samples = {}     # qci -> []Sample    samples finalized during this add
    ue_live     = set()  # of ue              ue that are present in ue_stats

    # go through all UEs and update/finalize flows from information on per-UE erabs.
    for ju in ue_stats['ue_list']:
        ue_id = ju['enb_ue_id']    # TODO 5G: -> ran_ue_id + qos_flow_list + sst?
        ue_live.add(ue_id)

        if len(ju['cells']) != 1:
            raise RuntimeError(("ue #%s belongs to %d cells;  "+
                "but only single-cell configurations are supported") % (ue_id, len(ju(['cells']))))
        cell = ju['cells'][0]

        cell_id = cell['cell_id']  # int
        scell = stats['cells'][str(cell_id)]

        u = _Utx()
        u.qtx_bytes  = {}  # qci  -> Σδerab_qci=qci
        u.cutx       = {}  # cell -> _UCtx

        uc = _UCtx()
        u.cutx[cell_id] = uc

        uc.tx       = cell['%s_tx'   % s.dir]     # in transport blocks
        uc.retx     = cell['%s_retx' % s.dir]     # ----//----
        uc.bitrate  = cell['%s_bitrate' % s.dir]  # bits/s
        assert uc.tx      >= 0, uc.tx
        assert uc.retx    >= 0, uc.retx
        assert uc.bitrate >= 0, uc.bitrate

        uc.rank       = cell['ri']  if s.use_ri  else 1
        uc.xl_use_avg = scell['%s_use_avg' % s.dir]

        ue = s.ues.get(ue_id)
        if ue is None:
            ue = s.ues[ue_id] = _UE(s.use_bitsync)

        # erabs: δ(tx_total_bytes) -> tx_bytes  ; prepare per-qci tx_bytes
        tx_bytes  = 0     # Σδerab
        eflows_live = set()  # of erab      erabs that are present in ue_stats for this ue
        for erab in ju['erab_list']:
            erab_id = erab['erab_id']
            qci     = erab['qci']
            eflows_live.add(erab_id)

            ef = ue.erab_flows.get(erab_id)
            if ef is None:
                ef = ue.erab_flows[erab_id] = _ERAB_Flow()
                ef.qci = qci
                ef.tx_total_bytes = 0

            etx_total_bytes = erab['%s_total_bytes' % s.dir]
            if not (ef.qci == qci  and  ef.tx_total_bytes <= etx_total_bytes):
                # restart erab flow on change of qci or tx_total_bytes↓
                ef.qci = qci
                ef.tx_total_bytes = 0

            etx_bytes = etx_total_bytes - ef.tx_total_bytes
            ef.tx_total_bytes = etx_total_bytes

            tx_bytes += etx_bytes
            if etx_bytes != 0:
                u.qtx_bytes[qci] = u.qtx_bytes.get(qci,0) + etx_bytes

            # debug
            if 0  and                   \
               s.dir == 'dl'  and  (    \
                 etx_bytes != 0 or      \
                 uc.tx != 0 or uc.retx != 0 or uc.bitrate != 0      \
               )  and qci==9:
                sfnx = ((t // tti) / 10) % 1024  # = SFN.subframe
                _debug('% 4.1f ue%s %s .%d: etx_total_bytes: %d  +%5d  tx: %2d  retx: %d  ri: %d  bitrate: %d' % \
                        (sfnx, ue_id, s.dir, qci, etx_total_bytes, etx_bytes, uc.tx, uc.retx, uc.rank, uc.bitrate))

        # gc non-live erabs
        for erab_id in set(ue.erab_flows.keys()):
            if erab_id not in eflows_live:
                del ue.erab_flows[erab_id]

        # bitsync <- (δt, tx_bytes, u)
        if ue.bitsync is not None:
            bitnext = ue.bitsync.next(δt, tx_bytes, u)
        else:
            bitnext = [(δt, tx_bytes, u)]

        # update qci flows
        if init:
            continue
        ue._update_qci_flows(bitnext, qci_samples)


    # finish non-live ue
    for ue_id in set(s.ues.keys()):
        if ue_id not in ue_live:
            ue = s.ues.pop(ue_id)
            if ue.bitsync is not None:
                bitnext = ue.bitsync.finish()
                ue._update_qci_flows(bitnext, qci_samples)

    return qci_samples


# _update_qci_flows updates .qci_flows for ue with (δt, tx_bytes, _Utx) yielded from bitsync.
#
# yielded samples are appended to qci_samples  ({} qci -> []Sample).
@func(_UE)
def _update_qci_flows(ue, bitnext, qci_samples):
    for (δt, tx_bytes, u) in bitnext:
        assert len(u.cutx) == 1
        uc = _peek(u.cutx.values())
        qflows_live = set()  # of qci       qci flows that get updated from current utx entry

        # estimate time for current transmission
        # normalize transport blocks to time in TTI units (if it is e.g.
        # 2x2 mimo, we have 2x more transport blocks).
        δt_tti = δt / tti
        tx = (uc.tx + uc.retx) / uc.rank    # both transmission and retransmission take time
        tx = min(tx, δt_tti)            # protection (should not happen)

        # it might happen that even with correct bitsync we could end up with receiving tx=0 here.
        # for example it happens if finish interrupts proper bitsync workflow e.g. as follows:
        #
        #   1000    0
        #               <-- finish
        #      0   10
        #
        # if we see #tx = 0 we say that it might be anything in between 1 and δt.
        tx_lo = tx_hi = tx
        if tx == 0:
            tx_hi = δt_tti
            tx_lo = min(1, tx_hi)

        # tx time on the cell is somewhere in [tx, δt_tti]
        if uc.xl_use_avg < 0.9:
            # not congested: it likely took the time to transmit ≈ tx
            pass
        else:
            # potentially congested: we don't know how much congested it is and
            # which QCIs are affected more and which less
            # -> all we can say tx_time is only somewhere in between limits
            tx_hi = δt_tti


        # share/distribute tx time over all QCIs.
        for qci, tx_bytes_qci in u.qtx_bytes.items():
            qflows_live.add(qci)

            qf = ue.qci_flows.get(qci)
            if qf is None:
                qf = ue.qci_flows[qci] = _QCI_Flow()

            # Consider two streams "x" and "o" and how LTE scheduler might
            # place them into resource map: if the streams have the same
            # priority they might be scheduled e.g. as shown in case "a".
            # However if "x" has higher priority compared to "o" the
            # streams might be scheduled as shown in case "b":
            #
            #    ^               ^
            #  RB│  x x o o    RB│  x x o o
            #    │  o o x x      │  x x o o
            #    │  x x o o      │  x x o o
            #    │  o o x x      │  x x o o
            #
            #       ───────>        ───────>
            #           time            time
            #
            #       case "a"        case "b"
            #    same priority    pri(x) > pri(o)
            #
            #
            # Here overall #tx=4, but #tx(x) = 4 for case "a" and = 2 for case "b".
            #
            # -> without knowing QCI priorities and actual behaviour of LTE
            # scheduler we can only estimate #tx(x) to be:
            #
            #       tx_bytes(x)
            #       ───────────·#tx  ≤  #tx(x)  ≤  #tx
            #        Σtx_bytes
            qtx_lo = tx_bytes_qci * tx_lo / tx_bytes
            if qtx_lo > tx_hi:  # e.g. 6.6 * 11308 / 11308 = 6.6 + ~1e-15
                qtx_lo -= 1e-4
            assert 0 < qtx_lo <= tx_hi, (qtx_lo, tx_hi, tx_bytes_qci, tx_bytes)
            _ = qf.update(δt, tx_bytes_qci, qtx_lo, tx_hi)
            for sample in _:
                qci_samples.setdefault(qci, []).append(sample)

        # finish flows that did not get an update
        for qci in set(ue.qci_flows.keys()):
            if qci not in qflows_live:
                qf = ue.qci_flows.pop(qci)
                _ = qf.finish()
                for sample in _:
                    qci_samples.setdefault(qci, []).append(sample)

# _QCI_Flow() creates new empty flow.
@func(_QCI_Flow)
def __init__(qf):
    qf.tx_bytes    = 0
    qf.tx_time     = 0
    qf.tx_time_err = 0

# update updates flow with information that so many bytes were transmitted during
# δt with using tx transmission time somewhere in [tx_lo,tx_hi].
@func(_QCI_Flow)
def update(qf, δt, tx_bytes, tx_lo, tx_hi):  # -> []Sample
    #_debug('QF.update %.2ftti %5db %.1f-%.1ftx' % (δt/tti, tx_bytes, tx_lo, tx_hi))

    vout = []
    s = qf._update(δt, tx_bytes, tx_lo, tx_hi)
    if s is not None:
        vout.append(s)
    return vout

@func(_QCI_Flow)
def _update(qf, δt, tx_bytes, tx_lo, tx_hi): # -> ?Sample
    assert tx_bytes > 0
    δt_tti = δt / tti

    tx_time     = (tx_lo + tx_hi) / 2 * tti
    tx_time_err = (tx_hi - tx_lo) / 2 * tti

    cont = (qf.tx_time != 0)  # if this update is continuing current sample

    qf.tx_bytes    += tx_bytes
    qf.tx_time     += tx_time
    qf.tx_time_err += tx_time_err

    # if we are continuing the sample, it might be that current update is either small or big.
    # - if it is big - the sample continues.
    # - if it is not big - it coalesces and ends the sample.
    # NOTE: without throwing away last tti the overall throughput statistics
    #       stays the same irregardless of whether we do coalesce small txes or not.
    if cont and tx_hi < 0.9*δt_tti:
        s = qf._sample()
        qf.tx_bytes    = 0
        qf.tx_time     = 0
        qf.tx_time_err = 0
        return s
    return None

# finish tells the flow that no updates will be coming anymore.
@func(_QCI_Flow)
def finish(qf):  # -> []Sample
    #_debug('QF.finish')
    vout = []
    if qf.tx_time != 0:
        s = qf._sample()
        qf.tx_bytes     = 0
        qf.tx_time      = 0
        qf.tx_time_err  = 0
        vout.append(s)
    return vout

# _sample creates new Sample from what accumulated in the flow.
@func(_QCI_Flow)
def _sample(qf):
    s = Sample()
    s.tx_bytes    = qf.tx_bytes
    s.tx_time     = qf.tx_time
    s.tx_time_err = qf.tx_time_err
    assert s.tx_bytes    >  0  and  \
           s.tx_time     >  0  and  \
           s.tx_time_err >= 0  and  \
           s.tx_time - s.tx_time_err > 0 \
           , s
    #_debug("  ", s)
    return s


# _BitSync creates new empty bitsync.
@func(_BitSync)
def __init__(s):
    s.txq = []
    s.i_txq     = 0
    s.i_lshift  = 0

# next feeds next (δt, tx_bytes, _Utx) into bitsync.
#
# and returns ready parts of adjusted stream.
@func(_BitSync)
def next(s, δt, tx_bytes, u: _Utx): # -> [](δt', tx_bytes', u')
    s.txq.append((δt, tx_bytes, u))

    # move all time to .tx
    assert len(u.cutx) == 1
    uc = _peek(u.cutx.values())
    uc.tx   += uc.retx
    uc.retx  = 0

    # XXX for simplicity we currently handle sync in between only current and
    # next frames. That is enough to support FDD. TODO handle next-next case to support TDD
    #
    # XXX for simplicity we also assume all δt are ~ 10·tti and do not generally handle them
    # TODO handle arbitrary δt

    # shift #tx to the left:
    #
    # in previous frame₁ we saw that transmitting tx_bytes₁ resulted in tx₁
    # transport blocks in that frame. In the next frame we saw tx_bytes₂
    # transmission and tx₂ transport blocks. That tx₂ is the sum of transport
    # blocks a) acknowledged in frame₂, but originally transmitted in frame₁,
    # and b) transmitted in frame₂ and acknowledged in that same frame₂:
    #
    #   tx_bytes₁     tx₁
    #   tx_bytes₂     tx₂  = t₂(1) + t₂(2)
    #
    # we can estimate t₂(2) by assuming that tx_bytes transmission results in
    # proportional #tx in that frame. i.e.
    #
    #     tx₁         t₂(2)
    #   ───────── = ─────────
    #   tx_bytes₁   tx_bytes₂
    #
    # and then having t₂(2) we can know t₂(1) = tx₂-t₂(2).
    #
    # The result of transport blocks associated with frame₁ is tx₁+t₂(1).
    def lshift(i):
        #print('  > lshift', i, s.txq)
        assert s.i_txq <= i < s.i_txq + len(s.txq)
        i -= s.i_txq

        δt1, b1, u1 = s.txq[i];     uc1 = _peek(u1.cutx.values());   t1 = uc1.tx
        δt2, b2, u2 = s.txq[i+1];   uc2 = _peek(u2.cutx.values());   t2 = uc2.tx
        if b1 != 0:
            t22 = b2*t1/b1
        else:
            t22 = t2
        t21 = t2-t22
        if t21 > 0:
            # e.g. b₁=1000 t₁=10, b₂=1000, t₂=0  yields t21=-10
            t1 += t21   # move t21 from frame₂ -> frame₁
            t2 -= t21
            assert t1 >= 0, t1
            assert t2 >= 0, t2

        uc1.tx = t1
        uc2.tx = t2
        s.txq[i]   = (δt1, b1, u1)
        s.txq[i+1] = (δt2, b2, u2)
        #print('  < lshift  ', s.txq)

    while s.i_lshift+1 < s.i_txq + len(s.txq):
        lshift(s.i_lshift)
        s.i_lshift += 1

    # we are close to be ready to yield txq[0].
    # yield it, after balancing #tx again a bit, since ^^^ procedure can yield
    # t=0 for b!=0 e.g. for
    #
    #   1000    0
    #   1000   10
    #      0    0
    vout = []
    while len(s.txq) >= 3:
        s._rebalance(2)
        _ = s.txq.pop(0)
        s.i_txq += 1
        vout.append(_)
    return vout

# finish tells bitsync to flush its output queue.
#
# the bitsync becomes reset.
@func(_BitSync)
def finish(s): # -> [](δt', tx_bytes', tx')
    assert len(s.txq) < 3
    s._rebalance(len(s.txq))
    vout = s.txq
    s.txq = []
    return vout

# _rebalance redistributes tx_i in .txq[:l] proportional to tx_bytes_i:
#
# We adjust #tx as follows: consider 3 transmission entries that each sent
# b_i bytes and yielded t_i for #tx. We want to adjust t_i -> t'_i so that
# t'_i correlates with b_i and that whole transmission time stays the same:
#
#       b₁  t₁      t'₁
#       b₂  t₂  ->  t'₂     t'_i = α·b_i   Σt' = Σt
#       b₃  t₃      t'₃
#
# that gives
#
#           Σt
#       α = ──
#           Σb
#
# and has the effect of moving #tx from periods with tx_bytes=0, to periods
# where transmission actually happened (tx_bytes > 0).
@func(_BitSync)
def _rebalance(s, l):
    #print('  > rebalance', s.txq[:l])
    assert l <= len(s.txq)
    assert l <= 3

    Σb = sum(_[1] for _ in s.txq[:l])
    Σt = sum(_peek(_[2].cutx.values()).tx for _ in s.txq[:l])
    if Σb != 0:
        for i in range(l):
            δt_i, b_i, u_i = s.txq[i];  uc_i = _peek(u_i.cutx.values());  t_i = uc_i.tx
            t_i = b_i * Σt / Σb
            assert t_i >= 0, t_i
            uc_i.tx = t_i
            s.txq[i] = (δt_i, b_i, u_i)
    #print('  < rebalance', s.txq[:l])


# __repr__ returns human-readable representation of Sample.
@func(Sample)
def __repr__(s):
    def div(a,b):
        if b != 0:
            return a/b
        return float('inf')  if a != 0  else \
               float('nan')

    t_lo = s.tx_time - s.tx_time_err
    t_hi = s.tx_time + s.tx_time_err
    b_lo = div(s.tx_bytes*8, t_hi)
    b_hi = div(s.tx_bytes*8, t_lo)
    return "Sample(%db, %.1f ±%.1ftti)\t# %.0f ±%.0f bit/s" % \
            (s.tx_bytes, s.tx_time/tti, s.tx_time_err/tti, div(s.tx_bytes*8, s.tx_time), (b_hi - b_lo)/2)


# ----------------------------------------

# _x_stats_srv provides server for x.drb_stats queries.
#
# To do so it polls eNB every 10ms at 100Hz frequency with `ue_get[stats]`
# and tries to further improve accuracy of retrieved DL/UL samples timing
# towards 1ms via heuristic on how much transport blocks were tx/rx'ed
# during each observation.
#
# This heuristic can be used unless eNB is congested. To detect congestion
# _x_stats_srv also polls eNB with `stats` at the same 100Hz frequency and
# synchronized in time with `ue_get[stats]`. The congestion is detected by
# dl_use_avg / ul_use_avg being close to 1.
#
# Since we can detect only the fact of likely congestion, but not the level
# of congestion, nor other details related to QCIs priorities, for congested
# case the heuristic is not used and throughput is reported via rough, but
# relatively true, interval estimates.
#
# NOTE we cannot go polling to higher than 100Hz frequency, since enb
# rate-limits websocket requests to execute not faster than 10ms each.
@func
def _x_stats_srv(ctx, reqch: chan, conn: amari.Conn):
    δt_rate = 10*tti

    # rx_ue_get_stats sends `ue_get[stats]` request and returns server response.
    rtt_ue_stats = _IncStats() # time it takes to send ue_get and to receive response
    δt_ue_stats  = _IncStats() # δ(ue_stats.timestamp)
    t_ue_stats   = None        # last ue_stats.timestamp
    def rx_ue_get_stats(ctx): # -> ue_stats
        nonlocal t_ue_stats
        t_tx = time.now()
        ue_stats = conn.req(ctx, 'ue_get', {'stats': True})
        t_rx = time.now()
        rtt_ue_stats.add(t_rx-t_tx)
        t = ue_stats['utc']
        if t_ue_stats is not None:
            δt_ue_stats.add(t-t_ue_stats)
        t_ue_stats = t
        return ue_stats

    # rx_stats sends `stats` request and returns server response.
    # we need to query stats to get dl_use/ul_use.
    # Establish separate connection for that since if we use the same conn for
    # both ue_get and stats queries, due to overall 100Hz rate-limiting, ue_get
    # would be retrieved at only 50Hz rate. With separate connection for stats
    # we can retrieve both ue_get and stats each at 100Hz simultaneously.
    conn_stats = amari.connect(ctx, conn.wsuri)
    defer(conn_stats.close)
    rtt_stats = _IncStats() # like rtt_ue_stats but for stat instead of ue_get
    δt_stats  = _IncStats() # δ(stats.timestamp)
    t_stats   = None        # last stats.timestamp
    def rx_stats(ctx): # -> stats
        nonlocal t_stats
        t_tx = time.now()
        stats = conn_stats.req(ctx, 'stats', {})
        t_rx = time.now()
        rtt_stats.add(t_rx-t_tx)
        t = stats['utc']
        if t_stats is not None:
            δt_stats.add(t-t_stats)
        t_stats = t
        return stats
    # issue first dummy stats. It won't report most of statistics due to
    # initial_delay=0, but it will make the next stats query avoid pausing for 0.4s.
    conn_stats.req(ctx, 'stats', {'initial_delay': 0})

    # rx_all simultaneously issues `ue_get[stats]` and `stats` requests and returns server responses.
    # the requests are issued synchronized in time.
    δ_ue_stats = _IncStats() # ue_stats.timestamp - stats.timestamp
    def rx_all(ctx): # -> ue_stats, stats
        uq = chan(1)
        sq = chan(1)

        _, _rx = select(
            ctx.done().recv,        # 0
            (ueget_reqch.send, uq), # 1
        )
        if _ == 0:
            raise ctx.err()

        _, _rx = select(
            ctx.done().recv,        # 0
            (stats_reqch.send, sq), # 1
        )
        if _ == 0:
            raise ctx.err()

        ue_stats = stats = None
        while ue_stats is None  or  stats is None:
            _, _rx = select(
                ctx.done().recv,    # 0
                uq.recv,            # 1
                sq.recv,            # 2
            )
            if _ == 0:
                raise ctx.err()
            if _ == 1:
                ue_stats = _rx
                uq = nilchan
            if _ == 2:
                stats = _rx
                sq = nilchan

        δ_ue_stats.add(ue_stats['utc'] - stats['utc'])
        return ue_stats, stats

    ueget_reqch = chan()
    def Trx_ue_get(ctx):
        while 1:
            _, _rx = select(
                ctx.done().recv,    # 0
                ueget_reqch.recv,   # 1
            )
            if _ == 0:
                raise ctx.err()
            retq = _rx

            ue_stats = rx_ue_get_stats(ctx)
            retq.send(ue_stats) # cap = 1

    stats_reqch = chan()
    def Trx_stats(ctx):
        while 1:
            _, _rx = select(
                ctx.done().recv,    # 0
                stats_reqch.recv,   # 1
            )
            if _ == 0:
                raise ctx.err()
            retq = _rx

            stats = rx_stats(ctx)
            retq.send(stats) # cap = 1

    # Tmain is the main thread that drives the process overall
    def Tmain(ctx):
        nonlocal rtt_ue_stats, δt_ue_stats
        nonlocal rtt_stats, δt_stats
        nonlocal δ_ue_stats

        t_req = time.now()
        ue_stats, stats = rx_all(ctx)

        S = Sampler(ue_stats, stats)
        qci_Σdl = {}  # qci -> _Σ  for dl
        qci_Σul = {}  # ----//---- for ul
        class _Σ:
            __slots__ = (
                'tx_bytes',
                'tx_time',
                'tx_time_err',
                'tx_time_notailtti',
                'tx_time_notailtti_err',
                'tx_nsamples',
            )
            def __init__(Σ):
                for x in Σ.__slots__:
                    setattr(Σ, x, 0)
        # account accounts samples into Σtx_time/Σtx_bytes in qci_Σ.
        def account(qci_Σ, qci_samples):
            for qci, samplev in qci_samples.items():
                Σ = qci_Σ.get(qci)
                if Σ is None:
                    Σ = qci_Σ[qci] = _Σ()
                for s in samplev:
                    # do not account short transmissions
                    # ( tx with 1 tti should be ignored per standard, but it is
                    #   also that small ICMP messages span 2 transport blocks sometimes )
                    t_lo = s.tx_time - s.tx_time_err
                    t_hi = s.tx_time + s.tx_time_err
                    if t_hi <= 1*tti  or  (t_hi <= 2 and s.tx_bytes < 1000):
                        continue
                    Σ.tx_nsamples += 1
                    Σ.tx_bytes    += s.tx_bytes
                    Σ.tx_time     += s.tx_time
                    Σ.tx_time_err += s.tx_time_err

                    # also aggregate .tx_time without tail tti (IP Throughput KPI needs this)
                    tt_hi = math.ceil(t_hi/tti - 1) # in tti
                    tt_lo = t_lo / tti              # in tti
                    if tt_lo > 1:
                        tt_lo = math.ceil(tt_lo - 1)
                    tt     = (tt_lo + tt_hi) / 2
                    tt_err = (tt_hi - tt_lo) / 2
                    Σ.tx_time_notailtti     += tt     * tti
                    Σ.tx_time_notailtti_err += tt_err * tti


        while 1:
            # TODO explicitly detect underrun?
            _, _rx = select(
                ctx.done().recv,    # 0
                reqch.recv,         # 1
                default,            # 2
            )
            if _ == 0:
                raise ctx.err()
            if _ == 1:
                # client requests to retrieve message for accumulated data
                opts, respch = _rx
                # TODO verify/handle opts?

                # wrap-up flows and account finalized samples
                qci_dl, qci_ul = S.finish()
                account(qci_Σdl, qci_dl)
                account(qci_Σul, qci_ul)

                _debug()
                _debug('rtt_ue:     %s  ms' % rtt_ue_stats .str('%.2f', time.millisecond))
                _debug('δt_ue:      %s  ms' % δt_ue_stats  .str('%.2f', time.millisecond))
                _debug('rtt_stats:  %s  ms' % rtt_stats    .str('%.2f', time.millisecond))
                _debug('δt_stats:   %s  ms' % δt_stats     .str('%.2f', time.millisecond))
                _debug('δ(ue,stat): %s  ms' % δ_ue_stats   .str('%.2f', time.millisecond))

                qci_dict = {}
                Σ0 = _Σ()
                for qci in set(qci_Σdl.keys()) .union(qci_Σul.keys()):
                    Σdl = qci_Σdl.get(qci, Σ0)
                    Σul = qci_Σul.get(qci, Σ0)
                    qci_dict[qci] = {
                         'dl_tx_bytes':               Σdl.tx_bytes,
                         'dl_tx_time':                Σdl.tx_time,
                         'dl_tx_time_err':            Σdl.tx_time_err,
                         'dl_tx_time_notailtti':      Σdl.tx_time_notailtti,
                         'dl_tx_time_notailtti_err':  Σdl.tx_time_notailtti_err,
                         'dl_tx_nsamples':            Σdl.tx_nsamples,
                         'ul_tx_bytes':               Σul.tx_bytes,
                         'ul_tx_time':                Σul.tx_time,
                         'ul_tx_time_err':            Σul.tx_time_err,
                         'ul_tx_time_notailtti':      Σul.tx_time_notailtti,
                         'ul_tx_time_notailtti_err':  Σul.tx_time_notailtti_err,
                         'u;_tx_nsamples':            Σul.tx_nsamples,
                    }

                r = {'time':       ue_stats['time'],
                     'utc':        ue_stats['utc'],
                     'qci_dict':   qci_dict,
                     'δt_ueget': {
                        'min': δt_ue_stats.min,
                        'avg': δt_ue_stats.avg(),
                        'max': δt_ue_stats.max,
                        'std': δt_ue_stats.std(),
                     },
                     'δ_ueget_vs_stats': {
                        'min': δ_ue_stats.min,
                        'avg': δ_ue_stats.avg(),
                        'max': δ_ue_stats.max,
                        'std': δ_ue_stats.std(),
                     },
                }

                respch.send(r)

                # reset
                qci_Σdl = {}
                qci_Σul = {}

                rtt_ue_stats = _IncStats()
                δt_ue_stats  = _IncStats()
                rtt_stats    = _IncStats()
                δt_stats     = _IncStats()
                δ_ue_stats   = _IncStats()

            # sync time to keep t_req' - t_req ≈ δt_rate
            # this should automatically translate to δt(ue_stats) ≈ δt_rate
            t = time.now()
            δtsleep = δt_rate - (t - t_req)
            if δtsleep > 0:
                time.sleep(δtsleep)

            # retrieve ue_get[stats] and stats data for next frame from enb
            t_req = time.now()
            ue_stats, stats = rx_all(ctx)

            # pass data to sampler and account already detected samples
            qci_dl, qci_ul = S.add(ue_stats, stats)
            account(qci_Σdl, qci_dl)
            account(qci_Σul, qci_ul)

    # run everything
    wg = sync.WorkGroup(ctx)
    wg.go(Trx_ue_get)
    wg.go(Trx_stats)
    wg.go(Tmain)
    wg.wait()


# _IncStats incrementally computes statistics on provided values.
#
# Provide values via .add().
# Retrieve statistical properties via .avg/.std/.var/.min/.max .
class _IncStats:
    __slots__ = (
        'n',    # number of samples seen so far
        'μ',    # current mean
        'σ2',   # ~ current variance
        'min',  # current min / max
        'max',
    )

    def __init__(s):
        s.n = 0
        s.μ = 0.
        s.σ2 = 0.
        s.min = +float('inf')
        s.max = -float('inf')

    def add(s, x):
        # https://www.johndcook.com/blog/standard_deviation/
        s.n  += 1
        μ_ = s.μ   # μ_{n-1}
        s.μ  += (x - μ_)/s.n
        s.σ2 += (x - μ_)*(x - s.μ)

        s.min = min(s.min, x)
        s.max = max(s.max, x)

    def avg(s):
        if s.n == 0:
            return float('nan')
        return s.μ

    def var(s):
        if s.n == 0:
            return float('nan')
        return s.σ2 / s.n   # note johndcook uses / (s.n-1) to unbias

    def std(s):
        return math.sqrt(s.var())


    def __str__(s):
        return s.str('%s', 1)

    def str(s, fmt, scale):
        t = "min/avg/max/σ  "
        if s.n == 0:
            t += "?/?/? ±?"
        else:
            μ   = s.avg() / scale
            σ   = s.std() / scale
            min = s.min   / scale
            max = s.max   / scale

            f = "%s/%s/%s ±%s" % ((fmt,)*4)
            t += f % (min, μ, max, σ)
        return t


# ----------------------------------------

__debug = False
def _debug(*argv):
    if __debug:
        print(*argv, file=sys.stderr)


# _peek peeks first item from a sequence.
# it is handy to use e.g. as _peek(dict.values()).
def _peek(seq):
    return next(iter(seq))
