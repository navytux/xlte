# -*- coding: utf-8 -*-
# Copyright (C) 2023  Nexedi SA and Contributors.
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
"""Package drb provides infrastructure to process flows on data radio bearers.

- Sampler converts information about data flows obtained via ue_get[stats] into
  Samples that represent bursts of continuous transmissions.
"""


from golang import func
from golang import time

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
#   .next(δt, tx_bytes, #tx, X)  ->  [](δt', tx_bytes', #tx', X')
#   .finish()                    ->  [](δt', tx_bytes', #tx', X')
#
# (*) see e.g. Figure 8.1 in "An introduction to LTE, 2nd ed."
class _BitSync:
    __slots__ = (
        'txq',          # [](δt,tx_bytes,#tx,X)     not-yet fully processed tail of whole txv
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

class _Utx:  # transmission state passed through bitsync
    __slots__ = (
        'qtx_bytes',
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
        tx   = cell['%s_tx'   % s.dir]  # in transport blocks
        retx = cell['%s_retx' % s.dir]  # ----//----
        assert tx   >= 0, tx
        assert retx >= 0, retx

        cell_id = cell['cell_id']  # int
        scell = stats['cells'][str(cell_id)]

        u = _Utx()
        u.qtx_bytes  = {}  # qci -> Σδerab_qci=qci
        u.rank       = cell['ri']  if s.use_ri  else 1
        u.xl_use_avg = scell['%s_use_avg' % s.dir]

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
            if 0  and  s.dir == 'dl'  and  (etx_bytes != 0 or tx != 0 or retx != 0)  and qci==9:
                sfnx = ((t // tti) / 10) % 1024  # = SFN.subframe
                _debug('% 4.1f ue%s %s .%d: etx_total_bytes: %d  +%5d  tx: %2d  retx: %d  ri: %d  bitrate: %d' % \
                        (sfnx, ue_id, s.dir, qci, etx_total_bytes, etx_bytes, tx, retx, u.rank, cell['%s_bitrate' % s.dir]))

        # gc non-live erabs
        for erab_id in set(ue.erab_flows.keys()):
            if erab_id not in eflows_live:
                del ue.erab_flows[erab_id]

        # bitsync <- (δt, tx_bytes, #tx, u)
        tx += retx # both transmission and retransmission take time
        if ue.bitsync is not None:
            bitnext = ue.bitsync.next(δt, tx_bytes, tx, u)
        else:
            bitnext = [(δt, tx_bytes, tx, u)]

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


# _update_qci_flows updates .qci_flows for ue with (δt, tx_bytes, #tx, _Utx) yielded from bitsync.
#
# yielded samples are appended to qci_samples  ({} qci -> []Sample).
@func(_UE)
def _update_qci_flows(ue, bitnext, qci_samples):
    for (δt, tx_bytes, tx, u) in bitnext:
        qflows_live = set()  # of qci       qci flows that get updated from current utx entry

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
            tx_hi = δt/tti
            tx_lo = min(1, tx_hi)

        for qci, tx_bytes_qci in u.qtx_bytes.items():
            qflows_live.add(qci)

            qf = ue.qci_flows.get(qci)
            if qf is None:
                qf = ue.qci_flows[qci] = _QCI_Flow()

            # share/distribute #tx transport blocks over all QCIs.
            #
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
            _ = qf.update(δt, tx_bytes_qci, qtx_lo, tx_hi, u.rank, u.xl_use_avg)
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
# δt with using #tx transport blocks somewhere in [tx_lo,tx_hi] and with
# specified rank. It is also known that overall average usage of resource
# blocks corresponding to tx direction in the resource map is xl_use_avg.
@func(_QCI_Flow)
def update(qf, δt, tx_bytes, tx_lo, tx_hi, rank, xl_use_avg):  # -> []Sample
    #_debug('QF.update %.2ftti %5db %.1f-%.1ftx %drank %.2fuse' % (δt/tti, tx_bytes, tx_lo, tx_hi, rank, xl_use_avg))

    tx_lo /= rank # normalize TB to TTI (if it is e.g. 2x2 mimo, we have 2x more transport blocks)
    tx_hi /= rank

    vout = []
    s = qf._update(δt, tx_bytes, tx_lo, tx_hi, xl_use_avg)
    if s is not None:
        vout.append(s)
    return vout

@func(_QCI_Flow)
def _update(qf, δt, tx_bytes, tx_lo, tx_hi, xl_use_avg): # -> ?Sample
    assert tx_bytes > 0
    δt_tti = δt / tti

    tx_lo = min(tx_lo, δt_tti)  # protection (should not happen)
    tx_hi = min(tx_hi, δt_tti)  # protection (should not happen)

    # tx time is somewhere in [tx, δt_tti]
    if xl_use_avg < 0.9:
        # not congested: it likely took the time to transmit ≈ #tx
        pass
    else:
        # potentially congested: we don't know how much congested it is and
        # which QCIs are affected more and which less
        # -> all we can say tx_time is only somewhere in between limits
        tx_hi = δt_tti
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

# next feeds next (δt, tx_bytes, tx) into bitsync.
#
# and returns ready parts of adjusted stream.
@func(_BitSync)
def next(s, δt, tx_bytes, tx, X): # -> [](δt', tx_bytes', tx', X')
    s.txq.append((δt, tx_bytes, tx, X))

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

        δt1, b1, t1, X1 = s.txq[i]
        δt2, b2, t2, X2 = s.txq[i+1]
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

        s.txq[i]   = (δt1, b1, t1, X1)
        s.txq[i+1] = (δt2, b2, t2, X2)
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
def finish(s): # -> [](δt', tx_bytes', tx', X')
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
    Σt = sum(_[2] for _ in s.txq[:l])
    if Σb != 0:
        for i in range(l):
            δt_i, b_i, t_i, X_i = s.txq[i]
            t_i = b_i * Σt / Σb
            assert t_i >= 0, t_i
            s.txq[i] = (δt_i, b_i, t_i, X_i)
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

__debug = False
def _debug(*argv):
    if __debug:
        print(*argv, file=sys.stderr)
