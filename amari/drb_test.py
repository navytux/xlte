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

from __future__ import print_function, division, absolute_import

from xlte.amari.drb import _Sampler, Sample, _BitSync, tti, _IncStats
import numpy as np
from golang import func


# tSampler, UE, Etx and S provide infrastructure for testing _Sampler:

# Etx represents transmission on erab with qci of tx_bytes.
class Etx:
    def __init__(etx, erab_id, qci, tx_bytes, tx_total=False):
        etx.erab_id  = erab_id
        etx.qci      = qci
        etx.tx_bytes = tx_bytes
        etx.tx_total = tx_total

# UE represents one entry about an UE in ue_get[stats].ue_list .
class UE:
    def __init__(ue, ue_id, tx, retx, *etxv, ri=1):
        for _ in etxv:
            assert isinstance(_, Etx)
        ue.ue_id = ue_id
        ue.tx   = tx
        ue.retx = retx
        ue.etxv = etxv
        ue.ri   = ri

# tSampler provides testing environment for _Sampler.
#
# For easier testing and contrary to _Sampler collected samples are returned as
# a whole from final get, not incrementally.
class tSampler:
    def __init__(t, *uev, use_bitsync=False, use_ri=False):
        t.tstats = _tUEstats()
        ue_stats0, stats0 = t.tstats.next(0, *uev)
        t.sampler = _Sampler('zz', ue_stats0, stats0, use_bitsync=use_bitsync, use_ri=use_ri)
        t.qci_samples = {}  # in-progress collection until final get

    def add(t, δt_tti, *uev):
        ue_stats, stats = t.tstats.next(δt_tti, *uev)
        qci_samples = t.sampler.add(ue_stats, stats)
        t._update_qci_samples(qci_samples)

    def get(t):
        qci_samples = t.sampler.finish()
        t._update_qci_samples(qci_samples)
        qci_samples = t.qci_samples
        t.qci_samples = {}
        return qci_samples

    def _update_qci_samples(t, qci_samples):
        for (qci, samplev) in qci_samples.items():
            t.qci_samples.setdefault(qci, []).extend(samplev)


# _tUEstats provides environment to generate test ue_get[stats].
class _tUEstats:
    def __init__(t):
        t.τ = 0
        t.tx_total = {} # (ue,erab) -> tx_total_bytes

    # next returns next (ue_stats, stats) with specified ue transmissions
    def next(t, δτ_tti, *uev):
        for _ in uev:
            assert isinstance(_, UE)
        t.τ += δτ_tti * tti
        tx_total = t.tx_total
        t.tx_total = {} # if ue/erab is missing in ue_stats, its tx_total is reset

        ue_list = []
        ue_stats = {
            'time': t.τ,
            'utc':  100 + t.τ,
            'ue_list': ue_list
        }
        for ue in uev:
            erab_list = []
            ue_list.append({
                'enb_ue_id': ue.ue_id,  # TODO test both 4G and 5G flavours
                'cells': [
                    {
                        'cell_id': 1,
                        'ri':      ue.ri,
                        'zz_tx':   ue.tx,
                        'zz_retx': ue.retx,
                    }
                ],
                'erab_list': erab_list,
            })
            for etx in ue.etxv:
                efkey = (ue.ue_id, etx.erab_id)
                etx_total = etx.tx_bytes
                if not etx.tx_total:
                    etx_total += tx_total.get(efkey, 0)
                t.tx_total[efkey] = etx_total

                erab_list.append({
                    'erab_id':          etx.erab_id,
                    'qci':              etx.qci,
                    'zz_total_bytes':   etx_total,
                })

        stats = {
            'time':  ue_stats['time'],
            'utc':   ue_stats['utc'],
            'cells': {
                '1': {
                    'zz_use_avg': 0.1   # TODO add test for congested case
                }
            }
        }

        return ue_stats, stats


# S is shortcut to create Sample.
def S(tx_bytes, tx_time_tti):
    if isinstance(tx_time_tti, tuple):
        τ_lo, τ_hi = tx_time_tti
    else:
        τ_lo = τ_hi = tx_time_tti

    s = Sample()
    s.tx_bytes    = tx_bytes
    s.tx_time     = (τ_lo + τ_hi) / 2 * tti
    s.tx_time_err = (τ_hi - τ_lo) / 2 * tti
    return s


# -------- tests --------

# test_Sampler1 verifies Sampler on single erab/qci flows.
def test_Sampler1():
    # _ constructs tSampler, feeds tx stats into it and returns yielded Samples.
    #
    # tx_statsv = [](δt_tti, tx_bytes, #tx, #retx)
    #
    # only 1 ue, 1 qci and 1 erab are used in this test to verify the tricky
    # parts of the Sampler in how single flow is divided into samples. The other
    # tests verify how Sampler handles other aspects - e.g. multiple erabs,
    # multiple qci, etc...
    def _(*tx_statsv, bitsync=None):  # -> []Sample
        def b(bitsync):
            t = tSampler(use_bitsync=bitsync)
            for (δt_tti, tx_bytes, tx, retx) in tx_statsv:
                t.add(δt_tti, UE(17, tx, retx, Etx(23, 4, tx_bytes)))
            qci_samplev = t.get()
            if len(qci_samplev) == 0:
                return []
            assert set(qci_samplev.keys()) == {4}
            return qci_samplev[4]
        boff = None # verify with both bitsync=off/on if bitsync=None
        bon  = None
        if bitsync is None  or (not bitsync):
            boff = b(False)
        if bitsync is None  or      bitsync:
            bon  = b(True)
        if bitsync is None:
            assert boff == bon
        return bon  if bitsync else  boff


    #      δt_tti tx_bytes  #tx #retx
    assert _()                          == []
    assert _((10, 1000,      1,  0))    == [S(1000, 1)]
    assert _((10, 1000,      2,  0))    == [S(1000, 2)]
    assert _((10, 1000,      3,  0))    == [S(1000, 3)]
    for tx in range(2,10+1):
        assert _((10,1000,  tx,  0))    == [S(1000, tx)]

    assert _((10, 1000,      1,  1))    == [S(1000, 2)] # 1 tx + 1 retx = 2 TTI
    assert _((10, 1000,      1,  2))    == [S(1000, 3)] # tx_time is estimated via (tx+retx)
    for tx in range(1,10+1):
      for retx in range(1,10-tx+1):
        assert _((10,1000,  tx, retx))  == [S(1000, tx+retx)]

    assert _((10, 1000,      77, 88))   == [S(1000, 10)]  # tx_time ≤ δt  (bug in #tx / #retx)

    # coalesce/wrap-up 2 frames
    def _2tx(tx1, tx2):  return _((10, 100*tx1, tx1, 0),
                                  (10, 100*tx2, tx2, 0))
    assert _2tx(4, 3)   == [S(700,7)]   # small tx1 and tx2: coalesce as if tx1 comes in the end of frame₁
    assert _2tx(4, 4)   == [S(800,8)]   # and tx2 in the beginning of frame₂
    assert _2tx(4, 5)   == [S(900,9)]   # ----//----
    assert _2tx(3, 5)   == [S(800,8)]   # ...
    assert _2tx(2, 5)   == [S(700,7)]
    assert _2tx(5, 4)   == [S(900,9)]
    assert _2tx(5, 3)   == [S(800,8)]
    assert _2tx(5, 2)   == [S(700,7)]
    assert _2tx(10, 0)  == [S(1000,10)] # full + no tx
    assert _2tx(10, 1)  == [S(1100,11)] # full + 1 tti tx
    assert _2tx(10, 2)  == [S(1200,12)] # full + 2 ttis
    for tx2 in range(2,10+1):
        assert _2tx(10, tx2)  == [S((10+tx2)*100, 10+tx2)]

    # coalesce/wrap-up 3 frames: small tx + med-full + small tx
    def _3tx(tx1, tx2, tx3):  return _((10, 100*tx1, tx1, 0),
                                       (10, 100*tx2, tx2, 0),
                                       (10, 100*tx3, tx3, 0))
    assert _3tx(4, 0, 3)  == [S(400,4), S(300,3)]   # empty middle
    assert _3tx(4, 1, 3)  == [S(500,5), S(300,3)]   # middle only 1 tti - coalesced to left
    assert _3tx(4, 2, 3)  == [S(600,6), S(300,3)]   # middle small      - coalesced to left
    assert _3tx(4, 3, 3)  == [S(700,7), S(300,3)]   # ----//----
    assert _3tx(4, 4, 3)  == [S(800,8), S(300,3)]   # ----//----
    assert _3tx(4, 8, 3)  == [S(1200,12), S(300,3)] # ----//----
    assert _3tx(4, 9, 3)  == [S(1600,16)]           # middle big - coalesced to left and right
    assert _3tx(4,10, 3)  == [S(1700,17)]           # ----//----

    # coalesce/wrap-up 4 frames: small tx + med-full + med-full + small tx
    def _4tx(tx1, tx2, tx3, tx4):  return _((10, 100*tx1, tx1, 0),
                                            (10, 100*tx2, tx2, 0),
                                            (10, 100*tx3, tx3, 0),
                                            (10, 100*tx4, tx4, 0))
    assert _4tx(4, 0, 0, 3)  == [S(400,4), S(300,3)]    # empty m1, m2
    assert _4tx(4, 1, 0, 3)  == [S(500,5), S(300,3)]    # m1 - only 1 tti - coalesces to left
    assert _4tx(4, 0, 1, 3)  == [S(400,4), S(400,4)]    # m2 - only 1 tti - coalesces to right
    assert _4tx(4, 2, 0, 3)  == [S(600,6), S(300,3)]    # m1 small - coalesces to left
    assert _4tx(4, 0, 2, 3)  == [S(400,4), S(500,5)]    # m2 small - coalesces to right
    assert _4tx(4, 3, 4, 3)  == [S(700,7), S(700,7)]    # m1 and m2 small - m1 coalesces to left, m2 to right
    assert _4tx(4, 9, 4, 3)  == [S(400+900+400,4+9+4), S(300,3)]    # m1 big - coalesces s1 and m2
    assert _4tx(4, 3, 9, 3)  == [S(700,7), S(1200,12)]  # m2 big - it only starts new sample and coalesces to right
    assert _4tx(4, 9,10, 3)  == [S(400+900+1000+300,4+9+10+3)]  # m1 and m2 big - all coalesces


    # zero #tx
    # this might happen even with bitsync if finish divides the stream at an
    # unfortunate moment e.g. as follows:
    #
    #   1000    0
    #               <-- finish
    #      0   10
    assert _((10, 1000,      0,  0))    == [S(1000, (1,10))]

    # bitsync lightly (BitSync itself is verified in details in test_BitSync)
    def b(*btx_statsv):
        tx_statsv = []
        for (tx_bytes, tx) in btx_statsv:  # note: no δt_tti, #retx
            tx_statsv.append((10, tx_bytes, tx, 0))
        return _(*tx_statsv, bitsync=True)

    #      tx_bytes #tx
    assert b()              == []
    assert b((1000,  0))    == [S(1000, (1,10))]
    assert b((1000,  0),
             (0,    10))    == [S(1000, 10)]

    assert b((1000,  4), # 4
             ( 500,  8), # 6 2
             (1000,  7), #   3 4
             (   0,  6), #     6
             (   0,  0))    == [S(1000+500,10+5), S(1000,10)]


# sampler starts from non-scratch - correctly detects δ for erabs.
def test_Sampler_start_from_nonscratch():
    t = tSampler(UE(17, 0,0, Etx(23, 4, 10000, tx_total=True)))
    t.add(10, UE(17, 10,0, Etx(23, 4, 123)))
    assert t.get() == {4: [S(123,10)]}


# erab disappears and appears again -> tx_total_bytes is reset
def test_Sampler_erab_reestablish():
    def ue(tx, *etxv):  return UE(17, tx, 0, *etxv)
    def etx(tx_bytes):  return Etx(23, 4, tx_bytes, tx_total=True)

    t = tSampler()
    t.add(10, ue(2, etx(1000)))
    t.add(10, ue(0,          )) # erab disappears due to release
    t.add(10, ue(10,etx(5000))) # erab reappears - tx_total_bytes handling restarted from scratch
    assert t.get() == {4: [S(1000,2), S(5000,10)]}


# erab changes qci on the fly -> erab is considered to be reestablished
def test_Sampler_erab_change_qci():
    def ue(tx, *etxv):             return UE(17, tx, 0, *etxv)
    def etx(qci, tx_bytes, **kw):  return Etx(23, qci, tx_bytes, **kw)

    t = tSampler()
    t.add(10, ue(10, etx(9, 2000, tx_total=True)))  # tx with qci=9
    t.add(10, ue(10, etx(5, 3000, tx_total=True)))  # tx with qci=5
    assert t.get() == {9: [S(2000,10)], 5: [S(3000,10)]}  # would be S(3000,20) if δqci was not handled

# erab is considered to be reestablished on decreased tx_total_bytes
def test_Sampler_tx_total_down():
    def ue(tx, *etxv):        return UE(17, tx, 0, *etxv)
    def etx(tx_bytes, **kw):  return Etx(23, 4, tx_bytes, **kw)

    t = tSampler()
    t.add(10, ue(10, etx(4000, tx_total=True)))
    t.add(10, ue(10, etx(3000, tx_total=True)))
    assert t.get() == {4: [S(7000,20)]}  # would be e.g. S(4000,10) if tx_total_bytes↓ not handled

# N tx transport blocks is shared/distributed between multiple QCIs
#
# tx_lo ∼ tx_bytes / Σtx_bytes
# tx_hi = whole #tx even if tx_bytes are different
def test_Sampler_txtb_shared_between_qci():
    def ue(tx, *etxv):  return UE(17, tx, 0, *etxv)

    t = tSampler()
    t.add(10, ue(10, Etx(1, 9, 4000),
                     Etx(2, 5, 1000)))
    assert t.get() == {9: [S(4000, (8,10))], 5: [S(1000, (2,10))]}

# multiple UE are correctly taken into account
def test_Sampler_multiple_ue():
    def ue(ue_id, tx, *etxv):  return UE(ue_id, tx, 0, *etxv)
    def etx(tx_bytes):         return Etx(23, 4, tx_bytes)

    t = tSampler()
    t.add(10, ue(17, 4, etx(1000)),
              ue(18, 5, etx(2000)))
    assert t.get() == {4: [S(1000,4), S(2000,5)]}

# rank affects DL max #TB/TTI   (ul: no info)
def test_Sampler_rank():
    def ue(tx, *etxv):  return UE(17, tx, 0, *etxv, ri=2)
    def etx(tx_bytes):  return Etx(23, 4, tx_bytes)

    t = tSampler(use_ri=True)
    t.add(10, ue(3, etx(1000)))
    assert t.get() == {4: [S(1000, 1.5)]} # tx_time=1.5, not 3
    t.add(10, ue(10, etx(1000)))
    assert t.get() == {4: [S(1000, 5)]}   # tx_time=5, not 10
    t.add(10, ue(10*2, etx(1000)))
    assert t.get() == {4: [S(1000,10)]}   # now tx_time=10

    # verify that use_ri=False does not take ue.ri into account
    t = tSampler(use_ri=False)
    t.add(10, ue(3, etx(1000)))
    assert t.get() == {4: [S(1000,3)]}  # tx_time=3, not 1.5


# verify _BitSync works ok.
def test_BitSync():
    # _ passes txv_in into _BitSync and returns output stream.
    #
    # txv_in = [](tx_bytes, #tx)    ; δt=10·tti
    def _(*txv_in):
        def do_bitsync(*txv_in):
            txv_out = []
            xv_out  = ''
            bitsync = _BitSync()
            for x, (tx_bytes, tx) in enumerate(txv_in):
                _ =  bitsync.next(10*tti, tx_bytes, tx,
                                  chr(ord('a')+x))
                for (δt, tx_bytes, tx, x_) in _:
                    assert δt == 10*tti
                    txv_out.append((tx_bytes, tx))
                    xv_out += x_

            _ = bitsync.finish()
            for (δt, tx_bytes, tx, x_) in _:
                assert δt == 10*tti
                txv_out.append((tx_bytes, tx))
                xv_out += x_

            assert xv_out == 'abcdefghijklmnopqrstuvwxyz'[:len(txv_in)]
            return txv_out

        txv_out = do_bitsync(*txv_in)
        # also check with 0-tail -> it should give the same
        txv_out_ = do_bitsync(*(txv_in + ((0,0),)*10))
        assert txv_out_ == txv_out + [(0,0)]*10

        return txv_out


    #      tx_bytes tx
    assert _((1000, 10),    # all ACK in the same frame
             (   0,  0),
             (   0,  0))    == [(1000, 10),
                                (   0,  0),
                                (   0,  0)]

    assert _((1000,  0),    # all ACK in next frame
             (   0, 10),
             (   0,  0))    == [(1000, 10),
                                (   0,  0),
                                (   0,  0)]

    #assert _((1000,  0),    # all ACK in next-next frame
    #         (   0,  0),
    #         (   0, 10))    == [(1000, 10),
    #                            (   0,  0),
    #                            (   0,  0)]

    assert _((1000,  2),    # some ACK in the same frame, some in next
             (   0,  8),
             (   0,  0))    == [(1000, 10),
                                (   0,  0),
                                (   0,  0)]

    #assert _((1000,  2),    # some ACK in the same frame, some in next, some in next-next
    #         (   0,  5),
    #         (   0,  3))    == [(1000, 10),
    #                            (   0,  0),
    #                            (   0,  0)]

    # 1000 1000
    assert _((1000, 10),    # consecutive transmission (ack in same)
             (1000, 10),
             ( 500,  5),
             (   0,  0),
             (   0,  0))    == [(1000, 10),
                                (1000, 10),
                                ( 500,  5),
                                (   0,  0),
                                (   0,  0)]

    assert _((1000,  0),    # consecutive transmission (ack in next)
             (1000, 10),
             ( 500, 10),
             (   0,  5),
             (   0,  0))    == [(1000, 10),
                                (1000, 10),
                                ( 500,  5),
                                (   0,  0),
                                (   0,  0)]

    assert _((1000,  4),    # consecutive transmission (ack scattered)
             (1000, 10),    # 6 4
             ( 500,  8),    #   6 2
             (   0,  3),    #     3
             (   0,  0))    == [(1000, 10),
                                (1000, 10),
                                ( 500,  5),
                                (   0,  0),
                                (   0,  0)]

    #assert _((1000,  2),    # consecutive transmission (ack scattered to next and next-next)
    #         (1000,  8),    # 5 3
    #         ( 500,  8),    # 3 5 0
    #         (   0,  6),    #   2 4
    #         (   0,  1),    #     1
    #         (   0,  0))    == [(1000, 10),
    #                            (1000, 10),
    #                            ( 500,  5),
    #                            (   0,  0),
    #                            (   0,  0)]

    # 1000 500 1000
    assert _((1000, 10),    # consecutive transmission (ack in same)
             ( 500,  5),
             (1000, 10),
             (   0,  0),
             (   0,  0))    == [(1000, 10),
                                ( 500,  5),
                                (1000, 10),
                                (   0,  0),
                                (   0,  0)]

    assert _((1000,  0),    # consecutive transmission (ack in next)
             ( 500, 10),
             (1000,  5),
             (   0, 10),
             (   0,  0))    == [(1000, 10),
                                ( 500,  5),
                                (1000, 10),
                                (   0,  0),
                                (   0,  0)]

    assert _((1000,  4),    # consecutive transmission (ack scattered)
             ( 500,  8),    # 6 2
             (1000,  7),    #   3 4
             (   0,  6),    #     6
             (   0,  0))    == [(1000, 10),
                                ( 500,  5),
                                (1000, 10),
                                (   0,  0),
                                (   0,  0)]

    #assert _((1000,  2),    # consecutive transmission (ack scattered to next and next-next)
    #         ( 500,  8),    # 5 3
    #         (1000,  5),    # 3 1 1
    #         (   0,  5),    #   1 4
    #         (   0,  5),    #     5
    #         (   0,  0))    == [(1000, 10),
    #                            ( 500,  5),
    #                            (1000, 10),
    #                            (   0,  0),
    #                            (   0,  0)]

    # transmission is scattered to two frames with all acks only in the second frame
    assert _((1000,  0),
             (1000, 10))    == [(1000,  5),
                                (1000,  5)]

    assert _((1000,  0),
             (1000, 10),
             (   0,  0))    == [(1000,  5),
                                (1000,  5),
                                (   0,  0)]


    assert _((1000,  0),    # steady tx (ack in next)
             (1000, 10),
             ( 500, 10),
             ( 500,  5),
             ( 500,  5),
             (   0,  5),
             (   0,  0))    == [(1000, 10),
                                (1000, 10),
                                ( 500,  5),
                                ( 500,  5),
                                ( 500,  5),
                                (   0,  0),
                                (   0,  0)]

    #assert _((1000,  0),    # steady tx (ack in next-next)
    #         (1000,  0),
    #         ( 500, 10),
    #         ( 500, 10),
    #         ( 500,  5),
    #         (   0,  5),
    #         (   0,  5),
    #         (   0,  0))    == [(1000, 10),
    #                            (1000, 10),
    #                            ( 500,  5),
    #                            ( 500,  5),
    #                            ( 500,  5),
    #                            (   0,  0),
    #                            (   0,  0),
    #                            (   0,  0)]

    assert _((1000, 10),    # yields t21 < 0 in lshift
             (1000,  0),
             (   0, 10))    == [(1000, 10),
                                (1000, 10),
                                (   0,  0)]

    # real-life example
    assert _(( 6168, 0),
             (14392, 8),
             (   0,  0))    == [( 6168, 2.4),
                                (14392, 5.6),
                                (    0, 0  )]


# ---- misc ----

# teach tests to compare Samples
@func(Sample)
def __eq__(a, b):
    if not isinstance(b, Sample):
        return False
    # compare tx_time with tolerance to level-out floating point errors
    return (abs(a.tx_time - b.tx_time) < (tti / 1e6))  and \
           (a.tx_bytes == b.tx_bytes)


def test_incstats():
    X = list(3+_ for _ in range(20))
    Xs = _IncStats()
    for (n,x) in enumerate(X):
        Xs.add(x)
        Xn = X[:n+1]
        assert Xs.avg() == np.mean(Xn)
        assert Xs.std() == np.std(Xn)
        assert Xs.min == min(Xn)
        assert Xs.max == max(Xn)
