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

from xlte.kpi import Calc, MeasurementLog, Measurement, ΣMeasurement, Interval, \
                     Stat, StatT, NA, isNA, Σqci, Σcause, nqci
import numpy as np
from pytest import raises

ms = 1e-3


def test_Measurement():
    m = Measurement()
    assert type(m) is Measurement

    # verify that all fields are initialized to NA
    def _(name):
        v = m[name]
        if v.shape == ():
            assert isNA(v)          # scalar
        else:
            assert isNA(v).all()    # array
    # several fields explicitly
    _('X.Tstart')                       # time
    _('RRC.ConnEstabAtt.sum')           # Tcc
    _('DRB.PdcpSduBitrateDl.sum')       # float32
    _('DRB.IPVolDl.sum')                # int64
    _('DRB.IPTimeDl.7')                 # .QCI alias
    _('DRB.IPTimeDl.QCI')               # .QCI array
    _('DRB.IPLatDl.7')                  # .QCI alias to Stat
    _('DRB.IPLatDl.QCI')                # .QCI array of Stat
    # everything automatically
    for name in m.dtype.names:
        _(name)

    # setting values
    with raises(ValueError): m['XXXunknownfield']
    m['S1SIG.ConnEstabAtt'] = 123
    assert m['S1SIG.ConnEstabAtt'] == 123
    m['RRC.ConnEstabAtt.sum'] = 17
    assert m['RRC.ConnEstabAtt.sum'] == 17
    m['DRB.UEActive']['min'] = 1
    m['DRB.UEActive']['avg'] = 6
    m['DRB.UEActive']['max'] = 8
    assert m['DRB.UEActive']['min'] == 1
    assert m['DRB.UEActive']['avg'] == 6
    assert m['DRB.UEActive']['max'] == 8
    m['DRB.IPVolDl.QCI'][:] = 0
    m['DRB.IPVolDl.5'] = 55
    m['DRB.IPVolDl.7'] = NA(m['DRB.IPVolDl.7'].dtype)
    m['DRB.IPVolDl.QCI'][9] = 99
    assert m['DRB.IPVolDl.5'] == 55;  assert m['DRB.IPVolDl.QCI'][5] == 55
    assert isNA(m['DRB.IPVolDl.7']);  assert isNA(m['DRB.IPVolDl.QCI'][7])
    assert m['DRB.IPVolDl.9'] == 99;  assert m['DRB.IPVolDl.QCI'][9] == 99
    for k in range(len(m['DRB.IPVolDl.QCI'])):
        if k in {5,7,9}:
            continue
        assert m['DRB.IPVolDl.%d' % k] == 0
        assert m['DRB.IPVolDl.QCI'][k] == 0
    m['DRB.IPLatDl.QCI'][:]['avg'] = 0
    m['DRB.IPLatDl.QCI'][:]['min'] = 0
    m['DRB.IPLatDl.QCI'][:]['max'] = 0
    m['DRB.IPLatDl.QCI'][:]['n']   = 0
    m['DRB.IPLatDl.QCI'][3]['avg'] = 33
    m['DRB.IPLatDl.QCI'][3]['n']   = 123
    m['DRB.IPLatDl.4']['avg'] = 44
    m['DRB.IPLatDl.4']['n']   = 432
    m['DRB.IPLatDl.8']['avg'] = NA(m['DRB.IPLatDl.8']['avg'].dtype)
    m['DRB.IPLatDl.8']['n']   = NA(m['DRB.IPLatDl.8']['n']  .dtype)


    # str/repr
    assert repr(m) == "Measurement(RRC.ConnEstabAtt.sum=17, DRB.UEActive=<1 6.0 8>, DRB.IPLatDl.QCI={3:<0.0 33.0 0.0>·123 4:<0.0 44.0 0.0>·432 8:<0.0 ø 0.0>·ø}, DRB.IPVolDl.QCI={5:55 7:ø 9:99}, S1SIG.ConnEstabAtt=123)"
    assert repr(m['DRB.UEActive'])  == "StatT(1, 6.0, 8, dtype=int32)"
    assert repr(m['DRB.IPLatDl.3']) == "Stat(0.0, 33.0, 0.0, 123, dtype=float64)"
    s = str(m)
    assert s[0]  == '('
    assert s[-1] == ')'
    v = s[1:-1].split(', ')
    vok = ['ø'] * len(m._dtype0.names)
    vok[m.dtype.names.index("RRC.ConnEstabAtt.sum")]   = "17"
    vok[m.dtype.names.index("DRB.UEActive")]           = "<1 6.0 8>"
    vok[m.dtype.names.index("S1SIG.ConnEstabAtt")]     = "123"
    vok[m.dtype.names.index("DRB.IPVolDl.QCI")]        = "{5:55 7:ø 9:99}"
    vok[m.dtype.names.index("DRB.IPLatDl.QCI")]        = "{3:<0.0 33.0 0.0>·123 4:<0.0 44.0 0.0>·432 8:<0.0 ø 0.0>·ø}"
    assert v == vok

    # verify that time fields has enough precision
    t2022 = 1670691601.8999548  # in 2022.Dec
    t2118 = 4670691601.1234567  # in 2118.Jan
    def _(τ):
        m['X.Tstart'] = τ
        τ_ = m['X.Tstart']
        assert τ_ == τ
    _(t2022)
    _(t2118)


def test_MeasurementLog():
    # empty
    mlog = MeasurementLog()
    _ = mlog.data()
    assert isinstance(_, np.ndarray)
    assert _.dtype == (Measurement, Measurement._dtype)
    assert _.shape == (0,)

    # append₁
    m1 = Measurement()
    m1['X.Tstart'] = 1
    m1['X.δT']     = 1
    m1['S1SIG.ConnEstabAtt'] = 11
    mlog.append(m1)
    _ = mlog.data()
    assert isinstance(_, np.ndarray)
    assert _.dtype == (Measurement, Measurement._dtype)
    assert _.shape == (1,)
    m1_ = _[0]
    assert isinstance(m1_, Measurement)
    assert m1_['X.Tstart'] == 1
    assert m1_ == m1

    # append₂
    m2 = Measurement()
    m2['X.Tstart'] = 2
    m2['X.δT']     = 1
    m2['S1SIG.ConnEstabSucc'] = 22
    mlog.append(m2)
    _ = mlog.data()
    assert isinstance(_, np.ndarray)
    assert _.dtype == (Measurement, Measurement._dtype)
    assert _.shape == (2,)
    assert _[0] == m1
    assert _[1] == m2

    # append₃
    m3 = Measurement()
    m3['X.Tstart'] = 3
    m3['X.δT']     = 1
    m3['RRC.ConnEstabAtt.sum'] = 333
    mlog.append(m3)
    _ = mlog.data()
    assert isinstance(_, np.ndarray)
    assert _.dtype == (Measurement, Measurement._dtype)
    assert _.shape == (3,)
    assert _[0] == m1
    assert _[1] == m2
    assert _[2] == m3

    # forget₀
    mlog.forget_past(0)
    _ = mlog.data()
    assert isinstance(_, np.ndarray)
    assert _.dtype == (Measurement, Measurement._dtype)
    assert _.shape == (3,)
    assert _[0] == m1
    assert _[1] == m2
    assert _[2] == m3

    # forget₁
    mlog.forget_past(1)
    _ = mlog.data()
    assert isinstance(_, np.ndarray)
    assert _.dtype == (Measurement, Measurement._dtype)
    assert _.shape == (2,)
    assert _[0] == m2
    assert _[1] == m3

    # forget₃
    mlog.forget_past(3)
    _ = mlog.data()
    assert isinstance(_, np.ndarray)
    assert _.dtype == (Measurement, Measurement._dtype)
    assert _.shape == (0,)


# verify (τ_lo, τ_hi) widening and overlapping with Measurements on Calc initialization.
def test_Calc_init():
    mlog = MeasurementLog()

    # _ asserts that Calc(mlog, τ_lo,τ_hi) has .τ_lo/.τ_hi as specified by
    # τ_wlo/τ_whi, and ._data as specified by mokv.
    def _(τ_lo, τ_hi, τ_wlo, τ_whi, *mokv):
        c = Calc(mlog, τ_lo,τ_hi)
        assert (c.τ_lo, c.τ_hi) == (τ_wlo, τ_whi)
        mv = list(c._data[i] for i in range(len(c._data)))
        assert mv == list(mokv)

    # mlog(ø)
    _( 0, 0,     0,0)
    _( 0,99,     0,99)
    _(10,20,    10,20)

    # m1[10,20)
    m1 = Measurement()
    m1['X.Tstart'] = 10
    m1['X.δT']     = 10
    mlog.append(m1)

    _( 0, 0,     0, 0)
    _( 0,99,     0,99,  m1)
    _(10,20,    10,20,  m1)
    _(12,18,    10,20,  m1)
    _( 5, 7,     5, 7)
    _( 5,15,     5,20,  m1)
    _(15,25,    10,25,  m1)
    _(25,30,    25,30)

    # m1[10,20) m2[30,40)
    m2 = Measurement()
    m2['X.Tstart'] = 30
    m2['X.δT']     = 10
    mlog.append(m2)

    _( 0, 0,     0, 0)
    _( 0,99,     0,99,  m1, m2)
    _(10,20,    10,20,  m1)
    _(12,18,    10,20,  m1)
    _( 5, 7,     5, 7)
    _( 5,15,     5,20,  m1)
    _(15,25,    10,25,  m1)
    _(25,30,    25,30)
    _(25,35,    25,40,      m2)
    _(35,45,    30,45,      m2)
    _(45,47,    45,47)
    _(32,38,    30,40,      m2)
    _(30,40,    30,40,      m2)
    _(99,99,    99,99)

# verify Calc internal iteration over measurements and holes.
def test_Calc_miter():
    mlog = MeasurementLog()

    # _ asserts that Calc(mlog, τ_lo,τ_hi)._miter yields Measurement as specified by mokv.
    def _(τ_lo, τ_hi, *mokv):
        c = Calc(mlog, τ_lo,τ_hi)
        mv = list(c._miter())
        assert mv == list(mokv)

    # na returns Measurement with specified τ_lo/τ_hi and NA for all other data.
    def na(τ_lo, τ_hi):
        assert τ_lo <= τ_hi
        m = Measurement()
        m['X.Tstart']  = τ_lo
        m['X.δT']      = τ_hi - τ_lo
        return m

    # mlog(ø)
    _( 0, 0)
    _( 0,99,    na(0,99))
    _(10,20,    na(10,20))

    # m1[10,20)
    m1 = Measurement()
    m1['X.Tstart'] = 10
    m1['X.δT']     = 10
    mlog.append(m1)

    _( 0, 0)
    _( 0,99,    na(0,10),  m1,  na(20,99))
    _(10,20,               m1)
    _( 7,20,    na(7,10),  m1)
    _(10,23,               m1,  na(20,23))

    # m1[10,20) m2[30,40)
    m2 = Measurement()
    m2['X.Tstart'] = 30
    m2['X.δT']     = 10
    mlog.append(m2)

    _( 0, 0)
    _( 0,99,    na(0,10),  m1,  na(20,30),  m2, na(40,99))
    _(10,20,               m1)
    _(10,30,               m1,  na(20,30))
    _(10,40,               m1,  na(20,30),  m2)


# verify Calc internal function that computes success rate of fini/init events.
def test_Calc_success_rate():
    mlog = MeasurementLog()

    init = "S1SIG.ConnEstabAtt"
    fini = "S1SIG.ConnEstabSucc"

    # M returns Measurement with specified time coverage and init/fini values.
    def M(τ_lo,τ_hi, vinit=None, vfini=None):
        m = Measurement()
        m['X.Tstart']  = τ_lo
        m['X.δT']      = τ_hi - τ_lo
        if vinit is not None:
            m[init]    = vinit
        if vfini is not None:
            m[fini]    = vfini
        return m

    # Mlog reinitializes mlog according to specified Measurements in mv.
    def Mlog(*mv):
        nonlocal mlog
        mlog = MeasurementLog()
        for m in mv:
            mlog.append(m)

    # _ asserts that Calc(mlog, τ_lo,τ_hi)._success_rate(fini, init) returns Interval(sok_lo, sok_hi).
    def _(τ_lo, τ_hi, sok_lo, sok_hi):
        sok = Interval(sok_lo, sok_hi)
        c = Calc(mlog, τ_lo, τ_hi)
        s = c._success_rate(fini, init)
        assert type(s) is Interval
        eps = np.finfo(s['lo'].dtype).eps
        assert abs(s['lo']-sok['lo'])  < eps
        assert abs(s['hi']-sok['hi'])  < eps

    # ø -> full uncertainty
    Mlog()
    _( 0, 0,     0,1)
    _( 0,99,     0,1)
    _(10,20,     0,1)

    # m[10,20,  {ø,0}/{ø,0})    -> full uncertainty
    for i in (None,0):
        for f in (None,0):
            Mlog(M(10,20,  i,f))
            _( 0, 0,     0,1)
            _( 0,99,     0,1)
            _(10,20,     0,1)
            _( 7,20,     0,1)
            _(10,25,     0,1)

    # m[10,20,  8,4)           -> 1/2 if counted in [10,20)
    #
    #         i₁=8
    #         f₁=4
    #   ────|──────|─────────────|──────────
    #      10  t₁ 20 ←── t₂ ──→ τ_hi
    #
    # t with data:                      t₁
    # t with no data:                   t₂
    # t total:                          T = t₁+t₂
    # extrapolation for incoming                t₂
    # events for "no data" period:      i₂ = i₁·──
    #                                           t₁
    # termination events for "no data"
    # period is full uncertainty        f₂ ∈ [0,i₂]
    #
    # => success rate over whole time is uncertain in between
    #
    #    f₁           f₁+i₂
    #  ─────  ≤ SR ≤  ─────
    #  i₁+i₂          i₁+i₂
    #
    Mlog(M(10,20, 8,4))
    _( 0, 0,   0,                    1)                  # no overlap - full uncertainty
    _(10,20,   0.5,                  0.5)                # t₂=0  - no uncertainty
    _( 7,20,   0.3846153846153846,   0.6153846153846154) # t₂=3
    _(10,25,   0.3333333333333333,   0.6666666666666666) # t₂=5
    _( 0,99,   0.050505050505050504, 0.9494949494949495) # t₂=10+79

    # m[10,20,  8,4)  m[30,40, 50,50]
    #
    # similar to the above case but with t₁ and t₂ coming with data, while t₃
    # represents whole "no data" time:
    #
    #         i₁=8          i₂=50
    #         f₁=4          f₂=50
    #   ────|──────|──────|───────|──────────────────|──────────
    #      10  t₁ 20  ↑  30  t₂  40       ↑         τ_hi
    #                 │                   │
    #                 │                   │
    #                 `────────────────── t₃
    #
    # t with data:                      t₁+t₂
    # t with no data:                   t₃
    # t total:                          T = t₁+t₂+t₃
    # extrapolation for incoming                      t₃
    # events for "no data" period:      i₃ = (i₁+i₂)·────
    #                                                t₁+t₂
    # termination events for "no data"
    # period is full uncertainty        f₃ ∈ [0,i₃]
    #
    # => success rate over whole time is uncertain in between
    #
    #    f₁+f₂           f₁+f₂+i₃
    #  ────────  ≤ SR ≤  ───────
    #  i₁+i₂+i₃          i₁+i₂+i₃
    #
    Mlog(M(10,20, 8,4), M(30,40, 50,50))
    _( 0, 0,   0,                    1)                  # no overlap - full uncertainty
    _(10,20,   0.5,                  0.5)                # exact 1/2 in [10,20)
    _(30,40,   1,                    1)                  # exact  1  in [30,40)
    _( 7,20,   0.3846153846153846,   0.6153846153846154) # overlaps only with t₁ -> as ^^^
    _(10,25,   0.3333333333333333,   0.6666666666666666) # overlaps only with t₁ -> as ^^^
    _(10,40,   0.6206896551724138,   0.9540229885057471) # t₃=10
    _( 7,40,   0.5642633228840125,   0.9582027168234065) # t₃=13
    _( 7,45,   0.4900181488203267,   0.9637023593466425) # t₃=18
    _( 0,99,   0.18808777429467083,  0.9860675722744688) # t₃=79


    # Σqci
    init = "Σqci ERAB.EstabInitAttNbr.QCI"
    fini = "Σqci ERAB.EstabInitSuccNbr.QCI"
    m = M(10,20)
    m['ERAB.EstabInitAttNbr.sum']  = 10
    m['ERAB.EstabInitSuccNbr.sum'] = 2
    Mlog(m)
    _(10,20,    1/5, 1/5)

    # Σcause
    init = "Σcause RRC.ConnEstabAtt.CAUSE"
    fini = "Σcause RRC.ConnEstabSucc.CAUSE"
    m = M(10,20)
    m['RRC.ConnEstabSucc.sum'] = 5
    m['RRC.ConnEstabAtt.sum']  = 10
    Mlog(m)
    _(10,20,    1/2, 1/2)


# verify Calc.erab_accessibility .
def test_Calc_erab_accessibility():
    # most of the job is done by _success_rate.
    # here we verify final wrapping, that erab_accessibility does, only lightly.
    m = Measurement()
    m['X.Tstart'] = 10
    m['X.δT']     = 10

    m['RRC.ConnEstabSucc.sum']      = 2
    m['RRC.ConnEstabAtt.sum']       = 7

    m['S1SIG.ConnEstabSucc']        = 3
    m['S1SIG.ConnEstabAtt']         = 8

    m['ERAB.EstabInitSuccNbr.sum']  = 4
    m['ERAB.EstabInitAttNbr.sum']   = 9

    m['ERAB.EstabAddSuccNbr.sum']   = 5
    m['ERAB.EstabAddAttNbr.sum']    = 10

    mlog = MeasurementLog()
    mlog.append(m)

    calc = Calc(mlog, 10,20)

    # _ asserts that provided interval is precise and equals vok.
    def _(i: Interval, vok):
        assert i['lo'] == i['hi']
        assert i['lo'] == vok

    InititialEPSBEstabSR, AddedEPSBEstabSR = calc.erab_accessibility()
    _(AddedEPSBEstabSR,     50)
    _(InititialEPSBEstabSR, 100 * 2*3*4 / (7*8*9))


# verify Calc.eutran_ip_throughput .
def test_Calc_eutran_ip_throughput():
    # most of the job is done by drivers collecting DRB.IPVol{Dl,Ul} and DRB.IPTime{Dl,Ul}
    # here we verify final aggregation, that eutran_ip_throughput does, only lightly.
    m = Measurement()
    m['X.Tstart']   = 10
    m['X.δT']       = 10

    m['DRB.IPVolDl.5']  = 55e6
    m['DRB.IPVolUl.5']  = 55e5
    m['DRB.IPTimeDl.5'] =  1e2
    m['DRB.IPTimeUl.5'] =  1e2

    m['DRB.IPVolDl.7']  = 75e6
    m['DRB.IPVolUl.7']  = 75e5
    m['DRB.IPTimeDl.7'] =  1e2
    m['DRB.IPTimeUl.7'] =  1e2

    m['DRB.IPVolDl.9']  =    0
    m['DRB.IPVolUl.9']  =    0
    m['DRB.IPTimeDl.9'] =    0
    m['DRB.IPTimeUl.9'] =    0

    for qci in {5,7,9}:
        m['XXX.DRB.IPTimeDl_err.QCI'][qci] = 0
        m['XXX.DRB.IPTimeUl_err.QCI'][qci] = 0

    # (other QCIs are left with na)
    for qci in set(range(nqci)).difference({5,7,9}):
        assert isNA(m['DRB.IPVolDl.QCI'][qci])
        assert isNA(m['DRB.IPVolUl.QCI'][qci])
        assert isNA(m['DRB.IPTimeDl.QCI'][qci])
        assert isNA(m['DRB.IPTimeUl.QCI'][qci])
        assert isNA(m['XXX.DRB.IPTimeDl_err.QCI'][qci])
        assert isNA(m['XXX.DRB.IPTimeUl_err.QCI'][qci])

    mlog = MeasurementLog()
    mlog.append(m)

    calc = Calc(mlog, 10,20)

    thp = calc.eutran_ip_throughput()
    def I(x): return Interval(x,x)
    assert thp[5]['dl'] == I(55e4)
    assert thp[5]['ul'] == I(55e3)
    assert thp[7]['dl'] == I(75e4)
    assert thp[7]['ul'] == I(75e3)
    assert thp[9]['dl'] == I(0)
    assert thp[9]['ul'] == I(0)

    for qci in set(range(nqci)).difference({5,7,9}):
        assert thp[qci]['dl'] == I(0)
        assert thp[qci]['ul'] == I(0)


# verify Calc.aggregate .
def test_Calc_aggregate():
    mlog = MeasurementLog()

    m1 = Measurement()
    m1['X.Tstart']  = 1
    m1['X.δT']      = 2
    m1['S1SIG.ConnEstabAtt'] = 12                               # Tcc
    m1['ERAB.SessionTimeUE'] = 1.2                              # Ttime
    m1['DRB.UEActive']       = StatT(1, 3.7, 5)                 # StatT
    m1['DRB.IPLatDl.7']      = Stat(4*ms, 7.32*ms, 25*ms, 17)   # Stat

    m2 = Measurement()
    m2['X.Tstart']  = 5 # NOTE [3,5) is NA hole
    m2['X.δT']      = 3
    m2['S1SIG.ConnEstabAtt'] = 11
    m2['ERAB.SessionTimeUE'] = 0.7
    m2['DRB.UEActive']       = StatT(2, 3.2, 7)
    m2['DRB.IPLatDl.7']      = Stat(3*ms, 5.23*ms, 11*ms, 11)

    mlog.append(m1)
    mlog.append(m2)

    calc = Calc(mlog, 0, 10)
    assert calc.τ_lo == 0
    assert calc.τ_hi == 10

    M = calc.aggregate()
    assert isinstance(M, ΣMeasurement)

    assert M['X.Tstart'] == 0
    assert M['X.δT']     == 10

    assert M['S1SIG.ConnEstabAtt']['value'] == 12 + 11
    assert M['S1SIG.ConnEstabAtt']['τ_na']  == 5    # [0,1) [3,5) [8,10)

    assert M['ERAB.SessionTimeUE']['value'] == 1.2 + 0.7
    assert M['ERAB.SessionTimeUE']['τ_na']  == 5

    assert M['DRB.UEActive']['value']   == StatT(1, (3.7*2 + 3.2*3)/(2+3), 7)
    assert M['DRB.UEActive']['τ_na']    == 5

    assert M['DRB.IPLatDl.7']['value']  == Stat(3*ms, (7.32*17 + 5.23*11)/(17+11)*ms, 25*ms, 17+11)
    assert M['DRB.IPLatDl.7']['τ_na']   == 5


    # assert that everything else is NA with τ_na == 10
    def _(name):
        f = M[name]
        if f.shape != ():
            return  # don't check X.QCI - rely on aliases
        assert isNA(f['value'])
        assert f['τ_na'] == 10
    for name in M.dtype.names:
        if name not in ('X.Tstart', 'X.δT', 'S1SIG.ConnEstabAtt',
                        'ERAB.SessionTimeUE', 'DRB.UEActive', 'DRB.IPLatDl.7'):
            _(name)


# verify Σqci.
def test_Σqci():
    m = Measurement()
    x = 'ERAB.EstabInitAttNbr'
    def Σ():
        return Σqci(m, x+'.QCI')

    assert isNA(Σ())
    m[x+'.sum'] = 123
    assert Σ() == 123

    m[x+'.17']  = 17
    m[x+'.23']  = 23
    m[x+'.255'] = 255
    assert Σ() == 123   # from .sum

    m[x+'.sum'] = NA(m[x+'.sum'].dtype)
    assert isNA(Σ())    # from array, but NA values lead to sum being NA

    v = m[x+'.QCI']
    l = len(v)
    for i in range(l):
        v[i] = 1 + i
    assert Σ() == 1*l + (l-1)*l/2


# verify Σcause.
def test_Σcause():
    m = Measurement()
    x = 'RRC.ConnEstabAtt'
    def Σ():
        return Σcause(m, x+'.CAUSE')

    assert isNA(Σ())
    m[x+'.sum'] = 123
    assert Σ() == 123

    # TODO sum over individual causes (when implemented)


def test_NA():
    def _(typ):
        na = NA(typ(0).dtype)
        assert type(na) is typ
        assert isNA(na)
        return na

    assert np.isnan( _(np.float16) )
    assert np.isnan( _(np.float32) )
    assert np.isnan( _(np.float64) )

    assert _(np.int8)   == -0x80
    assert _(np.int16)  == -0x8000
    assert _(np.int32)  == -0x80000000
    assert _(np.int64)  == -0x8000000000000000
