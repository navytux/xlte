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

from xlte.kpi import MeasurementLog, Measurement, NA, isNA
import numpy as np
from pytest import raises


def test_Measurement():
    m = Measurement()
    assert type(m) is Measurement

    # verify that all fields are initialized to NA
    def _(name):
        assert isNA(m[name])
    # several fields explicitly
    _('X.Tstart')                       # time
    _('RRC.ConnEstabAtt.sum')           # Tcc
    _('DRB.PdcpSduBitrateDl.sum')       # float32
    _('DRB.IPThpVolDl.sum')             # int64
    # everything automatically
    for name in m.dtype.names:
        _(name)

    # setting values
    with raises(ValueError): m['XXXunknownfield']
    m['S1SIG.ConnEstabAtt'] = 123
    assert m['S1SIG.ConnEstabAtt'] == 123
    m['RRC.ConnEstabAtt.sum'] = 17
    assert m['RRC.ConnEstabAtt.sum'] == 17

    # str/repr
    assert repr(m) == "Measurement(RRC.ConnEstabAtt.sum=17, S1SIG.ConnEstabAtt=123)"
    s = str(m)
    assert s[0]  == '('
    assert s[-1] == ')'
    v = s[1:-1].split(', ')
    vok = ['ø'] * len(m.dtype.names)
    vok[m.dtype.names.index("RRC.ConnEstabAtt.sum")]   = "17"
    vok[m.dtype.names.index("S1SIG.ConnEstabAtt")]     = "123"
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


def test_NA():
    def _(typ):
        return NA(typ(0).dtype)

    assert np.isnan( _(np.float16) )
    assert np.isnan( _(np.float32) )
    assert np.isnan( _(np.float64) )

    assert _(np.int8)   == -0x80
    assert _(np.int16)  == -0x8000
    assert _(np.int32)  == -0x80000000
    assert _(np.int64)  == -0x8000000000000000
