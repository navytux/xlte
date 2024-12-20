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
"""Package kpi provides functionality to compute Key Performance Indicators of LTE services.

- Calc is KPI calculator. It can be instantiated on MeasurementLog and time
  interval over which to perform computations. Use Calc methods such as
  .erab_accessibility() and .eutran_ip_throughput() to compute KPIs, and .aggregate()
  to compute aggregated measurements.

- MeasurementLog maintains journal with result of measurements. Use .append()
  to populate it with data.

- Measurement represents measurement results. Its documentation establishes
  semantic for measurement results to be followed by drivers.

To actually compute a KPI for particular LTE service, a measurements driver
should exist for that LTE service(*). KPI computation pipeline is then as follows:

     ─────────────
    │ Measurement │  Measurements   ────────────────       ──────
    │             │ ─────────────→ │ MeasurementLog │ ──→ │ Calc │ ──→  KPI
    │   driver    │                 ────────────────       ──────
     ─────────────


See following 3GPP standards for KPI-related topics:

    - TS 32.401
    - TS 32.450
    - TS 32.425

(*) for example package amari.kpi provides such measurements driver for Amarisoft LTE stack.
"""

from __future__ import print_function, division, absolute_import

import numpy as np
from golang import func

import warnings


# Calc provides way to compute KPIs over given measurement data and time interval.
#
# It is constructed from MeasurementLog and [τ_lo, τ_hi) and further provides
# following methods for computing 3GPP KPIs:
#
#   .erab_accessibility()    -  TS 32.450 6.1.1 "E-RAB Accessibility"
#   .eutran_ip_throughput()  -  TS 32.450 6.3.1 "E-UTRAN IP Throughput"
#   TODO other KPIs
#
# Upon construction specified time interval is potentially widened to cover
# corresponding data in full granularity periods:
#
#                  τ'lo                  τ'hi
#       ──────|─────|────[────|────)──────|──────|────────>
#                    ←─ τ_lo      τ_hi ──→          time
#
# It is also possible to merely aggregate measured values via .aggregate() .
#
# See also: MeasurementLog, Measurement, ΣMeasurement.
class Calc:
    # ._data            []Measurement - fully inside [.τ_lo, .τ_hi)
    # [.τ_lo, .τ_hi)    time interval to compute over. Potentially wider than originally requested.
    pass


# MeasurementLog represents journal of performed Measurements.
#
# It semantically consists of
#
#       []Measurement       ; .Tstart↑
#
# where Measurement represents results of one observation.
#
# It can be also perceived as 2D array with the following organization:
#
#
#           ^
#     event₁│               |M|
#     event₂│               |e|
#     event₃│               |a|
#      ...  │               |s|
#           │               |u|
#           │               |r|
#     value₁│               |e|
#     value₂│               |m|
#     value₃│               |e|
#      ...  │               |n|
#           │               |t|
#           +──────────────────────────────────────>
#                                             time
#
#
# MeasurementLog provides following operations:
#
#   .append(Measurement)        - add new Measurement to the tail of MeasurementLog
#   .forget_past(Tcut)          - forget measurements with .Tstart ≤ Tcut
#   .data()                     - get 2D array with measurements data
#
# See also: Measurement.
class MeasurementLog:
    # ._data    []Measurement
    pass


# Stat[dtype] represents result of statistical profiling with arbitrary sampling
# for a value with specified dtype.
#
# It is organized as NumPy structured scalar with avg, min, max and n fields.
#
# It is used inside Measurement for e.g. DRB.IPLatDl.QCI .
class Stat(np.void):
    # _dtype_for returns dtype that Stat[dtype] will use.
    @classmethod
    def _dtype_for(cls, dtype):
        return np.dtype((cls, [
            ('avg', np.float64),    # NOTE even int becomes float on averaging
            ('min', dtype),
            ('max', dtype),
            ('n',   np.int64)]))

# StatT[dtype] represents result of statistical profiling with time-based sampling
# for a value with specified dtype.
#
# It is organized as NumPy structured scalar with avg, min and max fields.
#
# NOTE contrary to Stat there is no n field and containing Measurement.X.δT
#      should be taken to know during which time period the profile was collected.
#
# It is used inside Measurement for e.g. DRB.UEActive .
class StatT(np.void):
    # _dtype_for returns dtype that StatT[dtype] will use.
    @classmethod
    def _dtype_for(cls, dtype):
        return np.dtype((cls, [
            ('avg', np.float64),    # see avg note in Stat
            ('min', dtype),
            ('max', dtype)]))


# Measurement represents set of measured values and events observed and counted
# during one particular period of time.
#
# It is organized as NumPy structured scalar with each value/event represented
# by dedicated field. For example for Measurement m, m['S1SIG.ConnEstabAtt']
# depicts the number of S1AP connection establishment attempts.
#
# Everything is represented using SI units + bit. For example time is in
# seconds (not e.g. ms), throughput is in bit/s (not kbit/s), consumed energy
# is in J (not in kWh), etc.
#
# If a value/event was not measured - it is represented as NA.
#
# Important note (init/fini correction):
#
#   Termination events should be counted in the same granularity period, where
#   corresponding initiation event occurred, even if termination event happens
#   _after_ granularity period covering the initiation event. For example in the
#   following illustration "ConnEstab Success" event should be counted in the
#   same granularity period 1 as "ConnEstab Initiate" event:
#
#
#                  -----------------------
#                 '                       '
#         | p e r ' i o d 1       | p e r ' i o d 2    |
#         |       '               |       v            |
#     ────'───────x───────────────'───────x────────────'────────────>
#             ConnEstab               ConnEstab                time
#             Initiate                 Success
#
#   This preserves invariant that N(initiations) is always ≥ N(results) and
#   goes in line with what TS 32.401 4.3.2 "Perceived accuracy -> Same period
#   for the same two events" requires.
class Measurement(np.void):
    Tcc    = np.int32   # cumulative counter
    Ttime  = np.float64 # time is represented in seconds since epoch
    S  = Stat ._dtype_for   # statistical profile with arbitrary sampling
    St = StatT._dtype_for   # statistical profile with time-based sampling

    # _dtype defines measured values and events.
    _dtype = np.dtype([
        ('X.Tstart',                        Ttime),     # when the measurement started
        ('X.δT',                            Ttime),     # time interval during which the measurement was made

        # below comes definition of values/events as specified by TS 32.425 and TS 32.450
        #
        # - .QCI   suffix means a value comes as array of per-QCI values.
        # - .CAUSE suffix means a value comes as array of per-CAUSE values.
        #
        # NOTE both .QCI and .CAUSE are expanded from outside.
        #
        # NAME                            TYPE/DTYPE      UNIT      TS 32.425 reference + ...
        ('RRC.ConnEstabAtt.CAUSE',          Tcc),       # 1         4.1.1.1
        ('RRC.ConnEstabSucc.CAUSE',         Tcc),       # 1         4.1.1.2

        ('ERAB.EstabInitAttNbr.QCI',        Tcc),       # 1         4.2.1.1
        ('ERAB.EstabInitSuccNbr.QCI',       Tcc),       # 1         4.2.1.2
        ('ERAB.EstabAddAttNbr.QCI',         Tcc),       # 1         4.2.1.4
        ('ERAB.EstabAddSuccNbr.QCI',        Tcc),       # 1         4.2.1.5

        ('ERAB.RelActNbr.QCI',              Tcc),       # 1         4.2.2.6
        ('ERAB.SessionTimeUE',              Ttime),     # s         4.2.4.1
        ('ERAB.SessionTimeQCI.QCI',         Ttime),     # s         4.2.4.2

        ('DRB.PdcpSduBitrateUl.QCI',        np.float64),# bit/s     4.4.1.1                 NOTE not kbit/s
        ('DRB.PdcpSduBitrateDl.QCI',        np.float64),# bit/s     4.4.1.2                 NOTE not kbit/s

        ('DRB.UEActive',                 St(np.int32)), # 1         4.4.2.4  36.314:4.1.3.3

        ('DRB.IPLatDl.QCI',               S(Ttime)),    # s         4.4.5.1  32.450:6.3.2   NOTE not ms

        # DRB.IPThpX.QCI = DRB.IPVolX.QCI / DRB.IPTimeX.QCI         4.4.6.1-2 32.450:6.3.1
        ('DRB.IPVolDl.QCI',                 np.int64),  # bit       4.4.6.3  32.450:6.3.1   NOTE not kbit
        ('DRB.IPVolUl.QCI',                 np.int64),  # bit       4.4.6.4  32.450:6.3.1   NOTE not kbit
        ('DRB.IPTimeDl.QCI',                Ttime),     # s         4.4.6.5  32.450:6.3.1   NOTE not ms
        ('DRB.IPTimeUl.QCI',                Ttime),     # s         4.4.6.6  32.450:6.3.1   NOTE not ms
        ('XXX.DRB.IPTimeDl_err.QCI',        Ttime),     # s         XXX error for DRB.IPTimeDl.QCI (will be removed)
        ('XXX.DRB.IPTimeUl_err.QCI',        Ttime),     # s         XXX error for DRB.IPTimeUl.QCI (will be removed)

        ('RRU.CellUnavailableTime.CAUSE',   Ttime),     # s         4.5.6

        ('S1SIG.ConnEstabAtt',              Tcc),       # 1         4.6.1.1
        ('S1SIG.ConnEstabSucc',             Tcc),       # 1         4.6.1.2

        # XXX no such counters in 32.425
        # TODO -> HO.(Intra|Inter)(Enb|Denb) Prep|Att|Succ ...
        #('HO.ExeAtt',                      Tcc),       # 1
        #('HO.ExeSucc',                     Tcc),       # 1
        #('HO.PrepAtt.QCI',                 Tcc),       # 1
        #('HO.PrepSucc.QCI',                Tcc),       # 1

        ('PEE.Energy',                      np.float64),# J         4.12.2                  NOTE not kWh
    ])

    del S, St


# Interval is NumPy structured scalar that represents [lo,hi) interval.
#
# It is used by Calc to represent confidence interval for computed KPIs.
# NOTE Interval is likely to be transient solution and in the future its usage
#      will be probably changed to something like uncertainties.ufloat .
class Interval(np.void):
    _dtype = np.dtype([
        ('lo',  np.float64),
        ('hi',  np.float64),
    ])


# ΣMeasurement represents result of aggregation of several Measurements.
#
# It is similar to Measurement, but each value comes accompanied with
# information about how much time there was no data for that field:
#
#       Σ[f].value = Aggregate Mi[f]        if Mi[f] ≠ NA
#                           i
#
#       Σ[f].τ_na  =        Σ  Mi[X.δT]     if Mi[f] = NA
#                           i
class ΣMeasurement(np.void):
    _ = []
    for name in Measurement._dtype.names:
        dtyp = Measurement._dtype.fields[name][0]
        if not name.startswith('X.'):   # X.Tstart, X.δT
            dtyp = np.dtype([('value', dtyp), ('τ_na', Measurement.Ttime)])
        _.append((name, dtyp))
    _dtype = np.dtype(_)
    del _


# ----------------------------------------
# Measurement is the central part around which everything is organized.
# Let's have it go first.

# Measurement() creates new Measurement instance with all data initialized to NA.
@func(Measurement)
def __new__(cls):
    m = _newscalar(cls, cls._dtype)
    for field in m._dtype0.names:
        fdtype = m.dtype.fields[field][0]
        if fdtype.shape == ():
            m[field] = NA(fdtype)           # scalar
        else:
            m[field][:] = NA(fdtype.base)   # subarray
    return m

# ΣMeasurement() creates new ΣMeasurement instance.
#
# For all fields .value is initialized with NA and .τ_na with 0.
@func(ΣMeasurement)
def __new__(cls):
    Σ = _newscalar(cls, cls._dtype)
    for field in Σ.dtype.names:
        fdtype = Σ.dtype.fields[field][0]
        if fdtype.shape != ():              # skip subarrays - rely on aliases
            continue
        if field.startswith('X.'):          # X.Tstart, X.δT
            Σ[field] = NA(fdtype)
        else:
            Σ[field]['value'] = NA(fdtype.fields['value'][0])
            Σ[field]['τ_na']  = 0
    return Σ

# Stat() creates new Stat instance with specified values and dtype.
@func(Stat)
def __new__(cls, min, avg, max, n, dtype=np.float64):
    s = _newscalar(cls, cls._dtype_for(dtype))
    s['min'] = min
    s['avg'] = avg
    s['max'] = max
    s['n']   = n
    return s

# StatT() creates new StatT instance with specified values and dtype.
@func(StatT)
def __new__(cls, min, avg, max, dtype=np.float64):
    s = _newscalar(cls, cls._dtype_for(dtype))
    s['min'] = min
    s['avg'] = avg
    s['max'] = max
    return s


# _all_qci expands <name>.QCI into <name>.sum and [] of <name>.<qci> for all possible qci values.
# TODO remove and use direct array access (after causes are expanded into array too)
nqci = 256 # all possible QCIs ∈ [0,255], standard ones are described in 23.203 Table 6.1.7
def _all_qci(name_qci: str): # -> name_sum, ()name_qciv
    if not name_qci.endswith(".QCI"):
        raise AssertionError("invalid name_qci %r: no .QCI suffix" % name_qci)
    name = name_qci[:-len(".QCI")]
    name_qciv = tuple("%s.%d" % (name,q) for q in range(nqci))
    return name+".sum", name_qciv

# _all_cause expands <name>.CAUSE into <name>.sum and [] of <name>.<cause> for all possible cause values.
def _all_cause(name_cause: str): # -> name_sum, ()name_causev
    if not name_cause.endswith(".CAUSE"):
        raise AssertionError("invalid name_cause %r: no .CAUSE suffix" % name_cause)
    name = name_cause[:-len(".CAUSE")]
    return name+".sum", ()  # TODO add all possible CAUSEes - TS 36.331 (RRC)

# expand all .QCI and .CAUSE in ._dtype of Measurement and ΣMeasurement.
def _(Klass):
    # expand X.QCI -> X.sum  + X.QCI[nqci]
    qnamev = []  # X from X.QCI
    expv = []    # of (name, typ[, shape])
    for name in Klass._dtype .names:
        dtyp   = Klass._dtype .fields[name][0]
        if name.endswith('.QCI'):
            _ = name[:-len('.QCI')]
            qnamev.append(_)
            expv.append(('%s.sum' % _,  dtyp))        # X.sum
            expv.append((name,          dtyp, nqci))  # X.QCI[nqci]

        elif name.endswith('.CAUSE'):
           Σ, causev = _all_cause(name)
           for _ in (Σ,)+causev:
               expv.append((_, dtyp))

        else:
            expv.append((name, dtyp))

    _dtype = np.dtype(expv)

    # also provide .QCI aliases, e.g. X.5 -> X.QCI[5]
    namev   = []
    formatv = []
    offsetv = []
    for name in _dtype.names:
        fd, off = _dtype.fields[name]
        namev  .append(name)
        formatv.append(fd)
        offsetv.append(off)

    for qname in qnamev:
        qarr, off0 = _dtype.fields[qname+'.QCI']
        assert len(qarr.shape) == 1
        for qci in range(qarr.shape[0]):
            namev  .append('%s.%d' % (qname, qci))
            formatv.append(qarr.base)
            offsetv.append(off0 + qci*qarr.base.itemsize)

    Klass._dtype0 = _dtype  # ._dtype without aliases
    Klass._dtype  = np.dtype({
                            'names':   namev,
                            'formats': formatv,
                            'offsets': offsetv,
    })
    assert Klass._dtype.itemsize == Klass._dtype0.itemsize
_(Measurement)
_(ΣMeasurement)
del _


# __repr__ returns "Measurement(f1=..., f2=..., ...)".
# fields with NA value are omitted.
@func(Measurement)
def __repr__(m):
    initv = []
    for field in m._dtype0.names:
        vs = _vstr(m[field])
        if vs != 'ø':
            initv.append("%s=%s" % (field, vs))
    return "Measurement(%s)" % ', '.join(initv)

# __str__ returns "(v1, v2, ...)".
# NA values are represented as "ø".
# .QCI arrays are represented as {qci₁:v₁ qci₂:v₂ ...} with zero values omitted.
# if all values are NA - then the whole array is represented as ø.
@func(Measurement)
def __str__(m):
    vv = []
    for field in m._dtype0.names:
        vv.append(_vstr(m[field]))
    return "(%s)" % ', '.join(vv)


# __repr__ returns Stat(min, avg, max, n, dtype=...)
# NA values are represented as "ø".
@func(Stat)
def __repr__(s):
    return "Stat(%s, %s, %s, %s, dtype=%s)" % (_vstr(s['min']), _vstr(s['avg']),
                _vstr(s['max']), _vstr(s['n']), s['min'].dtype)

# __repr__ returns StatT(min, avg, max, dtype=...)
# NA values are represented as "ø".
@func(StatT)
def __repr__(s):
    return "StatT(%s, %s, %s, dtype=%s)" % (_vstr(s['min']), _vstr(s['avg']),
                _vstr(s['max']), s['min'].dtype)

# __str__ returns "<min avg max>·n"
# NA values are represented as "ø".
@func(Stat)
def __str__(s):
    return "<%s %s %s>·%s" % (_vstr(s['min']), _vstr(s['avg']), _vstr(s['max']), _vstr(s['n']))

# __str__ returns "<min avg max>"
# NA values are represented as "ø".
@func(StatT)
def __str__(s):
    return "<%s %s %s>" % (_vstr(s['min']), _vstr(s['avg']), _vstr(s['max']))


# _vstr returns string representation of scalar or subarray v.
def _vstr(v):  # -> str
    if v.shape == ():                       # scalar
        return 'ø' if isNA(v) else str(v)

    assert len(v.shape) == 1
    if isNA(v).all():                       # subarray full of ø
        return 'ø'

    va = []                                 # subarray with some non-ø data
    for k in range(v.shape[0]):
        vk = v[k]
        if isinstance(vk, np.void):
            for name in vk.dtype.names:
                if vk[name] != 0:
                    break
            else:
                continue
        else:
            if vk == 0:
                continue
        va.append('%d:%s' % (k, 'ø' if isNA(vk) else str(vk)))
    return "{%s}" % ' '.join(va)


# ==, != for Measurement.
@func(Measurement)
def __eq__(a, b):
    # NOTE does not work - https://github.com/numpy/numpy/issues/16377
    # return np.array_equal(a, b, equal_nan=True) # for NA==NA
    if not isinstance(b, Measurement):
        return False
    # cast to dtype without aliases to avoid
    # "dtypes with overlapping or out-of-order fields are not representable as buffers"
    return a.view(a._dtype0).data.tobytes() == \
           b.view(b._dtype0).data.tobytes()

@func(Measurement)
def __ne__(a, b):
    return not (a == b)

# _check_valid verifies Measurement data for validity.
#
# only basic verification are done - those that assert the most essential
# general invariants.
@func(Measurement)
def _check_valid(m):
    _badv = []
    def bad(text):
        _badv.append(text)

    # Tstart and δT must be present     TODO consider relaxing, e.g. we know δT, but not Tstart
    for f in ('X.Tstart', 'X.δT'):
        if isNA(m[f]):
            bad("%s = ø" % f)

    for field in m.dtype.names:
        v = m[field]
        if v.shape != ():   # skip subarrays - rely on aliases
            continue
        if isNA(v):
            continue

        # * ≥ 0
        if not isinstance(v, np.void):
            if v < 0:
                bad(".%s < 0  (%s)" % (field, v))
        else:
            for vfield in v.dtype.names:
                vf = v[vfield]
                if not isNA(vf) and vf < 0:
                    bad(".%s.%s < 0  (%s)" % (field, vfield, vf))

        # fini ≤ init
        if "Succ" in field:
            finit = field.replace("Succ", "Att")  # e.g. RRC.ConnEstabSucc.sum -> RRC.ConnEstabAtt.sum
            vinit = m[finit]
            if not isNA(vinit):
                if not (v <= vinit):
                    bad("fini > init (%s(%s) / %s(%s)" % (v, field, vinit, finit))

    if len(_badv) > 0:
        raise AssertionError("invalid Measurement data. the following problems were detected:" +
                             "\n- " + "\n- ".join(_badv))


# MeasurementLog() constructs new empty journal for logging measurements.
@func(MeasurementLog)
def __init__(mlog):
    mlog._data = np.ndarray((0,), dtype=(Measurement, Measurement._dtype))

# data returns all logged Measurements data as array.
@func(MeasurementLog)
def data(mlog):
    return mlog._data

# append adds new Measurement to the tail of MeasurementLog.
@func(MeasurementLog)
def append(mlog, m: Measurement):
    m._check_valid()
    # verify .Tstart↑
    if len(mlog._data) > 0:
        m_ = mlog._data[-1]
        τ   = m ['X.Tstart']
        τ_  = m_['X.Tstart']
        δτ_ = m_['X.δT']
        if not (τ_ < τ):
            raise AssertionError(".Tstart not ↑  (%s -> %s)" % (τ_, τ))
        if not (τ_ + δτ_ <= τ):
            raise AssertionError(".Tstart overlaps with previous measurement: %s ∈ [%s, %s)" %
                                    (τ, τ_, τ_ + δτ_))
    _ = np.append(
            mlog._data.view(Measurement._dtype0), # dtype0 because np.append does not handle aliased
            m.view(Measurement._dtype0))          # fields as such and increases out itemsize
    mlog._data = _.view((Measurement, Measurement._dtype))  # np.append looses Measurement from dtype

# forget_past deletes measurements with .Tstart ≤ Tcut
@func(MeasurementLog)
def forget_past(mlog, Tcut):
    # TODO use np.searchsorted
    i = 0
    while i < len(mlog._data):
        if Tcut < mlog._data[i]['X.Tstart']:
            break
        i += 1

    mlog._data = np.delete(mlog._data, slice(i))  # NOTE delete - contrary to append - preserves dtype

# ----------------------------------------


# Calc() is initialized from slice of data in the measurement log that is
# covered/overlapped with [τ_lo, τ_hi) time interval.
#
# The time interval, that will actually be used for computations, is potentially wider.
# See Calc class documentation for details.
@func(Calc)
def __init__(calc, mlog: MeasurementLog, τ_lo, τ_hi):
    assert τ_lo <= τ_hi
    data = mlog.data()
    l = len(data)

    # find min i: τ_lo < [i].(Tstart+δT)    ; i=l if not found
    # TODO binary search
    i = 0
    while i < l:
        m = data[i]
        m_τhi = m['X.Tstart'] + m['X.δT']
        if τ_lo < m_τhi:
            break
        i += 1

    # find min j: τ_hi ≤ [j].Tstart         ; j=l if not found
    j = i
    while j < l:
        m = data[j]
        m_τlo = m['X.Tstart']
        if τ_hi <= m_τlo:
            break
        j += 1

    data = data[i:j]
    if len(data) > 0:
        m_lo = data[0]
        m_hi = data[-1]
        τ_lo = min(τ_lo, m_lo['X.Tstart'])
        τ_hi = max(τ_hi, m_hi['X.Tstart']+m_hi['X.δT'])

    calc._data = data
    calc.τ_lo  = τ_lo
    calc.τ_hi  = τ_hi


# erab_accessibility computes "E-RAB Accessibility" KPI.
#
# It returns the following items:
#
#   - InitialEPSBEstabSR        probability of successful initial    E-RAB establishment    (%)
#   - AddedEPSBEstabSR          probability of successful additional E-RAB establishment    (%)
#
# The items are returned as Intervals with information about confidence for
# computed values.
#
# 3GPP reference: TS 32.450 6.1.1 "E-RAB Accessibility".
@func(Calc)
def erab_accessibility(calc): # -> InitialEPSBEstabSR, AddedEPSBEstabSR
    SR = calc._success_rate

    x = SR("Σcause RRC.ConnEstabSucc.CAUSE",
           "Σcause RRC.ConnEstabAtt.CAUSE")

    y = SR("S1SIG.ConnEstabSucc",
           "S1SIG.ConnEstabAtt")

    z = SR("Σqci ERAB.EstabInitSuccNbr.QCI",
           "Σqci ERAB.EstabInitAttNbr.QCI")

    InititialEPSBEstabSR = Interval(x['lo'] * y['lo'] * z['lo'],    # x·y·z
                                    x['hi'] * y['hi'] * z['hi'])

    AddedEPSBEstabSR = SR("Σqci ERAB.EstabAddSuccNbr.QCI",
                          "Σqci ERAB.EstabAddAttNbr.QCI")

    return _i2pc(InititialEPSBEstabSR), \
           _i2pc(AddedEPSBEstabSR)          # as %


# _success_rate computes success rate for fini/init events.
#
# i.e. ratio N(fini)/N(init).
#
# 3GPP defines success rate as N(successful-events) / N(total_events) ratio,
# for example N(connection_established) / N(connection_attempt). We take this
# definition as is for granularity periods with data, and extend it to also
# account for time intervals covered by Calc where measurements results are not
# available.
#
# To do so we extrapolate N(init) to be also contributed by "no data" periods
# proportionally to "no data" time coverage, and then we note that in those
# times, since no measurements have been made, the number of success events is
# unknown and can lie anywhere in between 0 and the number of added init events.
#
# This gives the following for resulting success rate confidence interval:
#
# time covered by periods with data:                    Σt
# time covered by periods with no data:                 t⁺      t⁺
# extrapolation for incoming initiation events:         init⁺ = ──·Σ(init)
#                                                               Σt
# fini events for "no data" time is full uncertainty:   fini⁺ ∈ [0,init⁺]
#
# => success rate over whole time is uncertain in between
#
#           Σ(fini)              Σ(fini) + init⁺
#       ──────────────   ≤ SR ≤  ──────────────
#       Σ(init) + init⁺          Σ(init) + init⁺
#
# that confidence interval is returned as the result.
#
# fini/init events can be prefixed with "Σqci " or "Σcause ". If such prefix is
# present, then fini/init value is obtained via call to Σqci or Σcause correspondingly.
@func(Calc)
def _success_rate(calc, fini, init): # -> Interval in [0,1]
    def vget(m, name):
        if name.startswith("Σqci "):
            return Σqci  (m, name[len("Σqci "):])
        if name.startswith("Σcause "):
            return Σcause(m, name[len("Σcause "):])
        return m[name]

    t_     = 0.
    Σt     = 0.
    Σinit  = 0
    Σfini  = 0
    Σufini = 0  # Σinit where fini=ø but init is not ø
    for m in calc._miter():
        τ = m['X.δT']
        vinit = vget(m, init)
        vfini = vget(m, fini)
        if isNA(vinit):
            t_ += τ
            # ignore fini, even if it is not ø.
            # TODO more correct approach: init⁺ for this period ∈ [fini,∞] and
            # once we extrapolate init⁺ we should check if it lies in that
            # interval and adjust if not. Then fini could be used as is.
        else:
            Σt += τ
            Σinit += vinit
            if isNA(vfini):
                Σufini += vinit
            else:
                Σfini += vfini

    if Σinit == 0 or Σt == 0:
        return Interval(0,1)    # full uncertainty

    init_ = t_ * Σinit / Σt
    a =  Σfini                   / (Σinit + init_)
    b = (Σfini + init_ + Σufini) / (Σinit + init_)
    return Interval(a,b)


# eutran_ip_throughput computes "E-UTRAN IP Throughput" KPI.
#
# It returns the following:
#
#   - IPThp[QCI][dl,ul]         IP throughput per QCI for downlink and uplink   (bit/s)
#
# All elements are returned as Intervals with information about confidence for
# computed values.
#
# NOTE: the unit of the result is bit/s, not kbit/s.
#
# 3GPP reference: TS 32.450 6.3.1 "E-UTRAN IP Throughput".
@func(Calc)
def eutran_ip_throughput(calc): # -> IPThp[QCI][dl,ul]
    qdlΣv  = np.zeros(nqci, dtype=np.float64)
    qdlΣt  = np.zeros(nqci, dtype=np.float64)
    qdlΣte = np.zeros(nqci, dtype=np.float64)
    qulΣv  = np.zeros(nqci, dtype=np.float64)
    qulΣt  = np.zeros(nqci, dtype=np.float64)
    qulΣte = np.zeros(nqci, dtype=np.float64)

    for m in calc._miter():
        for qci in range(nqci):
            dl_vol      = m["DRB.IPVolDl.QCI"]          [qci]
            dl_time     = m["DRB.IPTimeDl.QCI"]         [qci]
            dl_time_err = m["XXX.DRB.IPTimeDl_err.QCI"] [qci]
            ul_vol      = m["DRB.IPVolUl.QCI"]          [qci]
            ul_time     = m["DRB.IPTimeUl.QCI"]         [qci]
            ul_time_err = m["XXX.DRB.IPTimeUl_err.QCI"] [qci]

            if isNA(dl_vol) or isNA(dl_time) or isNA(dl_time_err):
                # don't account uncertainty - here it is harder to do compared
                # to erab_accessibility and the benefit is not clear. Follow
                # plain 3GPP spec for now.
                pass
            else:
                qdlΣv[qci]  += dl_vol
                qdlΣt[qci]  += dl_time
                qdlΣte[qci] += dl_time_err

            if isNA(ul_vol) or isNA(ul_time) or isNA(ul_time_err):
                # no uncertainty accounting - see ^^^
                pass
            else:
                qulΣv[qci]  += ul_vol
                qulΣt[qci]  += ul_time
                qulΣte[qci] += ul_time_err

    thp = np.zeros(nqci, dtype=np.dtype([
                            ('dl', Interval._dtype),
                            ('ul', Interval._dtype),
    ]))
    for qci in range(nqci):
        if qdlΣt[qci] > 0:
            thp[qci]['dl']['lo'] = qdlΣv[qci] / (qdlΣt[qci] + qdlΣte[qci])
            thp[qci]['dl']['hi'] = qdlΣv[qci] / (qdlΣt[qci] - qdlΣte[qci])
        if qulΣt[qci] > 0:
            thp[qci]['ul']['lo'] = qulΣv[qci] / (qulΣt[qci] + qulΣte[qci])
            thp[qci]['ul']['hi'] = qulΣv[qci] / (qulΣt[qci] - qulΣte[qci])

    return thp


# aggregate aggregates values of all Measurements in covered time interval.
@func(Calc)
def aggregate(calc): # -> ΣMeasurement
    Σ = ΣMeasurement()
    Σ['X.Tstart'] = calc.τ_lo
    Σ['X.δT']     = calc.τ_hi - calc.τ_lo

    def xmin(a, b):
        if isNA(a): return b
        if isNA(b): return a
        return min(a, b)

    def xmax(a, b):
        if isNA(a): return b
        if isNA(b): return a
        return max(a, b)

    def xavg(a, na, b, nb): # -> <ab>, na+nb
        if isNA(a) or isNA(na):
            return b, nb
        if isNA(b) or isNA(nb):
            return a, na
        nab = na+nb
        ab = (a*na + b*nb)/nab
        return ab, nab

    for m in calc._miter():
        for field in m.dtype.names:
            if field.startswith('X.'):  # X.Tstart, X.δT
                continue

            v = m[field]
            if v.shape != ():           # skip subarrays - rely on aliases
                continue

            Σf = Σ[field]       # view to Σ[field]
            Σv = Σf['value']    # view to Σ[field]['value']

            if isNA(v):
                Σf['τ_na'] += m['X.δT']
                continue

            if isNA(Σv):
                Σf['value'] = v
                continue

            if isinstance(v, np.number):
                Σf['value'] += v

            elif isinstance(v, StatT):
                Σv['min'] = xmin(Σv['min'], v['min'])
                Σv['max'] = xmax(Σv['max'], v['max'])
                # TODO better sum everything and then divide as a whole to avoid loss of precision
                Σv['avg'], _ = xavg(Σv['avg'], m['X.Tstart'] - Σ['X.Tstart'] - Σf['τ_na'],
                                     v['avg'], m['X.δT'])

            elif isinstance(v, Stat):
                Σv['min'] = xmin(Σv['min'], v['min'])
                Σv['max'] = xmax(Σv['max'], v['max'])
                # TODO better sum everything and then divide as a whole to avoid loss of precision
                Σv['avg'], Σv['n'] = xavg(Σv['avg'], Σv['n'],
                                           v['avg'],  v['n'])

            else:
                raise AssertionError("Calc.aggregate: unexpected type %r" % type(v))

    return Σ

# sum is deprecated alias to aggregate.
@func(Calc)
def sum(calc):
    warnings.warn("Calc.sum is deprecated -> use Calc.aggregate instead", DeprecationWarning, stacklevel=4)
    return calc.aggregate()


# _miter iterates through [.τ_lo, .τ_hi) yielding Measurements.
#
# The measurements are yielded with consecutive timestamps. There is no gaps
# as NA Measurements are yielded for time holes in original MeasurementLog data.
@func(Calc)
def _miter(calc): # -> iter(Measurement)
    τ = calc.τ_lo
    l = len(calc._data)
    i = 0  # current Measurement from data

    while i < l:
        m = calc._data[i]
        m_τlo = m['X.Tstart']
        m_τhi = m_τlo + m['X.δT']
        assert m_τlo < m_τhi

        if τ < m_τlo:
            # <- M(ø)[τ, m_τlo)
            h = Measurement()
            h['X.Tstart'] = τ
            h['X.δT']     = m_τlo - τ
            yield h

        # <- M from mlog
        yield m

        τ = m_τhi
        i += 1

    assert τ <= calc.τ_hi
    if τ < calc.τ_hi:
        # <- trailing M(ø)[τ, τ_hi)
        h = Measurement()
        h['X.Tstart'] = τ
        h['X.δT']     = calc.τ_hi - τ
        yield h


# Interval(lo,hi) creates new interval with specified boundaries.
@func(Interval)
def __new__(cls, lo, hi):
    i = _newscalar(cls, cls._dtype)
    i['lo'] = lo
    i['hi'] = hi
    return i


# Σqci performs summation over all qci for m[name_qci].
#
# usage example:
#
#   Σqci(m, 'ERAB.EstabInitSuccNbr.QCI')
#
# name_qci must have '.QCI' suffix.
def Σqci(m: Measurement, name_qci: str):
    return _Σx(m, name_qci, _all_qci)

# Σcause, performs summation over all causes for m[name_cause].
#
# usage example:
#
#   Σcause(m, 'RRC.ConnEstabSucc.CAUSE')
#
# name_cause must have '.CAUSE' suffix.
def Σcause(m: Measurement, name_cause: str):
    return _Σx(m, name_cause, _all_cause)

# _Σx serves Σqci and Σcause.
def _Σx(m: Measurement, name_x: str, _all_x: func):
    name_sum, name_xv = _all_x(name_x)
    s = m[name_sum]
    if not isNA(s):
        return s
    s  = s.dtype.type(0)
    ok = True  if len(name_xv) > 0  else False
    for _ in name_xv:
        v = m[_]
        # we don't know the answer even if single value is NA
        # (if data source does not support particular qci/cause, it should set it to 0)
        if isNA(v):
            ok = False
        else:
            s += v
    if not ok:
        return NA(s.dtype)
    else:
        return s


# _i2pc maps Interval in [0,1] to one in [0,100] by multiplying lo/hi by 1e2.
def _i2pc(x: Interval): # -> Interval
    return Interval(x['lo']*100, x['hi']*100)


# _newscalar creates new NumPy scalar instance with specified type and dtype.
def _newscalar(typ, dtype):
    dtyp = np.dtype((typ, dtype))   # dtype with .type adjusted to be typ
    assert dtyp == dtype
    assert dtyp.type is typ
    _ = np.zeros(shape=(), dtype=dtyp)
    s = _[()]
    assert type(s) is typ
    assert s.dtype is dtyp
    return s


# ---- NA ----

# NA returns "Not Available" value for dtype.
def NA(dtype):
    typ = dtype.type
    # float
    if issubclass(typ, np.floating):
        na = typ(np.nan)  # return the same type as dtype has, e.g. np.int32, not int
    # int: NA is min value
    elif issubclass(typ, np.signedinteger):
        na = typ(np.iinfo(typ).min)
    # structure: NA is combination of NAs for fields
    elif issubclass(typ, np.void):
        na = _newscalar(typ, dtype)
        for field in dtype.names:
            na[field] = NA(dtype.fields[field][0])
    else:
        raise AssertionError("NA not defined for dtype %s" % (dtype,))

    assert type(na) is typ
    return na


# isNA returns whether value represent NA.
#
# returns True/False if value is scalar.
# returns array(True/False) if value is array.
def isNA(value):
    na = NA(value.dtype)

    # `nan == nan` gives False
    # work it around by checking for nan explicitly
    if isinstance(na, np.void): # items are structured scalars
        vna = None
        for field in value.dtype.names:
            nf = na[field]
            vf = value[field]
            if np.isnan(nf):
                x = np.isnan(vf)
            else:
                x = (vf == nf)

            if vna is None:
                vna = x
            else:
                vna &= x
        return vna
    else:
        if np.isnan(na):
            return np.isnan(value)

    return value == na
