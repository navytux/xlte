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
"""Package kpi will provide functionality to compute Key Performance Indicators of LTE services.

- MeasurementLog maintains journal with result of measurements. Use .append()
  to populate it with data.

- Measurement represents measurement results. Its documentation establishes
  semantic for measurement results to be followed by drivers.


See following 3GPP standards for KPI-related topics:

    - TS 32.401
    - TS 32.450
    - TS 32.425
"""

import numpy as np
from golang import func


# MeasurementLog represent journal of performed Measurements.
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

    # _dtype defines measured values and events.
    _dtype = np.dtype([
        ('X.Tstart',                        Ttime),     # when the measurement started
        ('X.δT',                            Ttime),     # time interval during which the measurement was made

        # below come values/events as specified by TS 32.425 and TS 32.450
        # NOTE all .QCI and .CAUSE are expanded from outside.
        #
        # NAME                              TYPE          UNIT      TS 32.425 reference + ...
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
        # XXX mean is not good for our model
        # TODO mean -> total + npkt?
        #('DRB.IPLatDl.QCI',                Ttime),     # s         4.4.5.1  32.450:6.3.2   NOTE not ms

        # DRB.IPThpX.QCI = DRB.IPThpVolX.QCI / DRB.IPThpTimeX.QCI
        ('DRB.IPThpVolDl.QCI',              np.int64),  # bit       4.4.6.1  32.450:6.3.1   NOTE not kbit
        ('DRB.IPThpVolUl.QCI',              np.int64),  # bit       4.4.6.2  32.450:6.3.1   NOTE not kbit
        ('DRB.IPThpTimeDl.QCI',             Ttime),     # s
        ('DRB.IPThpTimeUl.QCI',             Ttime),     # s

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


# ----------------------------------------
# Measurement is the central part around which everything is organized.
# Let's have it go first.

# Measurement() creates new Measurement instance with all data initialized to NA.
@func(Measurement)
def __new__(cls):
    m = _newscalar(cls, cls._dtype)
    for field in m.dtype.names:
        fdtype = m.dtype.fields[field][0]
        m[field] = NA(fdtype)
    return m


# _all_qci expands <name>.QCI into <name>.sum and [] of <name>.<qci> for all possible qci values.
def _all_qci(name_qci: str): # -> name_sum, ()name_qciv
    if not name_qci.endswith(".QCI"):
        raise AssertionError("invalid name_qci %r: no .QCI suffix" % name_qci)
    name = name_qci[:-len(".QCI")]
    return name+".sum", ()  # TODO add all possible QCIs    - TS 36.413 (S1AP)

# _all_cause expands <name>.CAUSE into <name>.sum and [] of <name>.<cause> for all possible cause values.
def _all_cause(name_cause: str): # -> name_sum, ()name_causev
    if not name_cause.endswith(".CAUSE"):
        raise AssertionError("invalid name_cause %r: no .CAUSE suffix" % name_cause)
    name = name_cause[:-len(".CAUSE")]
    return name+".sum", ()  # TODO add all possible CAUSEes - TS 36.331 (RRC)

# expand all .QCI and .CAUSE in Measurement._dtype .
def _():
    expv = [] # of (name, typ)
    for name in Measurement._dtype .names:
        typ   = Measurement._dtype .fields[name][0].type
        if name.endswith('.QCI'):
           Σ, qciv = _all_qci(name)
           for _ in (Σ,)+qciv:
               expv.append((_, typ))

        elif name.endswith('.CAUSE'):
           Σ, causev = _all_cause(name)
           for _ in (Σ,)+causev:
               expv.append((_, typ))

        else:
            expv.append((name, typ))

    Measurement._dtype = np.dtype(expv)
_()
del _


# __repr__ returns "Measurement(f1=..., f2=..., ...)".
# fields with NA value are omitted.
@func(Measurement)
def __repr__(m):
    initv = []
    for field in m.dtype.names:
        v = m[field]
        if not isNA(v):
            initv.append("%s=%r" % (field, v))
    return "Measurement(%s)" % ', '.join(initv)

# __str__ returns "(v1, v2, ...)".
# NA values are represented as "ø".
@func(Measurement)
def __str__(m):
    vv = []
    for field in m.dtype.names:
        v = m[field]
        vv.append('ø' if isNA(v) else str(v))
    return "(%s)" % ', '.join(vv)

# ==, != for Measurement.
@func(Measurement)
def __eq__(a, b):
    # NOTE does not work - https://github.com/numpy/numpy/issues/16377
    # return np.array_equal(a, b, equal_nan=True) # for NA==NA
    if not isinstance(b, Measurement):
        return False
    return a.data.tobytes() == b.data.tobytes()

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
        if isNA(v):
            continue

        # * ≥ 0
        if v < 0:
            bad(".%s < 0  (%s)" % (field, v))

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

    _ = np.append(mlog._data, m)
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


# _newscalar creates new NumPy scalar instance with specified type and dtype.
def _newscalar(typ, dtype):
    _ = np.zeros(shape=(), dtype=(typ, dtype))
    s = _[()]
    assert type(s) is typ
    return s


# ---- NA ----

# NA returns "Not Available" value for dtype.
def NA(dtype):
    # float
    if issubclass(dtype.type, np.floating):
        return np.nan
    # int: NA is min value
    if issubclass(dtype.type, np.signedinteger):
        return np.iinfo(dtype.type).min

    raise AssertionError("NA not defined for dtype %s" % (dtype,))


# isNA returns whether value represent NA.
# value must be numpy scalar.
def isNA(value):
    na = NA(value.dtype)
    if np.isnan(na):
        return np.isnan(value)  # `nan == nan` gives False
    return value == na
