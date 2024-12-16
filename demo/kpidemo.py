#!/usr/bin/env python
"""kpidemo - plot KPIs computed from enb.xlog

Also print total for raw counters.

Usage: kpidemo <time period> <enb.xlog uri>
"""

from __future__ import print_function, division, absolute_import

from xlte import kpi
from xlte.amari import kpi as akpi
from golang import func, defer

import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib import ticker
from datetime import datetime, timedelta

import sys
from urllib.request import urlopen


@func
def main():
    # This program demonstrates how to compute KPIs with xlte.
    #
    # As outlined in xlte/kpi.py module documentation the pipeline to
    # compute a KPI is as follows:
    #
    #    ─────────────
    #   │ Measurement │  Measurements   ────────────────       ──────
    #   │             │ ─────────────→ │ MeasurementLog │ ──→ │ Calc │ ──→  KPI
    #   │   driver    │                 ────────────────       ──────
    #    ─────────────
    #
    # Below we will organize this pipeline and execute it step by step.

    # Step 1. Setup driver to provide measurements data. Such a driver is
    # specific to eNB implementation. Below we use amari.kpi.LogMeasure that is
    # specific to Amarisoft and retrieves measurements data from enb.xlog .
    xlog_uri = sys.argv[2]
    fxlog = urlopen(xlog_uri)
    alogm = akpi.LogMeasure(fxlog,  # amari.kpi.LogMeasure converts enb.xlog into kpi.Measurements
                            open('/dev/null', 'r'))
    defer(alogm.close)

    # Step 2. Setup kpi.MeasurementLog and load measurements data into it.
    # The data, as contained in the measurement log, is kept there in the form
    # of kpi.Measurement, which is driver-independent representation for
    # KPI-related measurement data.

    def load_measurements(alogm: akpi.LogMeasure) -> kpi.MeasurementLog:
        mlog = kpi.MeasurementLog()
        while 1:
            m = alogm.read()
            if m is None:
                break
            mlog.append(m)
        return mlog

    mlog = load_measurements(alogm)


    # Step 3. Compute KPIs over MeasurementLog with specified granularity
    # period. We partition entries in the measurement log by specified time
    # period, and further use kpi.Calc to compute the KPIs over each period.

    # calc_each_period partitions mlog data into periods and yields kpi.Calc for each period.
    def calc_each_period(mlog: kpi.MeasurementLog, tperiod: float): # -> yield kpi.Calc
        τ = mlog.data()[0]['X.Tstart']
        for m in mlog.data()[1:]:
            τ_ = m['X.Tstart']
            if (τ_ - τ) >= tperiod:
                calc = kpi.Calc(mlog, τ, τ+tperiod)
                τ = calc.τ_hi
                yield calc

    tperiod = float(sys.argv[1])
    vτ = []
    vInititialEPSBEstabSR = []
    vAddedEPSBEstabSR     = []
    vIPThp_qci            = []

    for calc in calc_each_period(mlog, tperiod):
        vτ.append(calc.τ_lo)

        _ = calc.erab_accessibility()       # E-RAB Accessibility
        vInititialEPSBEstabSR.append(_[0])
        vAddedEPSBEstabSR    .append(_[1])

        _ = calc.eutran_ip_throughput()     # E-UTRAN IP Throughput
        vIPThp_qci.append(_)

    vτ                      = np.asarray([datetime.fromtimestamp(_) for _ in vτ])
    vInititialEPSBEstabSR   = np.asarray(vInititialEPSBEstabSR)
    vAddedEPSBEstabSR       = np.asarray(vAddedEPSBEstabSR)
    vIPThp_qci              = np.asarray(vIPThp_qci)


    # Step 4. Plot computed KPIs.

    # 4a) The E-RAB Accessibility KPI has two parts: initial E-RAB establishment
    # success rate, and additional E-RAB establishment success rate. kpi.Calc
    # provides both of them in the form of their confidence intervals. The
    # lower margin of the confidence interval coincides with 3GPP definition of
    # the KPI. The upper margin, however, provides information of how
    # confident, or how unsure we are about described value. For example if
    # there is enough data to compute the KPI precisely during particular
    # period, the low and high margins of the confidence interval will be the
    # same. However if, during a period, there is no measurements data at all,
    # the confidence interval will be [0,100] meaning full uncertainty - because
    # there is no measurements data we don't know how accessible eNB was during
    # that period of time. The width of a confidence interval is not
    # necessarily 100. For example if during a period, there is no measurement
    # data only for part of that period, the KPI value is computed from the
    # other times in the period when there is data, and the confidence interval
    # will be thinner.
    #
    # For each of the parts we plot both its lower margin and the whole
    # confidence interval area.

    # 4b) The E-UTRAN IP Throughput KPI provides throughput measurements for
    # all QCIs and does not have uncertainty. QCIs for which throughput data is
    # all zeros are said to be silent and are not plotted.

    fig = plt.figure(constrained_layout=True, figsize=(12,8))
    facc, fthp = fig.subfigures(1, 2)
    figplot_erab_accessibility  (facc, vτ, vInititialEPSBEstabSR, vAddedEPSBEstabSR, tperiod)
    figplot_eutran_ip_throughput(fthp, vτ, vIPThp_qci, tperiod)
    defer(plt.show)


    # Step 5. Print total for raw counters.
    mhead = mlog.data()[0]
    mtail = mlog.data()[-1]
    calc_total = kpi.Calc(mlog, mhead['X.Tstart'], mtail['X.Tstart']+mtail['X.δT'])
    Σ = calc_total.aggregate()
    print_ΣMeasurement(Σ)


# ---- plotting routines ----

# figplot_erab_accessibility plots E-RAB Accessibility KPI data on the figure.
def figplot_erab_accessibility(fig: plt.Figure, vτ, vInititialEPSBEstabSR, vAddedEPSBEstabSR, tperiod=None):
    ax1, ax2 = fig.subplots(2, 1, sharex=True)
    fig.suptitle("E-RAB Accessibility / %s" % (tpretty(tperiod)  if tperiod is not None else
                                               vτ_period_pretty(vτ)))
    ax1.set_title("Initial E-RAB establishment success rate")
    ax2.set_title("Added E-RAB establishment success rate")

    plot_success_rate(ax1, vτ, vInititialEPSBEstabSR, "InititialEPSBEstabSR")
    plot_success_rate(ax2, vτ, vAddedEPSBEstabSR,     "AddedEPSBEstabSR")


# figplot_eutran_ip_throughput plots E-UTRAN IP Throughput KPI data on the figure.
def figplot_eutran_ip_throughput(fig: plt.Figure, vτ, vIPThp_qci, tperiod=None):
    ax1, ax2 = fig.subplots(2, 1, sharex=True)
    fig.suptitle("E-UTRAN IP Throughput / %s" % (tpretty(tperiod)  if tperiod is not None else
                                                 vτ_period_pretty(vτ)))
    ax1.set_title("Downlink")
    ax2.set_title("Uplink")
    ax1.set_ylabel("Mbit/s")
    ax2.set_ylabel("Mbit/s")

    v_qci = (vIPThp_qci .view(np.float64) / 1e6) \
                        .view(vIPThp_qci.dtype)
    plot_per_qci(ax1, vτ, v_qci[:,:]['dl'], 'IPThp')
    plot_per_qci(ax2, vτ, v_qci[:,:]['ul'], 'IPThp')

    _, dmax = ax1.get_ylim()
    _, umax = ax2.get_ylim()
    ax1.set_ylim(ymin=0, ymax=dmax*1.05)
    ax2.set_ylim(ymin=0, ymax=umax*1.05)


# plot_success_rate plots success-rate data from vector v on ax.
# v is array with Intervals.
def plot_success_rate(ax, vτ, v, label):
    ax.plot(vτ, v['lo'], drawstyle='steps-post', label=label)
    ax.fill_between(vτ, v['lo'], v['hi'],
                    step='post', alpha=0.1, label='%s\nuncertainty' % label)

    ax.set_ylabel("%")
    ax.set_ylim([0-10, 100+10])
    ax.set_yticks([0,20,40,60,80,100])

    fmt_dates_pretty(ax.xaxis)
    ax.grid(True)
    ax.legend(loc='upper left')


# plot_per_qci plots data from per-QCI vector v_qci.
#
# v_qci should be array[t, QCI].
# QCIs, for which v[:,qci] is all zeros, are said to be silent and are not plotted.
def plot_per_qci(ax, vτ, v_qci, label):
    ax.set_xlim((vτ[0], vτ[-1]))  # to have correct x range even if we have no data
    assert len(v_qci.shape) == 2
    silent = True
    propv = list(plt.rcParams['axes.prop_cycle'])
    for qci in range(v_qci.shape[1]):
        v = v_qci[:, qci]
        if (v['hi'] == 0).all():  # skip silent QCIs
            continue
        silent = False
        prop = propv[qci % len(propv)]  # to have same colors for same qci in different graphs
        ax.plot(vτ, v['lo'], label="%s.%d" % (label, qci), **prop)
        ax.fill_between(vτ, v['lo'], v['hi'], alpha=0.3, **prop)

    if silent:
        ax.plot([],[], ' ', label="all QCI silent")

    fmt_dates_pretty(ax.xaxis)
    ax.grid(True)
    ax.legend(loc='upper left')


# fmt_dates_pretty instructs axis to use concise dates formatting.
def fmt_dates_pretty(axis):
    xloc = mdates.AutoDateLocator()
    xfmt = mdates.ConciseDateFormatter(xloc)
    axis.set_major_locator(xloc)
    axis.set_major_formatter(xfmt)
    axis.set_minor_locator(ticker.AutoMinorLocator(5))


# tpretty returns pretty form for time, e.g. 1'2" for 62 seconds.
def tpretty(t):
    tmin, tsec = divmod(t, 60)
    return "%s%s" % ("%d'" % tmin if tmin else '',
                     '%d"' % tsec if tsec else '')

# vτ_period_pretty returns pretty form for time period in vector vτ.
# for example [2,5,8,11] gives 3'.
def vτ_period_pretty(vτ):
    if len(vτ) < 2:
        return "?"
    s = timedelta(seconds=1)
    δvτ = (vτ[1:] - vτ[:-1]) / s  # in seconds
    min = δvτ.min()
    avg = δvτ.mean()
    max = δvτ.max()
    std = δvτ.std()
    if min == max:
        return tpretty(min)
    return "%s ±%s  [%s, %s]" % (tpretty(avg), tpretty(std), tpretty(min), tpretty(max))


# ---- printing routines ----

# print_ΣMeasurement prints aggregated counters.
def print_ΣMeasurement(Σ: kpi.ΣMeasurement):
    print("Time:\t%s  -  %s" % (datetime.fromtimestamp(Σ['X.Tstart']),
                                datetime.fromtimestamp(Σ['X.Tstart'] + Σ['X.δT'])))
    # emit1 prints one field.
    def emit1(name, v, τ_na):
        fmt = "%12s   "
        if kpi.isNA(v):
            s = fmt % "NA"
        else:
            if isinstance(v, np.floating):
                fmt = "%15.2f"
            s = fmt % v
            pna = τ_na / Σ['X.δT'] * 100
            if pna >= 0.01:
                s += "  (%.2f%% NA)" % pna
        print("%-32s:\t%s" % (name, s))

    for field in Σ._dtype0.names:
        if field in ('X.Tstart', 'X.δT'):
            continue
        v    = Σ[field]['value']
        τ_na = Σ[field]['τ_na']
        if v.shape == ():                   # scalar
            emit1(field, v, τ_na)
        else:
            assert len(v.shape) == 1
            if kpi.isNA(v).all():           # subarray full of ø
                emit1(field, v[0], τ_na[0])
            else:                           # subarray with some non-ø data
                for k in range(v.shape[0]):
                    if v[k] != 0:
                        fieldk = '%s.%d' % (field[:field.rfind('.')], k)  # name.QCI -> name.k
                        emit1(fieldk, v[k], τ_na[k])


if __name__ == '__main__':
    main()
