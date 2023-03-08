#!/usr/bin/env python
"""kpidemo - plot KPIs computed from enb.xlog

Usage: kpidemo <time period> <enb.xlog uri>
"""

from xlte import kpi
from xlte.amari import kpi as akpi
from golang import func, defer

import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
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


    # Step 3. Compute E-RAB Accessibility KPI over MeasurementLog with
    # specified granularity period. We partition entries in the measurement log
    # by specified time period, and further use kpi.Calc to compute the KPI
    # over each period.

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

    for calc in calc_each_period(mlog, tperiod):
        vτ.append(calc.τ_lo)

        _ = calc.erab_accessibility()       # E-RAB Accessibility
        vInititialEPSBEstabSR.append(_[0])
        vAddedEPSBEstabSR    .append(_[1])

    vτ                      = np.asarray([datetime.fromtimestamp(_) for _ in vτ])
    vInititialEPSBEstabSR   = np.asarray(vInititialEPSBEstabSR)
    vAddedEPSBEstabSR       = np.asarray(vAddedEPSBEstabSR)


    # Step 4. Plot computed KPI.
    # The E-RAB Accessibility KPI has two parts: initial E-RAB establishment
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
    fig = plt.figure(constrained_layout=True, figsize=(6,8))
    figplot_erab_accessibility  (fig, vτ, vInititialEPSBEstabSR, vAddedEPSBEstabSR, tperiod)
    plt.show()


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


# fmt_dates_pretty instructs axis to use concise dates formatting.
def fmt_dates_pretty(axis):
    xloc = mdates.AutoDateLocator()
    xfmt = mdates.ConciseDateFormatter(xloc)
    axis.set_major_locator(xloc)
    axis.set_major_formatter(xfmt)


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


if __name__ == '__main__':
    main()
