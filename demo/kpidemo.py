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
from datetime import datetime

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
    mlog = kpi.MeasurementLog()
    while 1:
        m = alogm.read()
        if m is None:
            break
        mlog.append(m)


    # Step 3. Compute E-RAB Accessibility KPI over MeasurementLog with
    # specified granularity period. We partition entries in the measurement log
    # by specified time period, and further use kpi.Calc to compute the KPI
    # over each period.
    tperiod = float(sys.argv[1])
    vτ = []
    vInititialEPSBEstabSR = []
    vAddedEPSBEstabSR     = []

    τ = mlog.data()[0]['X.Tstart']
    for m in mlog.data()[1:]:
        τ_ = m['X.Tstart']
        if (τ_ - τ) >= tperiod:
            calc = kpi.Calc(mlog, τ, τ+tperiod)
            vτ.append(calc.τ_lo)
            τ = calc.τ_hi
            _ = calc.erab_accessibility()
            vInititialEPSBEstabSR.append(_[0])
            vAddedEPSBEstabSR    .append(_[1])


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
    fig, (ax1, ax2) = plt.subplots(2, 1, sharex=True, layout='constrained')
    pmin, psec = divmod(tperiod, 60)
    fig.suptitle("E-RAB Accessibility / %s%s" % ("%d'" % pmin if pmin else '',
                                                 '%d"' % psec if psec else ''))
    ax1.set_title("Initial E-RAB establishment success rate")
    ax2.set_title("Added E-RAB establishment success rate")

    vτ = [datetime.fromtimestamp(_) for _ in vτ]
    def plot1(ax, v, label):  # plot1 plots KPI data from vector v on ax.
        v = np.asarray(v)
        ax.plot(vτ, v['lo'], drawstyle='steps-post', label=label)
        ax.fill_between(vτ, v['lo'], v['hi'],
                        step='post', alpha=0.1, label='%s\nuncertainty' % label)

    plot1(ax1, vInititialEPSBEstabSR, "InititialEPSBEstabSR")
    plot1(ax2, vAddedEPSBEstabSR,     "AddedEPSBEstabSR")

    for ax in (ax1, ax2):
        ax.set_ylabel("%")
        ax.set_ylim([0-10, 100+10])
        ax.set_yticks([0,20,40,60,80,100])

        xloc = mdates.AutoDateLocator()
        xfmt = mdates.ConciseDateFormatter(xloc)
        ax.xaxis.set_major_locator(xloc)
        ax.xaxis.set_major_formatter(xfmt)

        ax.grid(True)
        ax.legend(loc='upper left')

    plt.show()


if __name__ == '__main__':
    main()
