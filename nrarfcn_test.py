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

from xlte.nrarfcn import frequency, dl2ul, ul2dl, dl2ssb
from xlte.nrarfcn import nr

from pytest import raises


# verify nrarfcn-related calculations wrt several points.
def test_nrarfcn():
    def _(band, dl_nr_arfcn, ul_nr_arfcn, fdl, ful, rf_mode, ssb_nr_arfcn, max_ssb_scs_khz):
        assert rf_mode == nr.get_duplex_mode(band).upper()
        if dl_nr_arfcn is not None:
            assert frequency(dl_nr_arfcn) == fdl
            if ul_nr_arfcn is not None:
                assert dl2ul(dl_nr_arfcn, band) == ul_nr_arfcn
            else:
                assert rf_mode in ('FDD', 'SDL')
                if rf_mode == 'FDD':
                    estr = 'does not have enough uplink spectrum'
                if rf_mode == 'SDL':
                    estr = 'does not have uplink spectrum'
                with raises(KeyError, match=estr):
                    dl2ul(dl_nr_arfcn, band)

        if ul_nr_arfcn is not None:
            assert frequency(ul_nr_arfcn) == ful
            if dl_nr_arfcn is not None:
                assert ul2dl(ul_nr_arfcn, band) == dl_nr_arfcn
            else:
                assert rf_mode in ('FDD', 'SUL')
                if rf_mode == 'FDD':
                    estr = 'does not have enough downlink spectrum'
                if rf_mode == 'SUL':
                    estr = 'does not have downlink spectrum'
                with raises(KeyError, match=estr):
                    ul2dl(ul_nr_arfcn, band)

        if dl_nr_arfcn is not None:
            if not isinstance(ssb_nr_arfcn, type):
                assert dl2ssb(dl_nr_arfcn, band) == (ssb_nr_arfcn, max_ssb_scs_khz)
            else:
                with raises(ssb_nr_arfcn):
                    dl2ssb(dl_nr_arfcn, band)

    # band   dl       ul      fdl       ful     rf_mode   ssb    max_ssb_scs_khz
    _(  1,  428000,  390000,  2140,     1950,     'FDD', 427970, 15)
    _(  2,  396000,  380000,  1980,     1900,     'FDD', 396030, 15)
    _(  5,  176300,  167300,   881.5,    836.5,   'FDD', 176210, 30)
    _(  5,  176320,  167320,   881.6,    836.6,   'FDD', 176410, 30)
    _(  7,  526000,  502000,  2630,     2510,     'FDD', 526090, 15)
    _( 29,  144500,   None,    722.5,   None,     'SDL', 144530, 15)
    _( 39,  378000,  378000,  1890,     1890,     'TDD', 378030, 30)    # % 30khz = 0
    _( 39,  378003,  378003,  1890.015, 1890.015, 'TDD', 378030, 15)    # % 15khz = 0   % 30khz ≠ 0
    _( 38,  520000,  520000,  2600,     2600,     'TDD', 520090, 30)
    _( 41,  523020,  523020,  2615.1,   2615.1,   'TDD', 522990, 30)    # % 30khz = 0
    _( 41,  523023,  523023,  2615.115, 2615.115, 'TDD', 522990, 15)    # % 15khz = 0   % 30khz ≠ 0
    _( 66,  431000,  351000,  2155,     1755,     'FDD', 431090, 30)
    _( 66,  437000,   None,   2185,     None,     'FDD', 437090, 30)    # NOTE in n66 range(dl) > range(ul)
    _( 78,  632628,  632628,  3489.42,  3489.42,  'TDD', 632640, 30)
    _( 91,  285900,  166900,  1429.5,    834.5,   'FDD', 285870, 15)
    _( 91,    None,  172400,  None,      862,     'FDD', None,   None)  # NOTE in n91 range(dl) < range(ul)
    _( 80,    None,  342000,  None,     1710,     'SUL', None,   None)

    _(257, 2079167, 2079167, 28000.08, 28000.08,  'TDD', 2079163, 240)  # FR2-1
    _(257, 2079169, 2079169, 28000.20, 28000.20,  'TDD', 2079163, 120)  # FR2-1  % 240khz ≠ 0
    _(263, 2680027, 2680027, 64051.68, 64051.68,  'TDD', 2679931, 960)  # FR2-2
    _(263, 2680003, 2680003, 64050.24, 64050.24,  'TDD', 2679931, 480)  # FR2-2  % 960khz ≠ 0
    _(263, 2679991, 2679991, 64049.52, 64049.52,  'TDD', 2679931, 120)  # FR2-2  % 480khz ≠ 0

    # some dl points not on ΔFraster -> ssb cannot be found
    _( 78,  632629,  632629,  3489.435, 3489.435, 'TDD', KeyError, None)
    _(257, 2079168, 2079168, 28000.14, 28000.14,  'TDD', KeyError, None)


    # error in input parameters -> ValueError
    def edl(band, dl_nr_arfcn, estr):
        for f in (dl2ul, dl2ssb):
            with raises(ValueError, match=estr):
                f(dl_nr_arfcn, band)
    def eul(band, ul_nr_arfcn, estr):
        for f in (ul2dl,):
            with raises(ValueError, match=estr):
                f(ul_nr_arfcn, band)
    # no x spectrum when requesting x2y
    edl(80, 10000, 'band80 does not have downlink spectrum')    # SUL
    eul(29, 10000, 'band29 does not have uplink spectrum')      # SDL
    # mismatch between x_nr_arfcn and band
    edl( 1, 10000, 'band1: NR-ARFCN=10000 is outside of downlink spectrum')
    eul( 1, 10000, 'band1: NR-ARFCN=10000 is outside of uplink spectrum')
