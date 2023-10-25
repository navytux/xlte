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

from xlte.earfcn import frequency, dl2ul, ul2dl, _band_tab
from xlte import earfcn

from pytest import raises


# verify earfcn-related calculations wrt several points.
def test_earfcn():
    def _(band, dl_earfcn, ul_earfcn, fdl, ful, rf_mode, fdl_lo, fdl_hi_, ful_lo, ful_hi_):
        assert frequency(dl_earfcn) == fdl
        if ul_earfcn is not None:
            assert dl2ul(dl_earfcn) == ul_earfcn
        else:
            assert rf_mode in ('FDD', 'SDL')
            if rf_mode == 'FDD':
                estr = 'does not have enough uplink spectrum'
            if rf_mode == 'SDL':
                estr = 'does not have uplink spectrum'
            with raises(KeyError, match=estr):
                dl2ul(dl_earfcn)
        b, isdl = earfcn.band(dl_earfcn)
        assert isdl  == True
        if ul_earfcn is not None:
            assert frequency(ul_earfcn) == ful
            assert ul2dl(ul_earfcn) == dl_earfcn
            b_, isdl_ = earfcn.band(ul_earfcn)
            assert isdl_ == (rf_mode == 'TDD')
            assert b == b_
        assert b.band == band
        assert b.rf_mode == rf_mode
        assert b.fdl_lo  == fdl_lo
        assert b.fdl_hi_ == fdl_hi_
        assert b.ful_lo  == ful_lo
        assert b.ful_hi_ == ful_hi_

    # band   dl   ul     fdl     ful   rf_mode      fdl_lo  fdl_hi_ ful_lo  ful_hi_
    _( 1,   300, 18300,  2140,   1950,   'FDD',     2110,   2170,   1920,   1980)
    _(37, 37555, 37555,  1910.5, 1910.5, 'TDD',     1910,   1930,   1910,   1930)
    _(29,  9700,  None,   721,   None,   'SDL',      717,    728,   None,   None)
    _(66, 67135, 132671, 2179.9, 1779.9, 'FDD',     2110,   2200,   1710,   1780)
    _(66, 67136,  None,  2180,   None,   'FDD',     2110,   2200,   1710,   1780)   # NOTE B66 has different amount in dl and ul ranges


# verify that earfcn regions of all bands do not overlap.
def test_bands_no_earfcn_overlap():
    rv = [] # of (nlo, nhi)
    for b in _band_tab:
        assert b.ndl_lo is not None
        assert b.ndl_hi is not None
        rv.append((b.ndl_lo, b.ndl_hi))
        if b.rf_mode not in ('TDD', 'SDL'):
            assert b.nul_lo is not None
            assert b.nul_hi is not None
            rv.append((b.nul_lo, b.nul_hi))

    for i in range(len(rv)):
        ilo, ihi = rv[i]
        assert ilo < ihi
        for j in range(len(rv)):
            if j == i:
                continue
            jlo, jhi = rv[j]
            assert jlo < jhi

            if not ((ihi < jlo) or (jhi < ilo)):
                assert False, "(%r, %r) overlaps with (%r, %r)" % (ilo, ihi, jlo, jhi)
