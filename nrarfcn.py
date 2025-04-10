# Copyright (C) 2023-2025  Nexedi SA and Contributors.
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
"""Package xlte.nrarfcn helps to do computations with NR bands, frequencies and NR-ARFCN numbers.

It complements pypi.org/project/nrarfcn and provides the following additional utilities:

- frequency converts NR-ARFCN to frequency.
- nrarfcn converts frequency to NR-ARFCN.
- dl2ul and ul2dl convert between DL NR-ARFCN and UL NR-ARFCN corresponding to
  each other in particular band.
- dl2ssb returns SSB NR-ARFCN that is located nearby DL NR-ARFCN on Global Synchronization Raster.

See also package xlte.earfcn which provides similar functionality for 4G.
"""

# import pypi.org/project/nrarfcn with avoiding name collision with xlte.nrarfcn even if xlte is installed in editable mode.
def _():
    modname = 'nrarfcn'
    import sys, importlib.util
    import xlte

    # if already imported - we are done.
    # but if previously ran `import nrarfcn` resolved to xlte/nrarfcn.py due to
    # the way how easy_install handles xlte editable install with adding xlte
    # onto sys.path, undo that.
    mod = sys.modules.get(modname)
    if mod is not None:
        if mod.__spec__.origin == __spec__.origin:
            del sys.modules[modname]
            mod = None
    if mod is not None:
        return mod

    # import nrarfcn with ignoring xlte.nrarfcn spec
    # based on https://docs.python.org/3/library/importlib.html#approximating-importlib-import-module
    # we also ignore cwd/xlte, if automatically injected to sys.path[0] by python and pytest, so that running things in xlte/ also work
    pathsave = {}    # idx -> sys.path[idx]
    for p in [''] + xlte.__path__:
        try:
            i = sys.path.index(p)
        except ValueError:
            pass
        else:
            pathsave[i] = p
    for i in sorted(pathsave, reverse=True):
        sys.path.pop(i)
    try:
        for finder in sys.meta_path:
            spec = finder.find_spec(modname, None)
            if spec is not None  and  spec.origin != __spec__.origin:
                break
        else:
            raise ModuleNotFoundError('Module %r not found' % modname)
    finally:
        for i in sorted(pathsave):
            sys.path.insert(i, pathsave[i])
    mod = importlib.util.module_from_spec(spec)
    assert modname not in sys.modules
    sys.modules[modname] = mod
    spec.loader.exec_module(mod)
    return mod
nr = _()


# dl2ul returns UL NR-ARFCN that corresponds to DL NR-ARFCN and band.
def dl2ul(dl_nr_arfcn, band): # -> ul_nr_arfcn
    dl_lo, dl_hi = nr.get_nrarfcn_range(band, 'dl')
    if dl_lo == 'N/A':
        raise ValueError('band%r does not have downlink spectrum' % band)
    if not (dl_lo <= dl_nr_arfcn <= dl_hi):
        raise ValueError('band%r: NR-ARFCN=%r is outside of downlink spectrum' % (band, dl_nr_arfcn))
    ul_lo, ul_hi = nr.get_nrarfcn_range(band, 'ul')
    if ul_lo == 'N/A':
        raise KeyError('band%r, to which DL NR-ARFCN=%r belongs, does not have uplink spectrum' % (band, dl_nr_arfcn))
    if dl_nr_arfcn - dl_lo > ul_hi - ul_lo:
        raise KeyError('band%r does not have enough uplink spectrum to provide pair for NR-ARFCN=%r' % (band, dl_nr_arfcn))
    ul_nr_arfcn = ul_lo + (dl_nr_arfcn - dl_lo)
    assert ul_lo <= ul_nr_arfcn <= ul_hi
    return ul_nr_arfcn

# ul2dl returns DL NR-ARFCN that corresponds to UL NR-ARFCN and band.
def ul2dl(ul_nr_arfcn, band): # -> dl_nr_arfcn
    ul_lo, ul_hi = nr.get_nrarfcn_range(band, 'ul')
    if ul_lo == 'N/A':
        raise ValueError('band%r does not have uplink spectrum' % band)
    if not (ul_lo <= ul_nr_arfcn <= ul_hi):
        raise ValueError('band%r: NR-ARFCN=%r is outside of uplink spectrum' % (band, ul_nr_arfcn))
    dl_lo, dl_hi = nr.get_nrarfcn_range(band, 'dl')
    if dl_lo == 'N/A':
        raise KeyError('band%r, to which UL NR-ARFCN=%r belongs, does not have downlink spectrum' % (band, ul_nr_arfcn))
    if ul_nr_arfcn - ul_lo > dl_hi - dl_lo:
        raise KeyError('band%r does not have enough downlink spectrum to provide pair for NR-ARFCN=%r' % (band, ul_nr_arfcn))
    dl_nr_arfcn = dl_lo + (ul_nr_arfcn - ul_lo)
    assert dl_lo <= dl_nr_arfcn <= dl_hi
    return dl_nr_arfcn


# dl2ssb returns SSB NR-ARFCN that is located nearby DL NR-ARFCN on Global Synchronization Raster.
#
# input Fdl should be aligned with ΔFraster.
# for return (Fdl - Fssb) is aligned with some SSB SubCarrier Spacing of given band.
# max_ssb_scs_khz indicates max SSB SubCarrier Spacing for which it was possible to find Fssb constrained with above alignment requirement.
#
# KeyError   is raised if Fssb is not possible to find for given Fdl and band.
# ValueError is raised if input parameters are incorrect.
def dl2ssb(dl_nr_arfcn, band): # -> ssb_nr_arfcn, max_ssb_scs_khz
    _trace('\ndl2ssb %r %r' % (dl_nr_arfcn, band))
    dl_lo, dl_hi = nr.get_nrarfcn_range(band, 'dl')
    if dl_lo == 'N/A':
        raise ValueError('band%r does not have downlink spectrum' % band)
    if not (dl_lo <= dl_nr_arfcn <= dl_hi):
        raise ValueError('band%r: NR-ARFCN=%r is outside of downlink spectrum' % (band, dl_nr_arfcn))

    f = frequency(nrarfcn=dl_nr_arfcn)
    _trace('f   %.16g' % f)

    # query all SS raster entries available in this band
    if isinstance(band, int):
        band = 'n%d' % band
    tab_fr1 = nr.tables.applicable_ss_raster_fr1.table_applicable_ss_raster_fr1()
    tab_fr2 = nr.tables.applicable_ss_raster_fr2.table_applicable_ss_raster_fr2()
    try_v = []  # of _SSRasterTabEntry
    for tab in (tab_fr1, tab_fr2):
        for row in tab.data:
            if tab.get_cell(row, 'band') == band:
                try_v.append( _SSRasterTabEntry(tab, row) )

    # for each scs↓ try to find suitable sync point
    for t in sorted(try_v, key=lambda _: _.scs, reverse=True):
        _trace('trying %s' % t)
        scs = t.scs / 1000  # khz -> mhz

        # locate nearby point on global sync raster and further search around it
        # until sync point aligns to be multiple of scs
        gscn = nr.get_gscn_by_frequency(f)
        while 1:
            f_sync = nr.get_frequency_by_gscn(gscn)
            f_sync_arfcn = nr.get_nrarfcn(f_sync)
            if not (dl_lo <= f_sync_arfcn <= dl_hi):
                break
            # check `(f_sync - f) % scs == 0` with tolerating fp rounding
            δf = f_sync - f
            q, r = divmod(δf, scs)
            r_scs = r / scs
            # if f_sync is good we can yield it only if gscn ∈ allowed set
            if t.has_gscn(gscn):
                _trace('gscn %d\tf_sync %.16g (%d)  δf %+.3f  //scs %d  %%scs %.16g·scs' % (gscn, f_sync, nr.get_nrarfcn(f_sync), δf, q, r_scs))
                if abs(r_scs - round(r_scs)) < 1e-5:
                    _trace('-> %d %d' % (f_sync_arfcn, t.scs))
                    return f_sync_arfcn, t.scs
            gscn += (+1 if δf >= 0  else  -1)

    raise KeyError('dl2ssb %r %s: cannot find SSB frequency that is both on GSR and aligns from dl modulo SSB SCS of the given band' % (dl_nr_arfcn, band))

# _SSRasterTabEntry serves dl2ssb by representing one entry from FR1 or FR2 SS raster entries table.
class _SSRasterTabEntry:
    __slots__ = ('band', 'scs', 'block_pattern', 'gscn_first', 'step_size', 'gscn_last', 'note')

    def __init__(tabe, tab, row):
        for key in tabe.__slots__:
            setattr(tabe, key, tab.get_cell(row, key))
        assert type(tabe.note) is set  or  tabe.note == {},  repr(tabe)
        if len(tabe.note) > 0:
            assert tabe.gscn_first == tabe.gscn_last == tabe.step_size == 0,  repr(tabe)
        else:
            assert tabe.gscn_first != 0,  repr(tabe)
            assert tabe.gscn_last  != 0,  repr(tabe)
            assert tabe.step_size  != 0,  repr(tabe)

    # has_gscn returns whether table entry covers given gscn.
    def has_gscn(tabe, gscn):
        if len(tabe.note) > 0:
            ok = tabe.note
        else:
            ok = range(tabe.gscn_first, tabe.gscn_last+1, tabe.step_size)
        return (gscn in ok)

    def __str__(tabe):
        s = '%s %s·scs  %s  ' % (tabe.band, tabe.scs, tabe.block_pattern)
        if len(tabe.note) > 0:
            s += '%r' % (tabe.note,)
        else:
            s += '%d-<%d>-%d' % (tabe.gscn_first, tabe.step_size, tabe.gscn_last)
        return s

    def __repr__(tabe):
        return '_SSRasterTabEntry('  + \
            ', '.join('%s=%r' % (k, getattr(tabe, k))  for k in tabe.__slots__)  + \
            ')'


# frequency returns frequency corresponding to DL or UL NR-ARFCN.
def frequency(nrarfcn): # -> freq (MHz)
    return nr.get_frequency(nrarfcn)


# nrarfcn returns NR-ARFCN that corresponds to frequency freq in MHz.
#
# By default the convertion is precise, but if nearby=True closest NR-ARFCN is returned.
def nrarfcn(freq, nearby=False):  # -> NR-ARFCN | ValueError
    n = nr.get_nrarfcn(freq)  # get_nrarfcn returns nearby nrarfcn
    if not nearby:
        if frequency(n) != freq:
            raise ValueError('frequency=%r MHz is not on NR-ARFCN raster' % freq)
    return n


_debug = False
def _trace(*argv):
    if _debug:
        print(*argv)
