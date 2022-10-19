# XLTE | Top-level in-tree python import redirector
# Based on https://lab.nexedi.com/nexedi/wendelin.core/blob/master/wendelin.py
# Copyright (C) 2014-2022  Nexedi SA and Contributors.
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

# tell python xlte.* modules hierarchy starts at top-level
#
# This allows e.g.          `import xlte.amari`
# to resolve to importing   `amari/__init__.py`
#
# and thus avoid putting everything in additional top-level xlte/
# directory in source tree.
#
# see https://www.python.org/doc/essays/packages/ about __path__


# first make sure setuptools will recognize xlte.py as package,
# but do not setup proper __path__ yet.
# ( _handle_ns() checks for __path__ attribute presence and refuses to further
#   process "not a package"
#
#   https://github.com/pypa/setuptools/blob/9803058d/pkg_resources/__init__.py#L2012 )
__path__ = []

# tell setuptools/pkg_resources 'xlte' is a namespace package
# ( so that xlte installed in development mode does not brake
#   'xlte' namespacing wrt other xlte software )
__import__('pkg_resources').declare_namespace(__name__)

# pkg_resources will append '.../xlte/xlte' to __path__ which is
# not right for in-tree setup and thus needs to be corrected:
# Rewrite '.../xlte/xlte' -> '.../xlte'
from os.path import dirname, realpath, splitext
myfile = realpath(__file__)
mymod  = splitext(myfile)[0]    # .../xlte.py   -> .../xlte
mydir  = dirname(myfile)        # .../xlte      -> ...
i = None    # in case vvv loop is empty, so we still can `del i` in the end
for i in range(len(__path__)):
    # NOTE realpath(...) for earlier setuptools, where __path__ entry could be
    # added as relative
    if realpath(__path__[i]) == mymod:
        __path__[i] = mydir
del dirname, realpath, splitext, myfile, mymod, mydir, i


# in the end we have:
# __path__ has >= 1 items
# __path__ entry for xlte points to top of working tree
# __name__ registered as namespace package
#
# so the following should work:
#   - importing from in-tree files
#   - importing from other children of xlte packages
