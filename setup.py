# XLTE pythonic package setup
# Copyright (C) 2014 - 2023  Nexedi SA and Contributors.
#                            Kirill Smelkov <kirr@nexedi.com>
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

from __future__ import print_function, division, absolute_import

from setuptools import setup, find_packages
from setuptools.command.build_py import build_py as _build_py


# build_py that
# - prevents in-tree xlte.py & setup.py to be installed
# - synthesizes xlte/__init__.py on install
#
# based on wendelin.core/setup.py:build.py
class build_py(_build_py):

    def find_package_modules(self, package, package_dir):
        modules = _build_py.find_package_modules(self, package, package_dir)
        try:
            modules.remove(('xlte', 'xlte',  'xlte.py'))
            modules.remove(('xlte', 'setup', 'setup.py'))
        except ValueError:
            pass    # was not there

        return modules

    def build_packages(self):
        _build_py.build_packages(self)
        # emit std namespacing mantra to xlte/__init__.py
        self.initfile = self.get_module_outfile(self.build_lib, ('xlte',), '__init__')
        with open(self.initfile, 'w') as f:
            f.write("# this is a namespace package (autogenerated)\n")
            f.write("__import__('pkg_resources').declare_namespace(__name__)\n")

    def get_outputs(self, include_bytecode=1):
        outputs = _build_py.get_outputs(self, include_bytecode)

        # add synthesized __init__.py to outputs, so that `pip uninstall`
        # works without leaving it
        outputs.append(self.initfile)
        if include_bytecode:
            if self.compile:
                outputs.append(self.initfile + 'c')
            if self.optimize:
                outputs.append(self.initfile + 'o')

        return outputs


# read file content
def readfile(path):
    with open(path, 'r') as f:
        return f.read()


setup(
    name        = 'xlte',
    version     = '0.0.0.dev1',
    description = 'Assorted LTE tools',
    long_description = '%s\n----\n\n%s' % (
                            readfile('README.rst'), readfile('CHANGELOG.rst')),
    url         = 'https://lab.nexedi.com/kirr/xlte',
    license     = 'GPLv3+ with wide exception for Open-Source',
    author      = 'Kirill Smelkov',
    author_email= 'kirr@nexedi.com',

    keywords    = 'lte amarisoft 4G',

    package_dir = {'xlte': ''},
    packages    = ['xlte'] + ['xlte.%s' % _ for _ in
                        find_packages()],
    install_requires = [
                   'websocket-client',
                   'pygolang',
                   'numpy',
                   'nrarfcn',
                  ],

    extras_require = {
                   'test': ['pytest'],
    },

    cmdclass    = {'build_py':      build_py,
                  },

    entry_points= {'console_scripts': [
                        'xamari     = xlte.amari.xamari:main',
                      ]
                  },

    classifiers = [_.strip() for _ in """\
        Development Status :: 2 - Alpha
        Programming Language :: Python
        Programming Language :: Python :: 3
        Programming Language :: Python :: 3.9
        Programming Language :: Python :: 3.10
        Programming Language :: Python :: 3.11
        Intended Audience :: Developers
        Intended Audience :: Telecommunications Industry
        Operating System :: POSIX :: Linux\
    """.splitlines()]
)
