# XLTE pythonic package setup
# Copyright (C) 2014 - 2022  Nexedi SA and Contributors.
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

from setuptools import setup, find_packages


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

    classifiers = [_.strip() for _ in """\
        Development Status :: 2 - Pre-Alpha
        Intended Audience :: Developers
        Intended Audience :: Telecommunications Industry
        Operating System :: POSIX :: Linux\
    """.splitlines()]
)
