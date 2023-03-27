# -*- coding: utf-8 -*-
# xamari - help topics
# Copyright (C) 2022-2023  Nexedi SA and Contributors.
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

from __future__ import print_function, division, absolute_import

from collections import OrderedDict

# topic_name -> (topic_summary, topic_help)
topic_dict = OrderedDict()

help_websock = """\
Every Amarisoft service supports so-called Remote API available via WebSocket
protocol. The address, where such Remote API is served, is specified via
com_addr in service configuration.

Xamari commands, that need to interoperate with a service, take WebSocket
URI of the service as their argument. Such URI has the following form:

    ws://<host>:<port>

for example

    ws://[2a11:9ac0:d::1]:9002
"""

help_jsonlog = """\
Some commands produce logs with JSON entries. Such logs are organized with JSON
Lines format(*) with each entry occupying one line.

Logs in JSON Lines format are handy to exchange in between programs, and with
corresponding tools, e.g. jq(+), they can be also displayed in human-readable
form and inspected quickly.

(*) https://jsonlines.org/
(+) https://stedolan.github.io/jq/
"""


topic_dict['websock']   = "specifying WebSocket URI of a service",      help_websock
topic_dict['jsonlog']   = "log with JSON entries",                      help_jsonlog
