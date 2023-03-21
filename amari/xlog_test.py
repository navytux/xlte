# -*- coding: utf-8 -*-
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

from xlte.amari import xlog
from golang import func, defer, b
import io

from pytest import raises


@func
def test_Reader():
    data = b"""\
{"meta": {"event": "start", "time": 0.01, "generator": "xlog ws://localhost:9001 ue_get[]/3.0s erab_get[]/3.0s"}}
{"meta": {"event": "service attach", "time": 0.02, "srv_name": "ENB", "srv_type": "ENB", "srv_version": "2022-12-01"}}
{"message":"ue_get","ue_list":[],"message_id":2,"time":123.4,"utc":9613.347}
zzzqqqrrrr
{"message":"hello","message_id":3,"utc":10000}
"""

    xr = xlog.Reader(io.BytesIO(data))
    defer(xr.close)

    # :1
    _ = xr.read()
    assert type(_) is xlog.SyncEvent
    assert _.event     == "start"
    assert _.timestamp == 0.01
    assert _ == {"meta": {"event":      "start",
                          "time":       0.01,
                          "generator":  "xlog ws://localhost:9001 ue_get[]/3.0s erab_get[]/3.0s"}}

    # :2
    _ = xr.read()
    assert type(_) is xlog.Event
    assert _.event     == "service attach"
    assert _.timestamp == 0.02
    assert _ == {"meta": {"event":       "service attach",
                          "time":        0.02,
                          "srv_name":    "ENB",
                          "srv_type":    "ENB",
                          "srv_version": "2022-12-01"}}
    # :3
    _ = xr.read()
    assert type(_) is xlog.Message
    assert _.message   == "ue_get"
    assert _.timestamp == 9613.347
    assert _ == {"message":     "ue_get",
                 "ue_list":     [],
                 "message_id":  2,
                 "time":        123.4,
                 "utc":         9613.347}

    # :4  (bad input)
    with raises(xlog.ParseError, match=":4 : invalid json"):
        _ = xr.read()

    # :5  (restore after bad input)
    _ = xr.read()
    assert type(_) is xlog.Message
    assert _.message   == "hello"
    assert _.timestamp == 10000
    assert _ == {"message":     "hello",
                 "message_id":  3,
                 "utc":         10000}

    # EOF
    _ = xr.read()
    assert _ is None


# verify that EOF is not returned prematurely due to readahead pre-hitting it
# sooner on the live stream.
@func
def test_Reader_readahead_vs_eof():
    fxlog = io.BytesIO(b'')
    def logit(line):
        line = b(line)
        assert b'\n' not in line
        pos = fxlog.tell()
        fxlog.seek(0, io.SEEK_END)
        fxlog.write(b'%s\n' % line)
        fxlog.seek(pos, io.SEEK_SET)

    xr = xlog.Reader(fxlog)
    def expect_msg(τ, msg):
        _ = xr.read()
        assert type(_) is xlog.Message
        assert _.timestamp == τ
        assert _.message   == msg

    logit('{"message": "aaa", "utc": 1}')
    logit('{"message": "bbb", "utc": 2}')
    expect_msg(1, "aaa")
    expect_msg(2, "bbb")

    # ^^^ readahead hit EOF internally, but at the time next .read() is called,
    # the stream has more data
    logit('{"message": "ccc", "utc": 3}')
    expect_msg(3, "ccc")

    # now, when read is called, the stream has no more data
    # -> EOF is reported to the caller
    _ = xr.read()
    assert _ is None

    # now the stream has more data again
    logit('{"message": "ddd", "utc": 4}')
    logit('{"message": "eee", "utc": 5}')
    expect_msg(4, "ddd")
    expect_msg(5, "eee")
    _ = xr.read()
    assert _ is None


def test_LogSpec():
    logspec = "stats[samples,rf]/60s"
    spec = xlog.LogSpec.parse(logspec)

    assert spec.query == "stats"
    assert spec.optv == ["samples", "rf"]
    assert spec.period == 60.0
