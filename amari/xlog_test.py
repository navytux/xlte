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

    f = io.BytesIO(data); f.name = "enb.xlog"
    xr = xlog.Reader(f)
    defer(xr.close)

    # :1
    _ = _1 = xr.read()
    assert type(_) is xlog.SyncEvent
    assert _.pos       == ("enb.xlog", 1)
    assert _.event     == "start"
    assert _.timestamp == 0.01
    assert _ == {"meta": {"event":      "start",
                          "time":       0.01,
                          "generator":  "xlog ws://localhost:9001 ue_get[]/3.0s erab_get[]/3.0s"}}

    # :2
    _ = _2 = xr.read()
    assert type(_) is xlog.Event
    assert _.pos       == ("enb.xlog", 2)
    assert _.event     == "service attach"
    assert _.timestamp == 0.02
    assert _ == {"meta": {"event":       "service attach",
                          "time":        0.02,
                          "srv_name":    "ENB",
                          "srv_type":    "ENB",
                          "srv_version": "2022-12-01"}}
    # :3
    _ = _3 = xr.read()
    assert type(_) is xlog.Message
    assert _.pos       == ("enb.xlog", 3)
    assert _.message   == "ue_get"
    assert _.timestamp == 9613.347
    assert _ == {"message":     "ue_get",
                 "ue_list":     [],
                 "message_id":  2,
                 "time":        123.4,
                 "utc":         9613.347}

    # :4  (bad input)
    with raises(xlog.ParseError, match="enb.xlog:4 : invalid json"):
        _ = xr.read()

    # :5  (restore after bad input)
    _ = _5 = xr.read()
    assert type(_) is xlog.Message
    assert _.pos       == ("enb.xlog", 5)
    assert _.message   == "hello"
    assert _.timestamp == 10000
    assert _ == {"message":     "hello",
                 "message_id":  3,
                 "utc":         10000}

    # EOF
    _ = xr.read()
    assert _ is None


    # ---- reverse ----
    f = io.BytesIO(data); f.name = "bbb.xlog"
    br = xlog.Reader(f, reverse=True)

    # :-1  (:5)
    _ = br.read()
    assert type(_) is xlog.Message
    assert _.pos  == ("bbb.xlog", -1)
    assert _      == _5

    # :-2  (:4)  (bad input)
    with raises(xlog.ParseError, match="bbb.xlog:-2 : invalid json"):
        _ = br.read()

    # :-3  (:3)  (restore after bad input)
    _ = br.read()
    assert type(_) is xlog.Message
    assert _.pos  == ("bbb.xlog", -3)
    assert _      == _3

    # :-4  (:2)
    _ = br.read()
    assert type(_) is xlog.Event
    assert _.pos  == ("bbb.xlog", -4)
    assert _      == _2

    # :-5  (:1)
    _ = br.read()
    assert type(_) is xlog.SyncEvent
    assert _.pos  == ("bbb.xlog", -5)
    assert _      == _1

    # EOF
    _ = br.read()
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


def test_ReverseLineReader():
    linev = [
        'hello world',
        'привет мир',
        'zzz',
        'αβγδ',             # 2-bytes UTF-8 characters
        '你好'              # 3-bytes ----//----
        '𩸽𩹨',             # 4-bytes ----//----
        '{"message":"hello"}',
    ]

    tdata = '\n'.join(linev) + '\n'     # text
    bdata = tdata.encode('utf-8')       # binary

    # check verifies _ReverseLineReader on tdata and bdata with particular bufsize.
    @func
    def check(bufsize):
        trr = xlog._ReverseLineReader(io.StringIO(tdata), bufsize)
        brr = xlog._ReverseLineReader(io.BytesIO (bdata), bufsize)
        defer(trr.close)
        defer(brr.close)

        tv = []
        while 1:
            tl = trr.readline()
            if tl == '':
                break
            assert tl.endswith('\n')
            tl = tl[:-1]
            assert '\n' not in tl
            tv.append(tl)

        bv = []
        while 1:
            bl = brr.readline()
            if bl == b'':
                break
            assert bl.endswith(b'\n')
            bl = bl[:-1]
            assert b'\n' not in bl
            bv.append(bl.decode('utf-8'))

        venil = list(reversed(linev))
        assert tv == venil
        assert bv == venil

    # verify all buffer sizes from 1 to 10x bigger the data.
    # this way we cover all tricky cases where e.g. an UTF8 character is split
    # in its middle by a buffer.
    for bufsize in range(1, 10*len(bdata)):
        check(bufsize)
