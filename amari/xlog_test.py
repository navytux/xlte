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

from __future__ import print_function, division, absolute_import

from xlte.amari import xlog
from golang import func, defer, b
import io
import json

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


# verify that for xlog stream produced by enb < 2022-12-01 the Reader can build
# Messages timestamps by itself based on sync.
@func
def test_Reader_timestamp_from_sync_wo_utc():
    def jevent(time, event, args_dict={}):
        d = {
            "event":     event,
            "time" :     time,
        }
        d.update(args_dict)
        return json.dumps({"meta": d})

    def jsync(time, srv_time):
        d = {
            "state":     "attached"  if srv_time is not None  else "detached",
            "reason":    "periodic",
            "generator": "...",
        }
        if srv_time is not None:
            d['srv_time'] = srv_time
        return jevent(time, "sync", d)

    assert jsync(1.1, 2.2)  == '{"meta": {"event": "sync", "time": 1.1, "state": "attached", "reason": "periodic", "generator": "...", "srv_time": 2.2}}'
    assert jsync(1.1, None) == '{"meta": {"event": "sync", "time": 1.1, "state": "detached", "reason": "periodic", "generator": "..."}}'

    def jmsg(srv_time, msg):
        return json.dumps({"message": msg, "time": srv_time})
    assert jmsg(123.4, "aaa")  == '{"message": "aaa", "time": 123.4}'

    data = b""
    def _(line):
        nonlocal data
        assert '\n' not in line
        data += b(line+'\n')

    A = "service attach"
    D = "service detach"
    S = "sync"
    _( jmsg(1, "aaa")    )  # no timestamp: separated from ↓ jsync(1005) by event
    _( jevent(1002,   A ))
    _( jmsg(3, "bbb")    )  # have timestamp from ↓ jsync(1005)
    _( jmsg(4, "ccc")    )  # ----//----
    _( jsync(1005, 5)    )  # jsync with srv_time
    _( jmsg(6, "ddd")    )  # have timestamp from ↑ jsync(1005)
    _( jmsg(7, "eee")    )  # ----//----
    _( jevent(1008,   D ))
    _( jmsg(9, "fff")    )  # no timestamp: separated from ↑ jsync(1005) by event,
                            # and ↓ jsync(1010) has no srv_time
    _( jsync(1010, None) )  # jsync without srv_time
    _( jmsg(11, "ggg")   )  # no timestamp


    # expect_notime asserts that "no timestamp" error is raised on next read.
    def expect_notime(xr, lineno):
        with raises(xlog.ParseError,
                match=":%d/ no `utc` and cannot compute timestamp with sync" % lineno):
            _ = xr.read()

    # expect_msg asserts that specified message with specified timestamp reads next.
    def expect_msg(xr, timestamp, msg):
        _ = xr.read()
        assert type(_) is xlog.Message
        assert _.message   == msg
        assert _.timestamp == timestamp

    # expect_event asserts that specified event reads next.
    def expect_event(xr, timestamp, event):
        _ = xr.read()
        assert type(_) is (xlog.SyncEvent  if event == "sync"  else xlog.Event)
        assert _.event     == event
        assert _.timestamp == timestamp


    xr = xlog.Reader(io.BytesIO(data))
    br = xlog.Reader(io.BytesIO(data), reverse=True)
    defer(xr.close)
    defer(br.close)

    expect_notime(xr,    1       )  # aaa
    expect_event (xr, 1002,   A  )
    expect_msg   (xr, 1003, "bbb")
    expect_msg   (xr, 1004, "ccc")
    expect_event (xr, 1005,   S  )
    expect_msg   (xr, 1006, "ddd")
    expect_msg   (xr, 1007, "eee")
    expect_event (xr, 1008,   D  )
    expect_notime(xr,    9       )  # fff
    expect_event (xr, 1010,   S  )
    expect_notime(xr,   11       )  # ggg

    expect_notime(br,   -1       )  # ggg
    expect_event (br, 1010,   S  )
    expect_notime(br,   -3       )  # fff
    expect_event (br, 1008,   D  )
    expect_msg   (br, 1007, "eee")
    expect_msg   (br, 1006, "ddd")
    expect_event (br, 1005,   S  )
    expect_msg   (br, 1004, "ccc")
    expect_msg   (br, 1003, "bbb")
    expect_event (br, 1002,   A  )
    expect_notime(br,   -11      )  # aaa

    # extra check that we can get timestamp of first message if proper sync goes after
    _( jsync(1012, 12)  )
    _( jmsg(13, "hhh")  )
    _( jmsg(14, "iii")  )
    bb = xlog.Reader(io.BytesIO(data), reverse=True)
    defer(bb.close)
    expect_msg   (bb, 1014, "iii")
    expect_msg   (bb, 1013, "hhh")
    expect_event (bb, 1012,   S  )
    expect_msg   (bb, 1011, "ggg")  # now has timestamp because it is covered by ↑ sync(1012)
    expect_event (bb, 1010,   S  )  # after sync(1010) it goes as for br
    expect_notime(bb, -3-3       )  # fff
    expect_event (bb, 1008,   D  )
    expect_msg   (bb, 1007, "eee")
    expect_msg   (bb, 1006, "ddd")
    expect_event (bb, 1005,   S  )
    expect_msg   (bb, 1004, "ccc")
    expect_msg   (bb, 1003, "bbb")
    expect_event (bb, 1002,   A  )
    expect_notime(bb, -3-11      )  # aaa


def test_LogSpec():
    logspec = 'stats[samples,rf,abc=123,def="hello world"]/60s'
    spec = xlog.LogSpec.parse(logspec)

    assert spec.query == "stats"
    assert spec.opts == {"samples": True, "rf": True, "abc": 123, "def": "hello world"}
    assert spec.period == 60.0
    assert str(spec) == logspec


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
