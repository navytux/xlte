# -*- coding: utf-8 -*-
# Copyright (C) 2022  Nexedi SA and Contributors.
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
"""Package xlte.amari is top-level home for functionality related to Amarisoft LTE stack.

- connect and Conn allow to interoperate with a service via WebSocket.
"""

import websocket
import json
from golang import chan, select, nilchan, func, defer, panic
from golang import context, sync, time


# ConnError represents an error happened during Conn IO operation.
class ConnError(IOError):

    # str(ConnError) -> "operation: str(cause)"
    def __str__(e):
        s = super().__str__()
        if e.__cause__ is not None:
            s += ": " + str(e.__cause__)
        return s


# ConnClosedError indicates IO operation on a closed Conn.
class ConnClosedError(ConnError):
    pass


# connect connects to a service via WebSocket.
def connect(wsuri):  # -> Conn
    #websocket.enableTrace(True)     # TODO on $XLTE_AMARI_WS_DEBUG=y ?
    ws = websocket.WebSocket()
    ws.settimeout(5)  # reasonable default
    try:
        ws.connect(wsuri)
    except Exception as ex:
        raise ConnError("connect") from ex
    return Conn(ws)

# Conn represents WebSocket connection to a service.
#
# It provides functionality to issue requests, and (TODO) to receive notifications.
# Conn should be created via connect.
class Conn:
    # ._ws              websocket connection to service
    # ._srv_ready_msg   message we got for "ready"

    # ._mu              sync.Mutex
    # ._rxtab           {} msgid -> (request, rx channel)  | None
    # ._msgid_next      next message_id to send
    # ._down_err        None | why this connection was shutdown

    # ._rx_wg           sync.WorkGroup for spawned _serve_recv
    # ._down_once       sync.Once

    def __init__(conn, ws):
        try:
            msg0_raw = ws.recv()
            msg0 = json.loads(msg0_raw)
            # TODO also support 'authenticate'
            if msg0['message'] != 'ready':
                raise ValueError("unexpected welcome message: %s" % msg0)
        except Exception as ex:
            ws.close()
            raise ConnError("handshake") from ex

        conn._ws = ws
        conn._srv_ready_msg = msg0

        conn._mu         = sync.Mutex()
        conn._rxtab      = {}
        conn._msgid_next = 1
        conn._down_err   = None

        conn._down_once = sync.Once()

        conn._rx_wg = sync.WorkGroup(context.background())
        conn._rx_wg.go(conn._serve_recv)


    # close releases resources associated with conn and wakes up all blocked operations.
    def close(conn):
        conn._shutdown(ConnClosedError("connection is closed"))
        conn._rx_wg.wait()
        err = conn._down_err  # no need to lock after shutdown/_rx_wg.wait()
        if not isinstance(err, ConnClosedError):
            raise ConnError("close") from err

    # _shutdown brings the connection down due to err.
    # only the first call has effect.
    def _shutdown(conn, err):
        def _():
            with conn._mu:
                conn._down_err = err
                rxtab = conn._rxtab
                conn._rxtab = None              # disallow _send_msg
                for _, rxq in rxtab.values():
                    rxq.close()                 # wakeup blocked reqs
            conn._ws.abort()                    # wakeup _serve_recv
        conn._down_once.do(_)


    # _serve_recv runs in separate thread receiving messages from server and
    # delivering them as corresponding request responses and (TODO) events.
    def _serve_recv(conn, ctx):
        try:
            conn.__serve_recv(ctx)
        except Exception as ex:
            conn._shutdown(ex)
            # do not raise -> the error is propagated to ._down_err
        else:
            panic("__serve_recv returned without error")

    def __serve_recv(conn, ctx):
        while 1:
            try:
                rx_raw = conn._ws.recv()
            except websocket.WebSocketTimeoutException:
                # ignore global rx timeout. Because Conn is multiplexed .req()
                # handles "wait for response" timeout individually for each
                # request. We still want to enable global ._ws timeout so that
                # ._sendmsg is not blocked forever.
                continue

            if len(rx_raw) == 0:
                raise ConnError("connection closed by peer")
            rx = json.loads(rx_raw)

            if 'message_id' not in rx:
                # TODO support events
                raise NotImplementedError("TODO support events; received %s" % (rx,))

            msgid = rx.pop('message_id')

            with conn._mu:
                if conn._rxtab is None:
                    raise conn._down_err

                if msgid not in conn._rxtab:
                    raise ConnError("unexpected reply .%s %s" % (msgid, rx))

                request_message, rxq = conn._rxtab.pop(msgid)

            if rx['message'] != request_message:
                raise ConnError(".%s: reply for %s, requested %s" %
                                    (msgid, rx['message'], request_message))

            rxq.send((rx, rx_raw))


    # req sends request and waits for response.
    def req(conn, msg, args_dict):   # -> response
        rx, _ = conn.req_(msg, args_dict)
        return rx

    @func
    def req_(conn, msg, args_dict):  # -> response, raw_response
        rxq = conn._send_msg(msg, args_dict)

        # handle rx timeout ourselves. We cannot rely on global rx timeout
        # since e.g. other replies might be coming in again and again.
        δt = conn._ws.gettimeout()
        rxt = nilchan
        if δt is not None:
            _ = time.Timer(δt)
            defer(_.stop)
            rxt = _.c

        _, _rx = select(
            rxt.recv,       # 0
            rxq.recv_,      # 1
        )
        if _ == 0:
            raise websocket.WebSocketTimeoutException("timed out waiting for response")

        _, ok = _rx
        if not ok:
            # NOTE no need to lock - rxq is closed after ._down_err is set
            raise ConnError("recv") from conn._down_err

        rx, rx_raw = _
        return (rx, rx_raw)


    # _send_msg sends message to the service.
    def _send_msg(conn, msg, args_dict): # -> rxq
        assert isinstance(args_dict, dict)
        assert 'message'    not in args_dict
        assert 'message_id' not in args_dict
        rxq = chan(1)
        with conn._mu:
            if conn._rxtab is None:
                raise conn._down_err
            msgid = conn._msgid_next
            conn._msgid_next += 1
            assert msgid not in conn._rxtab
            conn._rxtab[msgid] = (msg, rxq)
        d = {'message': msg, 'message_id': msgid}
        d.update(args_dict)
        jmsg = json.dumps(d)
        try:
            conn._ws.send(jmsg)
        except Exception as ex:
            raise ConnError("send") from ex
        return rxq



    # srv_type, srv_name and srv_version return service type, name and version
    # retrieved from first "welcome" message from the server.

    @property
    def srv_type(conn):
        return conn._srv_ready_msg['type']

    @property
    def srv_name(conn):
        return conn._srv_ready_msg['name']

    @property
    def srv_version(conn):
        return conn._srv_ready_msg['version']
