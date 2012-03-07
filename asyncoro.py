#!/usr/bin/env python

# asyncoro: Sockets with asynchronous I/O and coroutines;
# see accompanying 'asyncoro.html' for more details.

# Copyright (C) 2012 Giridhar Pemmasani (pgiri@yahoo.com)

# This file is part of dispy.

# dispy is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# dispy is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.

# You should have received a copy of the GNU Lesser General Public License
# along with dispy.  If not, see <http://www.gnu.org/licenses/>.

import time
import threading
import functools
import socket
import inspect
import traceback
import select
import sys
import types
import struct
import logging
import errno
import platform
import random
import ssl
from heapq import heappush, heappop
from bisect import bisect_left

if platform.system() == 'Windows':
    from errno import WSAEINPROGRESS as EINPROGRESS
    from errno import WSAEWOULDBLOCK as EWOULDBLOCK
    from time import clock as _time
    _time()
else:
    from errno import EINPROGRESS
    from errno import EWOULDBLOCK
    from time import time as _time

"""
AsynCoro and associated classes in this file provide framework
for developing programs with coroutines and asynchronous I/O for
sockets. The programs developed with asyncoro will have same logic as
python progorams with synchronous sockets and threads, except for
converting sockets to asynchronous model with AsynCoroSocket class,
'yield' when waiting for completion of tasks (i.e., socket operations,
sleep and waiting on CoroCondition) and using CoroLock, CoroCondition
in place of thread locking. Tehcnically, CoroLock is not needed (as
there is no forced preemption with coroutines and at most one
coroutine is executing at anytime), but if one wants to maintain same
program for both synchronous and asynchronous models, it should be
simple to interchange threading.Lock and CoroLock, threading.Condition
and CoroCondition, and a few syntactic changes.

For example, a simple tcp server looks like:

def process(sock, coro=None):
    sock = AsynCoroSocket(sock)
    # get (exactly) 4MB of data, for example, and let other coroutines
    # (process methods, in this case) execute in the meantime
    data = yield sock.read(4096*1024)
    ...
    yield sock.write(reply)
    sock.close()

if __name__ == '__main__':
    host, port = '', 3456
    # start asyncoro scheduler before creating coroutines
    asyncoro = AsynCoro()
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind((host, port))
    sock.listen(128)
    while True:
        conn, addr = sock.accept()
        Coro(process, conn)

Here we mixed synchronous sockets in server loop with asynchronous
sockets in the 'process' method for illustration. The server does not
do any processing, so the loop can quickly accept connections. Each
request is processed in a separate coroutine. A coroutine method must
have 'coro=None' default argument. The coroutine builder Coro will set
coro argument with the Coro instance, which is used for calling
methods in Coro class.

With 'yield', 'suspend' and 'resume' methods, coroutines can cooperate
scheduling their execution, send/receive values to/from each other,
etc.

See dispy files in this package for details on how to use asyncoro.
"""

class MetaSingleton(type):
    __instance = None
    def __call__(cls, *args, **kwargs):
        if cls.__instance is None:
            cls.__instance = super(MetaSingleton, cls).__call__(*args, **kwargs)
        return cls.__instance

class _AsynCoroSocket(socket.socket):
    """Base class socket for use with AsynCoro, for asynchronous I/O
    completion and coroutines. This class is for internal use
    only. Use AsnyCoroSocket, defined below, instead.
    """

    _default_timeout = None

    def __init__(self, sock, blocking=False, keyfile=None, certfile=None,
                 ssl_version=ssl.PROTOCOL_SSLv23):
        """Setup socket for use wih asyncoro.

        blocking=True implies synchronous sockets and blocking=False
        implies asynchronous sockets.

        keyfile, certfile and ssl_version are as per ssl's wrap_socket method.

        Only methods should be used; other attributes are for internal use only.
        """

        if isinstance(sock, AsynCoroSocket):
            logging.warning('Socket %s is already AsynCoroSocket', sock._fileno)
            self.__dict__ = sock.__dict__
        else:
            socket.socket.__init__(self, _sock=sock._sock)
            self._blocking = blocking == True
            self._rsock = sock
            self._keyfile = keyfile
            self._certfile = certfile
            self._ssl_version = ssl_version
            self._result = None
            self._fileno = sock.fileno()
            self._timeout = 0
            self._timeout_id = None
            self._coro = None
            self._task = None
            self._asyncoro = None
            self._notifier = None

            self.read = None
            self.write = None
            self.read_msg = None
            self.write_msg = None

            self.setblocking(self._blocking)
            # technically, we should set socket to blocking if
            # _default_timeout is None, but ignore this case
            if _AsynCoroSocket._default_timeout:
                self.settimeout(_AsynCoroSocket._default_timeout)

    def setblocking(self, blocking):
        self._blocking = blocking
        if blocking:
            if self._certfile:
                raise Exception('SSL is not supported for blocking sockets')
            self._rsock.setblocking(1)
            if self._rsock.type == socket.SOCK_STREAM:
                self.read = self.sync_read
                self.write = self.sync_write
                self.read_msg = self.sync_read_msg
                self.write_msg = self.sync_write_msg
            self._asyncoro = None
            self._unregister()
        else:
            self._rsock.setblocking(0)

            self.recv = self.async_recv
            self.send = self.async_send
            self.recvfrom = self.async_recvfrom
            self.sendto = self.async_sendto
            self.accept = self.async_accept
            self.connect = self.async_connect
            if self._rsock.type == socket.SOCK_STREAM:
                self.read = self.async_read
                self.write = self.async_write
                self.read_msg = self.async_read_msg
                self.write_msg = self.async_write_msg

            self._asyncoro = AsynCoro.instance()
            # for timeouts we need _notifier even for IOCP sockets
            self._notifier = _AsyncNotifier.instance()
            self._register()

    def _register(self):
        self._notifier.register(self)

    def _unregister(self):
        if self._notifier:
            if self._timeout_id:
                self._notifier._del_timeout(self)
            self._notifier.unregister(self)
            self._notifier = None

    def close(self):
        """'close' must be called when done with socket.
        """
        self._unregister()
        if self._rsock:
            self._rsock.close()
            self._rsock = None
        self._asyncoro = None
        self._coro = None

    # def shutdown(self, flags):
    #     if flags == socket.SHUT_RDWR:
    #         self._notifier.unregister(self)
    #         self._notifier = None
    #     self._rsock.shutdown(flags)

    def unwrap(self):
        """Get rid of AsynCoroSocket setup and return underlying socket object.
        """
        self._unregister()
        self._asyncoro = None
        self._notifier = None
        self._coro = None
        sock = self._rsock
        self._rsock = None
        return sock

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, trace):
        self.close()

    def setdefaulttimeout(self, timeout):
        if isinstance(timeout, (int, float)) and timeout > 0:
            self._rsock.setdefaulttimeout(timeout)
            self._default_timeout = timeout
        else:
            logging.warning('invalid timeout %s ignored', timeout)

    def getdefaulttimeout(self):
        if self._blocking:
            return self._rsock.getdefalttimeout()
        else:
            return _AsynCoroSocket._default_timeout

    def settimeout(self, timeout):
        if self._blocking:
            if timeout is None:
                pass
            elif not timeout:
                self.setblocking(0)
                self.settimeout(0.0)
            else:
                self._rsock.settimeout(timeout)
        else:
            if timeout is None:
                self.setblocking(1)
            elif isinstance(timeout, (int, float)) and timeout >= 0:
                self._timeout = timeout
                # self._notifier._del_timeout(self)
            else:
                logging.warning('invalid timeout %s ignored' % timeout)

    def gettimeout(self):
        if self._blocking:
            return self._rsock.gettimeout()
        else:
            return self._timeout

    def _timed_out(self):
        """Internal use only.
        """
        # don't clear _coro or _task; the task may complete before
        # this exception is thrown to coro
        if self._coro is not None:
            self._coro.throw(socket.timeout, socket.timeout('timed out'))

    def async_recv(self, bufsize, *args):
        """Asynchronous version of socket recv method.
        """
        def _recv(self, bufsize, *args):
            try:
                buf = self._rsock.recv(bufsize, *args)
            except ssl.SSLError, err:
                if err.args[0] != ssl.SSL_ERROR_WANT_READ:
                    raise socket.error(err)
            except:
                self._notifier.modify(self, 0)
                self._task = self._result = None
                coro, self._coro = self._coro, None
                coro.throw(*sys.exc_info())
            else:
                self._notifier.modify(self, 0)
                self._task = self._result = None
                coro, self._coro = self._coro, None
                coro.resume(buf)

        if self._certfile:
            # in case of SSL, attempt read first
            try:
                buf = self._rsock.recv(bufsize)
            except ssl.SSLError, err:
                if err.args[0] != ssl.SSL_ERROR_WANT_READ:
                    raise socket.error(err)
                elif buf:
                    return buf

        self._task = functools.partial(_recv, self, bufsize, *args)
        self._coro = self._asyncoro.cur_coro()
        self._coro.suspend()
        self._notifier.modify(self, _AsyncNotifier._Readable)

    def async_read(self, bufsize, *args):
        """Read exactly bufsize bytes.
        """
        def _read(self, pending, *args):
            try:
                buf = self._rsock.recv(pending, *args)
            except ssl.SSLError, err:
                if err.args[0] != ssl.SSL_ERROR_WANT_READ:
                    raise socket.error(err)
            except:
                self._notifier.modify(self, 0)
                self._task = self._result = None
                coro, self._coro = self._coro, None
                coro.throw(*sys.exc_info())
            else:
                if buf:
                    self._result.append(buf)
                    pending -= len(buf)
                    if pending == 0:
                        self._notifier.modify(self, 0)
                        buf = ''.join(self._result)
                        self._task = self._result = None
                        coro, self._coro = self._coro, None
                        coro.resume(buf)
                    else:
                        self._task = functools.partial(_read, self, pending, *args)
                else:
                    # TODO: check for error and delete?
                    self._notifier.modify(self, 0)
                    self._task = self._result = None
                    coro, self._coro = self._coro, None
                    coro.throw(socket.error, socket.error('read error'))

        self._result = []
        if self._certfile:
            # in case of SSL, attempt read first
            try:
                buf = self._rsock.read(bufsize)
            except ssl.SSLError, err:
                if err.args[0] != ssl.SSL_ERROR_WANT_READ:
                    raise socket.error(err)
            else:
                if len(buf) == bufsize:
                    self._task = self._result = None
                    return buf
                else:
                    self._result.append(buf)
                    bufsize -= len(buf)

        self._task = functools.partial(_read, self, bufsize, *args)
        self._coro = self._asyncoro.cur_coro()
        self._coro.suspend()
        self._notifier.modify(self, _AsyncNotifier._Readable)

    def sync_read(self, bufsize, *args):
        """Synchronous version of async_read.
        """
        self._result = []
        while bufsize:
            buf = self._rsock.recv(bufsize, *args)
            if not buf:
                raise socket.error('read error')
            bufsize -= len(buf)
            self._result.append(buf)
        buf = ''.join(self._result)
        self._result = None
        return buf

    def async_recvfrom(self, *args):
        """Asynchronous version of socket recvfrom method.
        """
        def _recvfrom(self, *args):
            try:
                buf = self._rsock.recvfrom(*args)
            except:
                self._notifier.modify(self, 0)
                self._task = self._result = None
                coro, self._coro = self._coro, None
                coro.throw(*sys.exc_info())
            else:
                self._notifier.modify(self, 0)
                self._task = self._result = None
                coro, self._coro = self._coro, None
                coro.resume(buf)

        self._task = functools.partial(_recvfrom, self, *args)
        self._coro = self._asyncoro.cur_coro()
        self._coro.suspend()
        self._notifier.modify(self, _AsyncNotifier._Readable)

    def async_send(self, *args):
        """Asynchronous version of socket send method.
        """
        # NB: send only what can be sent in one operation, instead of
        # sending all the data, as done in write/write_msg
        def _send(self, *args):
            try:
                sent = self._rsock.send(*args)
            except:
                self._notifier.modify(self, 0)
                self._task = self._result = None
                coro, self._coro = self._coro, None
                coro.throw(*sys.exc_info())
            else:
                self._notifier.modify(self, 0)
                self._task = self._result = None
                coro, self._coro = self._coro, None
                coro.resume(sent)

        self._task = functools.partial(_send, self, *args)
        self._coro = self._asyncoro.cur_coro()
        self._coro.suspend()
        self._notifier.modify(self, _AsyncNotifier._Writable)

    def async_sendto(self, *args):
        """Asynchronous version of socket sendto method.
        """

        # NB: send only what can be sent in one operation, instead of
        # sending all the data, as done in write/write_msg
        def _sendto(self, *args):
            try:
                sent = self._rsock.sendto(*args)
            except:
                self._notifier.modify(self, 0)
                self._task = self._result = None
                coro, self._coro = self._coro, None
                coro.throw(*sys.exc_info())
            else:
                self._notifier.modify(self, 0)
                self._task = self._result = None
                coro, self._coro = self._coro, None
                coro.resume(sent)

        self._task = functools.partial(_sendto, self, *args)
        self._coro = self._asyncoro.cur_coro()
        self._coro.suspend()
        self._notifier.modify(self, _AsyncNotifier._Writable)

    def async_write(self, data):
        """Send all the data (similar to sendall).
        """
        def _write(self):
            try:
                sent = self._rsock.send(self._result)
            except:
                self._notifier.modify(self, 0)
                self._task = self._result = None
                coro, self._coro = self._coro, None
                coro.throw(*sys.exc_info())
            else:
                if sent > 0:
                    self._result = buffer(self._result, sent)
                    if len(self._result) == 0:
                        self._notifier.modify(self, 0)
                        self._task = self._result = None
                        coro, self._coro = self._coro, None
                        coro.resume(0)

        self._result = buffer(data, 0)
        self._task = functools.partial(_write, self)
        self._coro = self._asyncoro.cur_coro()
        self._coro.suspend()
        self._notifier.modify(self, _AsyncNotifier._Writable)

    def sync_write(self, data):
        """Synchronous version of async_write.
        """
        # TODO: is sendall better?
        self._result = buffer(data, 0)
        while len(self._result) > 0:
            sent = self._rsock.send(self._result)
            if sent > 0:
                self._result = buffer(self._result, sent)
        return 0

    def async_accept(self):
        """Asynchronous version of socket accept method. Socket in
        returned pair is AsynCoroSocket.
        """
        def _accept(self):
            conn, addr = self._rsock.accept()
            self._task = None
            self._notifier.modify(self, 0)

            if self._certfile:
                def _ssl_handshake(self, conn, addr):
                    try:
                        conn._rsock.do_handshake()
                    except ssl.SSLError, err:
                        if err.args[0] == ssl.SSL_ERROR_WANT_READ:
                            conn._notifier.modify(conn, _AsyncNotifier._Readable)
                        elif err.args[0] == ssl.SSL_ERROR_WANT_WRITE:
                            conn._notifier.modify(conn, _AsyncNotifier._Writable)
                        else:
                            conn._task = None
                            coro, self._coro = self._coro, None
                            conn.close()
                            coro.throw(*sys.exc_info())
                    else:
                        conn._task = None
                        coro, self._coro = self._coro, None
                        conn._notifier.modify(conn, 0)
                        coro.resume((conn, addr))
                conn = AsynCoroSocket(conn, blocking=False, keyfile=self._keyfile,
                                      certfile=self._certfile, ssl_version=self._ssl_version)
                conn._rsock = ssl.wrap_socket(conn, keyfile=self._keyfile, certfile=self._certfile,
                                              server_side=True, do_handshake_on_connect=False,
                                              ssl_version=self._ssl_version)
                conn._task = functools.partial(_ssl_handshake, self, conn, addr)
                conn._task()
            else:
                coro, self._coro = self._coro, None
                conn = AsynCoroSocket(conn, blocking=False)
                coro.resume((conn, addr))

        self._task = functools.partial(_accept, self)
        self._coro = self._asyncoro.cur_coro()
        self._coro.suspend()
        self._notifier.modify(self, _AsyncNotifier._Readable)

    def async_connect(self, *args):
        """Asynchronous version of socket connect method.
        """
        def _connect(self, *args):
            err = self._rsock.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
            if err:
                logging.debug('connect error: %s', err)
            elif self._certfile:
                def _ssl_handshake(self):
                    try:
                        self._rsock.do_handshake()
                    except ssl.SSLError, err:
                        if err.args[0] == ssl.SSL_ERROR_WANT_READ:
                            self._notifier.modify(self, _AsyncNotifier._Readable)
                        elif err.args[0] == ssl.SSL_ERROR_WANT_WRITE:
                            self._notifier.modify(self, _AsyncNotifier._Writable)
                        else:
                            self._task = None
                            coro, self._coro = self._coro, None
                            self.close()
                            coro.throw(*sys.exc_info())
                    else:
                        coro, self._coro = self._coro, None
                        self._notifier.modify(self, 0)
                        coro.resume(0)

                self._rsock = ssl.wrap_socket(self._rsock, keyfile=self._keyfile,
                                              certfile=self._certfile, server_side=False,
                                              do_handshake_on_connect=False)
                self._task = functools.partial(_ssl_handshake, self)
                self._task()
            else:
                self._task = None
                coro, self._coro = self._coro, None
                self._notifier.modify(self, 0)
                coro.resume(0)

        self._task = functools.partial(_connect, self, *args)
        self._coro = self._asyncoro.cur_coro()
        self._coro.suspend()
        self._notifier.modify(self, _AsyncNotifier._Writable)
        try:
            self._rsock.connect(*args)
        except socket.error, e:
            if e.args[0] not in [EINPROGRESS, EWOULDBLOCK]:
                logging.debug('connect error: %s', e.args[0])

    def async_write_msg(self, data):
        """Messages are tagged with length of the data, so on the
        receiving side, read_msg knows how much data to read.
        """
        yield self.write(struct.pack('>L', len(data)) + data)

    def sync_write_msg(self, data):
        """Synchronous version of async_write_msg.
        """
        return self.sync_write(struct.pack('>L', len(data)) + data)

    def async_read_msg(self):
        """Message is tagged with length of the payload (data). This
        method reads length of payload, then the payload and returns
        the payload.
        """
        n = struct.calcsize('>L')
        data = yield self.read(n)
        if len(data) < n:
            logging.error('Socket disconnected?(%s, %s)', len(data), n)
            yield None
        n = struct.unpack('>L', data)[0]
        assert n > 0
        data = yield self.read(n)
        if len(data) < n:
            logging.error('Socket disconnected?(%s, %s)', len(data), n)
            yield None
        yield data

    def sync_read_msg(self):
        """Synchronous version of async_read_msg.
        """
        n = struct.calcsize('>L')
        data = self.sync_read(n)
        if len(data) < n:
            logging.error('Socket disconnected?(%s, %s)', len(data), n)
            return None
        n = struct.unpack('>L', data)[0]
        assert n > 0
        data = self.sync_read(n)
        if len(data) < n:
            logging.error('Socket disconnected?(%s, %s)', len(data), n)
            return None
        return data

if platform.system() == 'Windows':
    # use IOCP if pywin32 (http://pywin32.sf.net) is installed
    try:
        import win32file
        import win32event
        import pywintypes
        import winerror
    except:
        print 'Could not load pywin32 for I/O Completion Ports; using inefficient polling for sockets'
        AsynCoroSocket = _AsynCoroSocket
    else:
        class _IOCPNotifier(object):
            """Internal use only.
            """

            __metaclass__ = MetaSingleton
            __instance = None

            @classmethod
            def instance(cls):
                if cls.__instance is None:
                    cls.__instance = cls()
                return cls.__instance

            def __init__(self):
                if not hasattr(self, 'iocp'):
                    self.iocp = win32file.CreateIoCompletionPort(win32file.INVALID_HANDLE_VALUE,
                                                                 None, 0, 0)
                    self.poller = threading.Thread(target=self.poll)
                    self.poller.daemon = True
                    self.poller.start()

            def register(self, fd, event=0):
                win32file.CreateIoCompletionPort(fd._fileno, self.iocp, 1, 0)

            def unregister(self, fd):
                pass

            def modify(self, fd, event):
                pass

            def poll(self):
                while True:
                    err, n, key, overlap = win32file.GetQueuedCompletionStatus(self.iocp, win32event.INFINITE)
                    if not key:
                        break
                    if overlap and overlap.object:
                        overlap.object(err, n)
                    else:
                        logging.warning('no overlap!')
                win32file.CloseHandle(self.iocp)
                self.iocp = None

            def terminate(self):
                win32file.PostQueuedCompletionStatus(self.iocp, 0, 0, None)
                self.poller.join()

        class AsynCoroSocket(_AsynCoroSocket):
            """AsynCoroSocket with I/O Completion Ports (under
            Windows). This is (for now) minimal implementation to make
            TCP traffic asynchronous. UDP traffic is handled by
            SelectNotifier. If necessary and/or there is interest, it
            may be better to implement this separately instead of
            patching _AsynCoroSocket and _AsyncNotifier.
            """
            def __init__(self, *args, **kwargs):
                self._IOCPNotifier = None
                self._overlap = None
                _AsynCoroSocket.__init__(self, *args, **kwargs)

            def _register(self):
                if not self._blocking and self._rsock.type == socket.SOCK_STREAM:
                    self._overlap = pywintypes.OVERLAPPED()
                    self._IOCPNotifier = _IOCPNotifier.instance()
                    self._IOCPNotifier.register(self)
                else:
                    _AsynCoroSocket._register(self)

            def _unregister(self):
                if self._IOCPNotifier:
                    self._IOCPNotifier.unregister(self)
                    self._IOCPNotifier = None
                    # TODO: is there a race condition here?
                    if self._overlap.object:
                        win32file.CancelIo(self._fileno)
                    else:
                        self._overlap = None
                else:
                    _AsynCoroSocket._unregister(self)

            def setblocking(self, blocking):
                _AsynCoroSocket.setblocking(self, blocking)
                if not self._blocking and self._rsock.type == socket.SOCK_STREAM:
                    self.recv = self.iocp_recv
                    self.send = self.iocp_send
                    self.read = self.iocp_read
                    self.write = self.iocp_write
                    self.connect = self.iocp_connect
                    self.accept = self.iocp_accept

            def _timed_out(self):
                if self._coro:
                    self._coro.throw(socket.timeout, socket.timeout('timed out'))
                    # TODO: should we cancel pending IOs?
                    # win32file.CancelIo(self._fileno)

            def iocp_recv(self, bufsize, *args):
                def _recv(self, err, n):
                    if self._timeout and self._notifier:
                        self._notifier._del_timeout(self)
                    if err or n == 0:
                        self._overlap.object = self._result = None
                        coro, self._coro = self._coro, None
                        if not err:
                            err = winerror.ERROR_CONNECTION_INVALID
                        if err != winerror.ERROR_OPERATION_ABORTED:
                            coro.throw(socket.error, socket.error(err))
                    else:
                        buf = self._result[:n]
                        self._overlap.object = self._result = None
                        coro, self._coro = self._coro, None
                        coro.resume(buf)

                self._result = win32file.AllocateReadBuffer(bufsize)
                self._overlap.object = functools.partial(_recv, self)
                self._coro = self._asyncoro.cur_coro()
                self._coro.suspend()
                if self._timeout:
                    self._notifier._add_timeout(self)
                err, n = win32file.WSARecv(self._fileno, self._result, self._overlap, 0)
                if err and err != winerror.ERROR_IO_PENDING:
                    raise socket.error(err)

            def iocp_send(self, buf, *args):
                def _send(self, err, n):
                    if self._timeout and self._notifier:
                        self._notifier._del_timeout(self)
                    if err or n == 0:
                        self._overlap.object = self._result = None
                        coro, self._coro = self._coro, None
                        if not err:
                            err = winerror.ERROR_CONNECTION_INVALID
                        if err != winerror.ERROR_OPERATION_ABORTED:
                            coro.throw(socket.error, socket.error(err))
                    else:
                        self._overlap.object = self._result = None
                        coro, self._coro = self._coro, None
                        coro.resume(n)

                self._overlap.object = functools.partial(_send, self)
                self._coro = self._asyncoro.cur_coro()
                self._coro.suspend()
                if self._timeout:
                    self._notifier._add_timeout(self)
                err, n = win32file.WSASend(self._fileno, buf, self._overlap, 0)
                if err and err != winerror.ERROR_IO_PENDING:
                    raise socket.error(err)

            def iocp_read(self, bufsize, *args):
                def _read(self, pending, buf, err, n):
                    if err or n == 0:
                        self._overlap.object = self._result = None
                        coro, self._coro = self._coro, None
                        if self._timeout and self._notifier:
                            self._notifier._del_timeout(self)
                        if not err:
                            err = winerror.ERROR_CONNECTION_INVALID
                        if err != winerror.ERROR_OPERATION_ABORTED:
                            coro.throw(socket.error, socket.error(err))
                    else:
                        self._result.append(buf[:n])
                        pending -= n
                        if pending == 0:
                            buf = ''.join(self._result)
                            self._overlap.object = self._result = None
                            coro, self._coro = self._coro, None
                            if self._timeout and self._notifier:
                                self._notifier._del_timeout(self)
                            coro.resume(buf)
                        else:
                            buf = win32file.AllocateReadBuffer(min(pending, 4194304))
                            self._overlap.object = functools.partial(_read, self, pending, buf)
                            err, n = win32file.WSARecv(self._fileno, buf, self._overlap, 0)
                            if err and err != winerror.ERROR_IO_PENDING:
                                self._coro.throw(socket.error, socket.error(err))

                self._result = []
                buf = win32file.AllocateReadBuffer(min(bufsize, 4194304))
                self._overlap.object = functools.partial(_read, self, bufsize, buf)
                self._coro = self._asyncoro.cur_coro()
                self._coro.suspend()
                if self._timeout:
                    self._notifier._add_timeout(self)
                err, n = win32file.WSARecv(self._fileno, buf, self._overlap, 0)
                if err and err != winerror.ERROR_IO_PENDING:
                    raise socket.error(err)

            def iocp_write(self, data):
                def _write(self, err, n):
                    if err or n == 0:
                        self._overlap.object = self._result = None
                        coro, self._coro = self._coro, None
                        if self._timeout and self._notifier:
                            self._notifier._del_timeout(self)
                        if not err:
                            err = winerror.ERROR_CONNECTION_INVALID
                        if err != winerror.ERROR_OPERATION_ABORTED:
                            coro.throw(socket.error, socket.error(err))
                    else:
                        self._result = buffer(self._result, n)
                        if len(self._result) == 0:
                            self._overlap.object = self._result = None
                            coro, self._coro = self._coro, None
                            if self._timeout and self._notifier:
                                self._notifier._del_timeout(self)
                            coro.resume(0)
                        else:
                            err, n = win32file.WSASend(self._fileno, self._result, self._overlap, 0)
                            if err and err != winerror.ERROR_IO_PENDING:
                                self._coro.throw(socket.error, socket.error(err))

                self._result = buffer(data, 0)
                self._overlap.object = functools.partial(_write, self)
                self._coro = self._asyncoro.cur_coro()
                self._coro.suspend()
                err, n = win32file.WSASend(self._fileno, self._result, self._overlap, 0)
                if err and err != winerror.ERROR_IO_PENDING:
                    raise socket.error(err)

            def iocp_connect(self, (host, port)):
                def _connect(self, err, n):
                    def _ssl_handshake(self, err, n):
                        try:
                            self._rsock.do_handshake()
                        except ssl.SSLError, err:
                            if err.args[0] == ssl.SSL_ERROR_WANT_READ:
                                self._result = win32file.AllocateReadBuffer(0)
                                err, n = win32file.WSARecv(self._fileno, self._result, self._overlap, 0)
                            elif err.args[0] == ssl.SSL_ERROR_WANT_WRITE:
                                err, n = win32file.WSASend(self._fileno, '', self._overlap, 0)
                            else:
                                self._overlap.object = self._result = None
                                coro, self._coro = self._coro, None
                                self.close()
                                coro.throw(*sys.exc_info())
                        else:
                            if self._timeout and self._notifier:
                                self._notifier._del_timeout(self)
                            self._overlap.object = self._result = None
                            coro, self._coro = self._coro, None
                            coro.resume(0)

                    if err:
                        self._overlap.object = self._result = None
                        coro, self._coro = self._coro, None
                        if self._timeout and self._notifier:
                            self._notifier._del_timeout(self)
                        if err != winerror.ERROR_OPERATION_ABORTED:
                            coro.throw(socket.error, socket.error(err))
                    else:
                        self._rsock.setsockopt(socket.SOL_SOCKET, win32file.SO_UPDATE_CONNECT_CONTEXT, '')
                        if self._certfile:
                            self._rsock = ssl.wrap_socket(self._rsock, keyfile=self._keyfile,
                                                          certfile=self._certfile, server_side=False,
                                                          do_handshake_on_connect=False)
                            self._overlap.object = functools.partial(_ssl_handshake, self)
                            self._overlap.object(None, 0)
                        else:
                            if self._timeout and self._notifier:
                                self._notifier._del_timeout(self)
                            self._overlap.object = self._result = None
                            coro, self._coro = self._coro, None
                            coro.resume(0)

                # ConnectEX requires socket to be bound!
                try:
                    self._rsock.bind(('0.0.0.0', 0))
                except socket.error, exc:
                    if exc[0] not in [errno.EINVAL, errno.WSAEINVAL]:
                        raise
                self._overlap.object = functools.partial(_connect, self)
                self._coro = self._asyncoro.cur_coro()
                if self._timeout:
                    self._notifier._add_timeout(self)
                self._coro.suspend()
                err, n = win32file.ConnectEx(self._rsock, (host, port), self._overlap)
                if err and err != winerror.ERROR_IO_PENDING:
                    raise socket.error(err)

            def iocp_accept(self):
                def _accept(self, conn, err, n):
                    def _ssl_handshake(self, conn, addr, err, n):
                        try:
                            conn._rsock.do_handshake()
                        except ssl.SSLError, err:
                            if err.args[0] == ssl.SSL_ERROR_WANT_READ:
                                self._result = win32file.AllocateReadBuffer(0)
                                err, n = win32file.WSARecv(conn._fileno, self._result, self._overlap, 0)
                            elif err.args[0] == ssl.SSL_ERROR_WANT_WRITE:
                                err, n = win32file.WSASend(conn._fileno, '', self._overlap, 0)
                            else:
                                self._overlap.object = self._result = None
                                coro, self._coro = self._coro, None
                                conn.close()
                                coro.throw(*sys.exc_info())
                        else:
                            if self._timeout and self._notifier:
                                self._notifier._del_timeout(self)
                            self._overlap.object = self._result = None
                            coro, self._coro = self._coro, None
                            coro.resume((conn, addr))

                    if err:
                        self._overlap.object = self._result = None
                        coro, self._coro = self._coro, None
                        if self._timeout and self._notifier:
                            self._notifier._del_timeout(self)
                        if err != winerror.ERROR_OPERATION_ABORTED:
                            coro.throw(socket.error, socket.error(err))
                    else:
                        family, laddr, raddr = win32file.GetAcceptExSockaddrs(conn, self._result)
                        # TODO: unpack raddr if family != AF_INET
                        conn._rsock.setsockopt(socket.SOL_SOCKET, win32file.SO_UPDATE_ACCEPT_CONTEXT,
                                               struct.pack('I', conn._fileno))
                        self._overlap.object = self._result = None
                        if self._certfile:
                            conn._rsock = ssl.wrap_socket(conn, keyfile=self._keyfile, certfile=self._certfile,
                                                          server_side=True, do_handshake_on_connect=False,
                                                          ssl_version=self._ssl_version)
                            self._overlap.object = functools.partial(_ssl_handshake, self, conn, raddr)
                            self._overlap.object(None, 0)
                        else:
                            if self._timeout and self._notifier:
                                self._notifier._del_timeout(self)
                            coro, self._coro = self._coro, None
                            coro.resume((conn, raddr))

                sock = socket.socket(self._rsock.family, self._rsock.type, self._rsock.proto)
                conn = AsynCoroSocket(sock, keyfile=self._keyfile, certfile=self._certfile,
                                      ssl_version=self._ssl_version)
                self._result = win32file.AllocateReadBuffer(win32file.CalculateSocketEndPointSize(sock))
                self._overlap.object = functools.partial(_accept, self, conn)
                self._coro = self._asyncoro.cur_coro()
                self._coro.suspend()
                err = win32file.AcceptEx(self._fileno, conn._fileno, self._result, self._overlap)
                if err and err != winerror.ERROR_IO_PENDING:
                    raise socket.error(err)
        # end of Windows specific setup
else:
    AsynCoroSocket = _AsynCoroSocket

def sock_read(sock, bufsize, *args):
    """sync_read (see above) for regular sockets; useful for synchronous SSL sockets.
    """
    res = []
    while bufsize:
        buf = sock.recv(bufsize, *args)
        if not buf:
            raise socket.error('read error')
        bufsize -= len(buf)
        res.append(buf)
    return ''.join(res)

def sock_write(sock, data):
    """sync_write (see above) for regular sockets; useful for synchronous SSL sockets.
    """
    # TODO: is sendall better?
    buf = buffer(data, 0)
    while len(buf) > 0:
        sent = sock.send(buf)
        if sent > 0:
            buf = buffer(buf, sent)
    return 0

def sock_write_msg(sock, data):
    """sync_write_msg (see above) for regular sockets; useful for synchronous SSL sockets.
    """
    return sock_write(sock, struct.pack('>L', len(data)) + data)

def sock_read_msg(sock):
    """sync_read_msg (see above) for regular sockets; useful for synchronous SSL sockets.
    """
    n = struct.calcsize('>L')
    data = sock_read(sock, n)
    if len(data) < n:
        logging.error('Socket disconnected?(%s, %s)', len(data), n)
        return None
    n = struct.unpack('>L', data)[0]
    assert n > 0
    data = sock_read(sock, n)
    if len(data) < n:
        logging.error('Socket disconnected?(%s, %s)', len(data), n)
        return None
    return data

class Coro(object):
    """'Coroutine' factory to build coroutines to be scheduled with
    AsynCoro. Automatically starts executing 'func'.  The function
    definition should have 'coro' argument set to (default value)
    None. When the function is called, that argument will be this
    object.
    """
    def __init__(self, func, *args, **kwargs):
        if not inspect.isfunction(func) and not inspect.ismethod(func):
            raise Exception('Invalid coroutine function %s', func.__name__)
        if not inspect.isgeneratorfunction(func):
            raise Exception('%s is not a generator!' % func.__name__)
        if 'coro' in kwargs:
            raise Exception('Coro function %s should not be called with ' \
                            '"coro" parameter' % func.__name__)
        callargs = inspect.getcallargs(func, *args, **kwargs)
        if 'coro' not in callargs or callargs['coro'] is not None:
            raise Exception('Coro function "%s" should have "coro" argument with ' \
                            'default value None' % func.__name__)
        kwargs['coro'] = self
        self.name = func.__name__
        self._generator = func(*args, **kwargs)
        self._id = None
        self._state = None
        self._value = None
        self._exception = None
        self._callers = []
        self._timeout = None
        self._asyncoro = AsynCoro.instance()
        self._complete = threading.Event()
        self._asyncoro._add(self)

    def suspend(self, timeout=None):
        """Suspend/sleep coro (until woken up, usually by
        AsyncNotifier in the case of AsynCoroSockets).

        If timeout is a (floating point) number, this coro is
        suspended for that many seconds (or fractions of second). This
        method should be used with 'yield'; e.g., as 'x = yield
        coro.suspend(2.5)' to suspend execution of coro for 2.5
        seconds; in that time other coroutines can execute.
        """
        if self._asyncoro:
            return self._asyncoro._suspend(self._id, timeout)
        else:
            logging.warning('suspend: coroutine %s removed?', self.name)
            return -1

    sleep = suspend

    def resume(self, update=None):
        """Resume/wakeup this coro and send 'update' to it.

        The resuming coro gets 'update' for the 'yield' that caused it
        to suspend. Thus, if coro1 resumes coro2 with
        'coro2.resume({'a':1, 'b':2})', coro2 resumes from where it
        suspended itself and will have the dictionary with keys 'a'
        and 'b' for 'x' in the example above.
        """
        if self._asyncoro:
            return self._asyncoro._resume(self._id, update)
        else:
            logging.warning('resume: coroutine %s removed?', self.name)
            return -1

    wakeup = resume

    def throw(self, *args):
        """Throw exception in coroutine.
        """
        if len(args) < 2:
            logging.warning('throw: invalid argument(s)')
            return -1
        else:
            if self._asyncoro:
                return self._asyncoro._throw(self._id, *args)
            else:
                logging.warning('throw: coroutine %s removed?', self.name)
                return -1

    def value(self):
        """Get 'return' value of coro.

        NB: This method should _not_ be called from a coroutine! This
        method is meant for main thread in the user program to wait
        for (main) coroutine(s) it creates.

        Once coroutine stops (finishes) executing, the last value
        yielded by it is returned.

        If waiting for one coro from another is needed, there are
        multiple approaches: If one coro starts another, then caller
        can 'yield' to the generator instead of creating coro. If
        coros are conccurrent, they can use CoroCondition variable so
        coro being waited on uses .notify on that variable and the
        coro waiting on uses .wait on that variable (see
        CoroCondition).
        """
        self._complete.wait()
        return self._value

    def terminate(self):
        """Terminate coro.

        This method should be called by a coro (on some other
        coro). Otherwise, there is a chance that coro being terminated
        is currently running and can interfere with GenratorExit
        exception that will be thrown to coro.
        """
        if self._asyncoro:
            return self._asyncoro._terminate_coro(self._id)
        else:
            logging.warning('terminate: coroutine %s removed?', self.name)
            return -1

class CoroLock(object):
    """'Lock' primitive for coroutines.

    Since a coroutine runs until 'yield', there is no need for
    lock. The caller has to guarantee that 'yield' statement is not
    used from when lock is acquired and till when lock is released.
    """
    def __init__(self):
        self._owner = None
        self._asyncoro = AsynCoro.instance()

    def acquire(self):
        owner = self._asyncoro.cur_coro()
        assert self._owner == None, '"%s"/%s: lock owned by "%s"/%s' % \
               (owner.name, owner._id, self._owner.name, self._owner._id)
        self._owner = owner

    def release(self):
        owner = self._asyncoro.cur_coro()
        assert self._owner == owner, '"%s"/%s: invalid lock release - owned by "%s"/%s' % \
               (owner.name, owner._id, self._owner.name, self._owner._id)
        self._owner = None

class CoroCondition(object):
    """'Condition' primitive for coroutines.

    Since a coroutine runs until 'yield', there is no need for
    lock. The caller has to guarantee that once a lock is obtained,
    'yield' is not used, except for the case of 'wait'. See 'dispy.py'
    on how to use it.
    """
    def __init__(self):
        self._waitlist = []
        self._owner = None
        self._notify = False
        self._asyncoro = AsynCoro.instance()

    def acquire(self):
        owner = self._asyncoro.cur_coro()
        assert self._owner == None, '"%s"/%s: condition variable owned by "%s"/%s' % \
               (owner.name, owner._id, self._owner.name, self._owner._id)
        self._owner = owner

    def release(self):
        owner = self._asyncoro.cur_coro()
        assert self._owner == owner, '"%s"/%s: invalid condition variable release - owned by "%s"/%s' % \
               (owner.name, owner._id, self._owner.name, self._owner._id)
        self._owner = None

    def notify(self):
        owner = self._asyncoro.cur_coro()
        assert self._owner == owner, '"%s"/%s: invalid condition variable notify - owned by "%s"/%s' % \
               (owner.name, owner._id, self._owner.name, self._owner._id)
        self._notify = True
        if self._waitlist:
            wake = self._waitlist.pop(0)
            wake.resume(None)

    def wait(self):
        """If condition variable is called cv, then a typical use in consumer is:

        while True:
            cv.acquire()
            while not queue or cv.wait():
                yield None
            item = queue.pop(0)
            process(item)
            yield cv.release()
        
        """
        coro = self._asyncoro.cur_coro()
        if self._owner is not None:
            assert self._owner == coro, '"%s"/%s: invalid condition variable wait - owned by "%s"/%s' % \
                   (coro.name, coro._id, self._owner.name, self._owner._id)
        if self._notify:
            self._notify = False
            self._owner = coro
            return False
        else:
            self._owner = None
            self._waitlist.append(coro)
            coro.suspend()
            return True

class AsynCoro(object):
    """Coroutine scheduler.

    Once created (singleton) AsynCoro, any coroutines built with Coro
    class above will start executing. AsynCoro creates _AsyncNotifier
    to wake up suspended coroutines waiting for I/O completion. The
    only methods available to users are 'cur_coro', 'terminate' and
    'join'.
    """

    __metaclass__ = MetaSingleton
    __instance = None

    # in _scheduled set, waiting for turn to execute
    _Scheduled = 1
    # in _scheduled, currently executing
    _Running = 2
    # in _suspended
    _Suspended = 3

    _coro_id = 1

    def __init__(self):
        if self.__class__.__instance is None:
            self.__class__.__instance = self
            self._coros = {}
            self._cur_coro = None
            self._scheduled = set()
            self._suspended = set()
            self._timeouts = []
            # because AsyncNotifier runs in a separate thread and
            # calls coro functions (resume, throw etc.), we need to
            # lock
            self._sched_cv = threading.Condition()
            self._terminate = False
            self._complete = threading.Event()
            self._scheduler = threading.Thread(target=self._scheduler)
            self._scheduler.daemon = True
            self._scheduler.start()

            self._notifier = _AsyncNotifier()

    @classmethod
    def instance(cls):
        """Returns (singleton) instance of AsynCoro. This method
        should be called only after initializing AsynCoro.
        """
        return cls.__instance

    def cur_coro(self):
        """Must be called from a coro only.
        """
        return self._cur_coro

    def _add(self, coro):
        """Internal use only. See Coro class.
        """
        self._sched_cv.acquire()
        coro._id = AsynCoro._coro_id
        AsynCoro._coro_id += 1
        self._coros[coro._id] = coro
        self._complete.clear()
        coro._state = AsynCoro._Scheduled
        self._scheduled.add(coro._id)
        self._sched_cv.notify()
        self._sched_cv.release()

    def _suspend(self, cid, timeout=None):
        """Internal use only. See sleep/suspend in Coro.
        """
        if timeout is not None:
            if not isinstance(timeout, (float, int)) or timeout <= 0:
                logging.warning('invalid timeout %s', timeout)
                return -1
        self._sched_cv.acquire()
        coro = self._coros.get(cid, None)
        if coro is None or coro._state != AsynCoro._Running:
            self._sched_cv.release()
            logging.warning('invalid coroutine %s to suspend', cid)
            return -1
        self._scheduled.discard(cid)
        self._suspended.add(cid)
        coro._state = AsynCoro._Suspended
        if timeout is None:
            coro._timeout = None
        else:
            timeout = _time() + timeout
            heappush(self._timeouts, (timeout, cid))
            coro._timeout = timeout
            self._sched_cv.notify()
        self._sched_cv.release()
        return 0

    def _resume(self, cid, update):
        """Internal use only. See resume in Coro.
        """
        self._sched_cv.acquire()
        coro = self._coros.get(cid, None)
        if coro is None:
            self._sched_cv.release()
            logging.warning('invalid coroutine %s to resume', cid)
            return -1
        elif coro._state == AsynCoro._Scheduled:
            if coro._exception and coro._exception[0] != GeneratorExit:
                # this can happen with sockets with timeouts:
                # _AsyncNotifier may throw timeout exception, but
                # before exception is thrown to coro, I/O operation
                # may complete
                logging.debug('discarding exception for %s/%s', coro.name, cid)
                coro._exception = None
                coro._value = update
                self._sched_cv.release()
                return 0
            else:
                self._sched_cv.release()
                logging.warning('invalid coroutine %s/%s to resume', coro.name, cid)
                return -1
        coro._timeout = None
        coro._value = update
        self._suspended.discard(cid)
        self._scheduled.add(cid)
        coro._state = AsynCoro._Scheduled
        self._sched_cv.notify()
        self._sched_cv.release()
        return 0

    def _throw(self, cid, *args):
        """Internal use only. See throw in Coro.
        """
        self._sched_cv.acquire()
        coro = self._coros.get(cid, None)
        if coro is None or coro._state not in [AsynCoro._Scheduled, AsynCoro._Suspended]:
            logging.warning('invalid coroutine %s to throw exception', cid)
            self._sched_cv.release()
            return -1
        # prevent throwing more than once?
        coro._timeout = None
        coro._exception = args
        if coro._state == AsynCoro._Suspended:
            self._suspended.discard(coro._id)
            self._scheduled.add(coro._id)
        coro._state = AsynCoro._Scheduled
        self._sched_cv.notify()
        self._sched_cv.release()
        return 0

    def _terminate_coro(self, cid):
        """Internal use only.
        """
        self._sched_cv.acquire()
        coro = self._coros.get(cid, None)
        if coro is None:
            logging.warning('invalid coroutine %s to terminate', cid)
            self._sched_cv.release()
            return -1
        if coro._state == AsynCoro._Running:
            logging.warning('coroutine to terminate %s/%s is running', coro.name, cid)
            # if coro raises exception during current run, this exception will be ignored!
        coro._exception = (GeneratorExit, GeneratorExit('close'))
        coro._timeout = None
        if coro._state == AsynCoro._Suspended:
            self._suspended.discard(cid)
            self._scheduled.add(cid)
        coro._state = AsynCoro._Scheduled
        self._sched_cv.notify()
        self._sched_cv.release()
        return 0

    def _scheduler(self):
        """Internal use only.
        """
        while True:
            self._sched_cv.acquire()
            if self._timeouts:
                now = _time()
                timeout, cid = self._timeouts[0]
                timeout -= now
                # timeout can become negative?
                if timeout < 0:
                    timeout = 0
            else:
                timeout = None
            while (not self._scheduled) and (not self._terminate):
                self._sched_cv.wait(timeout)
                if timeout is not None:
                    break
            if self._terminate:
                for cid in self._scheduled.union(self._suspended):
                    coro = self._coros.get(cid, None)
                    if coro is None:
                        continue
                    logging.debug('terminating Coro %s/%s', coro.name, coro._id)
                    self._cur_coro = coro
                    coro._state = AsynCoro._Scheduled
                    while coro._generator:
                        try:
                            coro._generator.close()
                        except:
                            logging.debug('closing %s raised exception: %s',
                                          coro._generator.__name__, traceback.format_exc())
                        if coro._callers:
                            coro._generator, coro._value = coro._callers.pop(-1)
                        else:
                            coro._generator = None
                    coro._complete.set()
                self._scheduled = self._suspended = set()
                self._timeouts = []
                self._coros = {}
                self._sched_cv.release()
                self._complete.set()
                break
            if self._timeouts:
                # wake up timed suspends
                now = _time()
                while self._timeouts and self._timeouts[0][0] <= now:
                    timeout, cid = heappop(self._timeouts)
                    assert timeout <= now
                    coro = self._coros.get(cid, None)
                    if coro is None or coro._timeout != timeout:
                        continue
                    if coro._state != AsynCoro._Suspended:
                        logging.warning('coro %s/%s is in state %s for resume; ignored',
                                        coro.name, coro._id, coro._state)
                        continue
                    coro._timeout = None
                    self._suspended.discard(coro._id)
                    self._scheduled.add(coro._id)
                    coro._state = AsynCoro._Scheduled
                    coro._value = None
            scheduled = [self._coros.get(cid, None) for cid in self._scheduled]
            # random.shuffle(running)
            self._sched_cv.release()

            for coro in scheduled:
                if coro is None:
                    continue
                self._sched_cv.acquire()
                if coro._state != AsynCoro._Scheduled:
                    self._sched_cv.release()
                    logging.warning('ignoring %s with state %s', coro.name, coro._state)
                    continue
                coro._state = AsynCoro._Running
                self._cur_coro = coro
                self._sched_cv.release()

                try:
                    if coro._exception:
                        exc, coro._exception = coro._exception, None
                        if exc[0] == GeneratorExit:
                            # assert str(exc[1]) == 'close'
                            coro._generator.close()
                        else:
                            retval = coro._generator.throw(*exc)
                    else:
                        retval = coro._generator.send(coro._value)
                except:
                    self._sched_cv.acquire()
                    self._cur_coro = None
                    coro._exception = sys.exc_info()
                    if coro._exception[0] == StopIteration:
                        coro._exception = None
                    else:
                        coro._value = None

                    if coro._callers:
                        # return to caller
                        caller = coro._callers.pop(-1)
                        coro._generator = caller[0]
                        if coro._exception:
                            # callee raised exception, restore saved value
                            coro._value = caller[1]
                        coro._state = AsynCoro._Scheduled
                    else:
                        if coro._exception:
                            assert isinstance(coro._exception, tuple)
                            if len(coro._exception) == 2:
                                exc = ''.join(traceback.format_exception_only(*coro._exception))
                            else:
                                exc = ''.join(traceback.format_exception(*coro._exception))
                            logging.warning('uncaught exception in %s:\n%s', coro.name, exc)

                        # delete this coro
                        if self._coros.pop(coro._id, None) == coro:
                            if coro._state == AsynCoro._Suspended:
                                self._suspended.discard(coro._id)
                            else:
                                assert coro._state in [AsynCoro._Scheduled, AsynCoro._Running]
                                self._scheduled.discard(coro._id)
                            coro._asyncoro = None
                            coro._complete.set()
                            coro._state = None
                            if not self._coros:
                                self._complete.set()
                        else:
                            logging.warning('coro %s/%s already removed?', coro.name, coro._id)
                    self._sched_cv.release()
                else:
                    self._sched_cv.acquire()
                    self._cur_coro = None
                    if coro._state == AsynCoro._Running:
                        coro._state = AsynCoro._Scheduled
                        # if this coroutine is suspended, don't update
                        # the value; when it is resumed, it will be
                        # updated with the 'update' value
                        coro._value = retval

                    if isinstance(retval, types.GeneratorType):
                        # push current generator onto stack and
                        # activate new generator
                        coro._callers.append((coro._generator, coro._value))
                        coro._generator = retval
                        coro._value = None
                    self._sched_cv.release()

    def terminate(self):
        """Terminate (singleton) instance of AsynCoro. This 'kills'
        all running coroutines.
        """
        self._notifier.terminate()
        self._sched_cv.acquire()
        self._terminate = True
        self._sched_cv.notify()
        self._sched_cv.release()
        self._complete.wait()
        logging.debug('AsynCoro terminated')

    def join(self):
        """Wait for currently scheduled coroutines to finish. AsynCoro
        continues to execute, so new coroutines can be added if necessary.
        """
        self._sched_cv.acquire()
        for coro in self._coros.itervalues():
            logging.debug('waiting for %s', coro.name)
        self._sched_cv.release()

        self._complete.wait()

class _AsyncNotifier(object):
    """Asynchronous I/O notifier, to be used with AsynCoroSocket (and
    coroutines) and AsynCoro.

    We use separate thread for _AsyncNotifier so AsynCoro users don't
    prevent (by taking too much time before yielding) notifier from
    processing events.
    """

    __metaclass__ = MetaSingleton
    __instance = None

    _Readable = None
    _Writable = None
    _Hangup = None
    _Error = None

    @classmethod
    def instance(cls):
        """Returns instance of AsynCoro. This method should be called
        only after initializing AsynCoro.
        """
        return cls.__instance

    def __init__(self, poll_interval=5):
        if self.__class__.__instance is None:
            if poll_interval < 1:
                logging.warning('invalid poll_interval; using 1 as poll_interval')
                poll_interval = 1
            elif poll_interval > 10:
                logging.warning('invalid poll_interval; using 10 as poll_interval')
                poll_interval = 10
            self.__class__.__instance = self

            self._cmd_rsock, self._cmd_wsock = _AsyncNotifier._socketpair()

            if hasattr(select, 'epoll'):
                self._poller = select.epoll()
                self.__class__._Readable = select.EPOLLIN
                self.__class__._Writable = select.EPOLLOUT
                self.__class__._Hangup = select.EPOLLHUP
                self.__class__._Error = select.EPOLLHUP | select.EPOLLERR
            elif hasattr(select, 'kqueue'):
                self._poller = _KQueueNotifier()
                self.__class__._Readable = select.KQ_FILTER_READ
                self.__class__._Writable = select.KQ_FILTER_WRITE
                self.__class__._Hangup = select.KQ_EV_EOF
                self.__class__._Error = select.KQ_EV_ERROR
            elif hasattr(select, 'poll'):
                self._poller = select.poll()
                self.__class__._Readable = select.POLLIN
                self.__class__._Writable = select.POLLOUT
                self.__class__._Hangup = select.POLLHUP
                self.__class__._Error = select.POLLHUP | select.POLLERR
            else:
                self._poller = _SelectNotifier(self._cmd_rsock, self._cmd_wsock)
                self.__class__._Readable = 0x01
                self.__class__._Writable = 0x04
                self.__class__._Hangup = 0
                self.__class__._Error = 0x10

            self._fds = {}
            self._terminate = False
            self._timeouts = []
            self._timeout_fds = []
            self._lock = threading.Lock()
            self._complete = threading.Event()

            self._notifier_thread = threading.Thread(target=self._notifier, args=(poll_interval,))
            self._notifier_thread.daemon = True
            self._notifier_thread.start()

    def _notifier(self, poll_interval):
        """Calls 'task' method of registered fds when there is a
        read/write event for it. Since coroutines can do only one
        thing at a time, only one of read/write tasks can be done.
        """
        self._cmd_rsock = AsynCoroSocket(self._cmd_rsock)
        self.modify(self._cmd_rsock, _AsyncNotifier._Readable)
        setattr(self._cmd_rsock, '_task', lambda: None)
        self._cmd_wsock.setblocking(0)

        timeout = poll_interval
        while not self._terminate:
            try:
                events = self._poller.poll(timeout)
            except:
                logging.debug('poll failed')
                logging.debug(traceback.format_exc())
                # wait a bit to prevent tight loops
                time.sleep(poll_interval)
                continue
            now = _time()
            self._lock.acquire()
            events = [(self._fds.get(fileno, None), event) for fileno, event in events]
            self._lock.release()
            try:
                for fd, event in events:
                    if fd is None:
                        if event != _AsyncNotifier._Hangup:
                            logging.debug('invalid fd for event %s', event)
                        continue
                    if event == _AsyncNotifier._Readable:
                        # logging.debug('fd %s is readable', fd._fileno)
                        if fd._task is None:
                            logging.error('fd %s is not registered for read?', fd._fileno)
                        else:
                            fd._task()
                    elif event == _AsyncNotifier._Writable:
                        # logging.debug('fd %s is writable', fd._fileno)
                        if fd._task is None:
                            logging.error('fd %s is not registered for write?', fd._fileno)
                        else:
                            fd._task()

                    # if event & _AsyncNotifier._Hangup:
                    #     logging.debug('hangup on %s', fd._fileno)
            except:
                logging.debug(traceback.format_exc())

            self._lock.acquire()
            while self._timeouts and self._timeouts[0] <= now:
                fd = self._timeout_fds[0]
                if fd._timeout_id == self._timeouts[0]:
                    fd._timed_out()
                    fd._timeout_id = None
                del self._timeouts[0]
                del self._timeout_fds[0]
            if self._timeouts:
                now = _time()
                timeout = self._timeouts[0] - now
                if timeout < 0.01:
                    timeout = 0.01
            else:
                timeout = poll_interval
            self._lock.release()

        self._cmd_rsock.close()
        self._cmd_wsock.close()
        if hasattr(self._poller, 'terminate'):
            self._poller.terminate()
        else:
            self._lock.acquire()
            for fd in self._fds.itervalues():
                try:
                    self._poller.unregister(fd._fileno)
                except:
                    logging.warning('unregister of %s failed with %s',
                                    fd._fileno, traceback.format_exc())
            self._lock.release()
        self._poller = None
        iocp = getattr(sys.modules[__name__], '_IOCPNotifier', None)
        if isinstance(iocp, MetaSingleton):
            iocp.instance().terminate()
        self._complete.set()
        logging.debug('AsyncNotifier terminated')

    def _add_timeout(self, fd):
        if fd._timeout:
            timeout = _time() + fd._timeout
            self._lock.acquire()
            i = bisect_left(self._timeouts, timeout)
            self._timeouts.insert(i, timeout)
            self._timeout_fds.insert(i, fd)
            fd._timeout_id = timeout
            self._lock.release()
        else:
            fd._timeout_id = None

    def _del_timeout(self, fd):
        if fd._timeout_id:
            self._lock.acquire()
            i = bisect_left(self._timeouts, fd._timeout_id)
            # in case of identical timeouts (unlikely?), search for
            # correct index where fd is
            for i in xrange(i, len(self._timeouts)):
                if self._timeout_fds[i] == fd:
                    # assert fd._timeout_id == self._timeouts[i]
                    del self._timeouts[i]
                    del self._timeout_fds[i]
                    fd._timeout_id = None
                    break
                if fd._timeout_id != self._timeouts[i]:
                    logging.warning('fd %s with %s is not found', fd._fileno, fd._timeout_id)
                    break
            self._lock.release()

    def register(self, fd, event=0):
        self._lock.acquire()
        # if fd in self._fds:
        #     self._lock.release()
        #     logging.warning('fd %s is already registered', fd._fileno)
        #     return
        self._fds[fd._fileno] = fd
        self._lock.release()
        if event:
            self._add_timeout(fd)
        try:
            self._poller.register(fd._fileno, event)
        except:
            logging.warning('register of %s for %s failed with %s',
                            fd._fileno, event, traceback.format_exc())

    def unregister(self, fd):
        self._lock.acquire()
        if self._fds.pop(fd._fileno, None) is None:
            self._lock.release()
            logging.debug('fd %s is not registered', fd._fileno)
            return
        self._lock.release()
        self._del_timeout(fd)
        try:
            self._poller.unregister(fd._fileno)
        except:
            logging.warning('unregister of %s failed with %s', fd._fileno, traceback.format_exc())

    def modify(self, fd, event):
        if event:
            self._add_timeout(fd)
        else:
            self._del_timeout(fd)
        try:
            self._poller.modify(fd._fileno, event)
        except:
            logging.warning('modify of %s for %s failed with %s',
                            fd._fileno, event, traceback.format_exc())

    def terminate(self):
        self._terminate = True
        self._cmd_wsock.send('x')
        self._complete.wait()

    @staticmethod
    def _socketpair():
        srv_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv_sock.bind(('127.0.0.1', 0))
        srv_sock.listen(1)

        sock1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        conn_thread = threading.Thread(target=lambda sock, (addr, port): sock.connect((addr, port)),
                                       args=(sock1, srv_sock.getsockname()))
        conn_thread.daemon = True
        conn_thread.start()
        sock2, caddr = srv_sock.accept()
        srv_sock.close()
        return (sock1, sock2)

class _KQueueNotifier(object):
    """Internal use only.
    """

    __metaclass__ = MetaSingleton

    def __init__(self):
        if not hasattr(self, 'poller'):
            self.poller = select.kqueue()
            self.events = {}

    def register(self, fid, event):
        self.events[fid] = event
        self.update(fid, event, select.KQ_EV_ADD)

    def unregister(self, fid):
        event = self.events.pop(fid, None)
        if event is not None:
            self.update(fid, event, select.KQ_EV_DELETE)

    def modify(self, fid, event):
        self.unregister(fid)
        self.register(fid, event)

    def update(self, fid, event, flags):
        kevents = []
        if event == _AsyncNotifier._Readable:
            kevents = [select.kevent(fid, filter=select.KQ_FILTER_READ, flags=flags)]
        elif event == _AsyncNotifier._Writable:
            kevents = [select.kevent(fid, filter=select.KQ_FILTER_WRITE, flags=flags)]

        if kevents:
            self.poller.control(kevents, 0)

    def poll(self, timeout):
        kevents = self.poller.control(None, 500, timeout)
        events = [(kevent.ident, kevent.filter) for kevent in kevents]
        return events

class _SelectNotifier(object):
    """Internal use only.
    """

    __metaclass__ = MetaSingleton

    def __init__(self, cmd_rsock, cmd_wsock):
        if not hasattr(self, 'poller'):
            self.poller = select.select
            self.rset = set()
            self.wset = set()
            self.xset = set()

            self.cmd_rsock = cmd_rsock
            self.cmd_wsock = cmd_wsock
            self.read_fd = self.cmd_rsock.fileno()
            self.rset.add(self.read_fd)

    def register(self, fid, event, update=True):
        if event:
            if event == _AsyncNotifier._Readable:
                self.rset.add(fid)
            elif event == _AsyncNotifier._Writable:
                self.wset.add(fid)
            elif event & _AsyncNotifier._Error:
                self.xset.add(fid)
            if update:
                self.cmd_wsock.send('r')

    def unregister(self, fid, update=True):
        self.rset.discard(fid)
        self.wset.discard(fid)
        self.xset.discard(fid)
        if update:
            self.cmd_wsock.send('u')

    def modify(self, fid, event):
        self.unregister(fid, update=False)
        self.register(fid, event, update=False)
        self.cmd_wsock.send('m')

    def poll(self, timeout):
        rlist, wlist, xlist = self.poller(self.rset, self.wset, self.xset, timeout)
        events = {}
        for fid in rlist:
            events[fid] = _AsyncNotifier._Readable
        for fid in wlist:
            events[fid] = _AsyncNotifier._Writable
        for fid in xlist:
            events[fid] = _AsyncNotifier._Error

        if events.pop(self.read_fd, None) == _AsyncNotifier._Readable:
            cmd = self.cmd_rsock.recv(128)
        return events.iteritems()

    def terminate(self):
        self.rset = self.wset = self.xset = None

class RepeatTimer(threading.Thread):
    """Timer that calls given function every 'interval' seconds. The
    timer can be stopped, (re)started, reset to different interval
    etc. until terminated.

    This is not used in AsynCoro or dispy, so likely to be discarded.
    """
    def __init__(self, interval, function, args=(), kwargs={}):
        threading.Thread.__init__(self)
        if interval is not None:
            interval = float(interval)
            assert interval > 0
        self._interval = None
        self._func = function
        self._args = args
        self._kwargs = kwargs
        self._terminate = False
        self._tick = threading.Event()
        self._active = threading.Event()
        self._active.clear()
        self.daemon = True
        self.start(interval)

    def start(self, _interval=None):
        if self._interval is None and _interval is not None:
            self._interval = _interval
            self._active.set()
            super(RepeatTimer, self).start()
        elif self._interval is not None:
            self._active.set()

    def stop(self):
        self._active.clear()
        self._tick.set()
        self._tick.clear()

    def set_interval(self, interval):
        try:
            interval = float(interval)
            assert interval > 0
        except:
            return
        if self._interval is None:
            self.start(interval)
        else:
            self._interval = interval
            self._active.clear()
            self._tick.set()
            self._tick.clear()
            self._active.set()

    def terminate(self):
        self._terminate = True
        self._active.set()

    def run(self):
        while not self._terminate:
            self._tick.wait(self._interval)
            if not self._active.is_set():
                self._active.wait()
                continue
            try:
                self._func(*self._args, **self._kwargs)
            except:
                pass
