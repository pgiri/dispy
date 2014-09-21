"""
This file is part of asyncoro; see http://asyncoro.sourceforge.net
for details.

This module provides framework for concurrent, asynchronous network
programming with coroutines, asynchronous completions and message
passing. Other modules in asyncoro download package provide API for
distributed programming, asynchronous pipes, distributed concurrent
communicating processes.
"""

__author__ = "Giridhar Pemmasani (pgiri@yahoo.com)"
__email__ = "pgiri@yahoo.com"
__copyright__ = "Copyright (c) 2012-2014 Giridhar Pemmasani"
__contributors__ = []
__maintainer__ = "Giridhar Pemmasani (pgiri@yahoo.com)"
__license__ = "MIT"
__url__ = "http://asyncoro.sourceforge.net"
__status__ = "Production"
__version__ = "3.0"

__all__ = ['AsyncSocket', 'AsynCoroSocket', 'Coro', 'AsynCoro',
           'Lock', 'RLock', 'Event', 'Condition', 'Semaphore',
           'HotSwapException', 'MonitorException', 'Location', 'Channel',
           'CategorizeMessages', 'AsyncThreadPool', 'AsyncDBCursor',
           'MetaSingleton', 'logger', 'serialize', 'unserialize']

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
import os
import stat
import hashlib
import platform
import random
import ssl
from heapq import heappush, heappop
from bisect import bisect_left
import Queue
import atexit
import collections
import cPickle as pickle
import copy

logger = logging.getLogger('asyncoro')
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(message)s'))
logger.addHandler(handler)
del handler

if platform.system() == 'Windows':
    from errno import WSAEINPROGRESS as EINPROGRESS
    from errno import WSAEWOULDBLOCK as EWOULDBLOCK
    from errno import WSAEINVAL as EINVAL
    from time import clock as _time
    _time()
else:
    from errno import EINPROGRESS
    from errno import EWOULDBLOCK
    from errno import EINVAL
    from time import time as _time

def serialize(obj):
    return pickle.dumps(obj, pickle.HIGHEST_PROTOCOL)

def unserialize(pkl):
    return pickle.loads(pkl)

class MetaSingleton(type):
    __instance = None
    def __call__(cls, *args, **kwargs):
        if cls.__instance is None:
            cls.__instance = super(MetaSingleton, cls).__call__(*args, **kwargs)
        return cls.__instance

class _AsyncSocket(object):
    """Base class for use with AsynCoro, for asynchronous I/O
    completion and coroutines. This class is for internal use
    only. Use AsyncSocket, defined below, instead.
    """

    __slots__ = ('_rsock', '_keyfile', '_certfile', '_ssl_version', '_fileno', '_timeout',
                 '_timeout_id', '_read_coro', '_read_task', '_read_result', '_write_coro',
                 '_write_task', '_write_result', '_asyncoro', '_notifier', 'recvall', 'sendall',
                 'recv_msg', 'send_msg', '_blocking', 'recv', 'send', 'recvfrom', 'sendto',
                 'accept', 'connect')

    _default_timeout = None

    def __init__(self, sock, blocking=False, keyfile=None, certfile=None,
                 ssl_version=ssl.PROTOCOL_SSLv23):
        """Setup socket for use wih asyncoro.

        @blocking=True implies synchronous sockets and blocking=False
        implies asynchronous sockets.

        @keyfile, @certfile and @ssl_version are as per ssl's wrap_socket
        method.

        Only methods without leading underscore should be used; other
        attributes are for internal use only. In addition to usual
        socket I/O methods, AsyncSocket implemnents 'recvall',
        'send_msg', 'recv_msg' and 'unwrap' methods.
        """

        if isinstance(sock, AsyncSocket):
            logger.warning('Socket %s is already AsyncSocket', sock._fileno)
            for k in sock.__slots__:
                setattr(self, k, getattr(sock, k))
        else:
            self._rsock = sock
            self._keyfile = keyfile
            self._certfile = certfile
            self._ssl_version = ssl_version
            self._fileno = sock.fileno()
            self._timeout = 0
            self._timeout_id = None
            self._read_coro = None
            self._read_task = None
            self._read_result = None
            self._write_coro = None
            self._write_task = None
            self._write_result = None
            self._notifier = None

            self.recvall = None
            self.sendall = None
            self.recv_msg = None
            self.send_msg = None

            self._blocking = None
            self.setblocking(blocking)
            # technically, we should set socket to blocking if
            # _default_timeout is None, but ignore this case
            if _AsyncSocket._default_timeout:
                self.settimeout(_AsyncSocket._default_timeout)

    def __getattr__(self, name):
        return getattr(self._rsock, name)

    def setblocking(self, blocking):
        if blocking:
            blocking = True
        else:
            blocking = False
        if self._blocking == blocking:
            return
        self._blocking = blocking
        if self._blocking:
            self._unregister()
            self._rsock.setblocking(1)
            if self._certfile:
                self._rsock = ssl.wrap_socket(self._rsock, keyfile=self._keyfile,
                                              certfile=self._certfile,
                                              ssl_version=self._ssl_version)
            for name in ['recv', 'send', 'recvfrom', 'sendto', 'accept', 'connect']:
                setattr(self, name, getattr(self._rsock, name))
            if self._rsock.type & socket.SOCK_STREAM:
                self.recvall = self._sync_recvall
                self.sendall = self._sync_sendall
                self.recv_msg = self._sync_recv_msg
                self.send_msg = self._sync_send_msg
            self._notifier = None
        else:
            self._rsock.setblocking(0)
            self.recv = self._async_recv
            self.send = self._async_send
            self.recvfrom = self._async_recvfrom
            self.sendto = self._async_sendto
            self.accept = self._async_accept
            self.connect = self._async_connect
            if self._rsock.type & socket.SOCK_STREAM:
                self.recvall = self._async_recvall
                self.sendall = self._async_sendall
                self.recv_msg = self._async_recv_msg
                self.send_msg = self._async_send_msg
            self._notifier = _AsyncNotifier.instance()
            self._register()

    def _register(self):
        """Internal use only.
        """
        pass

    def _unregister(self):
        """Internal use only.
        """
        if self._notifier:
            self._notifier.unregister(self)
            self._notifier = None

    def close(self):
        """'close' must be called when done with socket.
        """
        self._unregister()
        if self._rsock:
            self._rsock.close()
            self._rsock = None
        self._read_coro = self._write_coro = None

    def unwrap(self):
        """Get rid of AsyncSocket setup and return underlying socket
        object.
        """
        self._unregister()
        self._notifier = None
        self._read_coro = self._write_coro = None
        sock, self._rsock = self._rsock, None
        return sock

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, trace):
        self.close()

    def __cmp__(self, other):
        if isinstance(other, AsyncSocket):
            return cmp(self._fileno, other._fileno)
        return 1

    def setdefaulttimeout(self, timeout):
        if isinstance(timeout, (int, float)) and timeout > 0:
            self._rsock.setdefaulttimeout(timeout)
            self._default_timeout = timeout
        else:
            logger.warning('invalid timeout %s ignored', timeout)

    def getdefaulttimeout(self):
        if self._blocking:
            return self._rsock.getdefalttimeout()
        else:
            return _AsyncSocket._default_timeout

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
                logger.warning('invalid timeout %s ignored' % timeout)

    def gettimeout(self):
        if self._blocking:
            return self._rsock.gettimeout()
        else:
            return self._timeout

    def _timed_out(self):
        """Internal use only.
        """
        if self._read_coro:
            buf = None
            if isinstance(self._read_result, bytearray):
                view = self._read_task.args[1]
                n = len(self._read_result) - len(view)
                if n > 0:
                    buf = bytes(self._read_result[:n])
            if buf:
                self._read_coro._proceed_(buf)
            else:
                self._read_coro.throw(socket.timeout('timed out'))
            self._notifier.clear(self, _AsyncPoller._Read)
            self._read_task = self._read_result = self._read_coro = None
        if self._write_coro:
            sent = 0
            if isinstance(self._write_result, buffer):
                sent = self._write_task.args[1] - len(self._write_result)
            if sent:
                self._write_coro._proceed_(sent)
            else:
                self._write_coro.throw(socket.timeout('timed out'))
            self._notifier.clear(self, _AsyncPoller._Write)
            self._write_task = self._write_result = self._write_coro = None

    def _eof(self):
        """Internal use only.
        """
        if self._read_task:
            self._read_task()

    def _async_recv(self, bufsize, *args):
        """Internal use only; use 'recv' with 'yield' instead.

        Asynchronous version of socket recv method.
        """
        def _recv(self, bufsize, *args):
            try:
                buf = self._rsock.recv(bufsize, *args)
            except ssl.SSLError as err:
                if err.args[0] != ssl.SSL_ERROR_WANT_READ:
                    raise socket.error(err)
            except:
                self._notifier.clear(self, _AsyncPoller._Read)
                self._read_task = None
                coro, self._read_coro = self._read_coro, None
                coro.throw(*sys.exc_info())
            else:
                self._notifier.clear(self, _AsyncPoller._Read)
                self._read_task = None
                coro, self._read_coro = self._read_coro, None
                coro._proceed_(buf)

        self._read_task = functools.partial(_recv, self, bufsize, *args)
        self._read_coro = AsynCoro.cur_coro()
        self._read_coro._await_()
        self._notifier.add(self, _AsyncPoller._Read)
        if self._certfile and self._rsock.pending():
            try:
                buf = self._rsock.recv(bufsize)
            except socket.error as err:
                if err.args[0] != EWOULDBLOCK:
                    self._read_task = None
                    self._notifier.clear(self, _AsyncPoller._Read)
                    coro, self._read_coro = self._read_coro, None
                    coro.throw(*sys.exc_info())
            else:
                if buf:
                    self._read_task = None
                    self._notifier.clear(self, _AsyncPoller._Read)
                    coro, self._read_coro = self._read_coro, None
                    coro._proceed_(buf)

    def _async_recvall(self, bufsize, *args):
        """Internal use only; use 'recvall' with 'yield' instead.

        Receive exactly bufsize bytes. If socket's timeout is set and
        it expires before all the data could be read, it returns
        partial data read if any data has been read at all. If no data
        has been read before timeout, then it causes 'socket.timeout'
        exception to be thrown.
        """
        def _recvall(self, view, *args):
            try:
                recvd = self._rsock.recv_into(view, len(view), *args)
            except ssl.SSLError as err:
                if err.args[0] != ssl.SSL_ERROR_WANT_READ:
                    raise socket.error(err)
            except:
                self._notifier.clear(self, _AsyncPoller._Read)
                self._read_task = self._read_result = None
                coro, self._read_coro = self._read_coro, None
                coro.throw(*sys.exc_info())
            else:
                if recvd:
                    view = view[recvd:]
                    if len(view) == 0:
                        buf = str(self._read_result)
                        self._notifier.clear(self, _AsyncPoller._Read)
                        self._read_task = self._read_result = None
                        coro, self._read_coro = self._read_coro, None
                        coro._proceed_(buf)
                    else:
                        self._read_task = functools.partial(_recvall, self, view, *args)
                else:
                    self._notifier.clear(self, _AsyncPoller._Read)
                    self._read_task = self._read_result = None
                    coro, self._read_coro = self._read_coro, None
                    coro._proceed_('')

        self._read_result = bytearray(bufsize)
        view = memoryview(self._read_result)
        self._read_task = functools.partial(_recvall, self, view, *args)
        self._read_coro = AsynCoro.cur_coro()
        self._read_coro._await_()
        self._notifier.add(self, _AsyncPoller._Read)
        if self._certfile and self._rsock.pending():
            try:
                recvd = self._rsock.recv_into(view, bufsize)
            except socket.error as err:
                if err.args[0] != EWOULDBLOCK:
                    self._read_task = self._read_result = None
                    self._notifier.clear(self, _AsyncPoller._Read)
                    coro, self._read_coro = self._read_coro, None
                    coro.throw(*sys.exc_info())
            else:
                if recvd == bufsize:
                    buf = str(self._read_result)
                    self._read_task = self._read_result = None
                    self._notifier.clear(self, _AsyncPoller._Read)
                    coro, self._read_coro = self._read_coro, None
                    coro._proceed_(buf)
                elif recvd:
                    view = view[recvd:]
                    self._read_task = functools.partial(_recvall, self, view, *args)

    def _sync_recvall(self, bufsize, *args):
        """Internal use only; use 'recvall' instead.

        Synchronous version of async_recvall.
        """
        self._read_result = bytearray(bufsize)
        view = memoryview(self._read_result)
        while len(view) > 0:
            recvd = self._rsock.recv_into(view, *args)
            if not recvd:
                self._read_result = None
                return ''
            view = view[recvd:]
        buf, self._read_result = str(self._read_result), None
        return buf

    def _async_recvfrom(self, *args):
        """Internal use only; use 'recvfrom' with 'yield' instead.

        Asynchronous version of socket recvfrom method.
        """
        def _recvfrom(self, *args):
            try:
                res = self._rsock.recvfrom(*args)
            except:
                self._notifier.clear(self, _AsyncPoller._Read)
                self._read_task = None
                coro, self._read_coro = self._read_coro, None
                coro.throw(*sys.exc_info())
            else:
                self._notifier.clear(self, _AsyncPoller._Read)
                self._read_task = None
                coro, self._read_coro = self._read_coro, None
                coro._proceed_(res)

        self._read_task = functools.partial(_recvfrom, self, *args)
        self._read_coro = AsynCoro.cur_coro()
        self._read_coro._await_()
        self._notifier.add(self, _AsyncPoller._Read)

    def _async_send(self, *args):
        """Internal use only; use 'send' with 'yield' instead.

        Asynchronous version of socket send method.
        """
        def _send(self, *args):
            try:
                sent = self._rsock.send(*args)
            except:
                self._notifier.clear(self, _AsyncPoller._Write)
                self._write_task = None
                coro, self._write_coro = self._write_coro, None
                coro.throw(*sys.exc_info())
            else:
                self._notifier.clear(self, _AsyncPoller._Write)
                self._write_task = None
                coro, self._write_coro = self._write_coro, None
                coro._proceed_(sent)

        self._write_task = functools.partial(_send, self, *args)
        self._write_coro = AsynCoro.cur_coro()
        self._write_coro._await_()
        self._notifier.add(self, _AsyncPoller._Write)

    def _async_sendto(self, *args):
        """Internal use only; use 'sendto' with 'yield' instead.

        Asynchronous version of socket sendto method.
        """
        def _sendto(self, *args):
            try:
                sent = self._rsock.sendto(*args)
            except:
                self._notifier.clear(self, _AsyncPoller._Write)
                self._write_task = None
                coro, self._write_coro = self._write_coro, None
                coro.throw(*sys.exc_info())
            else:
                self._notifier.clear(self, _AsyncPoller._Write)
                self._write_task = None
                coro, self._write_coro = self._write_coro, None
                coro._proceed_(sent)

        self._write_task = functools.partial(_sendto, self, *args)
        self._write_coro = AsynCoro.cur_coro()
        self._write_coro._await_()
        self._notifier.add(self, _AsyncPoller._Write)

    def _async_sendall(self, data):
        """Internal use only; use 'sendall' with 'yield' instead.

        Asynchronous version of socket sendall method. If socket's
        timeout is set and it expires before all the data could be
        sent, it returns the length of data sent if any data is
        sent. If no data has been sent before timeout, then it causes
        'socket.timeout' exception to be thrown.
        """
        def _sendall(self, data_len):
            try:
                sent = self._rsock.send(self._write_result)
                if sent < 0:
                    raise socket.error('hangup')
            except:
                self._notifier.clear(self, _AsyncPoller._Write)
                self._write_task = self._write_result = None
                coro, self._write_coro = self._write_coro, None
                coro.throw(*sys.exc_info())
            else:
                if sent > 0:
                    self._write_result = self._write_result[sent:]
                    if len(self._write_result) == 0:
                        self._notifier.clear(self, _AsyncPoller._Write)
                        self._write_task = self._write_result = None
                        coro, self._write_coro = self._write_coro, None
                        coro._proceed_(None)

        self._write_result = buffer(data)
        self._write_task = functools.partial(_sendall, self, len(data))
        self._write_coro = AsynCoro.cur_coro()
        self._write_coro._await_()
        self._notifier.add(self, _AsyncPoller._Write)

    def _sync_sendall(self, data):
        """Internal use only; use 'sendall' instead.

        Synchronous version of async_sendall.
        """
        # TODO: is socket's sendall better?
        buf = buffer(data)
        while len(buf) > 0:
            sent = self._rsock.send(buf)
            if sent < 0:
                raise socket.error('hangup')
            buf = buf[sent:]
        return None

    def _async_accept(self):
        """Internal use only; use 'accept' with 'yield' instead.

        Asynchronous version of socket accept method. Socket in
        returned pair is asynchronous socket (instance of
        AsyncSocket with blocking=False).
        """
        def _accept(self):
            conn, addr = self._rsock.accept()
            self._read_task = None
            self._notifier.unregister(self)

            if self._certfile:
                def _ssl_handshake(conn, addr):
                    try:
                        conn._rsock.do_handshake()
                    except ssl.SSLError as err:
                        if err.args[0] in (ssl.SSL_ERROR_WANT_READ, ssl.SSL_ERROR_WANT_WRITE):
                            pass
                        else:
                            conn._read_task = conn._write_task = None
                            coro, conn._read_coro = conn._read_coro, None
                            conn._write_coro = None
                            conn.close()
                            coro.throw(*sys.exc_info())
                    else:
                        conn._read_task = conn._write_task = None
                        coro, conn._read_coro = conn._read_coro, None
                        conn._notifier.clear(conn)
                        coro._proceed_((conn, addr))
                conn = AsyncSocket(conn, blocking=False, keyfile=self._keyfile,
                                   certfile=self._certfile, ssl_version=self._ssl_version)
                conn._rsock = ssl.wrap_socket(conn._rsock, keyfile=self._keyfile,
                                              certfile=self._certfile, server_side=True,
                                              do_handshake_on_connect=False,
                                              ssl_version=self._ssl_version)
                conn._read_task = conn._write_task = functools.partial(_ssl_handshake, conn, addr)
                conn._read_coro = conn._write_coro = self._read_coro
                self._read_coro = None
                conn._notifier.add(conn, _AsyncPoller._Read | _AsyncPoller._Write)
                conn._read_task()
            else:
                coro, self._read_coro = self._read_coro, None
                conn = AsyncSocket(conn, blocking=False)
                coro._proceed_((conn, addr))

        self._read_task = functools.partial(_accept, self)
        self._read_coro = AsynCoro.cur_coro()
        self._read_coro._await_()
        self._notifier.add(self, _AsyncPoller._Read)

    def _async_connect(self, *args):
        """Internal use only; use 'connect' with 'yield' instead.

        Asynchronous version of socket connect method.
        """
        def _connect(self, *args):
            err = self._rsock.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
            if err:
                self._notifier.unregister(self)
                self._write_task = None
                coro, self._write_coro = self._write_coro, None
                coro.throw(socket.error(err))
            elif self._certfile:
                def _ssl_handshake(self):
                    try:
                        self._rsock.do_handshake()
                    except ssl.SSLError as err:
                        if err.args[0] in (ssl.SSL_ERROR_WANT_READ, ssl.SSL_ERROR_WANT_WRITE):
                            pass
                        else:
                            self._notifier.unregister(self)
                            self._read_task = self._write_task = None
                            coro, self._write_coro = self._write_coro, None
                            self._read_coro = None
                            self.close()
                            coro.throw(*sys.exc_info())
                    else:
                        self._notifier.clear(self)
                        self._read_task = self._write_task = None
                        coro, self._write_coro = self._write_coro, None
                        self._read_coro = None
                        coro._proceed_(0)

                self._rsock = ssl.wrap_socket(self._rsock, keyfile=self._keyfile,
                                              certfile=self._certfile, server_side=False,
                                              do_handshake_on_connect=False)
                self._read_task = self._write_task = functools.partial(_ssl_handshake, self)
                self._read_coro = self._write_coro
                self._notifier.add(self, _AsyncPoller._Read)
                self._write_task()
            else:
                self._write_task = None
                coro, self._write_coro = self._write_coro, None
                self._notifier.clear(self, _AsyncPoller._Write)
                coro._proceed_(0)

        self._write_task = functools.partial(_connect, self, *args)
        self._write_coro = AsynCoro.cur_coro()
        self._write_coro._await_()
        self._notifier.add(self, _AsyncPoller._Write)
        try:
            self._rsock.connect(*args)
        except socket.error as e:
            if e.args[0] not in [EINPROGRESS, EWOULDBLOCK]:
                raise

    def _async_send_msg(self, data):
        """Internal use only; use 'send_msg' with 'yield' instead.

        Messages are tagged with length of the data, so on the
        receiving side, recv_msg knows how much data to receive.
        """
        yield self.sendall(struct.pack('>L', len(data)) + data)

    def _sync_send_msg(self, data):
        """Internal use only; use 'send_msg' instead.

        Synchronous version of async_send_msg.
        """
        return self._sync_sendall(struct.pack('>L', len(data)) + data)

    def _async_recv_msg(self):
        """Internal use only; use 'recv_msg' with 'yield' instead.

        Message is tagged with length of the payload (data). This
        method receives length of payload, then the payload and
        returns the payload.
        """
        n = struct.calcsize('>L')
        try:
            data = yield self.recvall(n)
        except socket.error as err:
            if err.args[0] == 'hangup':
                raise StopIteration('')
            else:
                raise
        if len(data) != n:
            raise StopIteration('')
        n = struct.unpack('>L', data)[0]
        assert n >= 0
        try:
            data = yield self.recvall(n)
        except socket.error as err:
            if err.args[0] == 'hangup':
                raise StopIteration('')
            else:
                raise
        if len(data) != n:
            raise StopIteration('')
        raise StopIteration(data)

    def _sync_recv_msg(self):
        """Internal use only; use 'recv_msg' instead.

        Synchronous version of async_recv_msg.
        """
        n = struct.calcsize('>L')
        try:
            data = self._sync_recvall(n)
        except socket.error as err:
            if err.args[0] == 'hangup':
                return ''
            else:
                raise
        if len(data) != n:
            return ''
        n = struct.unpack('>L', data)[0]
        assert n >= 0
        try:
            data = self._sync_recvall(n)
        except socket.error as err:
            if err.args[0] == 'hangup':
                return ''
            else:
                raise
        if len(data) != n:
            return ''
        return data

    def create_connection(self, host_port, timeout=None, source_address=None):
        if timeout is not None:
            self.settimeout(timeout)
        if source_address is not None:
            self._rsock.bind(source_address)
        yield self.connect(host_port)

if platform.system() == 'Windows':
    # use IOCP if pywin32 (http://pywin32.sf.net) is installed
    try:
        import win32file
        import win32event
        import pywintypes
        import winerror
    except:
        logger.warning('Could not load pywin32 for I/O Completion Ports; ' \
                       'using inefficient polling for sockets')
    else:
        # for UDP we need 'select' polling (pywin32 doesn't yet support
        # UDP); _AsyncPoller below is combination of the other
        # _AsyncPoller for epoll/poll/kqueue/select and _SelectNotifier
        # below. (Un)fortunately, most of it is duplicate code
        class _AsyncPoller(object):
            """Internal use only.
            """

            __metaclass__ = MetaSingleton
            __instance = None

            _Read = 0x1
            _Write = 0x2
            _Error = 0x4

            @classmethod
            def instance(cls):
                # assert cls.__instance is not None
                return cls.__instance

            def __init__(self, iocp_notifier):
                if not hasattr(self, 'poller'):
                    self.__class__.__instance = self
                    self._fds = {}
                    self._events = {}
                    self._lock = threading.Lock()
                    self.polling = False
                    self._terminate = False
                    self.rset = set()
                    self.wset = set()
                    self.xset = set()
                    self.iocp_notifier = iocp_notifier
                    self.cmd_rsock, self.cmd_wsock = _AsyncPoller._socketpair()
                    self.cmd_rsock.setblocking(0)
                    self.cmd_wsock.setblocking(0)
                    self.poller = select.select
                    self.poll_thread = threading.Thread(target=self.poll)
                    self.poll_thread.daemon = True
                    self.poll_thread.start()

            def unregister(self, fd, update=True):
                fid = fd._fileno
                if fd._timeout:
                    self.iocp_notifier._del_timeout(fd)
                self._lock.acquire()
                if update:
                    if self._fds.pop(fid, None) != fd:
                        self._lock.release()
                        logger.debug('fd %s is not registered', fid)
                        return
                    event = self._events.pop(fid, 0)
                else:
                    event = self._events.get(fd, 0)
                self._lock.release()
                if event & _AsyncPoller._Read:
                    self.rset.discard(fid)
                if event & _AsyncPoller._Write:
                    self.wset.discard(fid)
                if event & _AsyncPoller._Error:
                    self.xset.discard(fid)
                if update and self.polling:
                    self.cmd_wsock.send('u')

            def add(self, fd, event):
                fid = fd._fileno
                if fd._timeout:
                    self.iocp_notifier._del_timeout(fd)
                cur_event = self._events.get(fid, 0)
                if cur_event & _AsyncPoller._Read:
                    self.rset.discard(fid)
                if cur_event & _AsyncPoller._Write:
                    self.wset.discard(fid)
                if cur_event & _AsyncPoller._Error:
                    self.xset.discard(fid)
                event |= cur_event
                self._events[fid] = event
                self._fds[fid] = fd
                if event:
                    if event & _AsyncPoller._Read:
                        self.rset.add(fid)
                    if event & _AsyncPoller._Write:
                        self.wset.add(fid)
                    if event & _AsyncPoller._Error:
                        self.xset.add(fid)
                    if fd._timeout:
                        self.iocp_notifier._add_timeout(fd)
                        self.iocp_notifier.interrupt(fd._timeout)
                if self.polling:
                    self.cmd_wsock.send('m')

            def clear(self, fd, event=0):
                fid = fd._fileno
                cur_event = self._events.get(fid, 0)
                if cur_event:
                    if cur_event & _AsyncPoller._Read:
                        self.rset.discard(fid)
                    if cur_event & _AsyncPoller._Write:
                        self.wset.discard(fid)
                    if cur_event & _AsyncPoller._Error:
                        self.xset.discard(fid)
                    if event:
                        cur_event &= ~event
                    else:
                        cur_event = 0
                    self._events[fid] = cur_event
                    if cur_event:
                        if cur_event & _AsyncPoller._Read:
                            self.rset.add(fid)
                        if cur_event & _AsyncPoller._Write:
                            self.wset.add(fid)
                        if cur_event & _AsyncPoller._Error:
                            self.xset.add(fid)
                    elif fd._timeout_id:
                        self.iocp_notifier._del_timeout(fd)
                    if self.polling:
                        self.cmd_wsock.send('m')

            def poll(self):
                self.cmd_rsock = AsyncSocket(self.cmd_rsock)
                setattr(self.cmd_rsock, '_read_task', lambda : self.cmd_rsock._rsock.recv(128))
                self.add(self.cmd_rsock, _AsyncPoller._Read)
                while True:
                    self.polling = True
                    rlist, wlist, xlist = self.poller(self.rset, self.wset, self.xset)
                    self.polling = False
                    if self._terminate:
                        break
                    events = {}
                    for fid in rlist:
                        events[fid] = _AsyncPoller._Read
                    for fid in wlist:
                        events[fid] = events.get(fid, 0) | _AsyncPoller._Write
                    for fid in xlist:
                        events[fid] = events.get(fid, 0) | _AsyncPoller._Error

                    self._lock.acquire()
                    events = [(self._fds.get(fid, None), event) \
                              for (fid, event) in events.iteritems()]
                    self._lock.release()
                    iocp_notify = False
                    for fd, event in events:
                        if fd is None:
                            logger.debug('invalid fd')
                            continue
                        if event & _AsyncPoller._Read:
                            if fd._read_task:
                                if fd != self.cmd_rsock:
                                    iocp_notify = True
                                fd._read_task()
                            else:
                                logger.debug('fd %s is not registered for reading!', fd._fileno)
                        if event & _AsyncPoller._Write:
                            if fd._write_task:
                                iocp_notify = True
                                fd._write_task()
                            else:
                                logger.debug('fd %s is not registered for writing!', fd._fileno)
                        if event & _AsyncPoller._Error:
                            if fd._read_coro:
                                fd._read_coro.throw(socket.error(_AsyncPoller._Error))
                            if fd._write_coro:
                                fd._write_coro.throw(socket.error(_AsyncPoller._Error))
                    if iocp_notify:
                        self.iocp_notifier.interrupt()

                self.rset = set()
                self.wset = set()
                self.xset = set()
                self.cmd_rsock.close()
                self.cmd_wsock.close()

            def terminate(self):
                self._terminate = True
                self.cmd_wsock.send('x')
                self.poll_thread.join()
                self.__class__.__instance = None

            @staticmethod
            def _socketpair():
                srv_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                srv_sock.bind(('127.0.0.1', 0))
                srv_sock.listen(1)

                sock1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                conn_thread = threading.Thread(target=lambda sock, addr_port: sock.connect(addr_port),
                                               args=(sock1, srv_sock.getsockname()))
                conn_thread.daemon = True
                conn_thread.start()
                sock2, caddr = srv_sock.accept()
                srv_sock.close()
                return (sock1, sock2)

        class _AsyncNotifier(object):
            """Internal use only.
            """

            __metaclass__ = MetaSingleton
            __instance = None

            _Block = win32event.INFINITE

            @classmethod
            def instance(cls, *args, **kwargs):
                if cls.__instance is None:
                    cls.__instance = cls(*args, **kwargs)
                return cls.__instance

            def __init__(self):
                if not hasattr(self, 'iocp'):
                    self.__class__.__instance = self
                    self.iocp = win32file.CreateIoCompletionPort(win32file.INVALID_HANDLE_VALUE,
                                                                 None, 0, 0)
                    self._timeouts = []
                    self.poll_timeout = 0
                    self._lock = threading.Lock()
                    self.async_poller = _AsyncPoller(self)
                    self.cmd_rsock, self.cmd_wsock = _AsyncPoller._socketpair()
                    self.cmd_wsock.setblocking(0)
                    self.cmd_rsock = AsyncSocket(self.cmd_rsock)
                    self.cmd_rsock_buf = win32file.AllocateReadBuffer(128)
                    self.cmd_rsock._read_overlap.object = self.cmd_rsock_recv
                    err, n = win32file.WSARecv(self.cmd_rsock._fileno, self.cmd_rsock_buf,
                                               self.cmd_rsock._read_overlap, 0)
                    if err and err != winerror.ERROR_IO_PENDING:
                        logger.warning('WSARecv error: %s', err)
                    logger.debug('poller: IOCP')

            def cmd_rsock_recv(self, err, n):
                if n == 0:
                    err = winerror.ERROR_CONNECTION_INVALID
                if err:
                    logger.warning('iocp cmd recv error: %s', err)
                err, n = win32file.WSARecv(self.cmd_rsock._fileno, self.cmd_rsock_buf,
                                           self.cmd_rsock._read_overlap, 0)
                if err and err != winerror.ERROR_IO_PENDING:
                    logger.warning('WSARecv error: %s', err)

            def interrupt(self, timeout=None):
                if timeout is None:
                    self.cmd_wsock.send('i')
                elif self.poll_timeout == _AsyncNotifier._Block or timeout < self.poll_timeout:
                    self.cmd_wsock.send('I')

            def register(self, handle, event=0):
                win32file.CreateIoCompletionPort(handle, self.iocp, 1, 0)

            def unregister(self, handle):
                pass

            def modify(self, fd, event):
                pass

            def poll(self, timeout):
                self._lock.acquire()
                if timeout == 0:
                    self.poll_timeout = 0
                elif self._timeouts:
                    self.poll_timeout = self._timeouts[0][0] - _time()
                    if self.poll_timeout < 0.0001:
                        self.poll_timeout = 0
                    elif timeout is not None:
                        self.poll_timeout = min(timeout, self.poll_timeout)
                elif timeout is None:
                    self.poll_timeout = _AsyncNotifier._Block
                else:
                    self.poll_timeout = timeout
                timeout = self.poll_timeout
                self._lock.release()
                if timeout and timeout != _AsyncNotifier._Block:
                    timeout = int(timeout * 1000)
                err, n, key, overlap = win32file.GetQueuedCompletionStatus(self.iocp, timeout)
                while err != winerror.WAIT_TIMEOUT:
                    if overlap and overlap.object:
                        overlap.object(err, n)
                    else:
                        logger.warning('invalid overlap!')
                    err, n, key, overlap = win32file.GetQueuedCompletionStatus(self.iocp, 0)
                self.poll_timeout = 0
                if timeout == 0:
                    now = _time()
                    self._lock.acquire()
                    while self._timeouts and self._timeouts[0][0] <= now:
                        fd_timeout, fd = self._timeouts.pop(0)
                        if fd._timeout_id == fd_timeout:
                            fd._timeout_id = None
                            fd._timed_out()
                    self._lock.release()

            def _add_timeout(self, fd):
                if fd._timeout:
                    self._lock.acquire()
                    fd._timeout_id = _time() + fd._timeout
                    i = bisect_left(self._timeouts, (fd._timeout_id, fd))
                    self._timeouts.insert(i, (fd._timeout_id, fd))
                    self._lock.release()
                else:
                    fd._timeout_id = None

            def _del_timeout(self, fd):
                if fd._timeout_id:
                    self._lock.acquire()
                    i = bisect_left(self._timeouts, (fd._timeout_id, fd))
                    while i < len(self._timeouts):
                        if self._timeouts[i] == (fd._timeout_id, fd):
                            del self._timeouts[i]
                            fd._timeout_id = None
                            break
                        if fd._timeout_id != self._timeouts[i][0]:
                            logger.warning('fd %s with %s is not found', fd._fileno, fd._timeout_id)
                            break
                        i += 1
                    self._lock.release()

            def terminate(self):
                self.async_poller.terminate()
                self.cmd_rsock.close()
                self.cmd_wsock.close()
                if len(self._timeouts):
                    logger.warning('pending timeouts: %s' % (len(self._timeouts)))
                win32file.CloseHandle(self.iocp)
                self.iocp = None
                self.cmd_rsock_buf = None
                self.__class__.__instance = None

        class AsyncSocket(_AsyncSocket):
            """AsyncSocket with I/O Completion Ports (under
            Windows). See _AsyncSocket above for more details.  UDP
            traffic is handled by _AsyncPoller.
            """

            __slots__ = _AsyncSocket.__slots__ + ('_read_overlap', '_write_overlap')

            def __init__(self, *args, **kwargs):
                self._read_overlap = None
                self._write_overlap = None
                _AsyncSocket.__init__(self, *args, **kwargs)

            def _register(self):
                if not self._blocking:
                    if self._rsock.type & socket.SOCK_STREAM:
                        self._read_overlap = pywintypes.OVERLAPPED()
                        self._write_overlap = pywintypes.OVERLAPPED()
                        self._notifier.register(self._fileno)
                    else:
                        self._notifier = _AsyncPoller.instance()
                else:
                    _AsyncSocket._register(self)

            def _unregister(self):
                if self._notifier:
                    self._notifier.unregister(self)
                    if self._rsock.type & socket.SOCK_STREAM:
                        if (self._read_overlap and self._read_overlap.object) or \
                               (self._write_overlap and self._write_overlap.object):
                            def _cleanup_(self, rc, n):
                                self._read_overlap.object = self._write_overlap.object = None
                                self._read_overlap = self._write_overlap = None
                                self._notifier = None
                                if rc != winerror.ERROR_OPERATION_ABORTED:
                                    logger.warning('CancelIo failed?: %x' % rc)
                            if self._read_overlap and self._read_overlap.object:
                                self._read_overlap.object = functools.partial(_cleanup_, self)
                            if self._write_overlap and self._write_overlap.object:
                                self._read_overlap.object = functools.partial(_cleanup_, self)
                            win32file.CancelIo(self._fileno)
                        else:
                            self._read_overlap = self._write_overlap = None
                            self._notifier = None
                        self._read_result = self._write_result = None
                        self._read_coro = self._write_coro = None
                    else:
                        self._notifier = None

            def _timed_out(self):
                if self._rsock.type & socket.SOCK_STREAM:
                    if self._read_overlap or self._write_overlap:
                        win32file.CancelIo(self._fileno)
                if self._read_coro:
                    if self._rsock.type & socket.SOCK_DGRAM:
                        self._notifier.clear(self, _AsyncPoller._Read)
                        self._read_task = None
                    self._read_coro.throw(socket.timeout('timed out'))
                    self._read_result = self._read_coro = None
                if self._write_coro:
                    if self._rsock.type & socket.SOCK_DGRAM:
                        self._notifier.clear(self, _AsyncPoller._Write)
                        self._write_task = None
                    self._write_coro.throw(socket.timeout('timed out'))
                    self._write_result = self._write_coro = None

            def setblocking(self, blocking):
                _AsyncSocket.setblocking(self, blocking)
                if not self._blocking and self._rsock.type & socket.SOCK_STREAM:
                    self.recv = self._iocp_recv
                    self.send = self._iocp_send
                    self.recvall = self._iocp_recvall
                    self.sendall = self._iocp_sendall
                    self.connect = self._iocp_connect
                    self.accept = self._iocp_accept

            def _iocp_recv(self, bufsize, *args):
                """Internal use only; use 'recv' with 'yield' instead.
                """
                def _recv(self, err, n):
                    if self._timeout and self._notifier:
                        self._notifier._del_timeout(self)
                    if err or n == 0:
                        self._read_overlap.object = self._read_result = None
                        if err == winerror.ERROR_OPERATION_ABORTED:
                            self._read_coro = None
                        else:
                            if not err:
                                err = winerror.ERROR_CONNECTION_INVALID
                            coro, self._read_coro = self._read_coro, None
                            if coro:
                                if err == winerror.ERROR_CONNECTION_INVALID:
                                    coro._proceed_('')
                                else:
                                    coro.throw(socket.error(err))
                    else:
                        buf = self._read_result[:n]
                        self._read_overlap.object = self._read_result = None
                        coro, self._read_coro = self._read_coro, None
                        if coro:
                            coro._proceed_(buf)

                self._read_result = win32file.AllocateReadBuffer(bufsize)
                self._read_overlap.object = functools.partial(_recv, self)
                self._read_coro = AsynCoro.cur_coro()
                self._read_coro._await_()
                if self._timeout:
                    self._notifier._add_timeout(self)
                err, n = win32file.WSARecv(self._fileno, self._read_result, self._read_overlap, 0)
                if err and err != winerror.ERROR_IO_PENDING:
                    self._read_overlap.object = self._read_result = self._read_coro = None
                    raise socket.error(err)

            def _iocp_send(self, buf, *args):
                """Internal use only; use 'send' with 'yield' instead.
                """
                def _send(self, err, n):
                    if self._timeout and self._notifier:
                        self._notifier._del_timeout(self)
                    if err or n == 0:
                        self._write_overlap.object = self._write_result = None
                        if err == winerror.ERROR_OPERATION_ABORTED:
                            self._write_coro = None
                        else:
                            if not err:
                                err = winerror.ERROR_CONNECTION_INVALID
                            coro, self._write_coro = self._write_coro, None
                            if coro:
                                coro.throw(socket.error(err))
                    else:
                        self._write_overlap.object = None
                        coro, self._write_coro = self._write_coro, None
                        if coro:
                            coro._proceed_(n)

                self._write_overlap.object = functools.partial(_send, self)
                self._write_coro = AsynCoro.cur_coro()
                self._write_coro._await_()
                if self._timeout:
                    self._notifier._add_timeout(self)
                err, n = win32file.WSASend(self._fileno, buf, self._write_overlap, 0)
                if err and err != winerror.ERROR_IO_PENDING:
                    self._write_overlap.object = self._write_coro = None
                    raise socket.error(err)

            def _iocp_recvall(self, bufsize, *args):
                """Internal use only; use 'recvall' with 'yield' instead.
                """
                def _recvall(self, pending, buf, err, n):
                    if err or n == 0:
                        if self._timeout and self._notifier:
                            self._notifier._del_timeout(self)
                        self._read_overlap.object = self._read_result = None
                        if err == winerror.ERROR_OPERATION_ABORTED:
                            self._read_coro = None
                        else:
                            if not err:
                                err = winerror.ERROR_CONNECTION_INVALID
                            coro, self._read_coro = self._read_coro, None
                            if coro:
                                if err == winerror.ERROR_CONNECTION_INVALID:
                                    coro._proceed_('')
                                else:
                                    coro.throw(socket.error(err))
                    else:
                        self._read_result.append(buf[:n])
                        pending -= n
                        if pending == 0:
                            buf = ''.join(self._read_result)
                            if self._timeout and self._notifier:
                                self._notifier._del_timeout(self)
                            self._read_overlap.object = self._read_result = None
                            coro, self._read_coro = self._read_coro, None
                            if coro:
                                coro._proceed_(buf)
                        else:
                            buf = win32file.AllocateReadBuffer(min(pending, 1048576))
                            self._read_overlap.object = functools.partial(_recvall, self,
                                                                          pending, buf)
                            err, n = win32file.WSARecv(self._fileno, buf, self._read_overlap, 0)
                            if err and err != winerror.ERROR_IO_PENDING:
                                if self._timeout and self._notifier:
                                    self._notifier._del_timeout(self)
                                self._read_overlap.object = self._read_result = None
                                coro, self._read_coro = self._read_coro, None
                                if coro:
                                    coro.throw(socket.error(err))

                self._read_result = []
                buf = win32file.AllocateReadBuffer(min(bufsize, 1048576))
                self._read_overlap.object = functools.partial(_recvall, self, bufsize, buf)
                self._read_coro = AsynCoro.cur_coro()
                self._read_coro._await_()
                if self._timeout:
                    self._notifier._add_timeout(self)
                err, n = win32file.WSARecv(self._fileno, buf, self._read_overlap, 0)
                if err and err != winerror.ERROR_IO_PENDING:
                    self._read_overlap.object = self._read_result = self._read_coro = None
                    raise socket.error(err)

            def _iocp_sendall(self, data):
                """Internal use only; use 'sendall' with 'yield' instead.
                """
                def _sendall(self, err, n):
                    if err or n == 0:
                        if self._timeout and self._notifier:
                            self._notifier._del_timeout(self)
                        self._write_overlap.object = self._write_result = None
                        if err == winerror.ERROR_OPERATION_ABORTED:
                            self._write_coro = None
                        else:
                            if not err:
                                err = winerror.ERROR_CONNECTION_INVALID
                            coro, self._write_coro = self._write_coro, None
                            if coro:
                                coro.throw(socket.error(err))
                    else:
                        self._write_result = self._write_result[n:]
                        if len(self._write_result) == 0:
                            if self._timeout and self._notifier:
                                self._notifier._del_timeout(self)
                            self._write_overlap.object = self._write_result = None
                            coro, self._write_coro = self._write_coro, None
                            if coro:
                                coro._proceed_(0)
                        else:
                            err, n = win32file.WSASend(self._fileno, self._write_result,
                                                       self._write_overlap, 0)
                            if err and err != winerror.ERROR_IO_PENDING:
                                if self._timeout and self._notifier:
                                    self._notifier._del_timeout(self)
                                self._write_overlap.object = self._write_result = None
                                coro, self._write_coro = self._write_coro, None
                                if coro:
                                    coro.throw(socket.error(err))

                self._write_result = buffer(data)
                self._write_overlap.object = functools.partial(_sendall, self)
                self._write_coro = AsynCoro.cur_coro()
                self._write_coro._await_()
                if self._timeout:
                    self._notifier._add_timeout(self)
                err, n = win32file.WSASend(self._fileno, self._write_result, self._write_overlap, 0)
                if err and err != winerror.ERROR_IO_PENDING:
                    self._write_overlap.object = self._write_result = self._write_coro = None
                    raise socket.error(err)

            def _iocp_connect(self, host_port):
                """Internal use only; use 'connect' with 'yield' instead.
                """
                def _connect(self, err, n):
                    def _ssl_handshake(self, err, n):
                        try:
                            self._rsock.do_handshake()
                        except ssl.SSLError as err:
                            if err.args[0] == ssl.SSL_ERROR_WANT_READ:
                                err, n = win32file.WSARecv(self._fileno, self._read_result,
                                                           self._read_overlap, 0)
                            elif err.args[0] == ssl.SSL_ERROR_WANT_WRITE:
                                err, n = win32file.WSASend(self._fileno, '', self._read_overlap, 0)
                            else:
                                raise socket.error(err)
                        except:
                            if self._timeout and self._notifier:
                                self._notifier._del_timeout(self)
                            self._read_overlap.object = self._read_result = None
                            self.close()
                            if err == winerror.ERROR_OPERATION_ABORTED:
                                self._read_coro = None
                            else:
                                coro, self._read_coro = self._read_coro, None
                                if coro:
                                    coro.throw(socket.error(err))
                        else:
                            if self._timeout and self._notifier:
                                self._notifier._del_timeout(self)
                            self._read_overlap.object = self._read_result = None
                            coro, self._read_coro = self._read_coro, None
                            if coro:
                                coro._proceed_(0)

                    if err:
                        if self._timeout and self._notifier:
                            self._notifier._del_timeout(self)
                        self._read_overlap.object = self._read_result = None
                        if err == winerror.ERROR_OPERATION_ABORTED:
                            self._read_coro = None
                        else:
                            coro, self._read_coro = self._read_coro, None
                            if coro:
                                coro.throw(socket.error(err))
                    else:
                        self._rsock.setsockopt(socket.SOL_SOCKET, win32file.SO_UPDATE_CONNECT_CONTEXT, '')
                        if self._certfile:
                            self._rsock = ssl.wrap_socket(self._rsock, keyfile=self._keyfile,
                                                          certfile=self._certfile, server_side=False,
                                                          do_handshake_on_connect=False)
                            self._read_result = win32file.AllocateReadBuffer(0)
                            self._read_overlap.object = functools.partial(_ssl_handshake, self)
                            self._read_overlap.object(None, 0)
                        else:
                            if self._timeout and self._notifier:
                                self._notifier._del_timeout(self)
                            self._read_overlap.object = self._read_result = None
                            coro, self._read_coro = self._read_coro, None
                            if coro:
                                coro._proceed_(0)

                # ConnectEX requires socket to be bound!
                try:
                    self._rsock.bind(('0.0.0.0', 0))
                except socket.error as exc:
                    if exc[0] != EINVAL:
                        raise
                self._read_overlap.object = functools.partial(_connect, self)
                self._read_coro = AsynCoro.cur_coro()
                self._read_coro._await_()
                if self._timeout:
                    self._notifier._add_timeout(self)
                err, n = win32file.ConnectEx(self._rsock, host_port, self._read_overlap)
                if err and err != winerror.ERROR_IO_PENDING:
                    self._read_overlap.object = self._read_result = self._read_coro = None
                    raise socket.error(err)

            def _iocp_accept(self):
                """Internal use only; use 'accept' with 'yield'
                instead. Socket in returned pair is asynchronous
                socket (instance of AsyncSocket with blocking=False).
                """
                def _accept(self, conn, err, n):
                    def _ssl_handshake(self, conn, addr, err, n):
                        try:
                            conn._rsock.do_handshake()
                        except ssl.SSLError as err:
                            if err.args[0] == ssl.SSL_ERROR_WANT_READ:
                                err, n = win32file.WSARecv(conn._fileno, self._read_result,
                                                           self._read_overlap, 0)
                            elif err.args[0] == ssl.SSL_ERROR_WANT_WRITE:
                                err, n = win32file.WSASend(conn._fileno, '', self._read_overlap, 0)
                            else:
                                raise socket.error(err)
                        except:
                            if self._timeout and self._notifier:
                                self._notifier._del_timeout(self)
                            self._read_overlap.object = self._read_result = None
                            conn.close()
                            if err == winerror.ERROR_OPERATION_ABORTED:
                                self._read_coro = None
                            else:
                                coro, self._read_coro = self._read_coro, None
                                if coro:
                                    coro.throw(socket.error(err))
                        else:
                            if self._timeout and self._notifier:
                                self._notifier._del_timeout(self)
                            self._read_overlap.object = self._read_result = None
                            coro, self._read_coro = self._read_coro, None
                            if coro:
                                coro._proceed_((conn, addr))

                    if err:
                        if self._timeout and self._notifier:
                            self._notifier._del_timeout(self)
                        self._read_overlap.object = self._read_result = None
                        if err == winerror.ERROR_OPERATION_ABORTED:
                            self._read_coro = None
                        else:
                            coro, self._read_coro = self._read_coro, None
                            if coro:
                                coro.throw(socket.error(err))
                    else:
                        family, laddr, raddr = win32file.GetAcceptExSockaddrs(conn, self._read_result)
                        # TODO: unpack raddr if family != AF_INET
                        conn._rsock.setsockopt(socket.SOL_SOCKET, win32file.SO_UPDATE_ACCEPT_CONTEXT,
                                               struct.pack('P', self._fileno))
                        if self._certfile:
                            conn._rsock = ssl.wrap_socket(conn._rsock, keyfile=self._keyfile,
                                                          certfile=self._certfile, server_side=True,
                                                          do_handshake_on_connect=False,
                                                          ssl_version=self._ssl_version)
                            self._read_result = win32file.AllocateReadBuffer(0)
                            self._read_overlap.object = functools.partial(_ssl_handshake, self,
                                                                          conn, raddr)
                            self._read_overlap.object(None, 0)
                        else:
                            if self._timeout and self._notifier:
                                self._notifier._del_timeout(self)
                            self._read_overlap.object = self._read_result = None
                            coro, self._read_coro = self._read_coro, None
                            if coro:
                                coro._proceed_((conn, raddr))

                sock = socket.socket(self._rsock.family, self._rsock.type, self._rsock.proto)
                conn = AsyncSocket(sock, keyfile=self._keyfile, certfile=self._certfile,
                                   ssl_version=self._ssl_version)
                self._read_result = win32file.AllocateReadBuffer(win32file.CalculateSocketEndPointSize(sock))
                self._read_overlap.object = functools.partial(_accept, self, conn)
                self._read_coro = AsynCoro.cur_coro()
                self._read_coro._await_()
                if self._timeout:
                    self._notifier._add_timeout(self)
                err = win32file.AcceptEx(self._fileno, conn._fileno, self._read_result,
                                         self._read_overlap)
                if err and err != winerror.ERROR_IO_PENDING:
                    self._read_overlap.object = self._read_result = self._read_coro = None
                    raise socket.error(err)

if not isinstance(getattr(sys.modules[__name__], '_AsyncNotifier', None), MetaSingleton):
    class _AsyncPoller(object):
        """Internal use only.
        """

        __metaclass__ = MetaSingleton
        __instance = None

        _Read = None
        _Write = None
        _Hangup = None
        _Error = None

        _Block = None

        @classmethod
        def instance(cls, *args, **kwargs):
            if cls.__instance is None:
                cls.__instance = cls(*args, **kwargs)
            return cls.__instance

        def __init__(self):
            if self.__class__.__instance is None:
                self.__class__.__instance = self
                self.timeout_multiplier = 1

                if hasattr(select, 'epoll'):
                    logger.debug('poller: epoll')
                    self._poller = select.epoll()
                    _AsyncPoller._Read = select.EPOLLIN | select.EPOLLPRI
                    _AsyncPoller._Write = select.EPOLLOUT
                    _AsyncPoller._Hangup = select.EPOLLHUP
                    _AsyncPoller._Error = select.EPOLLHUP | select.EPOLLERR
                    _AsyncPoller._Block = -1
                elif hasattr(select, 'kqueue'):
                    logger.debug('poller: kqueue')
                    self._poller = _KQueueNotifier()
                    # kqueue filter values are negative numbers so using
                    # them as flags won't work, so define them as necessary
                    _AsyncPoller._Read = 0x01
                    _AsyncPoller._Write = 0x02
                    _AsyncPoller._Hangup = 0x04
                    _AsyncPoller._Error = 0x08
                    _AsyncPoller._Block = None
                elif hasattr(select, 'devpoll'):
                    logger.debug('poller: devpoll')
                    self._poller = select.devpoll()
                    _AsyncPoller._Read = select.POLLIN | select.POLLPRI
                    _AsyncPoller._Write = select.POLLOUT
                    _AsyncPoller._Hangup = select.POLLHUP
                    _AsyncPoller._Error = select.POLLHUP | select.POLLERR
                    _AsyncPoller._Block = -1
                    self.timeout_multiplier = 1000
                elif hasattr(select, 'poll'):
                    logger.debug('poller: poll')
                    self._poller = select.poll()
                    _AsyncPoller._Read = select.POLLIN | select.POLLPRI
                    _AsyncPoller._Write = select.POLLOUT
                    _AsyncPoller._Hangup = select.POLLHUP
                    _AsyncPoller._Error = select.POLLHUP | select.POLLERR
                    _AsyncPoller._Block = -1
                    self.timeout_multiplier = 1000
                else:
                    logger.debug('poller: select')
                    self._poller = _SelectNotifier()
                    _AsyncPoller._Read = 0x01
                    _AsyncPoller._Write = 0x02
                    _AsyncPoller._Hangup = 0x04
                    _AsyncPoller._Error = 0x08
                    _AsyncPoller._Block = None

                self._fds = {}
                self._events = {}
                self._timeouts = []
                self.cmd_rsock, self.cmd_wsock = _AsyncPoller._socketpair()
                self.cmd_wsock.setblocking(0)
                self.cmd_rsock = AsyncSocket(self.cmd_rsock)
                setattr(self.cmd_rsock, '_read_task', lambda : self.cmd_rsock._rsock.recv(128))
                self.add(self.cmd_rsock, _AsyncPoller._Read)

        def interrupt(self):
            self.cmd_wsock.send('I')

        def poll(self, timeout):
            """Calls 'task' method of registered fds when there is a
            read/write event for it.
            """

            if timeout == 0:
                poll_timeout = timeout
            elif self._timeouts:
                poll_timeout = self._timeouts[0][0] - _time()
                if poll_timeout < 0.0001:
                    poll_timeout = 0
                elif timeout is not None:
                    poll_timeout = min(timeout, poll_timeout)
            elif timeout is None:
                poll_timeout = _AsyncPoller._Block
            else:
                poll_timeout = timeout
            if poll_timeout and poll_timeout != _AsyncPoller._Block:
                poll_timeout *= self.timeout_multiplier

            try:
                events = self._poller.poll(poll_timeout)
            except:
                logger.debug('poll failed')
                logger.debug(traceback.format_exc())
                # prevent tight loops
                time.sleep(5)
                return
            try:
                for fileno, event in events:
                    fd = self._fds.get(fileno, None)
                    if fd is None:
                        if event != _AsyncPoller._Hangup:
                            logger.debug('invalid fd for event %s', event)
                        continue
                    if event & _AsyncPoller._Read:
                        if fd._read_task is None:
                            logger.debug('fd %s is not registered for read; unregistering it',
                                         fd._fileno)
                            self.unregister(fd)
                        else:
                            fd._read_task()
                    elif event & _AsyncPoller._Write:
                        if fd._write_task is None:
                            logger.debug('fd %s is not registered for write; unregistering it',
                                         fd._fileno)
                            self.unregister(fd)
                        else:
                            fd._write_task()
                    elif event & _AsyncPoller._Hangup:
                        fd._eof()
            except:
                logger.debug(traceback.format_exc())

            if timeout == 0:
                now = _time()
                while self._timeouts and self._timeouts[0][0] <= now:
                    fd_timeout, fd = self._timeouts.pop(0)
                    if fd._timeout_id == fd_timeout:
                        fd._timeout_id = None
                        fd._timed_out()

        def terminate(self):
            self.cmd_wsock.close()
            self.cmd_rsock.close()
            if len(self._timeouts):
                logger.warning('pending timeouts: %s' % (len(self._timeouts)))
            if hasattr(self._poller, 'terminate'):
                self._poller.terminate()
            else:
                for fd in self._fds.itervalues():
                    try:
                        self._poller.unregister(fd._fileno)
                    except:
                        logger.warning('unregister of %s failed with %s',
                                       fd._fileno, traceback.format_exc())
            self._poller = None
            self._timeouts = []
            self._fds = {}
            self.__class__.__instance = None

        def _add_timeout(self, fd):
            if fd._timeout:
                fd._timeout_id = _time() + fd._timeout
                i = bisect_left(self._timeouts, (fd._timeout_id, fd))
                self._timeouts.insert(i, (fd._timeout_id, fd))
            else:
                fd._timeout_id = None

        def _del_timeout(self, fd):
            if fd._timeout_id:
                i = bisect_left(self._timeouts, (fd._timeout_id, fd))
                # in case of identical timeouts (unlikely?), search for
                # correct index where fd is
                while i < len(self._timeouts):
                    if self._timeouts[i] == (fd._timeout_id, fd):
                        del self._timeouts[i]
                        fd._timeout_id = None
                        break
                    if fd._timeout_id != self._timeouts[i][0]:
                        logger.warning('fd %s with %s is not found', fd._fileno, fd._timeout_id)
                        break
                    i += 1

        def unregister(self, fd):
            if self._fds.pop(fd._fileno, None) is None:
                # logger.debug('fd %s is not registered', fd._fileno)
                return
            self._events.pop(fd._fileno, None)
            self._poller.unregister(fd._fileno)
            self._del_timeout(fd)

        def add(self, fd, event):
            cur_event = self._events.get(fd._fileno, None)
            if cur_event is None:
                self._fds[fd._fileno] = fd
                self._events[fd._fileno] = event
                self._poller.register(fd._fileno, event)
            else:
                event |= cur_event
                self._events[fd._fileno] = event
                self._poller.modify(fd._fileno, event)
            self._add_timeout(fd)

        def clear(self, fd, event=0):
            cur_event = self._events.get(fd._fileno, None)
            if cur_event:
                if event:
                    cur_event &= ~event
                else:
                    cur_event = 0
                self._events[fd._fileno] = cur_event
                self._poller.modify(fd._fileno, cur_event)
                if not cur_event:
                    self._del_timeout(fd)

        @staticmethod
        def _socketpair():
            if hasattr(socket, 'socketpair'):
                return socket.socketpair()
            srv_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            srv_sock.bind(('127.0.0.1', 0))
            srv_sock.listen(1)

            sock1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            conn_thread = threading.Thread(target=lambda sock, addr_port: sock.connect(addr_port),
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
            if event & _AsyncPoller._Read:
                self.poller.control([select.kevent(fid, filter=select.KQ_FILTER_READ, flags=flags)], 0)
            if event & _AsyncPoller._Write:
                self.poller.control([select.kevent(fid, filter=select.KQ_FILTER_WRITE, flags=flags)], 0)

        def poll(self, timeout):
            kevents = self.poller.control(None, 500, timeout)
            events = [(kevent.ident,
                       _AsyncPoller._Read if kevent.filter == select.KQ_FILTER_READ else \
                           _AsyncPoller._Write if kevent.filter == select.KQ_FILTER_WRITE else 0 | \
                           _AsyncPoller._Hangup if kevent.flags == select.KQ_EV_EOF else \
                           _AsyncPoller._Error if kevent.flags == select.KQ_EV_ERROR else 0) \
                          for kevent in kevents]
            return events

    class _SelectNotifier(object):
        """Internal use only.
        """

        __metaclass__ = MetaSingleton

        def __init__(self):
            if not hasattr(self, 'poller'):
                self.poller = select.select
                self.rset = set()
                self.wset = set()
                self.xset = set()

        def register(self, fid, event):
            if event:
                if event & _AsyncPoller._Read:
                    self.rset.add(fid)
                if event & _AsyncPoller._Write:
                    self.wset.add(fid)
                if event & _AsyncPoller._Error:
                    self.xset.add(fid)

        def unregister(self, fid):
            self.rset.discard(fid)
            self.wset.discard(fid)
            self.xset.discard(fid)

        def modify(self, fid, event):
            self.unregister(fid)
            self.register(fid, event)

        def poll(self, timeout):
            rlist, wlist, xlist = self.poller(self.rset, self.wset, self.xset, timeout)
            events = {}
            for fid in rlist:
                events[fid] = _AsyncPoller._Read
            for fid in wlist:
                events[fid] = events.get(fid, 0) | _AsyncPoller._Write
            for fid in xlist:
                events[fid] = events.get(fid, 0) | _AsyncPoller._Error

            return events.iteritems()

        def terminate(self):
            self.rset = set()
            self.wset = set()
            self.xset = set()

    AsyncSocket = _AsyncSocket
    _AsyncNotifier = _AsyncPoller

AsynCoroSocket = AsyncSocket

class Lock(object):
    """'Lock' primitive for coroutines.
    """
    def __init__(self):
        self._owner = None
        self._waitlist = []

    def acquire(self, blocking=True):
        """Must be used with 'yield' as 'yield lock.acquire()'.
        """
        if not blocking and self._owner is not None:
            raise StopIteration(False)
        coro = AsynCoro.cur_coro()
        while self._owner is not None:
            self._waitlist.append(coro)
            yield coro._await_()
        self._owner = coro
        raise StopIteration(True)

    def release(self):
        """May be used with 'yield'.
        """
        coro = AsynCoro.cur_coro()
        if self._owner != coro:
            raise RuntimeError('"%s"/%s: invalid lock release - not locked' % (coro._name, coro._id))
        self._owner = None
        if self._waitlist:
            wake = self._waitlist.pop(0)
            wake._proceed_(True)

class RLock(object):
    """'RLock' primitive for coroutines.
    """
    def __init__(self):
        self._owner = None
        self._depth = 0
        self._waitlist = []

    def acquire(self, blocking=True):
        """Must be used with 'yield' as 'yield rlock.acquire()'.
        """
        coro = AsynCoro.cur_coro()
        if self._owner == coro:
            assert self._depth > 0
            self._depth += 1
            raise StopIteration(True)
        if not blocking and self._owner is not None:
            raise StopIteration(False)
        while self._owner is not None:
            self._waitlist.append(coro)
            yield coro._await_()
        assert self._depth == 0
        self._owner = coro
        self._depth = 1
        raise StopIteration(True)

    def release(self):
        """May be used with 'yield'.
        """
        coro = AsynCoro.cur_coro()
        if self._owner != coro:
            raise RuntimeError('"%s"/%s: invalid lock release - owned by "%s"/%s' % \
                               (coro._name, coro._id, self._owner._name, self._owner._id))
        self._depth -= 1
        if self._depth == 0:
            self._owner = None
            if self._waitlist:
                wake = self._waitlist.pop(0)
                wake._proceed_(True)

class Condition(object):
    """'Condition' primitive for coroutines.
    """
    def __init__(self):
        """TODO: support lock argument?
        """
        self._owner = None
        self._depth = 0
        self._waitlist = []
        self._notifylist = []

    def acquire(self, blocking=True):
        """Must be used with 'yield' as 'yield cv.acquire()'.
        """
        coro = AsynCoro.cur_coro()
        if self._owner == coro:
            self._depth += 1
            raise StopIteration(True)
        if not blocking and self._owner is not None:
            raise StopIteration(False)
        while self._owner is not None:
            self._waitlist.append(coro)
            yield coro._await_()
        assert self._depth == 0
        self._owner = coro
        self._depth = 1
        raise StopIteration(True)

    def release(self):
        """May be used with 'yield'.
        """
        coro = AsynCoro.cur_coro()
        if self._owner != coro:
            raise RuntimeError('"%s"/%s: invalid lock release - owned by "%s"/%s' % \
                               (coro._name, coro._id, self._owner._name, self._owner._id))
        self._depth -= 1
        if self._depth == 0:
            self._owner = None
            if self._waitlist:
                wake = self._waitlist.pop(0)
                wake._proceed_(True)

    def notify(self, n=1):
        """May not be used with 'yield'.
        """
        while self._notifylist and n:
            wake = self._notifylist.pop(0)
            wake._proceed_(True)
            n -= 1

    def notify_all(self):
        self.notify(len(self._notifylist))

    notifyAll = notify_all

    def wait(self, timeout=None):
        """Must be used with 'yield' as 'yield cv.wait()'.
        """
        coro = AsynCoro.cur_coro()
        if self._owner != coro:
            raise RuntimeError('"%s"/%s: invalid lock release - owned by "%s"/%s' % \
                               (coro._name, coro._id, self._owner._name, self._owner._id))
        assert self._depth > 0
        depth = self._depth
        self._depth = 0
        self._owner = None
        if self._waitlist:
            wake = self._waitlist.pop(0)
            wake._proceed_(True)
        self._notifylist.append(coro)
        start = _time()
        if (yield coro._await_(timeout)) is None:
            self._notifylist.remove(coro)
            raise StopIteration(False)
        while self._owner is not None:
            self._waitlist.insert(0, coro)
            if timeout is not None:
                timeout -= (_time() - start)
                if timeout <= 0:
                    raise StopIteration(False)
                start = _time()
            if (yield coro._await_(timeout)) is None:
                self._waitlist.remove(coro)
                raise StopIteration(False)
        assert self._depth == 0
        self._owner = coro
        self._depth = depth
        raise StopIteration(True)

class Event(object):
    """'Event' primitive for coroutines.
    """
    def __init__(self):
        self._flag = False
        self._waitlist = []

    def set(self):
        """May be used with 'yield'.
        """
        self._flag = True
        for coro in self._waitlist:
            coro._proceed_(True)
        self._waitlist = []

    def is_set(self):
        """No need to use with 'yield'.
        """
        return self._flag

    isSet = is_set

    def clear(self):
        """No need to use with 'yield'.
        """
        self._flag = False

    def wait(self, timeout=None):
        """Must be used with 'yield' as 'yield event.wait()' .
        """
        if self._flag:
            raise StopIteration(True)
        coro = AsynCoro.cur_coro()
        if timeout is not None:
            if timeout <= 0:
                raise StopIteration(False)
        self._waitlist.append(coro)
        if (yield coro._await_(timeout)) is None:
            self._waitlist.remove(coro)
            raise StopIteration(False)
        else:
            raise StopIteration(True)

class Semaphore(object):
    """'Semaphore' primitive for coroutines.
    """
    def __init__(self, value=1):
        assert value >= 1
        self._waitlist = []
        self._counter = value

    def acquire(self, blocking=True):
        """Must be used with 'yield' as 'yield sem.acquire()'.
        """
        if blocking:
            coro = AsynCoro.cur_coro()
            while self._counter == 0:
                self._waitlist.append(coro)
                yield coro._await_()
        elif self._counter == 0:
            raise StopIteration(False)
        self._counter -= 1
        raise StopIteration(True)

    def release(self):
        """May be used with 'yield'.
        """
        self._counter += 1
        assert self._counter > 0
        if self._waitlist:
            wake = self._waitlist.pop(0)
            wake._proceed_(True)

class HotSwapException(Exception):
    """This exception is used to indicate hot-swap request and
    response.

    If 'exc' is the exception, then len(exc.args) == 1 and
    exc.args[0] is the new generator method.
    """
    pass

class MonitorException(Exception):
    """This execption is used to indicate that a coroutine being
    monitored has finished or terminated.

    If 'exc' is the exception, then

    exc.args[0] is the coroutine (either local or remote) about
    which this exception is thrown,

    If the coroutine finished executing normally, then len(exc.args)
    == 2, type(exc.args[1]) == tuple, len(exc.args[1]) == 2,
    exc.args[1][0] is StopIteration and exc.args[1][1] is the value of
    coroutine (i.e., last value yielded). If the coroutine is remote
    and value (i.e., exc.args[1][1]) is not serializable, then
    exc.args[1][1] is just the type of that value.

    If the coroutine terminated due to exception, then exc.args[1:]
    (i.e., rest of exc.args) is the list of exceptions pending at the
    time coroutine terminated.
    """
    pass

class Coro(object):
    """Creates coroutine with the given generator function and
    schedules that coroutine to be executed with AsynCoro. If the
    function definition has 'coro' keyword argument set to (default
    value) None, that argument will be set to the coroutine created.
    """

    __slots__ = ('_generator', '_name', '_id', '_state', '_value', '_exceptions', '_callers',
                 '_timeout', '_daemon', '_complete', '_msgs', '_monitors', '_new_generator',
                 '_hot_swappable', '_location')

    _asyncoro = None

    def __init__(self, target, *args, **kwargs):
        self._generator = self.__get_generator(target, *args, **kwargs)
        self._name = target.__name__
        self._id = id(self)
        self._state = None
        self._value = None
        self._exceptions = []
        self._callers = []
        self._timeout = None
        self._daemon = False
        self._complete = None
        self._msgs = collections.deque()
        self._monitors = set()
        self._new_generator = None
        self._hot_swappable = False
        if Coro._asyncoro is None:
            Coro._asyncoro = AsynCoro.instance()
        self._location = Coro._asyncoro._location
        Coro._asyncoro._add(self)

    @property
    def location(self):
        """Get Location instance where this coro is running.

        Can also be used on remotely running coroutines.
        """
        return copy.copy(self._location)

    @property
    def name(self):
        """Get name of coroutine.

        Can also be used on remotely running coroutines.
        """
        return self._name

    @staticmethod
    def locate(name, location=None, timeout=None):
        """Must be used with 'yield' as
        'rcoro = yield Coro.locate("name")'.

        Returns Coro instance to coroutine at remote peers so it can
        be used to exchange messages, monitor etc. (those methods
        explicitly marked as callable on remote coroutines).
        """
        if Coro._asyncoro is None:
            Coro._asyncoro = AsynCoro.instance()
        if location is None or location == Coro._asyncoro._location:
            rcoro = Coro._asyncoro._rcoros.get(name, None)
            if rcoro is not None or location == Coro._asyncoro._location:
                raise StopIteration(rcoro)
        if location is None:
            req = _NetRequest('locate_coro', kwargs={'name':name},
                              src=Coro._asyncoro._location, timeout=timeout)
            req.id = id(req)
            Coro._asyncoro._requests[req.id] = req
            req.event = Event()
            for (addr, port), peer in _Peer.peers.items():
                if req.event.is_set():
                    break
                yield Coro._asyncoro._async_reply(req, peer, dst=Location(addr, port))
            else:
                if (yield req.event.wait(timeout)) is False:
                    Coro._asyncoro._requests.pop(req.id, None)
                    req.reply = None
            rcoro = req.reply
        else:
            req = _NetRequest('locate_coro', kwargs={'name':name}, dst=location, timeout=timeout)
            rcoro = yield Coro._asyncoro._sync_reply(req)
        raise StopIteration(rcoro)

    def register(self, name=None):
        """Register this coroutine so coroutines running on a remote
        (peer) asyncoro can locate it (with 'locate') so they can
        exchange messages, monitored etc.
        """
        if self._location != Coro._asyncoro._location:
            return -1
        if not name:
            name = self._name
        return Coro._asyncoro._register_coro(self, name)

    def unregister(self, name=None):
        """Unregister this coroutine (see 'register' above).
        """
        if self._location != Coro._asyncoro._location:
            return -1
        if not name:
            name = self._name
        return Coro._asyncoro._unregister_coro(self, name)

    def set_daemon(self, flag=True):
        """Mark coroutine is a daemon process or not.

        AsynCoro scheduler waits for all non-daemon coroutines to
        terminate before it terminates itself.
        """
        return Coro._asyncoro._set_daemon(self, bool(flag))

    def suspend(self, timeout=None, alarm_value=None):
        """Must be used with 'yield' as 'yield coro.suspend()'.

        If timeout is a (floating point) number, this coro is
        suspended for that many seconds (or fractions of second),
        unless resumed by another coroutine, in which case the value
        it is resumed with is the return value of this suspend.

        If suspend times out (no other coroutine resumes it), AsynCoro
        resumes it with the value 'alarm_value'.
        """
        return Coro._asyncoro._suspend(self, timeout, alarm_value, AsynCoro._Suspended)

    sleep = suspend

    def resume(self, update=None):
        """May be used with 'yield'. Resume/wakeup this coro and send
        'update' to it.

        The resuming coro gets 'update' for the 'yield' that caused it
        to suspend. If coro is currently not suspended/sleeping,
        resume is ignored.
        """
        return Coro._asyncoro._resume(self, update, AsynCoro._Suspended)

    wakeup = resume

    def send(self, message):
        """May be used with 'yield'. Sends 'message' to coro.

        If coro is currently waiting with 'receive', it is resumed
        with 'message'. Otherwise, 'message' is queued so that next
        receive call will return message.

        Can also be used on remotely running coroutines.
        """
        if self._location == Coro._asyncoro._location:
            return Coro._asyncoro._resume(self, message, AsynCoro._AwaitMsg_)
        else:
            request = _NetRequest('send', kwargs={'coro':self._id, 'message':message},
                                  dst=self._location, timeout=2)
            # request is queued for asynchronous processing
            if _Peer.send_req(request):
                logger.warning('remote coro at %s may not be valid', self._location)
                # def _unsub(self, rchannels, coro=None):
                #     for rchannel in rchannels:
                #         yield rchannel.unsubscribe(self)
                # Coro(_unsub, self, Coro._asyncoro._rchannels.values())
                return -1
            else:
                return 0

    def deliver(self, message, timeout=None):
        """Must be used with 'yield' as 'yield coro.deliver(message)'.

        Deliver message to coroutine to the 'real' coroutine to which
        this instance refers to and return number of coroutines that
        received the message.

        Can also be used on remotely running coroutines.
        """
        if self._location == Coro._asyncoro._location:
            reply = Coro._asyncoro._resume(self, message, AsynCoro._AwaitMsg_)
        else:
            request = _NetRequest('deliver', kwargs={'coro':self._id, 'message':message},
                                  dst=self._location, timeout=timeout)
            reply = yield Coro._asyncoro._sync_reply(request, alarm_value=0)
            if reply < 0:
                logger.warning('remote coro at %s may not be valid', self._location)
                # def _unsub(self, rchannels, coro=None):
                #     for rchannel in rchannels:
                #         yield rchannel.unsubscribe(self)
                # Coro(_unsub, self, Coro._asyncoro._rchannels.values())
                reply = 0
        raise StopIteration(reply)

    def receive(self, timeout=None, alarm_value=None):
        """Must be used with 'yield' as 'message = yield coro.receive()'.
        Gets/waits for message.

        Gets earliest queued message if available (that has been sent
        earlier with 'send'). Otherwise, suspends until 'timeout'. If
        timeout happens, coro receives alarm_value.
        """
        return Coro._asyncoro._suspend(self, timeout, alarm_value, AsynCoro._AwaitMsg_)

    def throw(self, *args):
        """Throw exception in coroutine. This method must be called from
        coro only.
        """
        if len(args) == 0:
            logger.warning('throw: invalid argument(s)')
            return -1
        if len(args) == 1:
            if isinstance(args[0], tuple) and len(args[0]) > 1:
                args = args[0]
            else:
                args = (type(args[0]), args[0])
        return Coro._asyncoro._throw(self, *args)

    def value(self):
        """Get last value 'yield'ed / value of StopIteration of coro.

        NB: This method should _not_ be called from a coroutine! This
        method is meant for main thread in the user program to wait for
        (main) coroutine(s) it creates.

        Once coroutine stops (finishes) executing, the last value is
         returned.
        """
        Coro._asyncoro._lock.acquire()
        if self._complete is None:
            self._complete = threading.Event()
            Coro._asyncoro._lock.release()
            self._complete.wait()
        elif self._complete == 0:
            Coro._asyncoro._lock.release()
        else:
            Coro._asyncoro._lock.release()
            self._complete.wait()
        return self._value

    def finish(self):
        """Get last value 'yield'ed / value of StopIteration of
        coro. Must be used in a coroutine with 'yield' as
        'value = yield other_coro.finish()'

        Once coroutine stops (finishes) executing, the last value is
        returned.
        """
        if self._complete is None:
            self._complete = Event()
            yield self._complete.wait()
        elif self._complete == 0:
            pass
        elif isinstane(self._complete, Event):
            yield self._complete.wait()
        else:
            raise RuntimeError('invalid wait on %s/%s: %s' % \
                               (self._name, self._id, type(self._complete)))
        raise StopIteration(self._value)

    def terminate(self):
        """Terminate coro.

        If this method called by a thread (and not a coro), there is a
        chance that coro being terminated is currently running and can
        interfere with GenratorExit exception that will be thrown to
        coro.

        Can also be used on remotely running coroutines.
        """
        if self._location == Coro._asyncoro._location:
            return Coro._asyncoro._terminate_coro(self)
        else:
            request = _NetRequest('terminate_coro', kwargs={'coro':self._id},
                                  dst=self._location, timeout=2)
            if _Peer.send_req(request):
                logger.warning('remote coro at %s may not be valid', self._location)
                return -1
            return 0

    def hot_swappable(self, flag):
        if Coro._asyncoro.cur_coro() == self:
            if flag:
                self._hot_swappable = True
                if self._new_generator:
                    return Coro._asyncoro._swap_generator(self)
            else:
                self._hot_swappable = False
            return 0
        else:
            logger.warning('hot_swappable must be called from running coro')
            return -1

    def hot_swap(self, target, *args, **kwargs):
        """Replaces coro's generator function with given target(*args, **kwargs).

        The new generator starts executing from the beginning. If
        there are any pending messages, they will not be reset, so new
        generator can process them (or clear them with successive
        'receive' calls with timeout=0 until it returns
        'alarm_value').
        """
        try:
            generator = self.__get_generator(target, *args, **kwargs)
        except:
            logger.warning('%s is not a generator!' % target.__name__)
            return -1
        self._new_generator = generator
        return Coro._asyncoro._swap_generator(self)

    def monitor(self, observe):
        """Must be used with 'yield' as 'yield coro.monitor(observe)',
        where 'observe' is a coroutine which will be monitored by
        'coro'.  When 'observe' is finished (raises StopIteration or
        terminated due to uncaught exception), that exception is
        sent as a message to 'coro' (monitor coroutine).

        Monitor can inspect the exception and restart observed
        coroutine if necessary. 'observe' can be a remote coroutine.

        Can also be used on remotely running 'observe' coroutine.
        """
        if observe._location == Coro._asyncoro.location:
            reply = Coro._asyncoro._monitor(self, observe)
        else:
            # remote coro
            request = _NetRequest('monitor', kwargs={'monitor':self, 'coro':observe._id},
                                  dst=observe._location, timeout=2)
            reply = yield Coro._asyncoro._sync_reply(request)
        raise StopIteration(reply)
        
    def _await_(self, timeout=None, alarm_value=None):
        """Internal use only.
        """
        return Coro._asyncoro._suspend(self, timeout, alarm_value, AsynCoro._AwaitIO_)

    def _proceed_(self, update=None):
        """Internal use only.
        """
        return Coro._asyncoro._resume(self, update, AsynCoro._AwaitIO_)

    def __get_generator(self, target, *args, **kwargs):
        if not inspect.isgeneratorfunction(target):
            raise Exception('%s is not a generator!' % target.__name__)
        if not args and kwargs:
            args = kwargs.pop('args', ())
            kwargs = kwargs.pop('kwargs', kwargs)
        if target.func_defaults and \
           'coro' in target.func_code.co_varnames[:target.func_code.co_argcount][-len(target.func_defaults):]:
            kwargs['coro'] = self
        return target(*args, **kwargs)

    def __getstate__(self):
        state = {'_name':self._name, '_id':str(self._id), '_location':self._location}
        return state
    
    def __setstate__(self, state):
        self._name = state['_name']
        self._location = state['_location']
        self._id = state['_id']

    def __repr__(self):
        s = '%s/%s' % (self._name, self._id)
        if self._location:
            s = '%s@%s' % (s, self._location)
        return s

class Location(object):
    """Distributed asyncoro, coroutines, channels use Location to
    identify where they are running, where to send a message etc.

    Users can create instances (which can be passed to 'locate'
    methods) 'host' is either name or IP address and 'tcp_port' is TCP
    port where (peer) asyncoro runs network services.
    """

    __slots__ = ('addr', 'port')

    def __init__(self, host, tcp_port):
        self.addr = socket.gethostbyname(host)
        self.port = tcp_port

    def __eq__(self, other):
        return isinstance(other, type(self)) and \
               self.addr == other.addr and self.port == other.port

    def __ne__(self, other):
        return (not isinstance(other, type(self))) or \
               self.addr != other.addr or self.port != other.port

    def __repr__(self):
        return '%s:%s' % (self.addr, self.port)

class Channel(object):
    """Subscription based channel. Broadcasts a message to all
    registered subscribers, whether they are currently waiting for
    message or not. Messages are queued (buffered) at receiving
    coroutines. To get a message, a coro must use 'yield
    coro.receive()', with timeout and alarm_value, if necessary.

    Channels can be hierarchical, and subscribers can be remote.
    """

    __slots__ = ('_name', '_location', '_transform', '_subscribers',
                 '_subscribe_event')

    _asyncoro = None

    def __init__(self, name, transform=None):
        """'name' must be unique across all channels.

        'transform' is a function that can either filter or
        transform a message. If the function returns 'None', the
        message is filtered (ignored). The function is called with
        first parameter set to channel name and second parameter set
        to the message.
        """

        if Channel._asyncoro is None:
            Channel._asyncoro = AsynCoro.instance()
        self._name = name
        self._location = Channel._asyncoro._location
        if transform is not None:
            try:
                argspec = inspect.getargspec(transform)
                assert len(argspec.args) == 2
            except:
                logger.warning('invalid "transform" function ignored')
                transform = None
        self._transform = transform
        self._subscribers = set()
        self._subscribe_event = Event()
        Channel._asyncoro._lock.acquire()
        if name in Channel._asyncoro._channels:
            Channel._asyncoro._lock.release()
            logger.warning('duplicate channel "%s"!' % name)
        else:
            Channel._asyncoro._channels[name] = self
            Channel._asyncoro._lock.release()

    @property
    def location(self):
        """Get Location instance where this channel is located.

        Can also be used on remote channels.
        """
        return copy.copy(self._location)

    @property
    def name(self):
        """Get name of channel.

        Can also be used on remote channel.
        """
        return self._name

    @staticmethod
    def locate(name, location=None, timeout=None):
        """Must be used with 'yield' as
        'rchannel = yield Channel.locate("name")'.

        Returns Channel instance to registered channel at remote peers
        so it can be used to send/deliver messages..
        """
        if Channel._asyncoro is None:
            Channel._asyncoro = AsynCoro.instance()
        if location is None:
            req = _NetRequest('locate_channel', kwargs={'name':name},
                              src=Channel._asyncoro._location, timeout=None)
            req.event = Event()
            req.id = id(req)
            Channel._asyncoro._requests[req.id] = req
            for (addr, port), peer in _Peer.peers.items():
                if req.event.is_set():
                    break
                yield Channel._asyncoro._async_reply(req, peer, dst=Location(addr, port))
            else:
                if (yield req.event.wait(timeout)) is False:
                    Channel._asyncoro._requests.pop(req.id, None)
                    req.reply = None
            rchannel = req.reply
        else:
            req = _NetRequest('locate_channel', kwargs={'name':name}, dst=location, timeout=timeout)
            rchannel = yield Channel._asyncoro._sync_reply(req)
        raise StopIteration(rchannel)

    def register(self):
        """A registered channel can be located (with 'locate') by a
        coroutine on a remote asyncoro.
        """
        if self._location != Channel._asyncoro._location:
            return -1
        return Channel._asyncoro._register_channel(self, self._name)

    def unregister(self):
        """Unregister channel (see 'register' above).
        """
        if self._location != Channel._asyncoro._location:
            return -1
        return Channel._asyncoro._unregister_channel(self, self._name)

    def set_transform(self, transform):
        if self._location != Channel._asyncoro._location:
            return -1
        try:
            argspec = inspect.getargspec(transform)
            assert len(argspec.args) == 2
        except:
            logger.warning('invalid "transform" function ignored')
            return -1
        self._transform = transform
        return 0

    def subscribe(self, subscriber, timeout=None):
        """Must be used with 'yield', as, for example,
        'yield channel.subscribe(coro)'.

        Subscribe to receive messages. Senders don't need to
        subscribe. A message sent to this channel is delivered to all
        subscribers.

        Can also be used on remote channels.
        """
        if not isinstance(subscriber, Coro) and not isinstance(subscriber, Channel):
            logger.warning('invalid subscriber ignored')
            raise StopIteration(-1)
        if self._location == Channel._asyncoro._location:
            if subscriber._location != self._location:
                if isinstance(subscriber, Coro):
                    # remote coro
                    subscriber._id = int(subscriber._id)
                    for s in self._subscribers:
                        if isinstance(s, Coro) and \
                               s._id == subscriber._id and s._location == subscriber._location:
                            subscriber = s
                            break
                elif isinstance(subscriber, Channel):
                    # remote channel
                    for s in self._subscribers:
                        if isinstance(s, Channel) and \
                               s._name == subscriber._name and s._location == subscriber._location:
                            subscriber = s
                            break
            self._subscribers.add(subscriber)
            yield self._subscribe_event.set()
            reply = 0
        else:
            # remote channel
            kwargs = {'channel':self._name}
            kwargs['subscriber'] = subscriber
            request = _NetRequest('subscribe', kwargs=kwargs, dst=self._location, timeout=timeout)
            reply = yield Channel._asyncoro._sync_reply(request)
        raise StopIteration(reply)

    def unsubscribe(self, subscriber, timeout=None):
        """Must be called with 'yield' as, for example,
        'yield channel.unsubscribe(coro)'.

        Future messages will not be delivered after unsubscribing.

        Can also be used on remote channels.
        """
        if not isinstance(subscriber, Coro) and not isinstance(subscriber, Channel):
            logger.warning('invalid subscriber ignored')
            raise StopIteration(-1)
        if self._location == Channel._asyncoro._location:
            if subscriber._location != self._location:
                if isinstance(subscriber, Coro):
                    # remote coro
                    subscriber._id = int(subscriber._id)
                    for s in self._subscribers:
                        if isinstance(s, Coro) and \
                               s._id == subscriber._id and s._location == subscriber._location:
                            subscriber = s
                            break
                elif isinstance(subscriber, Channel):
                    # remote channel
                    for s in self._subscribers:
                        if isinstance(s, Channel) and \
                               s._name == subscriber._name and s._location == subscriber._location:
                            subscriber = s
                            break
            try:
                self._subscribers.remove(subscriber)
            except KeyError:
                reply = -1
            else:
                reply = 0
        else:
            # remote channel
            kwargs = {'channel':self._name}
            kwargs['subscriber'] = subscriber
            request = _NetRequest('unsubscribe', kwargs=kwargs, dst=self._location, timeout=timeout)
            reply = yield Channel._asyncoro._sync_reply(request)
        raise StopIteration(reply)

    def send(self, message):
        """Message is sent to currently registered subscribers.

        Can also be used on remote channels.
        """
        if self._location == Channel._asyncoro._location:
            if self._transform:
                try:
                    message = self._transform(self._name, message)
                except:
                    message = None
                if message is None:
                    return 0
            for subscriber in self._subscribers:
                subscriber.send(message)
        else:
            # remote channel
            request = _NetRequest('send', kwargs={'channel':self._name, 'message':message},
                                  dst=self._location, timeout=2)
            # request is queued for asynchronous processing
            if _Peer.send_req(request):
                logger.warning('remote channel at %s may not be valid', self._location)
                # def _unsub(channel, rchannels, coro=None):
                #     for rchannel in rchannels:
                #         yield rchannel.unsubscribe(channel)
                # Coro(_unsub, self, Channel._asyncoro._rchannels.values())
                return -1
        return 0

    def deliver(self, message, timeout=None, n=0):
        """Must be used with 'yield' as 'rcvd = yield channel.deliver(message)'.

        Blocking 'send': Wait until message can be delivered to at
        least 'n' subscribers before timeout. Returns number of
        end-point recipients (coroutines) the message is delivered to;
        i.e., in case of heirarchical channels, it is the sum of
        recipients of all the channels.

        Can also be used on remote channels.
        """
        if not isinstance(n, int) or n < 0:
            raise StopIteration(-1)
        if self._location == Channel._asyncoro._location:
            if self._transform:
                try:
                    message = self._transform(self._name, message)
                except:
                    message = None
                if message is None:
                    raise StopIteration(0)
            if n:
                while len(self._subscribers) < n:
                    start = _time()
                    self._subscribe_event.clear()
                    if (yield self._subscribe_event.wait(timeout)) is False:
                        raise StopIteration(0)
                    if timeout is not None:
                        timeout -= _time() - start
                        if timeout <= 0:
                            raise StopIteration(0)
            # during delivery, _subscribers may change, so make copy
            subscribers = list(self._subscribers)
            count = {'reply':0, 'pending':len(subscribers), 'success':0}
            done = Event()

            def _deliver(subscriber, c, n, event, timeout, coro=None):
                try:
                    reply = yield subscriber.deliver(message, timeout=timeout)
                    if reply > 0:
                        c['reply'] += reply
                        c['success'] += 1
                        if n > 0 and c['success'] >= n:
                            event.set()
                except:
                    pass
                c['pending'] -= 1
                if c['pending'] == 0:
                    event.set()
            for subscriber in subscribers:
                if isinstance(subscriber, Coro) and self._location == subscriber._location:
                    if subscriber.send(message) == 0:
                        count['reply'] += 1
                        count['success'] += 1
                    count['pending'] -= 1
                else:
                    # channel/remote coro
                    Coro(_deliver, subscriber, count, n, done, timeout)
            if count['pending'] == 0:
                done.set()
            if n == 0 or count['success'] < n:
                yield done.wait(timeout)
            raise StopIteration(count['reply'])
        else:
            # remote channel
            request = _NetRequest('deliver', kwargs={'channel':self._name, 'message':message, 'n':n},
                                  dst=self._location, timeout=timeout)
            reply = yield Channel._asyncoro._sync_reply(request, alarm_value=0)
            if reply < 0:
                logger.warning('remote channel "%s" at %s may have gone away!',
                               self._name, self._location)
                # def _unsub(channel, rchannels, coro=None):
                #     for rchannel in rchannels:
                #         yield rchannel.unsubscribe(channel)
                # Coro(_unsub, self, Channel._asyncoro._rchannels.values())
                reply = 0
            raise StopIteration(reply)

    def close(self):
        if self._location == Channel._asyncoro._location:
            self.unregister()
            self._subscribers = set()
            Channel._asyncoro._lock.acquire()
            Channel._asyncoro._channels.pop(self._name, None)
            Channel._asyncoro._lock.release()

    def __getstate__(self):
        state = {'_name':self._name, '_location':self._location}
        return state

    def __setstate__(self, state):
        self._name = state['_name']
        self._location = state['_location']
        self._transform = None

    def __repr__(self):
        s = '%s' % (self._name)
        if self._location:
            s = '%s@%s' % (s, self._location)
        return s

class CategorizeMessages(object):
    """Splits messages to coroutine in to categories so that they can
    be processed on priority basis, for example.
    """

    def __init__(self, coro):
        """Categorize messages to coroutine 'coro'.
        """
        self._coro = coro
        self._categories = {None:collections.deque()}
        self._categorize = []

    def add(self, categorize):
        """Add given method to categorize messages. When a message is
        received, each of the added methods (most recently added
        method first) is called with the message. The method should
        return a category (any hashable object) or None (in which case
        next recently added method is called with the same
        message). If all the methods return None for a given message,
        the message is queued with category=None, so that 'receive'
        method here works just as Coro.receive.
        """
        if inspect.isfunction(categorize):
            argspec = inspect.getargspec(categorize)
            if len(argspec.args) != 1:
                categorize = None
        elif type(categorize) != functools.partial:
            categorize = None

        if categorize:
            self._categorize.append(categorize)
        else:
            logger.warning('invalid categorize function ignored')

    def remove(self, categorize):
        """Remove given method (added earlier).
        """
        try:
            self._categorize.remove(categorize)
        except ValueError:
            logger.warning('invalid categorize function')

    def receive(self, category=None, timeout=None, alarm_value=None):
        """Similar to 'receive' of Coro, except it retrieves (waiting,
        if necessary) messages in given 'category'.
        """
        # assert AsynCoro.cur_coro() == self._coro
        c = self._categories.get(category, None)
        if c:
            msg = c.popleft()
            raise StopIteration(msg)
        if timeout:
            start = time.time()
        while True:
            msg = yield self._coro.receive(timeout=timeout, alarm_value=alarm_value)
            if msg == alarm_value:
                raise StopIteration(msg)
            for categorize in reversed(self._categorize):
                c = categorize(msg)
                if c == category:
                    raise StopIteration(msg)
                if c is not None:
                    bucket = self._categories.get(c, None)
                    if not bucket:
                        bucket = self._categories[c] = collections.deque()
                    bucket.append(msg)
                    break
            else:
                self._categories[None].append(msg)
            if timeout:
                now = time.time()
                timeout -= now - start
                start = now

class AsynCoro(object):
    """Coroutine scheduler.

    The scheduler is created and started automatically (when a
    coroutine is created, for example), so there is no reason to
    create it explicitly. To use distributed programming, AsynCoro in
    disasyncoro module should be used.
    """

    __metaclass__ = MetaSingleton
    __instance = None

    # in _scheduled set, waiting for turn to execute
    _Scheduled = 1
    # in _scheduled, currently executing
    _Running = 2
    # in _suspended, waiting for resume
    _Suspended = 3
    # in _suspended, waiting for I/O operation
    _AwaitIO_ = 4
    # in _suspended, waiting for message
    _AwaitMsg_ = 5

    def __init__(self, notifier=None):
        if self.__class__.__instance is None:
            self.__class__.__instance = self
            if notifier is None:
                self._notifier = _AsyncNotifier()
            else:
                self._notifier = notifier
            self._location = None
            self._name = None
            self._coros = {}
            self._cur_coro = None
            self._scheduled = set()
            self._suspended = set()
            self._timeouts = []
            # because Coro can be added from thread(s) and UDP poller in
            # the case of Windows (IOCP) runs in a separate thread, we
            # need to lock access to _scheduled, etc.
            self._lock = threading.RLock()
            self._quit = False
            self._complete = threading.Event()
            self._daemons = 0
            self._polling = False
            self._channels = {}
            self._atexit = []
            self._scheduler = threading.Thread(target=self._schedule)
            self._scheduler.daemon = True
            self._scheduler.start()
            atexit.register(self.finish)

    @property
    def location(self):
        """Get Location instance where this AsynCoro is running.
        """
        return copy.copy(self._location)

    @property
    def name(self):
        """Get name of AsynCoro.
        """
        return self._name

    @classmethod
    def instance(cls, *args, **kwargs):
        """Returns (singleton) instance of AsynCoro.
        """
        if cls.__instance is None:
            cls.__instance = cls(*args, **kwargs)
        return cls.__instance

    @staticmethod
    def cur_coro():
        """Must be called from a coro only.
        """
        return AsynCoro.__instance._cur_coro

    def _add(self, coro):
        """Internal use only. See Coro class.
        """
        self._lock.acquire()
        self._coros[coro._id] = coro
        self._complete.clear()
        coro._state = AsynCoro._Scheduled
        self._scheduled.add(coro._id)
        if self._polling and len(self._scheduled) == 1:
            self._notifier.interrupt()
        self._lock.release()

    def _set_daemon(self, coro, flag):
        """Internal use only. See set_daemon in Coro.
        """
        self._lock.acquire()
        if self._cur_coro != coro:
            self._lock.release()
            logger.warning('invalid "set_daemon" - "%s" != "%s"' % (coro, self._cur_coro))
            return -1
        cid = coro._id
        if coro._daemon != flag:
            coro._daemon = flag
            if flag:
                self._daemons += 1
                if len(self._coros) == self._daemons:
                    self._complete.set()
            else:
                self._daemons -= 1
                self._complete.clear()
        self._lock.release()
        return 0

    def _monitor(self, monitor, coro):
        """Internal use only. See monitor in Coro.
        """
        self._lock.acquire()
        cid = coro._id
        coro = self._coros.get(cid, None)
        if coro is None or not isinstance(monitor, Coro):
            self._lock.release()
            logger.warning('monitor: invalid coroutine')
            return -1
        coro._monitors.add(monitor)
        self._lock.release()
        return 0

    def _suspend(self, coro, timeout, alarm_value, state):
        """Internal use only. See sleep/suspend in Coro.
        """
        self._lock.acquire()
        if self._cur_coro != coro:
            self._lock.release()
            logger.warning('invalid "suspend" - "%s" != "%s"' % (coro, self._cur_coro))
            return -1
        cid = coro._id
        if state == AsynCoro._AwaitMsg_ and coro._msgs:
            s, update = coro._msgs[0]
            if s == state:
                coro._msgs.popleft()
                self._lock.release()
                return update
        if timeout is None:
            coro._timeout = None
        else:
            if not isinstance(timeout, (float, int)):
                logger.warning('invalid timeout %s', timeout)
                self._lock.release()
                return -1
            if timeout <= 0:
                self._lock.release()
                return alarm_value
            else:
                timeout = _time() + timeout
                heappush(self._timeouts, (timeout, cid, alarm_value))
                coro._timeout = timeout
        self._scheduled.discard(cid)
        self._suspended.add(cid)
        coro._state = state
        self._lock.release()
        return 0

    def _resume(self, coro, update, state):
        """Internal use only. See resume in Coro.
        """
        self._lock.acquire()
        cid = coro._id
        coro = self._coros.get(cid, None)
        if coro is None:
            self._lock.release()
            logger.warning('invalid coroutine %s to resume', cid)
            return -1
        if state == AsynCoro._AwaitMsg_ and coro._state != state:
            coro._msgs.append((state, update))
            self._lock.release()
            return 0
        if coro._state == state:
            coro._timeout = None
            coro._value = update
            self._suspended.discard(cid)
            self._scheduled.add(cid)
            coro._state = AsynCoro._Scheduled
            if self._polling and len(self._scheduled) == 1:
                self._notifier.interrupt()
        else:
            logger.warning('ignoring resume for %s/%s: %s', coro._name, cid, coro._state)
        self._lock.release()
        return 0

    def _throw(self, coro, *args):
        """Internal use only. See throw in Coro.
        """
        self._lock.acquire()
        cid = coro._id
        coro = self._coros.get(cid, None)
        if coro is None or coro._state not in (AsynCoro._Scheduled, AsynCoro._Suspended,
                                               AsynCoro._AwaitIO_, AsynCoro._AwaitMsg_):
            logger.warning('invalid coroutine %s to throw exception', cid)
            self._lock.release()
            return -1
        coro._timeout = None
        coro._exceptions.append(args)
        if coro._state in (AsynCoro._AwaitIO_, AsynCoro._Suspended, AsynCoro._AwaitMsg_):
            self._suspended.discard(cid)
            self._scheduled.add(cid)
            coro._state = AsynCoro._Scheduled
            if self._polling and len(self._scheduled) == 1:
                self._notifier.interrupt()
        self._lock.release()
        return 0

    def _terminate_coro(self, coro):
        """Internal use only.
        """
        self._lock.acquire()
        cid = coro._id
        coro = self._coros.get(cid, None)
        if coro is None:
            logger.warning('invalid coroutine %s to terminate', cid)
            self._lock.release()
            return -1
        # TODO: if currently waiting I/O or holding locks, warn?
        if coro._state == AsynCoro._Running:
            logger.warning('coroutine to terminate %s/%s is running', coro._name, cid)
        else:
            self._suspended.discard(cid)
            self._scheduled.add(cid)
        coro._exceptions.append((GeneratorExit, GeneratorExit('close')))
        coro._timeout = None
        coro._state = AsynCoro._Scheduled
        coro._callers = []
        if self._polling and len(self._scheduled) == 1:
            self._notifier.interrupt()
        self._lock.release()
        return 0

    def _swap_generator(self, coro):
        """Internal use only.
        """
        self._lock.acquire()
        cid = coro._id
        coro = self._coros.get(cid, None)
        if coro is None:
            logger.warning('invalid coroutine %s to swap', cid)
            self._lock.release()
            return -1
        if coro._callers or not coro._hot_swappable:
            logger.debug('postponing hot swapping of %s', str(coro))
            self._lock.release()
            return 0
        else:
            coro._timeout = None
            # TODO: check that another HotSwapException is not pending?
            if coro._state == None:
                # assert coro._id not in self._scheduled
                # assert coro._id not in self._suspended
                coro._generator = coro._new_generator
                coro._value = None
                if coro._complete == 0:
                    coro._complete = None
                elif isinstance(coro._complete, Event):
                    coro._complete.clear()
                self._scheduled.add(cid)
                coro._state = AsynCoro._Scheduled
                coro._hot_swappable = False
            else:
                coro._exceptions.append((HotSwapException, HotSwapException(coro._new_generator)))
                # assert coro._state != AsynCoro._AwaitIO_
                if coro._state in (AsynCoro._Suspended, AsynCoro._AwaitMsg_):
                    self._suspended.discard(cid)
                    self._scheduled.add(cid)
                    coro._state = AsynCoro._Scheduled
            coro._new_generator = None
            if self._polling and len(self._scheduled) == 1:
                self._notifier.interrupt()
        self._lock.release()
        return 0

    def _schedule(self):
        """Internal use only.
        """
        while not self._quit:
            # process I/O events
            self._notifier.poll(0)
            self._lock.acquire()
            if not self._scheduled:
                if self._timeouts:
                    now = _time()
                    timeout = self._timeouts[0][0]
                    timeout -= now
                    if timeout < 0.0001:
                        timeout = 0
                else:
                    timeout = None
                self._polling = True
                self._lock.release()
                self._notifier.poll(timeout)
                self._lock.acquire()
                self._polling = False
            if self._timeouts:
                # wake up timed suspends; pollers may timeout slightly
                # earlier, so give a bit of slack
                now = _time() + 0.0001
                while self._timeouts and self._timeouts[0][0] <= now:
                    timeout, cid, alarm_value = heappop(self._timeouts)
                    assert timeout <= now
                    coro = self._coros.get(cid, None)
                    if coro is None or coro._timeout != timeout:
                        continue
                    if coro._state not in (AsynCoro._AwaitIO_, AsynCoro._Suspended,
                                           AsynCoro._AwaitMsg_):
                        logger.warning('coro %s/%s is in state %s for resume; ignored',
                                       coro._name, coro._id, coro._state)
                        continue
                    coro._timeout = None
                    self._suspended.discard(cid)
                    self._scheduled.add(cid)
                    coro._state = AsynCoro._Scheduled
                    coro._value = alarm_value
            scheduled = [self._coros[cid] for cid in self._scheduled]
            # random.shuffle(scheduled)
            self._lock.release()

            for coro in scheduled:
                coro._state = AsynCoro._Running
                self._cur_coro = coro

                try:
                    if coro._exceptions:
                        exc = coro._exceptions.pop(0)
                        if exc[0] == GeneratorExit:
                            # assert str(exc[1]) == 'close'
                            coro._generator.close()
                            retval = coro._value
                        else:
                            retval = coro._generator.throw(*exc)
                    else:
                        retval = coro._generator.send(coro._value)
                except:
                    self._lock.acquire()
                    exc = sys.exc_info()
                    if exc[0] == StopIteration:
                        v = exc[1].args
                        if v:
                            if len(v) == 1:
                                coro._value = v[0]
                            else:
                                coro._value = v
                        coro._exceptions = []
                    elif exc[0] == HotSwapException:
                        v = exc[1].args
                        if isinstance(v, tuple) and len(v) == 1 and inspect.isgenerator(v[0]) and \
                               coro._hot_swappable and not coro._callers:
                            try:
                                coro._generator.close()
                            except:
                                logger.warning('closing %s/%s raised exception: %s',
                                               coro._name, coro._id, traceback.format_exc())
                            coro._generator = v[0]
                            coro._name = coro._generator.__name__
                            coro._exceptions = []
                            coro._value = None
                            # coro._msgs is not reset, so new
                            # generator can process pending messages
                            coro._state = AsynCoro._Scheduled
                        else:
                            logger.warning('invalid HotSwapException from %s/%s ignored',
                                           coro._name, coro._id)
                        self._lock.release()
                        continue
                    else:
                        coro._exceptions.append(exc)

                    if coro._callers:
                        # return to caller
                        caller = coro._callers.pop(-1)
                        coro._generator = caller[0]
                        if coro._new_generator and not coro._callers and coro._hot_swappable:
                            coro._exceptions.append((HotSwapException,
                                                     HotSwapException(coro._new_generator)))
                            coro._new_generator = None
                            coro._state = AsynCoro._Scheduled
                        elif coro._exceptions:
                            # exception in callee, restore saved value
                            coro._value = caller[1]
                            self._suspended.discard(coro._id)
                            self._scheduled.add(coro._id)
                            coro._state = AsynCoro._Scheduled
                        elif coro._state == AsynCoro._Running:
                            coro._state = AsynCoro._Scheduled
                    else:
                        if coro._exceptions:
                            exc = coro._exceptions[0]
                            assert isinstance(exc, tuple)
                            if len(exc) == 2:
                                exc = ''.join(traceback.format_exception_only(*exc))
                            else:
                                exc = ''.join(traceback.format_exception(*exc))
                            logger.warning('uncaught exception in %s:\n%s', coro, exc)
                            try:
                                coro._generator.close()
                            except:
                                logger.warning('closing %s raised exception: %s',
                                               coro._name, traceback.format_exc())
                        # delete this coro
                        if coro._state not in (AsynCoro._Scheduled, AsynCoro._Running):
                            logger.warning('coro "%s" is in state: %s' % (coro._name, coro._state))
                        monitors = list(coro._monitors)
                        for monitor in monitors:
                            if monitor._location == self._location:
                                if coro._exceptions:
                                    exc = MonitorException(coro, coro._exceptions[0])
                                else:
                                    exc = MonitorException(coro, (StopIteration,
                                                                  StopIteration(coro._value)))
                                if self._coros.get(monitor._id, None) == monitor:
                                    monitor.send(exc)
                                else:
                                    logger.warning('monitor for %s/%s has gone away!',
                                                   coro._name, coro._id)
                                    coro._monitors.discard(monitor)
                            else:
                                # remote monitor; prepare serializable data
                                if coro._exceptions:
                                    exc = coro._exceptions[0][:2]
                                    try:
                                        serialize(exc[1])
                                    except pickle.PicklingError:
                                        # send only the type
                                        exc = (exc[0], type(exc[1].args[0]))
                                    exc = MonitorException(coro, exc)
                                    coro._exceptions = []
                                else:
                                    exc = coro._value
                                    try:
                                        serialize(exc)
                                    except pickle.PicklingError:
                                        exc = type(exc)
                                    exc = MonitorException(coro, (StopIteration,
                                                                  StopIteration(exc)))
                                monitor.send(exc)
                        if not coro._monitors or not coro._exceptions:
                            coro._msgs.clear()
                            coro._monitors.clear()
                            coro._exceptions = []
                            if self._coros.pop(coro._id, None) != coro:
                                logger.warning('invalid coro: %s, %s' % (coro._id, coro._state))
                            if coro._daemon is True:
                                self._daemons -= 1
                        elif coro._monitors:
                            # a (local) monitor can restart it with hot_swap
                            coro._hot_swappable = True
                            coro._exceptions = []
                        coro._state = None
                        coro._generator = None
                        if coro._complete:
                            coro._complete.set()
                        else:
                            coro._complete = 0
                        self._scheduled.discard(coro._id)
                        if len(self._coros) == self._daemons:
                            self._complete.set()
                    self._lock.release()
                else:
                    self._lock.acquire()
                    if coro._state == AsynCoro._Running:
                        coro._state = AsynCoro._Scheduled
                        # if this coroutine is suspended, don't update
                        # the value; when it is resumed, it will be
                        # updated with the 'update' value
                        coro._value = retval

                    if isinstance(retval, types.GeneratorType):
                        # push current generator onto stack and activate
                        # new generator
                        coro._callers.append((coro._generator, coro._value))
                        coro._generator = retval
                        coro._value = None
                    self._lock.release()
            self._cur_coro = None

        self._lock.acquire()
        for coro in self._coros.itervalues():
            logger.debug('terminating Coro %s/%s%s', coro._name, coro._id,
                         ' (daemon)' if coro._daemon else '')
            self._cur_coro = coro
            coro._state = AsynCoro._Running
            while coro._generator:
                try:
                    coro._generator.close()
                except:
                    logger.warning('closing %s raised exception: %s',
                                   coro._generator.__name__, traceback.format_exc())
                if coro._callers:
                    coro._generator, coro._value = coro._callers.pop(-1)
                else:
                    coro._generator = None
            if coro._complete:
                coro._complete.set()
            else:
                coro._complete = 0
        self._scheduled = set()
        self._suspended = set()
        self._timeouts = []
        self._coros = {}
        self._channels = {}
        self.__class__.__instance = None
        self._lock.release()
        self._notifier.terminate()
        logger.debug('AsynCoro terminated')
        while self._atexit:
            func, fargs, fkwargs = self._atexit.pop()
            try:
                func(*fargs, **fkwargs)
            except:
                logger.warning('running %s failed:' % (func.__name__))
                logger.warning(traceback.format_exc())
        logging.shutdown()
        self._complete.set()

    def _exit(self, await_non_daemons):
        """Internal use only.
        """
        if not self._quit:
            if await_non_daemons:
                if len(self._coros) > self._daemons:
                    logger.debug('waiting for %s coroutines to terminate',
                                 (len(self._coros) - self._daemons))
            else:
                self._lock.acquire()
                for coro in self._coros.itervalues():
                    if not coro._daemon:
                        coro._daemon = True
                        self._daemons += 1
                if len(self._coros) != self._daemons:
                    logger.warning('dameons mismatch: %s != %s' % (len(self._coros), self._daemons))
                self._complete.set()
                self._lock.release()

            self._complete.wait()

            if self._location:
                def _close(self, peer, coro=None):
                    req = _NetRequest('peer_closed', kwargs={'loc':self._location},
                                      dst=peer.location, timeout=2)
                    yield self._sync_reply(req)
                for peer in _Peer.peers.values():
                    Coro(_close, self, peer)
                self._complete.wait()
            self._quit = True
            self._notifier.interrupt()
            self._complete.wait()
            # wait a bit for logger to flush
            time.sleep(0.01)

    def finish(self):
        """Wait until all non-daemon coroutines finish and then
        shutdown the scheduler.
        
        Should be called from main program (or a thread, but _not_
        from coroutines).
        """
        self._exit(True)

    def terminate(self):
        """Kill all non-daemon coroutines and shutdown the scheduler.

        Should be called from main program (or a thread, but _not_
        from coroutines).
        """
        self._exit(False)

    def join(self, show_running=False):
        """Wait for currently scheduled coroutines to finish. AsynCoro
        continues to execute, so new coroutines can be added if
        necessary.
        """
        if show_running:
            self._lock.acquire()
            for coro in self._coros.itervalues():
                logger.info('waiting for %s/%s%s', coro._name, coro._id,
                            ' (daemon)' if coro._daemon else '')
            self._lock.release()
        self._complete.wait()

    def atexit(self, func, *fargs, **fkwargs):
        """Function 'func' will be called after the scheduler has
        terminated.
        """
        self._atexit.append((func, fargs, fkwargs))

    def __repr__(self):
        return ''

class AsyncThreadPool(object):
    """Schedule synchronous tasks with threads to be executed
    asynchronously.

    NB: As coroutines run in a separate thread, any variables shared
    between coroutines and tasks scheduled with thread pool must be
    protected by thread locking (not coroutine locking).
    """

    def __init__(self, num_threads):
        self._num_threads = num_threads
        self._task_queue = Queue.Queue()
        for n in xrange(num_threads):
            tasklet = threading.Thread(target=self._tasklet)
            tasklet.daemon = True
            tasklet.start()

    def _tasklet(self):
        while True:
            item = self._task_queue.get(block=True)
            if item is None:
                self._task_queue.task_done()
                break
            coro, target, args, kwargs = item
            try:
                val = target(*args, **kwargs)
                coro._proceed_(val)
            except:
                coro.throw(*sys.exc_info())
            finally:
                self._task_queue.task_done()

    def async_task(self, target, *args, **kwargs):
        """Must be used with 'yield', as
        'val = yield pool.async_task(target, args, kwargs)'.

        @coro is coroutine where this method is called.

        @target is function/method that will be executed asynchronously
        in a thread.

        @args and @kwargs are arguments and keyword arguments passed
        to @target.

        This call effectively returns result of executing
        'target(*args, **kwargs)'.
        """

        if not inspect.isroutine(target):
            raise ValueError('invalid usage: "target" must be function or method')
        coro = AsynCoro.cur_coro()
        # assert isinstance(coro, Coro)
        # if arguments are passed as per Thread call, get args and kwargs
        if not args and kwargs:
            args = kwargs.pop('args', ())
            kwargs = kwargs.pop('kwargs', kwargs)
        coro._await_()
        self._task_queue.put((coro, target, args, kwargs))

    def join(self):
        """Wait till all scheduled tasks are completed.
        """
        self._task_queue.join()

    def terminate(self):
        """Wait for all scheduled tasks to complete and terminate
        threads.
        """
        for n in xrange(self._num_threads):
            self._task_queue.put(None)
        self._task_queue.join()

class AsyncDBCursor(object):
    """Database cursor proxy for asynchronous processing of executions.

    Since connections (and cursors) can't be shared in threads,
    operations on same cursor are run sequentially.
    """

    def __init__(self, thread_pool, cursor):
        self._thread_pool = thread_pool
        self._cursor = cursor
        self._sem = Semaphore()

    def __getattr__(self, name):
        return getattr(self._cursor, name)

    def _exec_task(self, func):
        try:
            return func()
        finally:
            self._sem.release()

    def execute(self, query, args=None):
        """Must be used with 'yield' as 'n = yield cursor.execute(stmt)'.
        """
        yield self._sem.acquire()
        self._thread_pool.async_task(self._exec_task,
                                     functools.partial(self._cursor.execute, query, args))

    def executemany(self, query, args):
        """Must be used with 'yield' as 'n = yield cursor.executemany(stmt)'.
        """
        yield self._sem.acquire()
        self._thread_pool.async_task(self._exec_task,
                                     functools.partial(self._cursor.executemany, query, args))

    def callproc(self, proc, args=()):
        """Must be used with 'yield' as 'yield cursor.callproc(proc)'.
        """
        yield self._sem.acquire()
        self._thread_pool.async_task(self._exec_task,
                                     functools.partial(self._cursor.callproc, proc, args))
