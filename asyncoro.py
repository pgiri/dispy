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
    from time import clock as _time
    _time()
else:
    from errno import EINPROGRESS
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

class AsynCoroSocket(socket.socket):
    """Socket to be used with AsynCoro. This makes use of asynchronous
    I/O completion and coroutines.
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
            if AsynCoroSocket._default_timeout:
                self.settimeout(AsynCoroSocket._default_timeout)

    def setblocking(self, blocking):
        self._blocking = blocking
        if blocking:
            if self._certfile:
                raise Exception('SSL is not supported for blocking sockets')
            self._rsock.setblocking(1)
            self.read = self.sync_read
            self.write = self.sync_write
            self.read_msg = self.sync_read_msg
            self.write_msg = self.sync_write_msg
            self._asyncoro = None
            if self._notifier:
                self._notifier.unregister(self)
            self._notifier = None
        else:
            self._rsock.setblocking(0)

            self.recv = self.async_recv
            self.send = self.async_send
            self.read = self.async_read
            self.write = self.async_write
            self.recvfrom = self.async_recvfrom
            self.sendto = self.async_sendto
            self.accept = self.async_accept
            self.connect = self.async_connect
            self.read_msg = self.async_read_msg
            self.write_msg = self.async_write_msg

            self._asyncoro = AsynCoro.instance()
            self._notifier = _AsyncNotifier.instance()
            self._notifier.register(self)

    def close(self):
        """'close' must be called when done with socket.
        """
        if self._notifier:
            self._notifier.unregister(self)
            self._notifier = None
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
        if self._notifier:
            self._notifier.unregister(self)
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
            return AsynCoroSocket._default_timeout

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

    def async_recv(self, bufsize, *args):
        """Asynchronous version of socket recv method.
        """
        def _recv(self, bufsize, *args):
            try:
                self._result = self._rsock.recv(bufsize, *args)
            except ssl.SSLError, err:
                if err.args[0] != ssl.SSL_ERROR_WANT_READ:
                    raise Exception('socket reading error')
            except:
                self._task = None
                self._notifier.unregister(self)
                coro, self._coro = self._coro, None
                coro.throw(*sys.exc_info())
            else:
                self._task = None
                self._notifier.modify(self, 0)
                coro, self._coro = self._coro, None
                if self._result > 0:
                    coro.resume(self._result)
                else:
                    coro.throw(Exception, Exception('socket recv error'), None)

        if self._certfile:
            # in case of SSL, attempt read first
            try:
                self._result = self._rsock.recv(bufsize)
            except ssl.SSLError, err:
                if err.args[0] != ssl.SSL_ERROR_WANT_READ:
                    logging.debug('reading error: %s for %s', sys.exc_type, self._fileno)
                    # TODO: throw this exception?
                elif len(self._result) > 0:
                    return self._result

        self._task = functools.partial(_recv, self, bufsize, *args)
        self._coro = self._asyncoro.cur_coro()
        self._coro.suspend()
        self._notifier.modify(self, _AsyncNotifier._Readable)

    def async_read(self, bufsize, *args):
        """Read exactly bufsize bytes.
        """
        self._result = bytearray()
        def _read(self, bufsize, *args):
            plen = len(self._result)
            try:
                self._result[len(self._result):] = self._rsock.recv(bufsize - len(self._result), *args)
            except ssl.SSLError, err:
                if err.args[0] != ssl.SSL_ERROR_WANT_READ:
                    raise Exception('socket reading error')
            except:
                self._task = None
                self._notifier.unregister(self)
                coro, self._coro = self._coro, None
                coro.throw(*sys.exc_info())
            else:
                if len(self._result) == bufsize or self._rsock.type == socket.SOCK_DGRAM:
                    self._task = None
                    self._notifier.modify(self, 0)
                    coro, self._coro = self._coro, None
                    if coro.resume(str(self._result)):
                        logging.debug('resume failed for %s, %s', coro._id, self._fileno)
                elif len(self._result) == plen:
                    # TODO: check for error and delete?
                    self._task = None
                    self._result = None
                    self._notifier.modify(self, 0)
                    coro, self._coro = self._coro, None
                    coro.throw(Exception, Exception('socket reading error'), None)

        if self._certfile:
            # in case of SSL, attempt read first
            try:
                self._result[len(self._result):] = self._rsock.read(bufsize)
            except ssl.SSLError, err:
                if err.args[0] != ssl.SSL_ERROR_WANT_READ:
                    logging.debug('reading error: %s for %s', sys.exc_type, self._fileno)
            else:
                if len(self._result) == bufsize:
                    return str(self._result)

        self._task = functools.partial(_read, self, bufsize, *args)
        self._coro = self._asyncoro.cur_coro()
        self._coro.suspend()
        self._notifier.modify(self, _AsyncNotifier._Readable)

    def sync_read(self, bufsize, *args):
        """Synchronous version of async_read.
        """
        self._result = bytearray()
        while len(self._result) < bufsize:
            self._result[len(self._result):] = self._rsock.recv(bufsize - len(self._result), *args)
        return str(self._result)

    def async_recvfrom(self, *args):
        """Asynchronous version of socket recvfrom method.
        """
        def _recvfrom(self, *args):
            try:
                self._result, addr = self._rsock.recvfrom(*args)
            except:
                self._task = None
                self._notifier.unregister(self)
                coro, self._coro = self._coro, None
                coro.throw(*sys.exc_info())
            else:
                self._task = None
                self._notifier.modify(self, 0)
                self._result = (self._result, addr)
                coro, self._coro = self._coro, None
                coro.resume(self._result)

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
                self._result = self._rsock.send(*args)
            except:
                # TODO: close socket, inform coro
                self._task = None
                logging.debug('write error', self._fileno)
                self._notifier.unregister(self)
                coro, self._coro = self._coro, None
                coro.throw(*sys.exc_info())
            else:
                self._task = None
                self._notifier.modify(self, 0)
                coro, self._coro = self._coro, None
                coro.resume(self._result)

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
                self._result = self._rsock.sendto(*args)
            except:
                self._task = None
                self._notifier.unregister(self)
                coro, self._coro = self._coro, None
                coro.throw(*sys.exc_info())
            else:
                self._task = None
                self._notifier.modify(self, 0)
                coro, self._coro = self._coro, None
                coro.resume(self._result)

        self._task = functools.partial(_sendto, self, *args)
        self._coro = self._asyncoro.cur_coro()
        self._coro.suspend()
        self._notifier.modify(self, _AsyncNotifier._Writable)

    def async_write(self, data):
        """Send all the data (similar to sendall).
        """
        self._result = buffer(data, 0)
        def _write(self):
            try:
                n = self._rsock.send(self._result)
            except:
                self._task = None
                self._result = None
                self._notifier.unregister(self)
                coro, self._coro = self._coro, None
                coro.throw(*sys.exc_info())
            else:
                if n > 0:
                    self._result = buffer(self._result, n)
                    if len(self._result) == 0:
                        self._task = None
                        self._notifier.modify(self, 0)
                        coro, self._coro = self._coro, None
                        coro.resume(0)

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
            n = self._rsock.send(self._result)
            if n > 0:
                self._result = buffer(self._result, n)
        return 0

    def async_accept(self):
        """Asynchronous version of socket accept method. Socket in
        returned pair is AsynCoroSocket.
        """
        def _accept(self):
            conn, addr = self._rsock.accept()
            self._notifier.modify(self, 0)
            coro, self._coro = self._coro, None
            self._task = None

            if self._certfile:
                def _ssl_handshake(conn, addr, coro):
                    try:
                        conn._rsock.do_handshake()
                    except ssl.SSLError, err:
                        if err.args[0] == ssl.SSL_ERROR_WANT_READ:
                            conn._notifier.modify(conn, _AsyncNotifier._Readable)
                        elif err.args[0] == ssl.SSL_ERROR_WANT_WRITE:
                            conn._notifier.modify(conn, _AsyncNotifier._Writable)
                        else:
                            conn._task = None
                            conn._notifier.unregister(conn)
                            conn.close()
                            coro.throw(*sys.exc_info())
                    else:
                        conn._task = None
                        conn._notifier.modify(conn, 0)
                        coro.resume((conn, addr))
                conn = AsynCoroSocket(conn, blocking=False, keyfile=self._keyfile,
                                      certfile=self._certfile, ssl_version=self._ssl_version)
                conn._rsock = ssl.wrap_socket(conn, keyfile=self._keyfile, certfile=self._certfile,
                                              server_side=True, do_handshake_on_connect=False,
                                              ssl_version=self._ssl_version)

                conn._task = functools.partial(_ssl_handshake, conn, addr, coro)
                conn._task()
            else:
                conn = AsynCoroSocket(conn, blocking=False)
                self._result = (conn, addr)
                coro.resume(self._result)

        self._task = functools.partial(_accept, self)
        self._coro = self._asyncoro.cur_coro()
        self._coro.suspend()
        self._notifier.modify(self, _AsyncNotifier._Readable)

    def async_connect(self, *args):
        """Asynchronous version of socket connect method.
        """
        def _connect(self, *args):
            err = self._rsock.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
            if not err:
                self._task = None
                self._notifier.modify(self, 0)
                coro, self._coro = self._coro, None

                if self._certfile:
                    def _ssl_handshake(conn, coro):
                        try:
                            conn._rsock.do_handshake()
                        except ssl.SSLError, err:
                            if err.args[0] == ssl.SSL_ERROR_WANT_READ:
                                conn._notifier.modify(conn, _AsyncNotifier._Readable)
                            elif err.args[0] == ssl.SSL_ERROR_WANT_WRITE:
                                conn._notifier.modify(conn, _AsyncNotifier._Writable)
                            else:
                                conn._task = None
                                conn._notifier.unregister(conn)
                                conn.close()
                                coro.throw(*sys.exc_info())
                        else:
                            conn._notifier.modify(conn, 0)
                            coro.resume(0)

                    self._rsock = ssl.wrap_socket(self._rsock, keyfile=self._keyfile,
                                                  certfile=self._certfile, server_side=False,
                                                  do_handshake_on_connect=False)
                    self._task = functools.partial(_ssl_handshake, self, coro)
                    self._task()
                else:
                    coro.resume(0)
            else:
                logging.debug('connect error: %s', err)

        self._task = functools.partial(_connect, self, *args)
        self._coro = self._asyncoro.cur_coro()
        self._coro.suspend()
        self._notifier.modify(self, _AsyncNotifier._Writable)
        try:
            self._rsock.connect(*args)
        except socket.error, e:
            if e.args[0] != EINPROGRESS:
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
        suspended till that many seconds (or fractions of second).
        """
        if self._asyncoro:
            return self._asyncoro._suspend(self._id, timeout)
        else:
            logging.warning('suspend: coroutine %s removed?', self.name)
            return -1

    sleep = suspend

    def resume(self, update=None):
        """Resume/wake up this coro and send 'update' to it.

        The resuming coro gets 'update' for the 'yield' that caused it
        to suspend.
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

        NB: This method should not be called from a coroutine! This
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

    # in _running set, waiting for turn to execute
    _Scheduled = 1
    # in _running, currently executing
    _Running = 2
    # in _suspended, but executing
    _Suspended = 3
    # in _suspended, not executing
    _Stopped = 4

    _coro_id = 1

    def __init__(self):
        if self.__class__.__instance is None:
            self.__class__.__instance = self
            self._coros = {}
            self._cur_coro = None
            self._running = set()
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
        self._running.add(coro._id)
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
        self._running.discard(cid)
        coro._state = AsynCoro._Suspended
        self._suspended.add(cid)
        if timeout is not None:
            timeout = _time() + timeout
            heappush(self._timeouts, (timeout, cid))
            coro._timeout = timeout
            self._sched_cv.notify()
        else:
            coro._timeout = None
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
            if coro._exception:
                # this can happen with sockets with timeouts:
                # _AsyncNotifier may throw timeout exception, but
                # before exception is thrown to coro, I/O operation
                # may complete
                logging.debug('throwing away exception for %s/%s', coro.name, cid)
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
        self._running.add(cid)
        coro._state = AsynCoro._Scheduled
        self._sched_cv.notify()
        self._sched_cv.release()
        return 0

    def _delete(self, coro):
        """Internal use only.
        """
        # called with _sched_cv locked
        cid = self._coros.pop(coro._id, None)
        if cid is None:
            logging.debug('%s/%s is already removed', coro.name, coro._id)
        else:
            self._running.discard(coro._id)
            self._suspended.discard(coro._id)
            coro._asyncoro = None
            assert not coro._callers
            coro._complete.set()
            coro._state = None
            if not self._coros:
                self._complete.set()

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
        coro._exception = args
        self._suspended.discard(coro._id)
        self._running.add(coro._id)
        coro._state = AsynCoro._Scheduled
        self._sched_cv.notify()
        self._sched_cv.release()
        return 0

    def _terminate_coro(self, cid):
        """Internal use only.
        """
        self._sched_cv.acquire()
        coro = self._coros.get(cid, None)
        if coro is None or coro._state not in [AsynCoro._Scheduled, AsynCoro._Suspended]:
            logging.warning('invalid coroutine %s to terminate', cid)
            self._sched_cv.release()
            return -1
        coro._exception = (GeneratorExit, GeneratorExit('close'))
        self._suspended.discard(cid)
        self._running.add(cid)
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
            while (not self._running) and (not self._terminate):
                self._sched_cv.wait(timeout)
                if timeout is not None:
                    break
            if self._terminate:
                for cid in self._running.union(self._suspended):
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
                self._running = self._suspended = set()
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
                    coro._timeout = None
                    self._suspended.discard(coro._id)
                    self._running.add(coro._id)
                    coro._state = AsynCoro._Scheduled
                    coro._value = None
            running = [self._coros.get(cid, None) for cid in self._running]
            # random.shuffle(running)
            self._sched_cv.release()

            for coro in running:
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
                    if sys.exc_type == StopIteration:
                        coro._exception = None
                    else:
                        coro._exception = sys.exc_info()
                        coro._value = None

                    if coro._callers:
                        # return to caller
                        caller = coro._callers.pop(-1)
                        assert len(caller) == 2
                        assert isinstance(caller[0], types.GeneratorType)
                        coro._generator = caller[0]
                        if isinstance(coro._exception, tuple) and \
                               coro._exception[0] != StopIteration:
                            coro._value = caller[1]
                        coro._state = AsynCoro._Scheduled
                    else:
                        if coro._exception is not None:
                            assert isinstance(coro._exception, tuple)
                            if len(coro._exception) == 2:
                                exc = ''.join(traceback.format_exception_only(*coro._exception))
                            else:
                                exc = ''.join(traceback.format_exception(*coro._exception))
                            logging.warning('uncaught exception in %s:\n%s', coro.name, exc)
                        self._delete(coro)
                    self._sched_cv.release()
                else:
                    self._sched_cv.acquire()
                    self._cur_coro = None
                    if coro._state == AsynCoro._Running:
                        coro._state = AsynCoro._Scheduled
                        # if this coroutine is suspended, don't update
                        # the value; it will be updated with the value
                        # with which it is resumed
                        # TODO: not update if it is generator?
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

    Timeouts for socket operations are handled in a rather simplisitc
    way for efficiency: Instead of maintaining timeouts in sorted data
    structure, we check if any socket I/O operation has timedout every
    'timeout_interval' (default 10) seconds.
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

    def __init__(self, poll_interval=2):
        if self.__class__.__instance is None:
            if poll_interval < 1:
                logging.warning('invalid poll_interval; using 1 as poll_interval')
                poll_interval = 1
            elif poll_interval > 10:
                logging.warning('invalid poll_interval; using 10 as poll_interval')
                poll_interval = 10
            self.__class__.__instance = self

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
                self._poller = _SelectNotifier()
                self.__class__._Readable = 0x01
                self.__class__._Writable = 0x04
                self.__class__._Hangup = 0
                self.__class__._Error = 0x10

            self._fds = {}
            self._terminate = False
            self._timeouts = []
            self._timeout_fds = []
            self._lock = threading.Lock()
            self._notifier_thread = threading.Thread(target=self._notifier, args=(poll_interval,))
            self._notifier_thread.daemon = True
            self._notifier_thread.start()
            # TODO: add controlling fd to wake up poller (for
            # termination)

    def _notifier(self, poll_interval):
        """Calls 'task' method of registered fds when there is a
        read/write event for it. Since coroutuines can do only one
        thing at a time, only one of read, write tasks can be done.
        """
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
                for fd, evnt in events:
                    if fd is None:
                        logging.debug('Invalid fd for event %s!', event)
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
                assert fd._timeout_id == self._timeouts[0]
                if fd._coro:
                    try:
                        fd._coro.throw(socket.timeout, socket.timeout('timed out'))
                    except:
                        logging.debug(traceback.format_exc())
                    # don't clear coro/task fields; the operation may still succeed
                del self._timeouts[0]
                del self._timeout_fds[0]
                fd._timeout_id = None
            if self._timeouts:
                now = _time()
                timeout = self._timeouts[0] - now
                if timeout < 0.5:
                    timeout = 0.5
            else:
                timeout = poll_interval
            self._lock.release()
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
        if fd._timeout_id is not None:
            self._lock.acquire()
            i = bisect_left(self._timeouts, fd._timeout_id)
            # in case of identical timeouts (unlikely?), search for
            # correct index where fd is
            for i in xrange(i, len(self._timeouts)):
                if self._timeout_fds[i] == fd:
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
            logging.debug('fd %s is already unregistered', fd._fileno)
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
        self._lock.acquire()
        for fd in self._fds.itervalues():
            try:
                self._poller.unregister(fd._fileno)
            except:
                logging.warning('unregister of %s failed with %s',
                                fd._fileno, traceback.format_exc())
        self._terminate = True
        self._lock.release()
        if hasattr(self._poller, 'terminate'):
            self._poller.terminate()

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

    # TODO: For Windows more efficient asynchronous notifier is needed
    __metaclass__ = MetaSingleton

    def __init__(self):
        if not hasattr(self, 'poller'):
            self.poller = select.select
            self.rset = set()
            self.wset = set()
            self.xset = set()

            self.cmd_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.cmd_sock.setblocking(0)
            self.cmd_sock.bind(('', 0))
            self.cmd_addr = self.cmd_sock.getsockname()
            self.cmd_fd = self.cmd_sock.fileno()
            self.update_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.rset.add(self.cmd_fd)

    def register(self, fid, event):
        if event == _AsyncNotifier._Readable:
            self.rset.add(fid)
        elif event == _AsyncNotifier._Writable:
            self.wset.add(fid)
        elif event & _AsyncNotifier._Error:
            self.xset.add(fid)
        self.update_sock.sendto('update', self.cmd_addr)

    def unregister(self, fid):
        self.rset.discard(fid)
        self.wset.discard(fid)
        self.xset.discard(fid)
        self.update_sock.sendto('update', self.cmd_addr)

    def modify(self, fid, event):
        self.unregister(fid)
        self.register(fid, event)
        self.update_sock.sendto('update', self.cmd_addr)

    def poll(self, timeout):
        rlist, wlist, xlist = self.poller(self.rset, self.wset, self.xset, timeout)
        events = {}
        for fid in rlist:
            events[fid] = _AsyncNotifier._Readable
        for fid in wlist:
            events[fid] = _AsyncNotifier._Writable
        for fid in xlist:
            events[fid] = _AsyncNotifier._Error

        if events.pop(self.cmd_fd, None) == self.cmd_fd:
            cmd, addr = self.cmd_sock.recvfrom(128)
            # assert addr[1] == self.update_sock.getsockname()[1]
        return events.iteritems()

    def terminate(self):
        self.update_sock.close()
        self.cmd_sock.close()
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
