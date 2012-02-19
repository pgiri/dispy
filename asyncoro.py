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
import hashlib
import logging
import errno
import platform
import random
import ssl
from bisect import bisect_left

if platform.system() == 'Windows':
    from errno import WSAEINPROGRESS as EINPROGRESS
else:
    from errno import EINPROGRESS

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
    sock = AsynCoroSocket(sock, blocking=False)
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

    def __init__(self, sock, blocking=False, timeout=True, keyfile=None, certfile=None,
                 server_side=False):
        """Setup sock for use wih asyncoro.

        blocking=True implies synchronous sockets and blocking=False
        implies asynchronous sockets.

        For synchronous sockets timeout must be a number. For
        asynchronous sockets, it must be either True or False. For
        server sockets (that wait for incoming connections) timeout
        must be False; otherwise, those sockets will timeout, causing
        exception.

        keyfile and certfile are as per ssl's wrap_socket method.
        """

        socket.socket.__init__(self, _sock=sock._sock)
        self.blocking = blocking == True
        self.sock = sock
        self.keyfile = keyfile
        self.certfile = certfile
        self.result = None
        self.fileno = sock.fileno()

        if self.blocking:
            if self.certfile:
                self.sock = ssl.wrap_socket(self.sock, keyfile=self.keyfile, certfile=self.certfile,
                                            server_side=server_side)
                # self.fileno = sock.fileno()
            self.timestamp = None
            if isinstance(timeout, (int, float)):
                self.sock.settimeout(timeout)
            self.read = self.sync_read
            self.write = self.sync_write
            self.read_msg = self.sync_read_msg
            self.write_msg = self.sync_write_msg
            self._notifier = None
            self._asyncoro = None
            self._coro = None
        else:
            self.sock.setblocking(0)
            if timeout:
                self.timestamp = 0
            else:
                self.timestamp = None

            self.task = None
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
            self._coro = None
            self._notifier = _AsyncNotifier.instance()
            self._notifier.add_fd(self)

    def async_recv(self, bufsize, *args):
        """Asynchronous version of socket recv method.
        """
        def _recv(self, bufsize, *args):
            try:
                self.result = self.sock.recv(bufsize, *args)
            except:
                logging.warning(traceback.format_exc())
            else:
                if self.result >= 0:
                    self._notifier.modify(self, 0)
                    coro, self._coro = self._coro, None
                    coro.resume(self.result)
                else:
                    self._notifier.modify(self, 0)
                    coro, self._coro = self._coro, None
                    coro.throw(Exception, Exception('socket recv error'), None)

        if self.certfile:
            # in case of SSL, attempt read first
            try:
                self.result= self.sock.recv(bufsize)
            except ssl.SSLError, err:
                if err.args[0] != ssl.SSL_ERROR_WANT_READ:
                    logging.debug('reading error: %s for %s', sys.exc_type, self.fileno)
                    # TODO: throw this exception?
            else:
                return self.result

        self.task = functools.partial(_recv, self, bufsize, *args)
        if self.timestamp is not None:
            self.timestamp = time.time()
        self._coro = self._asyncoro.cur_coro()
        self._coro.suspend()
        self._notifier.modify(self, _AsyncNotifier._Readable)

    def async_read(self, bufsize, *args):
        """Read exactly bufsize bytes.
        """
        self.result = bytearray()
        def _read(self, bufsize, *args):
            plen = len(self.result)
            try:
                self.result[len(self.result):] = self.sock.recv(bufsize - len(self.result), *args)
            except:
                logging.warning(traceback.format_exc())
            else:
                if len(self.result) == bufsize or self.sock.type == socket.SOCK_DGRAM:
                    self._notifier.modify(self, 0)
                    self.result = str(self.result)
                    coro, self._coro = self._coro, None
                    coro.resume(self.result)
                elif len(self.result) == plen:
                    # TODO: check for error and delete?
                    self.result = None
                    self._notifier.modify(self, 0)
                    coro, self._coro = self._coro, None
                    coro.throw(Exception, Exception('socket reading error'), None)

        if self.certfile:
            # in case of SSL, attempt read first
            try:
                self.result[len(self.result):] = self.sock.read(bufsize)
            except ssl.SSLError, err:
                if err.args[0] != ssl.SSL_ERROR_WANT_READ:
                    logging.debug('reading error: %s for %s', sys.exc_type, self.fileno)
            else:
                if len(self.result) == bufsize:
                    return str(self.result)

        self.task = functools.partial(_read, self, bufsize, *args)
        if self.timestamp is not None:
            self.timestamp = time.time()
        self._coro = self._asyncoro.cur_coro()
        self._coro.suspend()
        self._notifier.modify(self, _AsyncNotifier._Readable)

    def sync_read(self, bufsize, *args):
        """Synchronous version of async_read.
        """
        self.result = bytearray()
        while len(self.result) < bufsize:
            self.result[len(self.result):] = self.sock.recv(bufsize - len(self.result), *args)
        return str(self.result)

    def async_recvfrom(self, *args):
        """Asynchronous version of socket recvfrom method.
        """
        def _recvfrom(self, *args):
            self.result, addr = self.sock.recvfrom(*args)
            self._notifier.modify(self, 0)
            self.result = (self.result, addr)
            coro, self._coro = self._coro, None
            coro.resume(self.result)

        self.task = functools.partial(_recvfrom, self, *args)
        if self.timestamp is not None:
            self.timestamp = time.time()
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
                self.result = self.sock.send(*args)
            except:
                # TODO: close socket, inform coro
                logging.debug('write error', self.fileno)
                self._notifier.unregister(self)
                coro, self._coro = self._coro, None
                coro.throw(*sys.exc_info())
            else:
                self._notifier.modify(self, 0)
                coro, self._coro = self._coro, None
                coro.resume(self.result)

        self.task = functools.partial(_send, self, *args)
        if self.timestamp is not None:
            self.timestamp = time.time()
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
                self.result = self.sock.sendto(*args)
            except:
                # TODO: close socket, inform coro
                logging.debug('write error: %s', traceback.format_exc())
                self._notifier.unregister(self)
                coro, self._coro = self._coro, None
                coro.throw(*sys.exc_info())
            else:
                self._notifier.modify(self, 0)
                coro, self._coro = self._coro, None
                coro.resume(self.result)

        self.task = functools.partial(_sendto, self, *args)
        if self.timestamp is not None:
            self.timestamp = time.time()
        self._coro = self._asyncoro.cur_coro()
        self._coro.suspend()
        self._notifier.modify(self, _AsyncNotifier._Writable)

    def async_write(self, data):
        """Send all the data (similar to sendall).
        """
        self.result = buffer(data, 0)
        def _write(self):
            try:
                n = self.sock.send(self.result)
            except:
                logging.error('writing failed %s: %s', self.fileno, traceback.format_exc())
                # TODO: close socket, inform coro
                self.result = None
                self._notifier.unregister(self)
                coro, self._coro = self._coro, None
                coro.throw(*sys.exc_info())
            else:
                if n > 0:
                    self.result = buffer(self.result, n)
                    if len(self.result) == 0:
                        self._notifier.modify(self, 0)
                        coro, self._coro = self._coro, None
                        coro.resume(0)

        self.task = functools.partial(_write, self)
        if self.timestamp is not None:
            self.timestamp = time.time()
        self._coro = self._asyncoro.cur_coro()
        self._coro.suspend()
        self._notifier.modify(self, _AsyncNotifier._Writable)

    def sync_write(self, data):
        """Synchronous version of async_write.
        """
        # TODO: is sendall better?
        self.result = buffer(data, 0)
        while len(self.result) > 0:
            n = self.sock.send(self.result)
            if n > 0:
                self.result = buffer(self.result, n)
        return 0

    def close(self):
        """Close AsynCoroSocket.
        """
        if self._notifier:
            self._notifier.del_fd(self)
        self.sock.close()

    def async_accept(self):
        """Asynchronous version of socket accept method.
        """
        def _accept(self):
            conn, addr = self.sock.accept()
            self._notifier.modify(self, 0)

            if self.certfile:
                def _ssl_handshake(conn, addr, coro):
                    try:
                        conn.sock.do_handshake()
                    except ssl.SSLError, err:
                        if err.args[0] == ssl.SSL_ERROR_WANT_READ:
                            conn._notifier.modify(conn, _AsyncNotifier._Readable)
                        elif err.args[0] == ssl.SSL_ERROR_WANT_WRITE:
                            conn._notifier.modify(conn, _AsyncNotifier._Writable)
                        else:
                            raise
                    else:
                        conn._notifier.modify(conn, 0)
                        conn.result = (conn, addr)
                        coro.resume(conn.result)
                conn = AsynCoroSocket(conn, blocking=False,
                                      keyfile=self.keyfile, certfile=self.certfile)
                conn.sock = ssl.wrap_socket(conn, keyfile=self.keyfile, certfile=self.certfile,
                                            server_side=True, do_handshake_on_connect=False)

                conn.task = functools.partial(_ssl_handshake, conn, addr, self._coro)
                conn.task()
            else:
                coro, self._coro = self._coro, None
                self.result = (conn, addr)
                coro.resume(self.result)

        self.task = functools.partial(_accept, self)
        self.timestamp = None
        self._coro = self._asyncoro.cur_coro()
        self._coro.suspend()
        self._notifier.modify(self, _AsyncNotifier._Readable)

    def async_connect(self, *args):
        """Asynchronous version of socket connect method.
        """
        def _connect(self, *args):
            # TODO: check with getsockopt
            err = self.sock.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
            if not err:
                self._notifier.modify(self, 0)
                coro, self._coro = self._coro, None

                if self.certfile:
                    def _ssl_handshake(conn, coro):
                        try:
                            conn.sock.do_handshake()
                        except ssl.SSLError, err:
                            if err.args[0] == ssl.SSL_ERROR_WANT_READ:
                                conn._notifier.modify(conn, _AsyncNotifier._Readable)
                            elif err.args[0] == ssl.SSL_ERROR_WANT_WRITE:
                                conn._notifier.modify(conn, _AsyncNotifier._Writable)
                            else:
                                raise
                        else:
                            conn._notifier.modify(conn, 0)
                            coro.resume(0)

                    self.sock = ssl.wrap_socket(self.sock, keyfile=self.keyfile, certfile=self.certfile,
                                                server_side=False, do_handshake_on_connect=False)
                    self.task = functools.partial(_ssl_handshake, self, coro)
                    self.task()
                else:
                    coro.resume(0)
            else:
                logging.debug('connect error: %s', err)

        self.task = functools.partial(_connect, self, *args)
        if self.timestamp is not None:
            self.timestamp = time.time()
        self._coro = self._asyncoro.cur_coro()
        self._coro.suspend()
        self._notifier.modify(self, _AsyncNotifier._Writable)
        try:
            self.sock.connect(*args)
        except socket.error, e:
            if e.args[0] != EINPROGRESS:
                logging.debug('connect error: %s', e.args[0])

    def async_read_msg(self):
        """Message is tagged with unique identifier (integer) and
        length of the payload (data). This method reads entire message and
        reads uid and the message as tuple.
        """
        try:
            info_len = struct.calcsize('>L')
            info = yield self.read(info_len)
            if len(info) < info_len:
                logging.error('Socket disconnected?(%s, %s)', len(info), info_len)
                yield (None, None)
            msg_len = struct.unpack('>L', info)[0]
            assert msg_len > 0
            msg = yield self.read(msg_len)
            if len(msg) < msg_len:
                logging.error('Socket disconnected?(%s, %s)', len(msg), msg_len)
                yield None
            yield msg
        except:
            logging.error('Socket reading error for %s: %s', self.fileno, traceback.format_exc())
            raise

    def sync_read_msg(self):
        """Synchronous version of async_read_msg.
        """
        try:
            info_len = struct.calcsize('>L')
            info = self.sync_read(info_len)
            if len(info) < info_len:
                logging.error('Socket disconnected?(%s, %s)', len(info), info_len)
                return None
            msg_len = struct.unpack('>L', info)[0]
            assert msg_len > 0
            msg = self.sync_read(msg_len)
            if len(msg) < msg_len:
                logging.error('Socket disconnected?(%s, %s)', len(msg), msg_len)
                return None
            return msg
        except socket.timeout:
            logging.error('Socket disconnected(timeout)?')
            return None

    def async_write_msg(self, data):
        """Messages are tagged with length of the data, so on the
        receiving side, read_msg knows how much data to read.
        """
        yield self.write(struct.pack('>L', len(data)) + data)

    def sync_write_msg(self, data):
        """Synchronous version of async_write_msg.
        """
        return self.sync_write(struct.pack('>L', len(data)) + data)

class Coro(object):
    """'Coroutine' factory to build coroutines to be scheduled with
    AsynCoro. Automatically starts executing 'func'.  The function
    definition should have 'coro' argument set to (default value)
    None. When the function is called, that argument will be this
    object.
    """
    def __init__(self, func, *args, **kwargs):
        self.name = func.__name__
        if inspect.isfunction(func) or inspect.ismethod(func):
            if not inspect.isgeneratorfunction(func):
                raise Exception('%s is not a generator!' % func.__name__)
            if 'coro' in kwargs:
                raise Exception('Coro function %s should not be called with ' \
                                '"coro" parameter' % self.name)
            callargs = inspect.getcallargs(func, *args, **kwargs)
            if 'coro' not in callargs or callargs['coro'] is not None:
                raise Exception('Coro function %s should have "coro" argument with ' \
                                'default value None' % self.name)
            kwargs['coro'] = self
            self._generator = func(*args, **kwargs)
        else:
            raise Exception('Invalid coroutine function %s', func.__name__)
        self._id = None
        self._state = None
        self._value = None
        self._exception = False
        self._callers = []
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
            self._asyncoro._suspend(self._id, timeout)
        else:
            logging.warning('suspend: coroutine %s removed?', self.name)

    sleep = suspend

    def resume(self, update=None):
        """Resume/wake up this coro and send 'update' to it.

        The resuming coro gets 'update' for the 'yield' that caused it
        to suspend.
        """
        if self._asyncoro:
            self._asyncoro._resume(self._id, update)
        else:
            logging.warning('resume: coroutine %s removed?', self.name)
      
    wakeup = resume

    def throw(self, *args):
        """Throw exception in coroutine.
        """
        if self._asyncoro:
            self._asyncoro._throw(self._id, *args)
        else:
            logging.warning('throw: coroutine %s removed?', self.name)

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
            self._asyncoro._terminate_coro(self._id)
        else:
            logging.warning('terminate: coroutine %s removed?', self.name)

class CoroLock(object):
    """'Lock' primitive for coroutines.

    Since a coroutine runs until 'yield', there is no need for
    lock. The caller has to guarantee that once a lock is obtained,
    'yield' call is not made.
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
    'yield' call is not made, except for the case of 'wait'. See
    'dispy.py' on how to use it.
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
            assert self._owner == coro, '"%s"/%s: invalid condition variable release - owned by "%s"/%s' % \
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
    only properties available to users are 'terminate' and 'join'
    methods.
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
    # called another coro, not in _running or _suspended
    # this should not be necessary (anymore)
    _Frozen = 5

    _coro_id = 1

    def __init__(self):
        if self.__class__.__instance is None:
            self.__class__.__instance = self
            self._coros = {}
            self._cur_coro = None
            self._running = set()
            self._suspended = set()
            # if a coro sleeps till timeout, then the timeout is
            # inserted into (sorted) _timeouts list and its id is
            # inserted into _timeout_cids at the same index
            self._timeouts = []
            self._timeout_cids = []
            # because AsyncNotifier runs in a separate thread and
            # calls coro functions (resume, throw etc.), we need to
            # lock
            self._sched_cv = threading.Condition()
            self._terminate = False
            self._lock = threading.RLock()
            self._complete = threading.Event(self._lock)
            self._scheduler = threading.Thread(target=self._scheduler)
            self._scheduler.daemon = True
            self._scheduler.start()

            self.notifier = _AsyncNotifier()

    @classmethod
    def instance(cls):
        """Returns (singleton) instance of AsynCoro. This method
        should be called only after initializing AsynCoro.
        """
        return cls.__instance

    def cur_coro(self):
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
                return
        self._sched_cv.acquire()
        coro = self._coros.get(cid, None)
        if coro is None:
            self._sched_cv.release()
            logging.warning('invalid coroutine %s to suspend', cid)
            return
        if coro._state == AsynCoro._Running:
            self._running.discard(cid)
            coro._state = AsynCoro._Suspended
            self._suspended.add(cid)
            if timeout is not None:
                timeout = time.time() + timeout
                i = bisect_left(self._timeouts, timeout)
                self._timeouts.insert(i, timeout)
                self._timeout_cids.insert(i, cid)
                self._sched_cv.notify()
        else:
            logging.warning('invalid coroutine %s/%s to suspend: %s',
                            coro.name, coro._id, coro._state)
        self._sched_cv.release()

    def _resume(self, cid, update):
        """Internal use only. See resume in Coro.
        """
        self._sched_cv.acquire()
        coro = self._coros.get(cid, None)
        if coro is None:
            self._sched_cv.release()
            logging.warning('invalid coroutine %s to resume', cid)
            return
        if coro._state in [AsynCoro._Stopped, AsynCoro._Suspended]:
            coro._value = update
            self._suspended.discard(cid)
            self._running.add(cid)
            coro._state = AsynCoro._Scheduled
            self._sched_cv.notify()
        else:
            logging.warning('invalid coroutine %s/%s to resume: %s',
                            coro.name, cid, coro._state)
        self._sched_cv.release()

    def _delete(self, coro):
        """Internal use only.
        """
        # called with _sched_cv locked
        self._running.discard(coro._id)
        self._suspended.discard(coro._id)
        coro._asyncoro = None
        assert not coro._callers
        coro._complete.set()
        del self._coros[coro._id]
        if not self._coros:
            self._complete.set()

    def _throw(self, cid, *args):
        """Internal use only. See throw in Coro.
        """
        self._sched_cv.acquire()
        coro = self._coros.get(cid, None)
        if coro is None:
            logging.warning('invalid coroutine %s to throw exception', cid)
        elif coro._state in [AsynCoro._Scheduled, AsynCoro._Stopped]:
            # prevent throwing more than once?
            coro._exception = args
            self._suspended.discard(coro._id)
            self._running.add(coro._id)
            coro._state = AsynCoro._Scheduled
            self._sched_cv.notify()
        else:
            logging.warning('invalid coroutine %s/%s to throw exception: %s',
                            coro.name, coro._id, coro._state)
        self._sched_cv.release()

    def _terminate_coro(self, cid):
        self._sched_cv.acquire()
        coro = self._coros.get(cid, None)
        if coro is None:
            logging.warning('invalid coroutine %s to close', cid)
        else:
            logging.debug('closing %s', coro.name)
            coro._generator.close()
            self._suspended.discard(cid)
            self._running.add(cid)
            coro._state = AsynCoro._Scheduled
            self._sched_cv.notify()
            self._sched_cv.release()
        #self._generator.throw(GeneratorExit, GeneratorExit('terminate'))

    def _scheduler(self):
        """Internal use only.
        """
        _time = time.time
        while True:
            self._sched_cv.acquire()
            if self._timeouts:
                now = _time()
                timeout = self._timeouts[0] - now
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
                # probably not needed?
                def close_coro(coro):
                    if self._coros.pop(coro._id, None) is None:
                        return
                    coro._generator.close()
                    while coro._callers:
                        caller = coro._callers.pop(-1)
                        if isinstance(caller, types.GeneratorType):
                            caller.close()
                        else:
                            assert isinstance(caller, Coro)
                            assert not coro._callers
                            close_coro(caller)
                for cid in self._running.union(self._suspended):
                    coro = self._coros.get(cid, None)
                    if coro is None:
                        continue
                    logging.warning('terminating coroutine %s', coro.name)
                    close_coro(coro)
                    coro._asyncoro = None
                self._running = self._suspended = set()
                self._coros = {}
                self._sched_cv.release()
                break
            if self._timeouts:
                # wake up timed suspends
                now = _time()
                while self._timeouts and self._timeouts[0] <= now:
                    coro = self._coros.get(self._timeout_cids[0], None)
                    if coro is not None:
                        self._suspended.discard(coro._id)
                        self._running.add(coro._id)
                        coro._state = AsynCoro._Scheduled
                        coro._value = None
                    del self._timeouts[0]
                    del self._timeout_cids[0]
            running = [self._coros.get(cid, None) for cid in self._running]
            # random.shuffle(running)
            self._sched_cv.release()

            for coro in running:
                if coro is None:
                    continue
                self._sched_cv.acquire()
                coro._state = AsynCoro._Running
                self._cur_coro = coro
                self._sched_cv.release()
                try:
                    if coro._exception:
                        retval = coro._generator.throw(*coro._exception)
                    else:
                        retval = coro._generator.send(coro._value)
                except:
                    self._sched_cv.acquire()
                    self._cur_coro = None
                    if sys.exc_type == StopIteration:
                        coro._exception = None
                    else:
                        coro._exception = sys.exc_info()

                    if coro._callers:
                        # return to caller
                        caller = coro._callers.pop(-1)
                        if isinstance(caller, types.GeneratorType):
                            coro._generator = caller
                            coro._state = AsynCoro._Scheduled
                        else:
                            assert isinstance(caller, Coro)
                            assert not coro._callers
                            assert caller._state == AsynCoro._Frozen
                            caller._state = AsynCoro._Scheduled
                            self._running.add(caller._id)
                            caller._value = coro._value
                            self._delete(coro)
                    else:
                        if coro._exception is not None:
                            logging.warning('uncaught exception in %s:\n%s', coro.name,
                                            ''.join(traceback.format_exception(*coro._exception)))
                        self._delete(coro)
                    self._sched_cv.release()
                else:
                    self._sched_cv.acquire()
                    self._cur_coro = None
                    if coro._state == AsynCoro._Suspended:
                        # if this coroutine is suspended, don't update
                        # the value; it will be updated with the value
                        # with which it is resumed
                        coro._state = AsynCoro._Stopped
                    elif coro._state == AsynCoro._Running:
                        coro._value = retval
                        coro._state = AsynCoro._Scheduled

                    if isinstance(retval, Coro):
                        # freeze current coroutine and activate new
                        # coroutine; control is returned to the caller
                        # when new coroutine is done
                        coro._state = AsyncCoro._Frozen
                        self._running.discard(coro._id)
                        assert not retval._callers
                        retval._callers.append(coro)
                    elif isinstance(retval, types.GeneratorType):
                        # push current generator onto stack and
                        # activate new generator
                        coro._callers.append(coro._generator)
                        coro._generator = retval
                        coro._value = None
                    self._sched_cv.release()

        self._complete.set()

    def terminate(self):
        """Terminate (singleton) instance of AsynCoro. This 'kills'
        all running coroutines.
        """
        self.notifier.terminate()
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
    way for efficiency: Instead of timeout for each socket, we provide
    only a global timeout value and check if any socket I/O operation
    has timedout every 'fd_timeout' seconds.
    """

    __metaclass__ = MetaSingleton
    __instance = None

    _Readable = None
    _Writable = None
    _Error = None

    @classmethod
    def instance(cls):
        """Returns instance of AsynCoro. This method should be called
        only after initializing AsynCoro.
        """
        return cls.__instance

    def __init__(self, poll_interval=2, fd_timeout=10):
        if self.__class__.__instance is None:
            assert fd_timeout >= 5 * poll_interval
            self.__class__.__instance = self

            if hasattr(select, 'epoll'):
                self._poller = select.epoll()
                self.__class__._Readable = select.EPOLLIN
                self.__class__._Writable = select.EPOLLOUT
                self.__class__._Error = select.EPOLLHUP | select.EPOLLERR
            elif hasattr(select, 'kqueue'):
                self._poller = _KQueueNotifier()
                self.__class__._Readable = select.KQ_FILTER_READ
                self.__class__._Writable = select.KQ_FILTER_WRITE
                self.__class__._Error = select.KQ_EV_ERROR
            elif hasattr(select, 'poll'):
                self._poller = select.poll()
                self.__class__._Readable = select.POLLIN
                self.__class__._Writable = select.POLLOUT
                self.__class__._Error = select.POLLHUP | select.POLLERR
            else:
                self._poller = _SelectNotifier()
                self.__class__._Readable = 0x01
                self.__class__._Writable = 0x04
                self.__class__._Error = 0x10

            self._poll_interval = poll_interval
            self._fd_timeout = fd_timeout
            self._fds = {}
            self._terminate = False
            self._lock = threading.Lock()
            self._notifier_thread = threading.Thread(target=self._notifier)
            self._notifier_thread.daemon = True
            self._notifier_thread.start()
            # TODO: add controlling fd to wake up poller (for
            # termination)

    def _notifier(self):
        """Calls 'task' method of registered fds when there is a
        read/write event for it. Since coroutuines can do only one
        thing, only one of read, write tasks can be done.
        """
        _time = time.time
        last_timeout = _time()
        while not self._terminate:
            try:
                events = self._poller.poll(self._poll_interval)
            except:
                logging.debug('poller failed?')
                logging.debug(traceback.format_exc())
                # wait a bit to prevent tight loops
                time.sleep(self._poll_interval)
                continue
            now = _time()
            self._lock.acquire()
            events = [(self._fds.get(fileno, None), event) for fileno, event in events]
            self._lock.release()
            try:
                for fd, evnt in events:
                    if fd is None:
                        logging.debug('Invalid fd!')
                        continue
                    if event == _AsyncNotifier._Readable:
                        # logging.debug('fd %s is readable', fd.fileno)
                        if fd.timestamp is not None:
                            fd.timestamp = now
                        if fd.task is None:
                            logging.error('fd %s is not registered?', fd.fileno)
                        else:
                            fd.task()
                    elif event == _AsyncNotifier._Writable:
                        # logging.debug('fd %s is writable', fd.fileno)
                        if fd.timestamp is not None:
                            fd.timestamp = now
                        if fd.task is None:
                            logging.error('fd %s is not registered?', fd.fileno)
                        else:
                            fd.task()
                    elif event == _AsyncNotifier._Error:
                        # logging.debug('Error on %s', fd.fileno)
                        # TODO: figure out what to do (e.g., register also for
                        # HUP and close?)
                        continue
            except:
                logging.debug(traceback.format_exc())
            if (now - last_timeout) >= self._fd_timeout:
                last_timeout = now
                self._lock.acquire()
                timeouts = [fd for fd in self._fds.itervalues() \
                            if fd.timestamp and (now - fd.timestamp) >= self._fd_timeout]
                try:
                    for fd in timeouts:
                        if fd._coro:
                            e = 'timed out %s' % (now - fd.timestamp)
                            fd.timestamp = None
                            fd._coro.throw(socket.timeout, socket.timeout(e))
                            # TODO: unregister and close?
                except:
                    logging.debug(traceback.format_exc())
                self._lock.release()
        logging.debug('AsyncNotifier terminated')

    def add_fd(self, fd):
        self._lock.acquire()
        self._fds[fd.fileno] = fd
        self._lock.release()
        self.register(fd, 0)

    def del_fd(self, fd):
        self._lock.acquire()
        fd = self._fds.pop(fd.fileno, None)
        self._lock.release()
        if fd is not None:
            self.unregister(fd)

    def register(self, fd, event):
        try:
            self._poller.register(fd.fileno, event)
        except:
            logging.warning('register of %s for %s failed with %s',
                            fd.fileno, event, traceback.format_exc())

    def modify(self, fd, event):
        if not event:
            if fd.timestamp is not None:
                fd.timestamp = 0
        try:
            self._poller.modify(fd.fileno, event)
        except:
            logging.warning('modify of %s for %s failed with %s',
                            fd.fileno, event, traceback.format_exc())

    def unregister(self, fd):
        try:
            self._poller.unregister(fd.fileno)
            fd.timestamp = None
        except:
            logging.warning('unregister of %s failed with %s', fd.fileno, traceback.format_exc())

    def terminate(self):
        self._terminate = True
        if hasattr(self._poller, 'terminate'):
            self._poller.terminate()

class _KQueueNotifier(object):
    """Internal use only.
    """

    __metaclass__ = MetaSingleton

    def __init__(self):
        if not hasattr(self, 'poller'):
            self.poller = select.kqueue()
            self.fids = {}

    def register(self, fid, event):
        self.fids[fid] = event
        self.update(fid, event, select.KQ_EV_ADD)

    def unregister(self, fid):
        event = self.fids.pop(fid, None)
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
        elif event == _AsyncNotifier._Error:
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
