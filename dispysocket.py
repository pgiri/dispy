#!/usr/bin/env python

# dispysocket: Asynchronous I/O and coroutine support for dispy;
# see accompanying 'dispy' for more details.

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
import ssl
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

if platform.system() == 'Windows':
    from errno import WSAEINPROGRESS as EINPROGRESS
else:
    from errno import EINPROGRESS

class MetaSingleton(type):
    __instance = None
    def __call__(cls, *args, **kwargs):
        if cls.__instance is None:
            cls.__instance = super(MetaSingleton, cls).__call__(*args, **kwargs)
        return cls.__instance

class DispySocket(object):
    """Socket for dispy implementation. This makes use of asynchronous
    notification and coroutines.
    """

    def __init__(self, sock, auth_code=None, certfile=None, keyfile=None, server=False,
                 blocking=False, timeout=True):
        if certfile:
            self.sock = ssl.wrap_socket(sock, server_side=server, keyfile=keyfile,
                                        certfile=certfile, ssl_version=ssl.PROTOCOL_TLSv1)
        else:
            self.sock = sock
        self.blocking = blocking == True
        if self.blocking:
            self.timestamp = None
            if isinstance(timeout, float):
                self.sock.settimeout(timeout)
        else:
            self.sock.setblocking(0)
            if timeout:
                self.timestamp = 0
            else:
                self.timestamp = None
        self.auth_code = auth_code
        self.result = None
        self.fileno = sock.fileno()
        self.coro = None
        for method in ['bind', 'listen', 'getsockname', 'setsockopt', 'getsockopt']:
            setattr(self, method, getattr(self.sock, method))

        if self.blocking:
            for method in ['settimeout', 'gettimeout']:
                setattr(self, method, getattr(self.sock, method))
            self.read = self.sync_read
            self.write = self.sync_write
            self.recvfrom = self.sync_recvfrom
            self.accept = self.sync_accept
            self.sendto = self.sync_sendto
            self.connect = self.sync_connect
            self.read_msg = self.sync_read_msg
            self.write_msg = self.sync_write_msg
            self.notifier = None
        else:
            self.task = None
            self.read = self.async_read
            self.write = self.async_write
            self.recvfrom = self.async_recvfrom
            self.sendto = self.async_sendto
            self.accept = self.async_accept
            self.connect = self.async_connect
            self.read_msg = self.async_read_msg
            self.write_msg = self.async_write_msg
            self.notifier = AsyncNotifier.instance()
            self.notifier.add_fd(self)

    def async_read(self, data_len, coro=None):
        self.result = bytearray()
        def _read(self, coro, data_len):
            plen = len(self.result)
            self.result[len(self.result):] = self.sock.recv(data_len - len(self.result))
            if len(self.result) == data_len or self.sock.type == socket.SOCK_DGRAM:
                self.result = str(self.result)
                self.notifier.modify(self, 0)
                coro.resume(self.result)
            elif len(self.result) == plen:
                # TODO: check for error and delete?
                self.result = None
                self.notifier.modify(self, 0)
                coro.throw(Exception, Exception('socket reading error'), None)

        self.task = functools.partial(_read, self, coro, data_len)
        self.coro = coro
        if self.timestamp is not None:
            self.timestamp = time.time()
        coro.suspend()
        self.notifier.modify(self, AsyncNotifier._Readable)

    def sync_read(self, data_len):
        self.result = bytearray()
        while len(self.result) < data_len:
            self.result[len(self.result):] = self.sock.recv(data_len - len(self.result))
        return str(self.result)

    def async_recvfrom(self, data_len, coro=None):
        def _read(self, coro, data_len):
            self.result, addr = self.sock.recvfrom(data_len)
            self.result = (str(self.result), addr)
            self.notifier.modify(self, 0)
            coro.resume(self.result)

        self.task = functools.partial(_read, self, coro, data_len)
        self.coro = coro
        if self.timestamp is not None:
            self.timestamp = time.time()
        coro.suspend()
        self.notifier.modify(self, AsyncNotifier._Readable)

    def sync_recvfrom(self, data_len):
        self.result = self.sock.recvfrom(data_len)
        return self.result

    def async_sendto(self, data, addr, coro=None):
        # NB: send only what can be sent in one operation, instead of
        # sending all the data, as done in write/write_msg
        def _sendto(self, data, addr, coro):
            try:
                self.result = self.sock.sendto(data, addr)
            except:
                # TODO: close socket, inform coro
                logging.debug('write error', self.fileno)
                self.notifier.unregister(self)
                coro.throw(*sys.exc_info())
            else:
                self.notifier.modify(self, 0)
                coro.resume(self.result)

        self.task = functools.partial(_sendto, self, data, addr, coro)
        self.coro = coro
        if self.timestamp is not None:
            self.timestamp = time.time()
        coro.suspend()
        self.notifier.modify(self, AsyncNotifier._Writable)

    def sync_sendto(self, data, addr):
        self.result = self.sock.sendto(data, addr)
        return self.result

    def async_write(self, data, coro=None):
        self.result = buffer(data, 0)
        def _write(self, coro):
            try:
                n = self.sock.send(self.result)
            except:
                logging.error('writing failed %s: %s', self.fileno, traceback.format_exc())
                # TODO: close socket, inform coro
                self.result = None
                self.notifier.unregister(self)
                coro.throw(*sys.exc_info())
            else:
                if n > 0:
                    self.result = buffer(self.result, n)
                    if len(self.result) == 0:
                        self.notifier.modify(self, 0)
                        coro.resume(0)

        self.task = functools.partial(_write, self, coro)
        self.coro = coro
        if self.timestamp is not None:
            self.timestamp = time.time()
        coro.suspend()
        self.notifier.modify(self, AsyncNotifier._Writable)

    def sync_write(self, data):
        self.result = buffer(data, 0)
        while len(self.result) > 0:
            n = self.sock.send(self.result)
            if n > 0:
                self.result = buffer(self.result, n)
        return 0

    def close(self):
        if self.notifier:
            self.notifier.del_fd(self)
        self.sock.close()

    def async_accept(self, coro=None):
        def _accept(self, coro):
            conn, addr = self.sock.accept()
            self.result = (conn, addr)
            self.notifier.modify(self, 0)
            coro.resume(self.result)

        self.task = functools.partial(_accept, self, coro)
        self.coro = coro
        self.timestamp = None
        coro.suspend()
        self.notifier.modify(self, AsyncNotifier._Readable)

    def sync_accept(self):
        conn, addr = self.sock.accept()
        sock = DispySocket(conn, blocking=True)
        return conn, addr

    def async_connect(self, (host, port), coro=None):
        def _connect(self, coro, host, port):
            # TODO: check with getsockopt
            err = self.sock.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
            if not err:
                self.notifier.modify(self, 0)
                coro.resume(0)
            else:
                logging.debug('connect error: %s', err)

        self.task = functools.partial(_connect, self, coro, host, port)
        self.coro = coro
        if self.timestamp is not None:
            self.timestamp = time.time()
        coro.suspend()
        self.notifier.modify(self, AsyncNotifier._Writable)
        try:
            self.sock.connect((host, port))
        except socket.error, e:
            if e.args[0] != EINPROGRESS:
                logging.debug('connect error: %s', e.args[0])

    def sync_connect(self, (host, port)):
        self.sock.connect((host, port))

    def async_read_msg(self, coro):
        try:
            info_len = struct.calcsize('>LL')
            info = yield self.read(info_len, coro=coro)
            if len(info) < info_len:
                logging.error('Socket disconnected?(%s, %s)', len(info), info_len)
                yield (None, None)
            (uid, msg_len) = struct.unpack('>LL', info)
            assert msg_len > 0
            msg = yield self.read(msg_len, coro=coro)
            if len(msg) < msg_len:
                logging.error('Socket disconnected?(%s, %s)', len(msg), msg_len)
                yield (None, None)
            yield (uid, msg)
        except:
            logging.error('Socket reading error: %s' % traceback.format_exc())
            raise

    def sync_read_msg(self):
        try:
            info_len = struct.calcsize('>LL')
            info = self.sync_read(info_len)
            if len(info) < info_len:
                logging.error('Socket disconnected?(%s, %s)', len(info), info_len)
                return (None, None)
            (uid, msg_len) = struct.unpack('>LL', info)
            assert msg_len > 0
            msg = self.sync_read(msg_len)
            if len(msg) < msg_len:
                logging.error('Socket disconnected?(%s, %s)', len(msg), msg_len)
                return (None, None)
            return (uid, msg)
        except socket.timeout:
            logging.error('Socket disconnected(timeout)?')
            return (None, None)

    def async_write_msg(self, uid, data, auth=True, coro=None):
        assert coro is not None
        if auth and self.auth_code:
            yield self.write(self.auth_code, coro=coro)
        yield self.write(struct.pack('>LL', uid, len(data)) + data, coro=coro)

    def sync_write_msg(self, uid, data, auth=True):
        if auth and self.auth_code:
            self.sync_write(self.auth_code)
        self.sync_write(struct.pack('>LL', uid, len(data)) + data)
        return 0

class Coro(object):
    """'Coroutine' factory to build coroutines to be scheduled with
    AsynCoro. Automatically starts executing 'func'.  The
    function definition should have 'coro' keyword argument set to
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
        self._value_is_exc = False
        self._stack = []
        self._scheduler = AsynCoro.instance()
        self._complete = threading.Event()
        self._scheduler._add(self)

    def suspend(self):
        if self._scheduler:
            self._scheduler._suspend(self._id)
        else:
            logging.warning('Coroutine %s removed?', self.name)

    def resume(self, value):
        if self._scheduler:
            self._scheduler._resume(self._id, value)
        else:
            logging.warning('Coroutine %s removed?', self.name)
      
    def throw(self, *args):
        if self._scheduler:
            self._scheduler._throw(self._id, *args)
        else:
            logging.warning('Coroutine %s removed?', self.name)

    def value(self):
        self._complete.wait()
        return self._value

class CoroLock(object):
    """'Lock' primitive for coroutines.

    Since a coroutine runs until 'yield', there is no need for
    lock. The caller has to guarantee that once a lock is obtained,
    'yield' call is not made.
    """
    def __init__(self):
        self._owner = None

    def acquire(self, coro):
        assert self._owner == None, 'invalid lock acquire: %s, %s' % (self._owner, coro._id)
        self._owner = coro._id

    def release(self, coro):
        assert self._owner == coro._id, 'invalid lock release %s != %s' % (self._owner, coro._id)
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
        # support multiple notifications?
        self._notify = False

    def acquire(self, coro):
        assert self._owner == None, 'invalid cv acquire: %s, %s' % (self._owner, coro._id)
        self._owner = coro._id

    def release(self, coro):
        assert self._owner == coro._id, 'invalid cv release %s != %s' % (self._owner, coro._id)
        self._owner = None

    def notify(self):
        self._notify = True
        if len(self._waitlist):
            wake = self._waitlist.pop(0)
            wake.resume(None)

    def wait(self, coro):
        if not self._notify:
            self._owner = None
            self._waitlist.append(coro)
            coro.suspend()
            return True
        self._notify = False
        self._owner = coro._id
        return False

class AsynCoro(object):
    """'Coroutine' scheduler. The only properties available to users
    are 'terminate' and 'join' methods.
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
    # called another generator or coro, not in _running or _suspended
    _Frozen = 5

    _coro_id = 1

    @classmethod
    def instance(cls):
        return cls.__instance

    def __init__(self):
        if self.__class__.__instance is None:
            self.__class__.__instance = self
            self._coros = {}
            self._running = set()
            self._suspended = set()
            self._sched_cv = threading.Condition()
            self._terminate = False
            self._lock = threading.RLock()
            self._complete = threading.Event(self._lock)
            self._scheduler = threading.Thread(target=self._scheduler)
            self._scheduler.daemon = True
            self._scheduler.start()

            self.notifier = AsyncNotifier()

    def _add(self, coro):
        coro._id = AsynCoro._coro_id
        self._sched_cv.acquire()
        AsynCoro._coro_id += 1
        self._coros[coro._id] = coro
        self._complete.clear()
        coro._state = AsynCoro._Scheduled
        self._running.add(coro._id)
        self._sched_cv.notify()
        self._sched_cv.release()
        # logging.debug('added coro %s with %s', coro.name, coro._id)

    def _suspend(self, cid):
        # for now only executing coro can call suspend (itself).
        # TODO: does it make sense to have (co-operating) coros
        # suspend others (will make things even more interesting!)?
        self._lock.acquire()
        coro = self._coros.get(cid, None)
        if coro is None:
            self._lock.release()
            logging.warning('invalid coroutine %s to suspend', cid)
            return
        if coro._state == AsynCoro._Running:
            self._running.discard(cid)
            coro._state = AsynCoro._Suspended
            self._suspended.add(cid)
        else:
            logging.warning('invalid coroutine %s/%s to suspend: %s',
                            coro.name, coro._id, coro._state)
        self._lock.release()

    def _resume(self, cid, value):
        self._sched_cv.acquire()
        coro = self._coros.get(cid, None)
        if coro is None:
            self._sched_cv.release()
            logging.warning('invalid coroutine %s to resume', cid)
            return
        if coro._state in [AsynCoro._Stopped, AsynCoro._Suspended]:
            coro._value = value
            self._suspended.discard(cid)
            self._running.add(cid)
            coro._state = AsynCoro._Scheduled
            self._sched_cv.notify()
        else:
            logging.warning('invalid coroutine %s/%s to resume: %s',
                            coro.name, cid, coro._state)
        self._sched_cv.release()

    def _delete(self, coro):
        # called with _sched_cv locked
        self._running.discard(coro._id)
        self._suspended.discard(coro._id)
        coro._scheduler = None
        assert not coro._stack
        del self._coros[coro._id]
        if not self._coros:
            self._complete.set()

    def _throw(self, cid, *args):
        self._sched_cv.acquire()
        coro = self._coros.get(cid, None)
        if coro is None:
            logging.warning('invalid coroutine %s to throw exception', cid)
        elif coro._state in [AsynCoro._Scheduled, AsynCoro._Stopped]:
            # prevent throwing more than once?
            logging.debug('throwing exception in %s/%s', coro.name, coro._id)
            coro._value = args
            coro._value_is_exc = True
            self._suspended.discard(coro._id)
            self._running.add(coro._id)
            coro._state = AsynCoro._Scheduled
            self._sched_cv.notify()
        else:
            logging.warning('invalid coroutine %s/%s to throw exception: %s',
                            coro.name, coro._id, coro._state)
        self._sched_cv.release()

    def _scheduler(self):
        while True:
            self._sched_cv.acquire()
            while (not self._running) and (not self._terminate):
                self._sched_cv.wait()
            if self._terminate:
                for cid in self._running.union(self._suspended):
                    coro = self._coros.get(cid, None)
                    if coro is None:
                        continue
                    coro._stack = []
                    coro._scheduler = None
                    del coro
                self._running = self._suspended = set()
                self._coros = {}
                self._sched_cv.release()
                break
            running = [self._coros.get(cid, None) for cid in self._running]
            random.shuffle(running)
            self._sched_cv.release()

            for coro in running:
                if coro is None:
                    continue
                self._sched_cv.acquire()
                coro._state = AsynCoro._Running
                self._sched_cv.release()
                try:
                    if coro._value_is_exc:
                        retval = coro._generator.throw(*coro._value)
                    else:
                        retval = coro._generator.send(coro._value)
                except:
                    self._sched_cv.acquire()
                    if sys.exc_type == StopIteration:
                        coro._value_is_exc = False
                    else:
                        retval = sys.exc_info()
                        coro._value_is_exc = True

                    if coro._stack:
                        # return to caller
                        caller = coro._stack.pop(-1)
                        if isinstance(caller, Coro):
                            assert not coro._stack
                            assert caller._state == AsynCoro._Frozen
                            caller._state = AsynCoro._Running
                            self._running.add(caller._id)
                            caller._value = coro._value
                            self._delete(coro)
                        else:
                            assert isinstance(caller, types.GeneratorType)
                            coro._generator = caller
                            coro._state = AsynCoro._Scheduled
                    else:
                        coro._complete.set()
                        self._delete(coro)
                    self._sched_cv.release()
                else:
                    self._sched_cv.acquire()
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
                        assert not retval._stack
                        retval._stack.append(coro)
                    elif isinstance(retval, types.GeneratorType):
                        # push current generator onto stack and
                        # activate new generator
                        coro._stack.append(coro._generator)
                        coro._generator = retval
                        coro._value = None
                    self._sched_cv.release()

        self._complete.set()

    def terminate(self):
        self.notifier.terminate()
        self._sched_cv.acquire()
        self._terminate = True
        self._sched_cv.notify()
        self._sched_cv.release()
        self._complete.wait()
        logging.debug('AsynCoro terminated')

    def join(self):
        self._complete.wait()

class AsyncNotifier(object):
    """Asynchronous I/O notifier, to be used with DispySocket (and
    coroutines) and AsynCoro.

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
        return cls.__instance

    def __init__(self, poll_interval=1, fd_timeout=10):
        if self.__class__.__instance is None:
            assert fd_timeout >= 5 * poll_interval
            self.__class__.__instance = self

            if hasattr(select, 'epoll'):
                self._poller = select.epoll()
                AsyncNotifier._Readable = select.EPOLLIN
                AsyncNotifier._Writable = select.EPOLLOUT
                AsyncNotifier._Error = select.EPOLLHUP | select.EPOLLERR
            elif hasattr(select, 'kqueue'):
                self._poller = _KQueueNotifier()
                AsyncNotifier._Readable = select.KQ_FILTER_READ
                AsyncNotifier._Writable = select.KQ_FILTER_WRITE
                AsyncNotifier._Error = select.KQ_EV_ERROR
            elif hasattr(select, 'poll'):
                self._poller = select.poll()
                AsyncNotifier._Readable = select.POLLIN
                AsyncNotifier._Writable = select.POLLOUT
                AsyncNotifier._Error = select.POLLHUP | select.POLLERR
            else:
                self._poller = _SelectNotifier()
                AsyncNotifier._Readable = 0x01
                AsyncNotifier._Writable = 0x04
                AsyncNotifier._Error = 0x10

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
        last_timeout = time.time()
        while not self._terminate:
            events = self._poller.poll(self._poll_interval)
            now = time.time()
            self._lock.acquire()
            events = [(self._fds.get(fileno, None), event) for fileno, event in events]
            # logging.debug('events: %s', len(events))
            self._lock.release()
            try:
                for fd, evnt in events:
                    if fd is None:
                        logging.debug('Invalid fd!')
                        continue
                    if event == AsyncNotifier._Readable:
                        # logging.debug('fd %s is readable', fd.fileno)
                        if fd.timestamp is not None:
                            fd.timestamp = now
                        if fd.task is None:
                            logging.error('fd %s is not registered?', fd.fileno)
                        else:
                            fd.task()
                    elif event == AsyncNotifier._Writable:
                        # logging.debug('fd %s is writable', fd.fileno)
                        if fd.timestamp is not None:
                            fd.timestamp = now
                        if fd.task is None:
                            logging.error('fd %s is not registered?', fd.fileno)
                        else:
                            fd.task()
                    elif event == AsyncNotifier._Error:
                        # logging.debug('Error on %s', fd.fileno)
                        # TODO: figure out what to do (e.g., register also for
                        # HUP and close?)
                        continue
            except:
                pass
            if (now - last_timeout) >= self._fd_timeout:
                last_timeout = now
                self._lock.acquire()
                timeouts = [fd for fd in self._fds.itervalues() \
                            if fd.timestamp and (now - fd.timestamp) >= self._fd_timeout]
                try:
                    for fd in timeouts:
                        if fd.coro:
                            e = 'timeout %s' % (now - fd.timestamp)
                            fd.timestamp = None
                            fd.coro.throw(Exception, Exception(e))
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
        if event == AsyncNotifier._Readable:
            kevents = [select.kevent(fid, filter=select.KQ_FILTER_READ, flags=flags)]
        elif event == AsyncNotifier._Writable:
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

    def __init__(self):
        if not hasattr(self, 'poller'):
            self.poller = select.select
            self.rset = set()
            self.wset = set()
            self.xset = set()

    def register(self, fid, event):
        if event == AsyncNotifier._Readable:
            self.rset.add(fid)
        elif event == AsyncNotifier._Writable:
            self.wset.add(fid)
        elif event == AsyncNotifier._Error:
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
            events[fid] = AsyncNotifier._Readable
        for fid in wlist:
            events[fid] = AsyncNotifier._Writable
        for fid in xlist:
            events[fid] = AsyncNotifier._Error
        return events.iteritems()

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
