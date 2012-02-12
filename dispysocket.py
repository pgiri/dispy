#!/usr/bin/env python

import time
import threading
import functools
import socket
import ssl
import inspect
import traceback
import select
import sys
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

class CoroLock(object):
    def __init__(self):
        self._owner = None

    def acquire(self, coro):
        assert coro is not None
        assert self._owner == None, 'invalid lock acquire: %s, %s' % (self._owner, coro._id)
        self._owner = coro._id
        coro._atomic = True

    def release(self, coro):
        assert coro is not None
        coro._atomic = False
        assert self._owner == coro._id, 'invalid lock release %s != %s' % (self._owner, coro._id)
        self._owner = None

class CoroCondition(object):
    def __init__(self):
        self._waitlist = []
        self._owner = None
        # support multiple notifications with _notify as counter?
        self._notify = False

    def acquire(self, coro):
        assert coro is not None
        logging.debug('%s/%s getting cv', coro.name, coro._id)
        assert self._owner == None, 'invalid cv acquire: %s, %s' % (self._owner, coro._id)
        self._owner = coro._id
        coro._atomic = True

    def release(self, coro):
        assert coro is not None
        coro._atomic = False
        assert self._owner == coro._id, 'invalid cv release %s != %s' % (self._owner, coro._id)
        self._owner = None
        logging.debug('%s/%s released cv', coro.name, coro._id)

    def notify(self):
        self._notify = True
        if len(self._waitlist):
            wake = self._waitlist.pop(0)
            logging.debug('notifying %s', wake._id)
            wake.resume(None)
        else:
            logging.debug('noone to notify')

    def wait(self, coro):
        if not self._notify:
            # assert self._owner == coro._id, 'invalid wait: %s != %s' % (self._owner, coro._id)
            self._owner = None
            self._waitlist.append(coro)
            logging.debug('%s/%s wait released cv', coro.name, coro._id)
            coro.suspend()
            coro._atomic = False
            return True
        self._notify = False
        self._owner = coro._id
        coro._atomic = True
        logging.debug('cv wait over for %s/%s', coro.name, coro._id)
        return False

class _DispySocket(object):
    """Internal use only.
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
            self.notifier = _AsyncNotifier.instance()
            self.notifier.add_socket(self)

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
                coro.resume(None)

        self.task = functools.partial(_read, self, coro, data_len)
        self.coro = coro
        if self.timestamp is not None:
            self.timestamp = time.time()
        coro.suspend()
        self.notifier.modify(self, _AsyncNotifier._Readable)

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
        self.notifier.modify(self, _AsyncNotifier._Readable)

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
        self.notifier.modify(self, _AsyncNotifier._Writable)

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
        self.notifier.modify(self, _AsyncNotifier._Writable)

    def sync_write(self, data):
        self.result = buffer(data, 0)
        while len(self.result) > 0:
            n = self.sock.send(self.result)
            if n > 0:
                self.result = buffer(self.result, n)
        return 0

    def close(self):
        if self.notifier:
            self.notifier.del_socket(self)
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
        self.notifier.modify(self, _AsyncNotifier._Readable)

    def sync_accept(self):
        conn, addr = self.sock.accept()
        sock = _DispySocket(conn, blocking=True)
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
        self.notifier.modify(self, _AsyncNotifier._Writable)
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
            yield (None, None)

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
        self._atomic = False
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

class AsynCoro(object):
    """'Coroutine' scheduler. The only properties available to users
    are 'terminate' and 'join' methods.
    """

    __metaclass__ = MetaSingleton

    _Running = 1
    _Suspended = 2
    _Resumed = 3
    _Stopped = 4
    _Frozen = 5

    _coro_id = 1

    @classmethod
    def instance(cls, *args):
        if not hasattr(cls, '__instance'):
            cls.__instance = cls(*args)
        return cls.__instance

    def __init__(self):
        if not hasattr(self, '_coros'):
            self._coros = {}
            self._running = set()
            self._resumed = set()
            self._suspended = set()
            self._sched_cv = threading.Condition()
            self._terminate = False
            self._lock = threading.RLock()
            self._complete = threading.Event(self._lock)
            self._scheduler = threading.Thread(target=self._run)
            self._scheduler.daemon = True
            self._scheduler.start()

            self.notifier = _AsyncNotifier()

    def _add(self, coro):
        coro._id = AsynCoro._coro_id
        self._sched_cv.acquire()
        AsynCoro._coro_id += 1
        self._coros[coro._id] = coro
        self._complete.clear()
        coro._state = AsynCoro._Suspended
        self._suspended.add(coro._id)
        self._sched_cv.release()
        logging.debug('added coro %s with %s', coro.name, coro._id)
        self._resume(coro._id, None)

    def _suspend(self, cid):
        self._sched_cv.acquire()
        coro = self._coros.get(cid, None)
        if coro is None:
            self._sched_cv.release()
            logging.warning('          invalid coroutine %s to suspend', cid)
            assert False
            return
        if coro._state != AsynCoro._Running:
            logging.warning('           invalid coroutine %s/%s to suspend: %s',
                            coro.name, coro._id, coro._state)
            # assert False
        self._running.discard(cid)
        assert cid not in self._resumed
        coro._state = AsynCoro._Suspended
        self._suspended.add(cid)
        self._sched_cv.release()

    def _resume(self, cid, value):
        self._sched_cv.acquire()
        coro = self._coros.get(cid, None)
        if coro is None:
            self._sched_cv.release()
            logging.warning('         invalid coroutine %s to resume', cid)
            assert False
            return
        if coro._state in [AsynCoro._Suspended, AsynCoro._Stopped]:
            coro._value = value
        else:
            logging.warning('          invalid coroutine %s/%s to resume: %s',
                            coro.name, cid, coro._state)
            # assert False
        # assert cid not in self._resumed
        self._suspended.discard(cid)
        self._running.discard(cid)
        coro._state = AsynCoro._Resumed
        self._resumed.add(cid)
        self._sched_cv.notify()
        self._sched_cv.release()

    def _delete(self, cid):
        self._sched_cv.acquire()
        coro = self._coros.get(cid, None)
        if coro is None:
            self._sched_cv.release()
            logging.warning('           invalid coroutine %s to delete', cid)
            assert False
            return
        if coro._state == AsynCoro._Running:
            self._running.discard(cid)
        elif coro._state == AsynCoro._Resumed:
            self._resumed.discard(cid)
        elif coro._state == AsynCoro._Suspended or coro._state == AsynCoro._Stopped:
            self._suspended.discard(cid)
        coro._scheduler = None
        del self._coros[cid]
        if not self._coros:
            self._complete.set()
        self._sched_cv.release()

    def _throw(self, cid, *args):
        self._sched_cv.acquire()
        coro = self._coros.get(cid, None)
        if coro is None or coro._state != AsynCoro._Stopped:
            logging.warning('invalid coroutine %s to throw: %s', cid, coro._state)
        else:
            coro._generator.throw(*args)
            self._suspended.discard(coro._id)
            coro._state = AsynCoro._Resumed
            self._running.add(coro._id)
            self._sched_cv.notify()
        self._sched_cv.release()

    def _run(self):
        while True:
            self._sched_cv.acquire()
            while (not self._running) and (not self._resumed) and (not self._terminate):
                # logging.debug('waiting for coros to run')
                self._sched_cv.wait(1)
            if self._terminate:
                self._sched_cv.release()
                logging.debug('_run quitting')
                break
            running = [self._coros.get(cid, None) for cid in self._resumed]
            for coro in running:
                coro._state = AsynCoro._Running
            self._running.update(self._resumed)
            self._resumed = set()
            running = [self._coros.get(cid, None) for cid in self._running]
            self._sched_cv.release()

            freeze = []
            for coro in running:
                if coro is None:
                    continue
                assert coro._state in [AsynCoro._Running, AsynCoro._Resumed], \
                       'coro %s/%s is in invalid state: %s' % (coro.name, coro._id, coro._state)
                try:
                    # logging.debug('advancing %s/%s: %s, %s', coro.name, coro._id,
                    #               coro._generator.__name__, type(coro._value))
                    retval = coro._generator.send(coro._value)
                    assert coro._atomic is False
                    # logging.debug('advanced %s/%s: %s, %s', coro.name, coro._id,
                    #               coro._generator.__name__, type(retval))
                except StopIteration:
                    # logging.debug('%s/%s / %s is done', coro.name, coro._id,
                    #               coro._generator.__name__)
                    self._sched_cv.acquire()
                    if coro._stack:
                        # return to caller
                        caller = coro._stack.pop(-1)
                        if isinstance(caller, Coro):
                            assert not coro._stack
                            self._sched_cv.acquire()
                            caller._state = AsynCoro._Running
                            self._running.add(caller._id)
                            caller._value = coro._value
                            self._delete(coro._id)
                            self._sched_cv.release()
                        else:
                            assert inspect.isgenerator(caller)
                            coro._generator = caller
                    else:
                        coro._complete.set()
                        self._delete(coro._id)
                    self._sched_cv.acquire()
                except:
                    logging.warning(traceback.format_exc())
                    coro._generator.throw(*sys.exc_info())
                    self._delete(coro._id)
                else:
                    self._sched_cv.acquire()
                    if coro._state == AsynCoro._Suspended:
                        coro._state = AsynCoro._Stopped
                    # if this coroutine is suspended, don't update the
                    # value; it will be updated with the value with
                    # which it is resumed
                    if coro._state == AsynCoro._Running:
                        coro._value = retval
                    if isinstance(retval, Coro):
                        # freeze current coroutine and activate new
                        # coroutine; control is returned to the caller
                        # when new coroutine is done
                        freeze.append(coro)
                        assert not retval._stack
                        retval._stack.append(coro)
                        retval._value = None
                        retval._state = AsynCoro._Running
                        self._running.add(retval)
                    elif inspect.isgenerator(retval):
                        # push current generator onto stack and
                        # activate new generator
                        # logging.debug('%s/%s started %s', coro.name, coro._id,
                        #               coro._generator.__name__)
                        coro._stack.append(coro._generator)
                        coro._generator = retval
                        coro._value = None
                    self._sched_cv.release()

            self._sched_cv.acquire()
            for coro in freeze:
                coro._state = AsynCoro._Frozen
                self._running.discard(coro._id)
            self._sched_cv.release()
        self._complete.set()

    def terminate(self):
        self.notifier.terminate()
        self._sched_cv.acquire()
        for cid in self._running.union(self._suspended, self._resumed):
            coro = self._coros.get(cid, None)
            if coro is None:
                continue
            coro._scheduler = None
            del coro
        self._running = self._suspended = self._resumed = set()
        self._terminate = True
        self._sched_cv.notify()
        self._sched_cv.release()
        self._complete.wait()
        logging.debug('AsynCoro terminated')

    def join(self):
        self._complete.wait()

    def wake(self):
        self._sched_cv.acquire()
        self._sched_cv.notify()
        self._sched_cv.release()

class _AsyncNotifier(object):
    """Asynchronous I/O completion, to be used with _DispySocket (and
    coroutines).

    Timeouts for socket operations are handled in a rather simplisitc
    way for efficiency: Instead of timeout for each socket, we provide
    only a global timeout value and check if any socket I/O operation
    has timedout every 'socket_timeout' seconds.
    """

    __metaclass__ = MetaSingleton

    # TODO: handle other pollers (select / kqueue)

    _Readable = select.EPOLLIN
    _Writable = select.EPOLLOUT
    _Error = select.EPOLLHUP

    @classmethod
    def instance(cls, *args, **kwargs):
        if not hasattr(cls, '__instance'):
            cls.__instance = cls(*args, **kwargs)
        return cls.__instance

    def __init__(self, poll_interval=1, socket_timeout=10):
        if not hasattr(self, 'poll_interval'):
            assert socket_timeout >= 5 * poll_interval
            self._poll_interval = poll_interval
            self._socket_timeout = socket_timeout
            self._poller = select.epoll()
            self._socks = {}
            self._terminate = False
            self._lock = threading.Lock()
            self._notifier_thread = threading.Thread(target=self.notifier)
            self._notifier_thread.daemon = True
            self._notifier_thread.start()

    def notifier(self):
        last_timeout = time.time()
        while not self._terminate:
            events = self._poller.poll(self._poll_interval)
            now = time.time()
            self._lock.acquire()
            events = [(self._socks.get(fileno, None), event) for fileno, event in events]
            self._lock.release()
            # logging.debug('poll events: %s', len(events))
            try:
                for sock, event in events:
                    if sock is None:
                        logging.debug('Invalid socket!')
                        continue
                    if event & _AsyncNotifier._Readable:
                        # logging.debug('sock %s is readable', sock.fileno)
                        if sock.timestamp is not None:
                            sock.timestamp = now
                        if sock.task is not None:
                            sock.task()
                    elif event & _AsyncNotifier._Writable:
                        # logging.debug('sock %s is writable', sock.fileno)
                        if sock.timestamp is not None:
                            sock.timestamp = now
                        if sock.task is not None:
                            sock.task()
                    elif event & AsyncNotifier._Error:
                        # logging.debug('Error on %s', sock.fileno)
                        continue
                        self._lock.acquire()
                        sock = self._socks.pop(sock.fileno, None)
                        self._lock.release()
                        if sock is not None:
                            try:
                                self._poller.unregister(sock.fileno)
                                sock._sock.shutdown(socket.SHUT_RDWD)
                            except:
                                pass
            except:
                pass
            if (now - last_timeout) >= self._socket_timeout:
                last_timeout = now
                timeouts = [sock for sock in self._socks.itervalues() \
                            if sock.timestamp and (now - sock.timestamp) >= self._socket_timeout]
                try:
                    for sock in timeouts:
                        if sock.coro:
                            e = 'timeout %s' % (now - sock.timestamp)
                            sock.timestamp = None
                            sock.coro.throw(socket.timeout, socket.timeout(e))
                except:
                    logging.debug(traceback.format_exc())
        logging.debug('_AsyncNotifier terminated')

    def add_socket(self, sock):
        self._lock.acquire()
        self._socks[sock.fileno] = sock
        self._lock.release()
        self.register(sock, 0)

    def del_socket(self, sock):
        self.unregister(sock)
        self._lock.acquire()
        self._socks.pop(sock.fileno, None)
        self._lock.release()

    def register(self, sock, flags):
        if flags & _AsyncNotifier._Readable:
            event = select.EPOLLIN
        elif flags & _AsyncNotifier._Writable:
            event = select.EPOLLOUT
        elif flags & _AsyncNotifier._Error:
            event = select.EPOLLHUP
        else:
            event = 0

        try:
            self._poller.register(sock.fileno, event)
        except:
            logging.warning('register of %s failed with %s', sock.fileno, traceback.format_exc())

    def modify(self, sock, flags):
        if not flags:
            event = 0
            if sock.timestamp is not None:
                sock.timestamp = 0
        elif flags & _AsyncNotifier._Readable:
            event = select.EPOLLIN
        elif flags & _AsyncNotifier._Writable:
            event = select.EPOLLOUT
        elif flags & _AsyncNotifier._Error:
            event = select.EPOLLHUP

        try:
            self._poller.modify(sock.fileno, event)
        except:
            logging.warning('modify of %s failed with %s', sock.fileno, traceback.format_exc())

    def unregister(self, sock):
        try:
            self._poller.unregister(sock.fileno)
            sock.timestamp = None
        except:
            logging.warning('unregister of %s failed with %s', sock.fileno, traceback.format_exc())

    def terminate(self):
        self._terminate = True

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
            self._func(*self._args, **self._kwargs)
