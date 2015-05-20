#!/usr/bin/env python3

"""
dispynode: Execute computations on behalf of dispy clients;
see accompanying 'dispy' for more details.
"""

__author__ = "Giridhar Pemmasani (pgiri@yahoo.com)"
__email__ = "pgiri@yahoo.com"
__copyright__ = "Copyright 2011, Giridhar Pemmasani"
__contributors__ = []
__maintainer__ = "Giridhar Pemmasani (pgiri@yahoo.com)"
__license__ = "MIT"
__url__ = "http://dispy.sourceforge.net"
__status__ = "Production"

import os
import sys
import time
import stat
import socket
import multiprocessing
import threading
import subprocess
import signal
import ssl
import traceback
import logging
import marshal
import tempfile
import shutil
import shelve
import glob
import pickle
import io

from dispy import _JobReply, DispyJob, _Compute, _XferFile, _node_ipaddr, _dispy_version, \
    auth_code, num_min

import asyncoro
from asyncoro import Coro, AsynCoro, AsyncSocket, serialize, unserialize

__version__ = _dispy_version
__all__ = []

MaxFileSize = 10*(1024**2)
MsgTimeout = 5

logger = logging.getLogger('dispynode')
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(message)s'))
logger.addHandler(handler)
del handler


def dispy_provisional_result(result):
    """Sends provisional result of computation back to the client.

    In some cases, such as optimizations, computations may send
    current (best) result to the client and continue computation (for
    next iteration) so that the client may decide to terminate
    computations based on the results or alter computations if
    necessary. The computations can use this function in such cases
    with the current result of computation as argument.
    """

    __dispy_job_reply = __dispy_job_info.job_reply
    __dispy_job_reply.status = DispyJob.ProvisionalResult
    __dispy_job_reply.result = result
    __dispy_job_reply.end_time = time.time()
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock = AsyncSocket(sock, blocking=True, keyfile=__dispy_job_keyfile,
                       certfile=__dispy_job_certfile)
    sock.settimeout(MsgTimeout)
    try:
        sock.connect(__dispy_job_info.reply_addr)
        sock.send_msg(serialize(__dispy_job_reply))
        ack = sock.recv_msg()
        assert ack == b'ACK'
    except:
        logger.warning("Couldn't send provisional results %s:\n%s",
                       str(result), traceback.format_exc())
    sock.close()


def dispy_send_file(path):
    """Computations may use this function to send files back to the client.

    If the computations have small amount of data to be sent back to
    the client, then the return value can be used for that
    purpose. However, if (temporary) result is stored in file(s), then
    those file(s) can be sent back to the client.

    File at given 'path' is sent to the client, which saves the file
    with the same path under its working directory. If multiple jobs
    on different nodes send files, care must be taken to use different
    paths so files sent by one job don't overwrite files sent by other
    jobs.

    If file size exceeds 'MaxFileSize' bytes, this function returns -1,
    without sending it. Return value of 0 indicates successfull transfer.
    """
    path = os.path.expanduser(path)
    xf = _XferFile(path, os.stat(path))
    if MaxFileSize and xf.stat_buf.st_size > MaxFileSize:
        return -1
    xf.name = os.path.splitdrive(path)[1]
    if xf.name.startswith(os.sep):
        xf.name = xf.name[len(os.sep):]
    __dispy_job_reply = __dispy_job_info.job_reply
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock = AsyncSocket(sock, blocking=True,
                       keyfile=__dispy_job_keyfile, certfile=__dispy_job_certfile)
    sock.settimeout(MsgTimeout)
    try:
        sock.connect(__dispy_job_info.reply_addr)
        sock.send_msg(b'FILEXFER:' + serialize(xf))
        sock.send_msg(serialize(__dispy_job_reply))
        ack = sock.recv_msg()
        assert ack == b'ACK'
        fd = open(path, 'rb')
        n = 0
        while n < xf.stat_buf.st_size:
            data = fd.read(1024000)
            if not data:
                break
            sock.sendall(data)
            n += len(data)
        fd.close()
        assert n == xf.stat_buf.st_size
        ack = sock.recv_msg()
        assert ack == b'ACK'
    except:
        return -1
    else:
        return 0
    finally:
        sock.close()


class _DispyJobInfo(object):
    """Internal use only.
    """
    def __init__(self, job_reply, reply_addr, compute, xfer_files):
        self.job_reply = job_reply
        self.reply_addr = reply_addr
        self.compute_id = compute.id
        self.compute_dest_path = compute.dest_path
        self.xfer_files = xfer_files
        self.proc = None


def _same_file(tgt, xf):
    """Internal use only.
    """
    # TODO: compare checksum?
    try:
        stat_buf = os.stat(tgt)
        if stat_buf.st_size == xf.stat_buf.st_size and \
            abs(stat_buf.st_mtime - xf.stat_buf.st_mtime) <= 1 and \
                stat.S_IMODE(stat_buf.st_mode) == stat.S_IMODE(xf.stat_buf.st_mode):
            return True
    except:
        return False


def _dispy_job_func(__dispy_job_info, __dispy_job_certfile, __dispy_job_keyfile,
                    __dispy_job_args, __dispy_job_kwargs, __dispy_reply_Q,
                    __dispy_job_name, __dispy_job_code, __dispy_job_context, __dispy_path):
    """Internal use only.
    """
    os.chdir(__dispy_path)
    sys.stdout = io.StringIO()
    sys.stderr = io.StringIO()
    __dispy_job_reply = __dispy_job_info.job_reply
    try:
        exec(marshal.loads(__dispy_job_code[0])) in __dispy_job_context
        if __dispy_job_code[1]:
            exec(__dispy_job_code[1]) in __dispy_job_context
        __dispy_job_args = unserialize(__dispy_job_args)
        __dispy_job_kwargs = unserialize(__dispy_job_kwargs)
        __dispy_job_context.update(locals())
        exec("__dispy_job_reply.result = %s(*__dispy_job_args, **__dispy_job_kwargs)" % __dispy_job_name) in __dispy_job_context
        __dispy_job_reply.status = DispyJob.Finished
    except:
        __dispy_job_reply.exception = traceback.format_exc()
        __dispy_job_reply.status = DispyJob.Terminated
    __dispy_job_reply.stdout = sys.stdout.getvalue()
    __dispy_job_reply.stderr = sys.stderr.getvalue()
    __dispy_job_reply.end_time = time.time()
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    __dispy_reply_Q.put(__dispy_job_reply)


class _DispyNode(object):
    """Internal use only.
    """
    def __init__(self, cpus, ip_addr=None, ext_ip_addr=None, node_port=None,
                 name='', scheduler_node=None, scheduler_port=None,
                 dest_path_prefix='', clean=False, secret='', keyfile=None, certfile=None,
                 zombie_interval=60):
        assert 0 < cpus <= multiprocessing.cpu_count()
        self.num_cpus = cpus
        if name:
            self.name = name
        else:
            self.name = socket.gethostname()
        if ip_addr:
            ip_addr = _node_ipaddr(ip_addr)
            if not ip_addr:
                raise Exception('invalid ip_addr')
        else:
            ip_addr = socket.gethostbyname(socket.gethostname())
        if ip_addr.startswith('127.'):
            logger.warning('node IP address %s seems to be loopback address; this will prevent '
                           'communication with clients on other machines. ' % ip_addr)
        if ext_ip_addr:
            ext_ip_addr = _node_ipaddr(ext_ip_addr)
            if not ext_ip_addr:
                raise Exception('invalid ext_ip_addr')
        else:
            ext_ip_addr = ip_addr

        if not self.name:
            try:
                self.name = socket.gethostbyaddr(ext_ip_addr)[0]
            except:
                self.name = ''

        if not node_port:
            node_port = 51348

        self.scheduler = {'ip_addr': None, 'port': 51347, 'auth': []}

        self.ext_ip_addr = ext_ip_addr
        self.pulse_interval = None
        self.keyfile = keyfile
        self.certfile = certfile
        if self.keyfile:
            self.keyfile = os.path.abspath(self.keyfile)
        if self.certfile:
            self.certfile = os.path.abspath(self.certfile)

        self.asyncoro = AsynCoro()

        self.tcp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        if self.certfile:
            self.tcp_sock = ssl.wrap_socket(self.tcp_sock, keyfile=self.keyfile,
                                            certfile=self.certfile)
        self.tcp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.tcp_sock.bind((ip_addr, node_port))
        self.address = self.tcp_sock.getsockname()
        self.port = self.address[1]
        self.tcp_sock.listen(30)

        if dest_path_prefix:
            self.dest_path_prefix = dest_path_prefix.strip().rstrip(os.sep)
        else:
            self.dest_path_prefix = os.path.join(tempfile.gettempdir(), 'dispy', 'node')
        if clean:
            shutil.rmtree(self.dest_path_prefix, ignore_errors=True)
        if not os.path.isdir(self.dest_path_prefix):
            os.makedirs(self.dest_path_prefix)
            os.chmod(self.dest_path_prefix, stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)

        self.shelf = shelve.open(os.path.join(self.dest_path_prefix, 'shelf'),
                                 flag='c', writeback=True)

        self.avail_cpus = self.num_cpus
        self.computations = {}
        self.file_uses = {}
        self.job_infos = {}
        self.terminate = False
        self.signature = ''.join(hex(x)[2:] for x in os.urandom(10))
        self.secret = secret
        self.auth = auth_code(self.secret, self.signature)
        self.zombie_interval = 60 * zombie_interval
        if not scheduler_port:
            scheduler_port = 51347

        # prepend current directory in sys.path so computations can
        # load modules from current working directory
        sys.path.insert(0, '.')

        # TODO: use checkpointing (DMTCP?) if available instead?
        self.__init_globals = dict([(var, globals()[var]) for var in globals().keys() if var])
        self.__init_modules = sys.modules.keys()

        self.thread_lock = threading.Lock()
        self.udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.udp_sock.bind(('', node_port))
        logger.info('serving %s cpus at %s:%s', self.num_cpus, self.ext_ip_addr, node_port)
        logger.debug('tcp server at %s:%s', self.address[0], self.address[1])
        self.udp_sock = AsyncSocket(self.udp_sock)

        self.reply_Q = multiprocessing.Queue()
        self.reply_Q_thread = threading.Thread(target=self.__reply_Q)
        self.reply_Q_thread.start()

        self.timer_coro = Coro(self.timer_task)
        # self.tcp_coro = Coro(self.tcp_server)
        self.udp_coro = Coro(self.udp_server, _node_ipaddr(scheduler_node), scheduler_port)

    def broadcast_ping_msg(self, coro=None):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        sock = AsyncSocket(sock)
        sock.settimeout(2)
        ping_msg = {'ip_addr': self.ext_ip_addr, 'port': self.port, 'sign': self.signature,
                    'version': _dispy_version, 'scheduler_ip_addr': None}
        try:
            yield sock.sendto(b'PING:' + serialize(ping_msg),
                              ('<broadcast>', self.scheduler['port']))
        except:
            logger.debug(traceback.format_exc())
            pass
        sock.close()

    def send_pong_msg(self, info, addr, coro=None):
        if self.num_cpus != self.avail_cpus or self.scheduler['ip_addr'] is not None:
            logger.debug('Busy (%s/%s); ignoring ping message from %s:%s',
                         self.num_cpus, self.avail_cpus, addr[0], addr[1])
            raise StopIteration
        try:
            scheduler_ip_addrs = info['ip_addrs'] + [addr[0]]
            scheduler_port = info['port']
        except:
            logger.debug(traceback.format_exc())
            raise StopIteration

        if info.get('sign', None):
            pong_msg = {'ip_addr': self.ext_ip_addr, 'port': self.port, 'sign': self.signature,
                        'version': _dispy_version, 'name': self.name, 'cpus': self.num_cpus,
                        'auth': auth_code(self.secret, info['sign'])}
            for scheduler_ip_addr in scheduler_ip_addrs:
                addr = (scheduler_ip_addr, scheduler_port)
                pong_msg['scheduler_ip_addr'] = scheduler_ip_addr
                sock = AsyncSocket(socket.socket(socket.AF_INET, socket.SOCK_STREAM),
                                   keyfile=self.keyfile, certfile=self.certfile)
                sock.settimeout(2)
                try:
                    yield sock.connect(addr)
                    yield sock.send_msg(b'PONG:' + serialize(pong_msg))
                except:
                    pass
                finally:
                    sock.close()
        else:
            sock = AsyncSocket(socket.socket(socket.AF_INET, socket.SOCK_DGRAM))
            sock.settimeout(2)
            ping_msg = {'ip_addr': self.ext_ip_addr, 'port': self.port, 'sign': self.signature,
                        'version': _dispy_version}
            for scheduler_ip_addr in scheduler_ip_addrs:
                addr = (scheduler_ip_addr, scheduler_port)
                ping_msg['scheduler_ip_addr'] = scheduler_ip_addr
                try:
                    yield sock.sendto(b'PING:' + serialize(ping_msg), addr)
                except:
                    logger.debug(traceback.format_exc())
                    pass
            sock.close()

    def udp_server(self, scheduler_ip, scheduler_port, coro=None):
        coro.set_daemon()
        if self.avail_cpus == self.num_cpus:
            yield self.broadcast_ping_msg(coro=coro)
        ping_msg = {'ip_addr': self.ext_ip_addr, 'port': self.port, 'sign': self.signature,
                    'version': _dispy_version}

        def send_ping_msg(self, info, coro=None):
            sock = AsyncSocket(socket.socket(socket.AF_INET, socket.SOCK_DGRAM))
            sock.settimeout(2)
            addr = (info['ip_addr'], info['port'])
            info.update(ping_msg)
            info['scheduler_ip_addr'] = addr[0]
            try:
                yield sock.sendto(b'PING:' + serialize(info), addr)
            except:
                pass
            finally:
                sock.close()
            sock = AsyncSocket(socket.socket(socket.AF_INET, socket.SOCK_STREAM),
                               keyfile=self.keyfile, certfile=self.certfile)
            sock.settimeout(2)
            try:
                yield sock.connect(addr)
                yield sock.send_msg(b'PING:' + serialize(info))
            except:
                pass
            finally:
                sock.close()

        if scheduler_ip:
            Coro(send_ping_msg, self, {'ip_addr': scheduler_ip, 'port': scheduler_port})

        while True:
            msg, addr = yield self.udp_sock.recvfrom(1000)
            # TODO: process each message as separate Coro, so
            # exceptions are contained?
            if msg.startswith(b'PING:'):
                if self.num_cpus != self.avail_cpus or self.scheduler['ip_addr'] is not None:
                    logger.debug('Busy (%s/%s); ignoring ping message from %s:%s',
                                 self.num_cpus, self.avail_cpus, addr[0], addr[1])
                    continue
                try:
                    info = unserialize(msg[len(b'PING:'):])
                    if info['version'] != _dispy_version:
                        logger.warning('Ignoring %s due to version mismatch', addr[0])
                        continue
                except:
                    logger.debug('Ignoring ping message from %s (%s)', addr[0], addr[1])
                    continue
                Coro(self.send_pong_msg, info, addr)
            elif msg.startswith(b'PULSE:'):
                try:
                    info = unserialize(msg[len(b'PULSE:'):])
                except:
                    logger.warning('Ignoring PULSE from %s', addr[0])
                else:
                    if info['ip_addr'] == self.scheduler['ip_addr']:
                        now = time.time()
                        for compute in self.computations.values():
                            compute.last_pulse = now
            else:
                logger.warning('Ignoring ping message from %s', addr[0])

    def tcp_serve_task(self, conn, addr, coro=None):
        def job_request_task(msg):
            try:
                _job = unserialize(msg)
            except:
                logger.debug('Ignoring job request from %s', addr[0])
                # logger.debug(traceback.format_exc())
                raise StopIteration

            compute = self.computations.get(_job.compute_id, None)
            if compute is not None:
                if compute.scheduler_ip_addr != self.scheduler['ip_addr'] or \
                   compute.scheduler_port != self.scheduler['port'] or \
                   compute.auth not in self.scheduler['auth']:
                    logger.debug('Invalid scheduler IP address: scheduler %s:%s != %s:%s' %
                                 compute.scheduler_ip_addr, compute.scheduler_port,
                                 self.scheduler['ip_addr'], self.scheduler['port'])
                    compute = None
            if self.avail_cpus == 0:
                logger.warning('All cpus busy')
                try:
                    yield conn.send_msg(b'NAK (all cpus busy)')
                except:
                    pass
                raise StopIteration
            elif compute is None:
                logger.warning('Invalid computation %s', _job.compute_id)
                try:
                    yield conn.send_msg(bytes('NAK (invalid computation %s)' % _job.compute_id,
                                              'ascii'))
                except:
                    pass
                raise StopIteration

            for xf in _job.xfer_files:
                if MaxFileSize and xf.stat_buf.st_size > MaxFileSize:
                    try:
                        yield conn.send_msg(b'NAK')
                    except:
                        pass
                    raise StopIteration

            reply_addr = (compute.scheduler_ip_addr, compute.job_result_port)
            logger.debug('New job id %s from %s/%s', _job.uid, addr[0], compute.scheduler_ip_addr)

            if compute.type == _Compute.func_type:
                reply = _JobReply(_job, self.ext_ip_addr)
                reply.start_time = time.time()
                job_info = _DispyJobInfo(reply, reply_addr, compute, _job.xfer_files)
                args = (job_info, self.certfile, self.keyfile,
                        _job.args, _job.kwargs, self.reply_Q,
                        compute.name, (compute.code, _job.code), compute.context.copy(), compute.dest_path)
                try:
                    yield conn.send_msg(b'ACK')
                except:
                    logger.warning('Failed to send response for new job to %s', str(addr))
                    raise StopIteration
                job_info.job_reply.status = DispyJob.Running
                job_info.proc = multiprocessing.Process(target=_dispy_job_func, args=args)
                self.avail_cpus -= 1
                compute.pending_jobs += 1
                self.thread_lock.acquire()
                self.job_infos[_job.uid] = job_info
                self.thread_lock.release()
                job_info.proc.start()
                raise StopIteration
            elif compute.type == _Compute.prog_type:
                try:
                    yield conn.send_msg(b'ACK')
                except:
                    logger.warning('Failed to send response for new job to %s', str(addr))
                    raise StopIteration
                reply = _JobReply(_job, self.ext_ip_addr)
                reply.start_time = time.time()
                job_info = _DispyJobInfo(reply, reply_addr, compute, _job.xfer_files)
                job_info.job_reply.status = DispyJob.Running
                self.thread_lock.acquire()
                self.job_infos[_job.uid] = job_info
                self.thread_lock.release()
                self.avail_cpus -= 1
                compute.pending_jobs += 1
                prog_thread = threading.Thread(target=self.__job_program, args=(_job, job_info))
                prog_thread.start()
                raise StopIteration
            else:
                try:
                    yield conn.send_msg(bytes('NAK (invalid computation type "%s")' % compute.type,
                                              'ascii'))
                except:
                    logger.warning('Failed to send response for new job to %s', str(addr))

        def add_computation_task(msg):
            try:
                compute = unserialize(msg)
            except:
                logger.debug('Ignoring computation request from %s', addr[0])
                try:
                    yield conn.send_msg(b'Invalid computation request')
                except:
                    logger.warning('Failed to send reply to %s', str(addr))
                raise StopIteration
            if not ((self.scheduler['ip_addr'] is None and self.scheduler['auth'] == []) or
                    (self.scheduler['ip_addr'] == compute.scheduler_ip_addr and
                     self.scheduler['port'] == compute.scheduler_port)):
                logger.debug('Ignoring computation request from %s: %s, %s, %s',
                             compute.scheduler_ip_addr, self.scheduler['ip_addr'],
                             self.avail_cpus, self.num_cpus)
                try:
                    yield conn.send_msg(b'Busy')
                except:
                    pass
                raise StopIteration

            for xf in compute.xfer_files:
                if MaxFileSize and xf.stat_buf.st_size > MaxFileSize:
                    try:
                        yield conn.send_msg(b'NAK')
                    except:
                        pass
                    raise StopIteration

            resp = b'ACK'
            dest = os.path.join(self.dest_path_prefix, compute.scheduler_ip_addr)
            if not os.path.isdir(dest):
                try:
                    os.mkdir(dest)
                except:
                    yield conn.send_msg(b'Could not create destination directory')
                    raise StopIteration
            if compute.dest_path and isinstance(compute.dest_path, str):
                # TODO: get os.sep from client and convert (in case of mixed environments)?
                if not compute.dest_path.startswith(os.sep):
                    compute.dest_path = os.path.join(dest, compute.dest_path)
                if not os.path.isdir(compute.dest_path):
                    try:
                        os.makedirs(compute.dest_path)
                    except:
                        try:
                            yield conn.send_msg(b'NAK (Invalid dest_path)')
                        except:
                            logger.warning('Failed to send reply to %s', str(addr))
                            raise StopIteration
            else:
                compute.dest_path = tempfile.mkdtemp(prefix=compute.name + '_', dir=dest)
            logger.debug('dest_path for "%s"/%s: %s', compute.name, compute.id, compute.dest_path)
            os.chmod(compute.dest_path, stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)

            if compute.id in self.computations:
                logger.warning('Computation "%s" (%s) is being replaced',
                               compute.name, compute.id)
            setattr(compute, 'last_pulse', time.time())
            setattr(compute, 'pending_jobs', 0)
            setattr(compute, 'pending_results', 0)
            setattr(compute, 'zombie', False)

            if compute.type == _Compute.func_type:
                try:
                    code = compile(compute.code, '<string>', 'exec')
                except:
                    logger.warning('Computation "%s" could not be compiled', compute.name)
                    if os.path.isdir(compute.dest_path):
                        os.rmdir(compute.dest_path)
                    try:
                        yield conn.send_msg(b'NAK (Compilation failed)')
                    except:
                        logger.warning('Failed to send reply to %s', str(addr))
                    raise StopIteration
                compute.code = marshal.dumps(code)
            elif compute.type == _Compute.prog_type:
                assert not compute.code
                compute.name = os.path.join(compute.dest_path, os.path.basename(compute.name))

            if resp == b'ACK' and \
               not ((self.scheduler['ip_addr'] is None) or
                    (self.scheduler['ip_addr'] == compute.scheduler_ip_addr and
                     self.scheduler['port'] == compute.scheduler_port)):
                resp = b'NAK (busy)'
            if resp == b'ACK':
                self.computations[compute.id] = compute
                self.scheduler['ip_addr'] = compute.scheduler_ip_addr
                self.scheduler['port'] = compute.scheduler_port
                self.scheduler['auth'].append(compute.auth)
                self.pulse_interval = compute.pulse_interval
                if not self.pulse_interval:
                    self.pulse_interval = 10 * 60
                if self.zombie_interval:
                    self.pulse_interval = num_min(self.pulse_interval, self.zombie_interval / 5.0)
                self.shelf['%s_%s' % (compute.auth, compute.id)] = compute
                self.shelf.sync()

                try:
                    yield conn.send_msg(resp)
                except:
                    del self.computations[compute.id]
                    self.scheduler['ip_addr'] = None
                    self.scheduler['port'] = None
                    self.scheduler['auth'].remove(compute.auth)
                    self.pulse_interval = None
                else:
                    self.timer_coro.resume(True)
            else:
                if os.path.isdir(compute.dest_path):
                    try:
                        os.rmdir(compute.dest_path)
                        yield conn.send_msg(resp)
                    except:
                        pass

        def xfer_file_task(msg):
            try:
                xf = unserialize(msg)
            except:
                logger.debug('Ignoring file trasnfer request from %s', addr[0])
                raise StopIteration

            if xf.compute_id not in self.computations or \
               (MaxFileSize and xf.stat_buf.st_size > MaxFileSize):
                logger.error('Invalid file transfer for "%s"' % xf.name)
                yield conn.send_msg(b'NAK')
                raise StopIteration
            tgt = os.path.join(self.computations[xf.compute_id].dest_path,
                               os.path.basename(xf.name))
            if os.path.isfile(tgt) and _same_file(tgt, xf):
                if tgt in self.file_uses:
                    self.file_uses[tgt] += 1
                else:
                    self.file_uses[tgt] = 1
                resp = b'ACK'
            else:
                resp = b'NAK'

            yield conn.send_msg(resp)
            if resp != b'ACK':
                logger.debug('Copying file %s to %s (%s)', xf.name, tgt, xf.stat_buf.st_size)
                try:
                    fd = open(tgt, 'wb')
                    n = 0
                    while n < xf.stat_buf.st_size:
                        data = yield conn.recvall(min(xf.stat_buf.st_size-n, 1024000))
                        if not data:
                            break
                        fd.write(data)
                        n += len(data)
                        if MaxFileSize and n > MaxFileSize:
                            logger.warning('File "%s" is too big (%s); it is truncated', tgt, n)
                            break
                    fd.close()
                    if n < xf.stat_buf.st_size:
                        resp = b'NAK (read only %s bytes)' % n
                    else:
                        resp = b'ACK'
                        logger.debug('Copied file %s, %s', tgt, resp)
                        os.utime(tgt, (xf.stat_buf.st_atime, xf.stat_buf.st_mtime))
                        os.chmod(tgt, stat.S_IMODE(xf.stat_buf.st_mode))
                        if tgt in self.file_uses:
                            self.file_uses[tgt] += 1
                        else:
                            self.file_uses[tgt] = 1
                except:
                    logger.warning('Copying file "%s" failed with "%s"',
                                   xf.name, traceback.format_exc())
                    resp = b'NAK'
                try:
                    yield conn.send_msg(resp)
                except:
                    logger.debug('Could not send reply for "%s"', xf.name)
            raise StopIteration  # xfer_file_task

        def setup_computation(msg):
            try:
                compute_id = unserialize(msg)
                compute = self.computations[compute_id]
                assert isinstance(compute.setup, str)
                os.chdir(compute.dest_path)
                exec(marshal.loads(compute.code)) in compute.context
                exec("assert %s() == 0" % compute.setup) in compute.context
            except:
                logger.debug('Setup "%s" failed' % compute.setup)
                resp = traceback.format_exc().encode()
            else:
                resp = b'ACK'
            if resp != b'ACK':
                if not compute.cleanup:
                    compute.cleanup = True
                compute.zombie = True
                self.cleanup_computation(compute)
            yield conn.send_msg(resp)

        def terminate_job_task(msg):
            try:
                _job = unserialize(msg)
                compute = self.computations[_job.compute_id]
                # assert addr[0] == compute.scheduler_ip_addr
                self.thread_lock.acquire()
                job_info = self.job_infos.get(_job.uid, None)
                self.thread_lock.release()
            except:
                logger.debug('Ignoring job request from %s, %s', addr[0], compute.scheduler_ip_addr)
                # logger.debug(traceback.format_exc())
                raise StopIteration
            if job_info is None:
                logger.debug('Job %s completed; ignoring cancel request from %s',
                             _job.uid, addr[0])
                raise StopIteration
            logger.debug('Terminating job %s', _job.uid)
            job_info.proc.terminate()
            if isinstance(job_info.proc, multiprocessing.Process):
                for x in range(20):
                    if job_info.proc.is_alive():
                        yield coro.sleep(0.1)
                    else:
                        logger.debug('Process "%s" for job %s terminated', compute.name, _job.uid)
                        break
                else:
                    logger.warning('Could not kill process %s', compute.name)
                    raise StopIteration
            else:
                assert isinstance(job_info.proc, subprocess.Popen)
                for x in range(20):
                    rc = job_info.proc.poll()
                    logger.debug('Program "%s" for job %s terminated with %s',
                                 compute.name, _job.uid, rc)
                    if rc is not None:
                        break
                    if x == 10:
                        logger.debug('Killing job %s', _job.uid)
                        job_info.proc.kill()
                    yield coro.sleep(0.1)
                else:
                    logger.warning('Could not kill process %s', compute.name)
                    raise StopIteration
            job_info.job_reply.end_time = time.time()
            self.thread_lock.acquire()
            if self.job_infos.get(_job.uid, None) == job_info:
                job_info.job_reply.status = DispyJob.Terminated
                self.reply_Q.put(job_info.job_reply)
            self.thread_lock.release()

        def retrieve_job_task(msg):

            def send_reply(reply):
                try:
                    yield conn.send_msg(serialize(reply))
                except:
                    raise StopIteration(-1)
                raise StopIteration(0)

            # generator
            try:
                req = unserialize(msg)
                uid = req['uid']
                compute_id = req['compute_id']
                auth = req['auth']
                job_hash = req['hash']
            except:
                yield send_reply(None)
                raise StopIteration

            shelf_key = '%s_%s' % (auth, compute_id)
            compute = self.computations.get(compute_id, None)
            if compute is None or compute.auth != auth:
                compute = self.shelf.get(shelf_key, None)
                if compute is None:
                    yield send_reply(None)
                    raise StopIteration

            info_file = os.path.join(compute.dest_path, '_dispy_job_reply_%s' % uid)
            if not os.path.isfile(info_file):
                yield send_reply(None)
                raise StopIteration
            try:
                fd = open(info_file, 'rb')
                job_reply = pickle.load(fd)
                fd.close()
                assert job_reply.hash == job_hash
            except:
                yield send_reply(None)
                raise StopIteration

            try:
                yield conn.send_msg(serialize(job_reply))
                ack = yield conn.recv_msg()
                assert ack == b'ACK'
                compute.pending_results -= 1
                self.shelf[shelf_key] = compute
                self.shelf.sync()
            except:
                pass
            else:
                try:
                    os.remove(info_file)
                except:
                    pass

        # tcp_serve_task starts
        try:
            req = yield conn.recvall(len(self.auth))
        except:
            logger.warning('Ignoring request; invalid client authentication?')
            conn.close()
            raise StopIteration
        msg = yield conn.recv_msg()
        if req != self.auth:
            if msg.startswith(b'PING:'):
                pass
            else:
                logger.warning('Ignoring request; invalid client authentication?')
                conn.close()
                raise StopIteration
        if not msg:
            conn.close()
            raise StopIteration
        if msg.startswith(b'JOB:'):
            msg = msg[len(b'JOB:'):]
            yield job_request_task(msg)
            conn.close()
        elif msg.startswith(b'COMPUTE:'):
            msg = msg[len(b'COMPUTE:'):]
            yield add_computation_task(msg)
            conn.close()
        elif msg.startswith(b'FILEXFER:'):
            msg = msg[len(b'FILEXFER:'):]
            yield xfer_file_task(msg)
            conn.close()
        elif msg.startswith(b'SETUP:'):
            msg = msg[len(b'SETUP:'):]
            yield setup_computation(msg)
            conn.close()
        elif msg.startswith(b'CLOSE:'):
            msg = msg[len(b'CLOSE:'):]
            try:
                info = unserialize(msg)
                compute_id = info['compute_id']
                auth = info['auth']
            except:
                logger.debug('Deleting computation failed with %s',
                             traceback.format_exc())
            else:
                compute = self.computations.get(compute_id, None)
                if compute is None or compute.auth != auth:
                    logger.warning('Computation "%s" is not valid', compute_id)
                else:
                    compute.zombie = True
                    self.cleanup_computation(compute)
            conn.close()
        elif msg.startswith(b'TERMINATE_JOB:'):
            msg = msg[len(b'TERMINATE_JOB:'):]
            yield terminate_job_task(msg)
            conn.close()
        elif msg.startswith(b'RESEND_JOB_RESULTS:'):
            msg = msg[len(b'RESEND_JOB_RESULTS:'):]
            try:
                info = unserialize(msg)
                compute_id = info['compute_id']
                auth = info['auth']
            except:
                reply = 0
            else:
                compute = self.computations.get(compute_id, None)
                if compute is None or compute.auth != auth:
                    compute = self.shelf.get('%s_%s' % (auth, compute_id), None)
                if compute is None:
                    reply = 0
                else:
                    reply = compute.pending_results + compute.pending_jobs
            yield conn.send_msg(serialize(reply))
            conn.close()
            if reply > 0:
                yield self.resend_job_results(compute, coro=coro)
        elif msg.startswith(b'PING:'):
            try:
                info = unserialize(msg[len(b'PING:'):])
                if info['version'] == _dispy_version:
                    reply = {'ip_addr': self.ext_ip_addr, 'port': self.port,
                             'sign': self.signature, 'version': _dispy_version,
                             'name': self.name, 'cpus': self.num_cpus,
                             'auth': auth_code(self.secret, info['sign'])}
                    reply['scheduler_ip_addr'] = addr[0]
                    yield conn.send_msg(serialize(reply))
                    Coro(self.send_pong_msg, info, addr)
            except:
                logger.debug(traceback.format_exc())
            conn.close()
        elif msg.startswith(b'PENDING_JOBS:'):
            msg = msg[len(b'PENDING_JOBS:'):]
            reply = {'done': [], 'pending': 0}
            try:
                info = unserialize(msg)
                compute_id = info['compute_id']
                auth = info['auth']
            except:
                pass
            else:
                compute = self.computations.get(compute_id, None)
                if compute is None or compute.auth != auth:
                    compute = self.shelf.get('%s_%s' % (auth, compute_id), None)
                if compute is not None:
                    done = []
                    if compute.pending_results:
                        for result_file in glob.glob(os.path.join(compute.dest_path,
                                                                  '_dispy_job_reply_*')):
                            result_file = os.path.basename(result_file)
                            try:
                                uid = int(result_file[len('_dispy_job_reply_'):])
                            except:
                                pass
                            else:
                                done.append(uid)
                                # limit so as not to take up too much time
                                if len(done) > 50:
                                    break
                    reply['done'] = done
                    reply['pending'] = compute.pending_jobs
            yield conn.send_msg(serialize(reply))
            conn.close()
        elif msg.startswith(b'RETRIEVE_JOB:'):
            msg = msg[len(b'RETRIEVE_JOB:'):]
            yield retrieve_job_task(msg)
            conn.close()
        else:
            logger.warning('Invalid request "%s" from %s',
                           msg[:min(10, len(msg))], addr[0])
            resp = b'NAK (invalid command: %s)' % (msg[:min(10, len(msg))])
            try:
                yield conn.send_msg(resp)
            except:
                logger.warning('Failed to send reply to %s', str(addr))
            conn.close()

    def resend_job_results(self, compute, coro=None):
        # TODO: limit number queued so as not to take up too much space/time
        result_files = [f for f in os.listdir(compute.dest_path)
                        if f.startswith('_dispy_job_reply_')]
        result_files = result_files[:min(len(result_files), 64)]
        for result_file in result_files:
            result_file = os.path.join(compute.dest_path, result_file)
            try:
                fd = open(result_file, 'rb')
                job_result = pickle.load(fd)
                fd.close()
            except:
                logger.debug('Could not load "%s"', result_file)
                # logger.debug(traceback.format_exc())
                continue
            job_info = _DispyJobInfo(job_result, (compute.scheduler_ip_addr,
                                                  compute.job_result_port), compute, [])
            status = yield self._send_job_reply(job_info, resending=True)
            if status:
                break

    def timer_task(self, coro=None):
        coro.set_daemon()
        last_pulse_time = last_zombie_time = time.time()
        while True:
            reset = yield coro.suspend(self.pulse_interval)
            if reset:
                continue

            now = time.time()
            if self.pulse_interval and (now - last_pulse_time) >= self.pulse_interval:
                if self.scheduler['ip_addr']:
                    last_pulse_time = now
                    sock = AsyncSocket(socket.socket(socket.AF_INET, socket.SOCK_DGRAM))
                    sock.settimeout(2)
                    info = {'ip_addr': self.ext_ip_addr, 'port': self.port,
                            'cpus': self.num_cpus - self.avail_cpus,
                            'scheduler_ip_addr': self.scheduler['ip_addr']}
                    yield sock.sendto(b'PULSE:' + serialize(info),
                                      (self.scheduler['ip_addr'], self.scheduler['port']))
                    sock.close()

                resend = [compute for compute in self.computations.values()
                          if compute.pending_results and not compute.zombie]
                for compute in resend:
                    Coro(self.resend_job_results, compute)

            if self.zombie_interval and (now - last_zombie_time) >= self.zombie_interval:
                last_zombie_time = now
                for compute in self.computations.values():
                    if (now - compute.last_pulse) > self.zombie_interval:
                        logger.warning('Computation "%s" is marked as zombie' % compute.name)
                        compute.zombie = True
                zombies = [compute for compute in self.computations.values()
                           if compute.zombie and compute.pending_jobs == 0]
                for compute in zombies:
                    logger.warning('Deleting zombie computation "%s"', compute.name)
                    self.cleanup_computation(compute)
                for compute in zombies:
                    sock = AsyncSocket(socket.socket(socket.AF_INET, socket.SOCK_STREAM),
                                       keyfile=self.keyfile, certfile=self.certfile)
                    sock.settimeout(2)
                    logger.debug('Sending TERMINATE to %s', compute.scheduler_ip_addr)
                    info = {'ip_addr': self.ext_ip_addr, 'port': self.port, 'sign': self.signature}
                    try:
                        yield sock.connect((compute.scheduler_ip_addr, compute.scheduler_port))
                        yield sock.send_msg(b'TERMINATED:' + serialize(info))
                    except:
                        pass
                    finally:
                        sock.close()
                if self.scheduler['ip_addr'] is None and self.avail_cpus == self.num_cpus:
                    self.pulse_interval = None
                    yield self.broadcast_ping_msg(coro=coro)

    def __job_program(self, _job, job_info):
        compute = self.computations[_job.compute_id]
        if compute.name.endswith('.py'):
            program = [sys.executable, compute.name]
        else:
            program = [compute.name]
        args = unserialize(_job.args)
        program.extend(args)
        reply = job_info.job_reply
        try:
            os.chdir(compute.dest_path)
            env = {}
            env.update(os.environ)
            env['PATH'] = compute.dest_path + os.pathsep + env['PATH']
            job_info.proc = subprocess.Popen(program, stdout=subprocess.PIPE,
                                             stderr=subprocess.PIPE, env=env)

            assert isinstance(job_info.proc, subprocess.Popen)
            reply.stdout, reply.stderr = job_info.proc.communicate()
            reply.result = job_info.proc.returncode
            reply.status = DispyJob.Finished
        except:
            reply.exception = traceback.format_exc()
            reply.status = DispyJob.Terminated
        reply.end_time = time.time()
        self.reply_Q.put(reply)

    def __reply_Q(self):
        while True:
            job_reply = self.reply_Q.get()
            if job_reply is None:
                break
            self.thread_lock.acquire()
            job_info = self.job_infos.get(job_reply.uid, None)
            if job_info is not None:
                job_info.job_reply = job_reply
            self.thread_lock.release()
            if job_info is not None:
                if job_info.proc is not None:
                    if isinstance(job_info.proc, multiprocessing.Process):
                        job_info.proc.join(2)
                    else:
                        job_info.proc.wait()
                for xf in job_info.xfer_files:
                    tgt = os.path.join(self.computations[xf.compute_id].dest_path,
                                       os.path.basename(xf.name))
                    self.file_uses[tgt] -= 1
                    if self.file_uses[tgt] == 0:
                        self.file_uses.pop(tgt)
                        try:
                            os.remove(tgt)
                        except:
                            logger.warning('Failed to remove "%s"' % tgt)
                Coro(self._send_job_reply, job_info, resending=False)

    def _send_job_reply(self, job_info, resending=False, coro=None):
        """Internal use only.
        """
        job_reply = job_info.job_reply
        logger.debug('Sending result for job %s (%s) to %s',
                     job_reply.uid, job_reply.status, str(job_info.reply_addr))
        if not resending:
            self.thread_lock.acquire()
            assert self.job_infos.pop(job_reply.uid, None) is not None
            self.thread_lock.release()
            self.avail_cpus += 1
            assert self.avail_cpus <= self.num_cpus
        compute = self.computations.get(job_info.compute_id, None)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock = AsyncSocket(sock, keyfile=self.keyfile, certfile=self.certfile)
        sock.settimeout(MsgTimeout)
        try:
            yield sock.connect(job_info.reply_addr)
            yield sock.send_msg(b'JOB_REPLY:' + serialize(job_reply))
            ack = yield sock.recv_msg()
            assert ack == b'ACK'
        except:
            status = -1
            if not resending:
                # store job result even if computation has not enabled
                # fault recovery; user may be able to access node and
                # retrieve result manually
                f = os.path.join(job_info.compute_dest_path, '_dispy_job_reply_%s' % job_reply.uid)
                logger.error('Could not send reply for job %s to %s; saving it in "%s"',
                             job_reply.uid, str(job_info.reply_addr), f)
                try:
                    fd = open(f, 'wb')
                    pickle.dump(job_reply, fd)
                    fd.close()
                except:
                    logger.debug('Could not save reply for job %s', job_reply.uid)
                else:
                    if compute is not None:
                        compute.pending_results += 1
        else:
            status = 0
            if compute is not None:
                compute.last_pulse = time.time()
                if resending:
                    compute.pending_results -= 1
                    f = os.path.join(job_info.compute_dest_path,
                                     '_dispy_job_reply_%s' % job_reply.uid)
                    if os.path.isfile(f):
                        try:
                            os.remove(f)
                        except:
                            logger.warning('Could not remove "%s"' % f)
                if compute.pending_results:
                    Coro(self.resend_job_results, compute)
        finally:
            sock.close()
            if not resending:
                if compute is not None:
                    compute.pending_jobs -= 1
                    if compute.pending_jobs == 0 and compute.zombie:
                        self.cleanup_computation(compute)
        raise StopIteration(status)

    def cleanup_computation(self, compute):
        if not compute.zombie:
            return
        if compute.pending_jobs != 0:
            logger.debug('pending jobs for computation "%s"/%s: %s',
                         compute.name, compute.id, compute.pending_jobs)
            if compute.pending_jobs > 0:
                return

        shelf_key = '%s_%s' % (compute.auth, compute.id)
        if compute.pending_results == 0:
            self.shelf.pop(shelf_key, None)
        else:
            self.shelf[shelf_key] = compute
        self.shelf.sync()
        self.computations.pop(compute.id)
        try:
            self.scheduler['auth'].remove(compute.auth)
        except ValueError:
            pass
        if (not self.computations) and \
           compute.scheduler_ip_addr == self.scheduler['ip_addr'] and \
           compute.scheduler_port == self.scheduler['port'] and \
           self.scheduler['auth'] == [] and \
           all(c.scheduler_ip_addr != self.scheduler['ip_addr']
               for c in self.computations.values()):
            assert self.avail_cpus == self.num_cpus
            self.scheduler['ip_addr'] = None
            self.pulse_interval = None

        if self.scheduler['ip_addr'] is None and self.avail_cpus == self.num_cpus:
            self.timer_coro.resume(None)
            Coro(self.broadcast_ping_msg)
        if not compute.cleanup:
            return
        os.chdir(self.dest_path_prefix)
        if isinstance(compute.cleanup, str):
            try:
                exec(marshal.loads(compute.code)) in compute.context
                exec("%s()" % compute.cleanup) in compute.context
            except:
                logger.debug('cleanup "%s" failed' % compute.cleanup)
                logger.debug(traceback.format_exc())

        for module in list(sys.modules.keys()):
            if module.startswith('multiprocessing.'):
                continue
            if module not in self.__init_modules:
                logger.debug('Module "%s" is left behind by "%s" at %s' %
                             (module, compute.name, compute.scheduler_ip_addr))
        for var in list(globals().keys()):
            if var == '_dispy_node' or var == '_dispy_conn' or var == '_dispy_addr':
                continue
            # if var is module, leave it alone?
            # if var in sys.modules:
            #     continue
            if var not in self.__init_globals:
                logger.warning('Variable "%s" left behind by "%s" at %s is being removed' %
                               (var, compute.name, compute.scheduler_ip_addr))
                globals().pop(var, None)

        for var, state in self.__init_globals.items():
            if var == '_dispy_node' or var == '_dispy_config':
                continue
            if state != globals().get(var, None):
                logger.warning('Variable "%s" changed by "%s" is being reset' %
                               (var, compute.name))
                globals()[var] = state

        for xf in compute.xfer_files:
            tgt = os.path.join(compute.dest_path, os.path.basename(xf.name))
            if tgt not in self.file_uses:
                logger.debug('File "%s" is unknown', tgt)
                continue
            self.file_uses[tgt] -= 1
            if self.file_uses[tgt] == 0:
                self.file_uses.pop(tgt)
                try:
                    os.remove(tgt)
                    if os.path.splitext(tgt)[1] == '.py' and os.path.isfile(tgt + 'c'):
                        os.remove(tgt + 'c')
                except:
                    logger.warning('Could not remove file "%s"', tgt)

        if os.path.isdir(compute.dest_path) and \
           compute.dest_path.startswith(self.dest_path_prefix) and \
           len(compute.dest_path) > len(self.dest_path_prefix) and \
           len(os.listdir(compute.dest_path)) == 0:
            logger.debug('Removing "%s"', compute.dest_path)
            try:
                os.rmdir(compute.dest_path)
            except:
                logger.warning('Could not remove directory "%s"', compute.dest_path)

    def shutdown(self):
        def _shutdown(self, coro=None):
            self.thread_lock.acquire()
            job_infos = self.job_infos
            self.job_infos = {}
            computations = list(self.computations.items())
            self.computations = {}
            if self.reply_Q:
                self.reply_Q.put(None)
            self.thread_lock.release()
            for uid, job_info in job_infos.items():
                job_info.proc.terminate()
                logger.debug('process for %s is killed', uid)
                if isinstance(job_info.proc, multiprocessing.Process):
                    job_info.proc.join(2)
                else:
                    job_info.proc.wait()
            for cid, compute in computations:
                sock = AsyncSocket(socket.socket(socket.AF_INET, socket.SOCK_STREAM),
                                   keyfile=self.keyfile, certfile=self.certfile)
                sock.settimeout(2)
                logger.debug('Sending TERMINATE to %s', compute.scheduler_ip_addr)
                info = {'ip_addr': self.ext_ip_addr, 'port': self.port, 'sign': self.signature}
                try:
                    yield sock.connect((compute.scheduler_ip_addr, compute.scheduler_port))
                    yield sock.send_msg(b'TERMINATED:' + serialize(info))
                except:
                    pass
                sock.close()

        if hasattr(self, 'job_infos'):
            Coro(_shutdown, self).value()
            # self.asyncoro.join()
            self.asyncoro.finish()


if __name__ == '__main__':
    import argparse
    import re

    logger.info('dispynode version %s' % _dispy_version)

    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--cpus', dest='cpus', type=int, default=0,
                        help='number of cpus used by dispy; if negative, '
                        'that many cpus are not used')
    parser.add_argument('-d', '--debug', action='store_true', dest='loglevel', default=False,
                        help='if given, debug messages are printed')
    parser.add_argument('-i', '--ip_addr', dest='ip_addr', default=None,
                        help='IP address to use (may be needed in case of multiple interfaces)')
    parser.add_argument('--ext_ip_addr', dest='ext_ip_addr', default=None,
                        help='External IP address to use (needed in case of NAT firewall/gateway)')
    parser.add_argument('-p', '--node_port', dest='node_port', type=int, default=51348,
                        help='port number to use')
    parser.add_argument('--name', dest='name', type=str, default='',
                        help='name asscoiated to this node; default is obtained with gethostname()')
    parser.add_argument('--dest_path_prefix', dest='dest_path_prefix', default=None,
                        help='path prefix where files sent by dispy are stored')
    parser.add_argument('--scheduler_node', dest='scheduler_node', default=None,
                        help='name or IP address of scheduler to announce when starting')
    parser.add_argument('--scheduler_port', dest='scheduler_port', type=int, default=51347,
                        help='port number used by scheduler')
    parser.add_argument('--max_file_size', dest='max_file_size', default=str(MaxFileSize), type=str,
                        help='maximum file size of any file transferred (use 0 for unlimited size)')
    parser.add_argument('--zombie_interval', dest='zombie_interval', default=60, type=float,
                        help='interval in minutes to presume unresponsive scheduler is zombie')
    parser.add_argument('-s', '--secret', dest='secret', default='',
                        help='authentication secret for handshake with dispy clients')
    parser.add_argument('--certfile', dest='certfile', default=None,
                        help='file containing SSL certificate')
    parser.add_argument('--keyfile', dest='keyfile', default=None,
                        help='file containing SSL key')
    parser.add_argument('--clean', action='store_true', dest='clean', default=False,
                        help='if given, files copied from or generated by clients will be removed')
    _dispy_config = vars(parser.parse_args(sys.argv[1:]))
    del parser

    if _dispy_config['loglevel']:
        logger.setLevel(logging.DEBUG)
        asyncoro.logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
    del _dispy_config['loglevel']

    cpus = multiprocessing.cpu_count()
    if _dispy_config['cpus']:
        if _dispy_config['cpus'] > 0:
            if _dispy_config['cpus'] > cpus:
                raise Exception('CPU count must be <= %s' % cpus)
        else:
            if _dispy_config['cpus'] <= -cpus:
                raise Exception('CPU count must be > -%s' % cpus)
            cpus += _dispy_config['cpus']
            _dispy_config['cpus'] = cpus
    else:
        _dispy_config['cpus'] = cpus
    del cpus

    if _dispy_config['zombie_interval']:
        _dispy_config['zombie_interval'] = float(_dispy_config['zombie_interval'])
        if _dispy_config['zombie_interval'] < 1:
            raise Exception('zombie_interval must be at least 1')

    m = re.match(r'(\d+)([kKmMgGtT]?)', _dispy_config['max_file_size'])
    if m:
        MaxFileSize = int(m.group(1))
        if m.group(2):
            m = m.group(2).lower()
            if m == 'k':
                MaxFileSize *= 1024
            elif m == 'm':
                MaxFileSize *= 1024**2
            elif m == 'g':
                MaxFileSize *= 1024**3
            elif m == 't':
                MaxFileSize *= 1024**4
            else:
                raise Exception('invalid max_file_size option')
    else:
        raise Exception('max_file_size must be >= 0')
    del m
    del _dispy_config['max_file_size']

    _dispy_node = _DispyNode(**_dispy_config)
    del _dispy_config

    while True:
        try:
            _dispy_conn, _dispy_addr = _dispy_node.tcp_sock.accept()
        except KeyboardInterrupt:
            # TODO: terminate even if jobs are scheduled?
            logger.info('Interrupted; terminating')
            _dispy_node.shutdown()
            break
        except:
            logger.debug(traceback.format_exc())
            continue
        _dispy_conn = AsyncSocket(_dispy_conn, keyfile=_dispy_node.keyfile,
                                  certfile=_dispy_node.certfile)
        Coro(_dispy_node.tcp_serve_task, _dispy_conn, _dispy_addr)
