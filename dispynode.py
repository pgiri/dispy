#!/usr/bin/env python

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
import hashlib
import logging
import marshal
import tempfile
import shelve
import cPickle as pickle
import cStringIO as io

from dispy import _DispyJob_, _JobReply, DispyJob, _Compute, _XferFile, \
     _node_ipaddr, _dispy_version

import asyncoro
from asyncoro import Coro, AsynCoro, AsyncSocket, MetaSingleton, serialize, unserialize

__version__ = _dispy_version
__all__ = []

MaxFileSize = 10*1024*1024

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
    # logger.debug('Sending provisional result for job %s to %s',
    #              __dispy_job_reply.uid, __dispy_job_info.reply_addr)
    __dispy_job_reply.status = DispyJob.ProvisionalResult
    __dispy_job_reply.result = result
    __dispy_job_reply.end_time = time.time()
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock = AsyncSocket(sock, blocking=True, keyfile=__dispy_job_keyfile,
                       certfile=__dispy_job_certfile)
    sock.settimeout(5)
    try:
        sock.connect(__dispy_job_info.reply_addr)
        sock.send_msg(serialize(__dispy_job_reply))
        ack = sock.recv_msg()
        assert ack == 'ACK'
    except:
        logger.warning("Couldn't send provisional results %s:\n%s",
                       str(result), traceback.format_exc())
    sock.close()

class _DispyJobInfo(object):
    """Internal use only.
    """
    def __init__(self, job_reply, reply_addr, compute):
        self.job_reply = job_reply
        self.reply_addr = reply_addr
        self.compute_id = compute.id
        self.compute_dest_path = compute.dest_path
        self.proc = None

def _same_file(tgt, xf):
    """Internal use only.
    """
    # TODO: compare checksum?
    try:
        stat_buf = os.stat(tgt)
        if abs(stat_buf.st_mtime - xf.stat_buf.st_mtime) <= 1 and \
               stat_buf.st_size == xf.stat_buf.st_size and \
               stat.S_IMODE(stat_buf.st_mode) == stat.S_IMODE(xf.stat_buf.st_mode):
            return True
    except:
        return False

def _dispy_job_func(__dispy_job_info, __dispy_job_certfile, __dispy_job_keyfile,
                    __dispy_job_args, __dispy_job_kwargs, __dispy_reply_Q,
                    __dispy_job_name, __dispy_job_code, __dispy_path, __dispy_job_files=[]):
    """Internal use only.
    """
    os.chdir(__dispy_path)
    sys.stdout = io.StringIO()
    sys.stderr = io.StringIO()
    __dispy_job_reply = __dispy_job_info.job_reply
    sys.path = [__dispy_path] + sys.path
    try:
        exec(marshal.loads(__dispy_job_code))
        globals().update(locals())
        __dispy_job_args = unserialize(__dispy_job_args)
        __dispy_job_kwargs = unserialize(__dispy_job_kwargs)
        __func = globals()[__dispy_job_name]
        __dispy_job_reply.result = __func(*__dispy_job_args, **__dispy_job_kwargs)
        __dispy_job_reply.status = DispyJob.Finished
    except:
        __dispy_job_reply.exception = traceback.format_exc()
        __dispy_job_reply.status = DispyJob.Terminated
    for f in __dispy_job_files:
        if os.path.isfile(f):
            try:
                os.remove(f)
            except:
                logger.debug('Could not remove "%s"', f)
    __dispy_job_reply.stdout = sys.stdout.getvalue()
    __dispy_job_reply.stderr = sys.stderr.getvalue()
    __dispy_job_reply.end_time = time.time()
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    __dispy_reply_Q.put(__dispy_job_reply)

class _DispyNode(object):
    """Internal use only.
    """
    def __init__(self, cpus, ip_addr=None, ext_ip_addr=None, node_port=None,
                 scheduler_node=None, scheduler_port=None,
                 dest_path_prefix='', secret='', keyfile=None, certfile=None,
                 max_file_size=None, zombie_interval=60):
        assert 0 < cpus <= multiprocessing.cpu_count()
        self.num_cpus = cpus
        if ip_addr:
            ip_addr = _node_ipaddr(ip_addr)
            if not ip_addr:
                raise Exception('invalid ip_addr')
        else:
            self.name = socket.gethostname()
            ip_addr = socket.gethostbyname(self.name)
        if ip_addr.startswith('127.'):
            logger.warning('node IP address %s seems to be loopback address; this will prevent '
                           'communication with clients on other machines. ' % ip_addr)
        if ext_ip_addr:
            ext_ip_addr = _node_ipaddr(ext_ip_addr)
            if not ext_ip_addr:
                raise Exception('invalid ext_ip_addr')
        else:
            ext_ip_addr = ip_addr
        try:
            self.name = socket.gethostbyaddr(ext_ip_addr)[0]
        except:
            self.name = socket.gethostname()
        if not node_port:
            node_port = 51348

        self.scheduler = {'ip':None, 'port':51347, 'auth':None}

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
            self.dest_path_prefix = os.path.join(os.sep, tempfile.gettempdir(), 'dispy')
        if not os.path.isdir(self.dest_path_prefix):
            os.makedirs(self.dest_path_prefix)
            os.chmod(self.dest_path_prefix, stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)
        if max_file_size is None:
            max_file_size = MaxFileSize
        self.max_file_size = max_file_size

        self.avail_cpus = self.num_cpus
        self.computations = {}
        self.file_uses = {}
        self.job_infos = {}
        self.thread_lock = threading.Lock()
        self.terminate = False
        self.signature = os.urandom(20).encode('hex')
        self.secret = secret
        self.auth_code = hashlib.sha1(self.signature + secret).hexdigest()
        self.zombie_interval = 60 * zombie_interval

        self.udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.udp_sock.bind(('', node_port))
        logger.info('serving %s cpus at %s:%s', self.num_cpus, self.ext_ip_addr, node_port)
        logger.debug('tcp server at %s:%s', self.address[0], self.address[1])
        self.udp_sock = AsyncSocket(self.udp_sock, blocking=False)

        self.reply_Q = multiprocessing.Queue()
        self.reply_Q_thread = threading.Thread(target=self.__reply_Q)
        self.reply_Q_thread.start()

        self.timer_coro = Coro(self.timer_task)
        # self.tcp_coro = Coro(self.tcp_server)
        if not scheduler_port:
            scheduler_port = 51347
        self.udp_coro = Coro(self.udp_server, _node_ipaddr(scheduler_node), scheduler_port)

    def broadcast_ping_msg(self, coro=None):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        sock = AsyncSocket(sock)
        sock.settimeout(2)
        ping_msg = {'ip_addr':self.ext_ip_addr, 'port':self.port, 'sign':self.signature,
                    'version':_dispy_version, 'scheduler_ip_addr':None}
        try:
            yield sock.sendto('PING:' + serialize(ping_msg), ('<broadcast>', self.scheduler['port']))
        except:
            logger.debug(traceback.format_exc())
            pass
        sock.close()

    def udp_server(self, scheduler_ip, scheduler_port, coro=None):
        coro.set_daemon()
        if self.avail_cpus == self.num_cpus:
            yield self.broadcast_ping_msg(coro=coro)
        ping_msg = {'ip_addr':self.ext_ip_addr, 'port':self.port, 'sign':self.signature,
                    'version':_dispy_version, 'scheduler_ip_addr':scheduler_ip}
        pong_msg = {'ip_addr':self.ext_ip_addr, 'port':self.port, 'sign':self.signature,
                    'version':_dispy_version, 'cpus':self.num_cpus, 'name':self.name}

        def send_ping_msg(self, addr, coro=None):
            sock = AsyncSocket(socket.socket(socket.AF_INET, socket.SOCK_DGRAM))
            sock.settimeout(2)
            try:
                yield sock.sendto('PING:' + serialize(ping_msg), addr)
            except:
                pass
            finally:
                sock.close()
            sock = AsyncSocket(socket.socket(socket.AF_INET, socket.SOCK_STREAM))
            sock.settimeout(2)
            try:
                yield sock.connect(addr)
                yield sock.send_msg('PING:' + serialize(ping_msg))
            except:
                pass
            finally:
                sock.close()

        def send_pong_msg(self, addr, auth_code, coro=None):
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock = AsyncSocket(sock, certfile=self.certfile, keyfile=self.keyfile)
            sock.settimeout(2)
            try:
                pong_msg['scheduler_ip_addr'] = addr[0]
                pong_msg['auth_code'] = auth_code
                yield sock.connect(addr)
                yield sock.send_msg('PONG:' + serialize(pong_msg))
            except:
                # logger.debug(traceback.format_exc())
                pass
            sock.close()

        if scheduler_ip:
            Coro(send_ping_msg, self, (scheduler_ip, scheduler_port))

        while True:
            msg, addr = yield self.udp_sock.recvfrom(1000)
            # TODO: process each message as separate Coro, so
            # exceptions are contained?
            if msg.startswith('PING:'):
                if self.num_cpus != self.avail_cpus:
                    logger.debug('Busy (%s/%s); ignoring ping message from %s:%s',
                                 self.num_cpus, self.avail_cpus, addr[0], addr[1])
                    continue
                try:
                    info = unserialize(msg[len('PING:'):])
                    assert info['version'] == _dispy_version
                    if info['ip_addr'] is None:
                        addr = (addr[0], info['port'])
                    else:
                        addr = (info['ip_addr'], info['port'])
                except:
                    logger.debug('Ignoring ping message from %s (%s)', addr[0], addr[1])
                    continue
                if info['sign']:
                    auth_code = hashlib.sha1(info['sign'] + self.secret).hexdigest()
                    Coro(send_pong_msg, self, addr, auth_code)
                else:
                    Coro(send_ping_msg, self, addr)
            elif msg.startswith('PULSE:'):
                try:
                    info = unserialize(msg[len('PULSE:'):])
                    assert info['ip_addr'] == self.scheduler['ip']
                    now = time.time()
                    for compute in self.computations.itervalues():
                        compute.last_pulse = now
                except:
                    logger.warning('Ignoring PULSE from %s', addr[0])
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
                if compute.scheduler_ip_addr != self.scheduler['ip'] or \
                       compute.scheduler_port != self.scheduler['port'] or \
                       compute.scheduler_auth != self.scheduler['auth']:
                    logger.debug('invalid scheduler IP address: scheduler %s:%s != %s:%s' % \
                                 compute.scheduler_ip_addr, compute.scheduler_port,
                                 self.scheduler['ip'], self.scheduler['port'])
                    compute = None
            if self.avail_cpus == 0:
                logger.warning('All cpus busy')
                try:
                    yield conn.send_msg('NAK (all cpus busy)')
                except:
                    pass
                raise StopIteration
            elif compute is None:
                logger.warning('Invalid computation %s', _job.compute_id)
                try:
                    yield conn.send_msg('NAK (invalid computation %s)' % _job.compute_id)
                except:
                    pass
                raise StopIteration

            reply_addr = (compute.scheduler_ip_addr, compute.job_result_port)
            logger.debug('New job id %s from %s/%s', _job.uid, addr[0], compute.scheduler_ip_addr)
            files = []
            for f in _job.files:
                tgt = os.path.join(compute.dest_path, os.path.basename(f['name']))
                try:
                    fd = open(tgt, 'wb')
                    fd.write(f['data'])
                    fd.close()
                except:
                    logger.warning('Could not save file "%s"', tgt)
                    continue
                try:
                    os.utime(tgt, (f['stat'].st_atime, f['stat'].st_mtime))
                    os.chmod(tgt, stat.S_IMODE(f['stat'].st_mode))
                except:
                    logger.debug('Could not set modes for "%s"', tgt)
                files.append(tgt)
            _job.files = files

            if compute.type == _Compute.func_type:
                reply = _JobReply(_job, self.ext_ip_addr)
                reply.start_time = time.time()
                job_info = _DispyJobInfo(reply, reply_addr, compute)
                args = (job_info, self.certfile, self.keyfile,
                        _job.args, _job.kwargs, self.reply_Q,
                        compute.name, compute.code, compute.dest_path, _job.files)
                try:
                    yield conn.send_msg('ACK')
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
                    yield conn.send_msg('ACK')
                except:
                    logger.warning('Failed to send response for new job to %s', str(addr))
                    raise StopIteration
                reply = _JobReply(_job, self.ext_ip_addr)
                reply.start_time = time.time()
                job_info = _DispyJobInfo(reply, reply_addr, compute)
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
                    yield conn.send_msg('NAK (invalid computation type "%s")' % compute.type)
                except:
                    logger.warning('Failed to send response for new job to %s', str(addr))

        def add_computation_task(msg):
            try:
                compute = unserialize(msg)
            except:
                logger.debug('Ignoring computation request from %s', addr[0])
                try:
                    yield conn.send_msg('Invalid computation request')
                except:
                    logger.warning('Failed to send reply to %s', str(addr))
                raise StopIteration
            if not ((self.scheduler['ip'] is None and self.scheduler['auth'] is None) or
                    (self.scheduler['ip'] == compute.scheduler_ip_addr and \
                     self.scheduler['port'] == compute.scheduler_port and \
                     self.scheduler['auth'] == compute.scheduler_auth)):
                logger.debug('Ignoring computation request from %s: %s, %s, %s',
                             compute.scheduler_ip_addr, self.scheduler['ip'],
                             self.avail_cpus, self.num_cpus)
                try:
                    yield conn.send_msg('Busy')
                except:
                    pass
                raise StopIteration

            resp = 'ACK'
            if compute.dest_path and isinstance(compute.dest_path, str):
                compute.dest_path = compute.dest_path.strip(os.sep)
            else:
                for x in range(20):
                    compute.dest_path = os.urandom(8).encode('hex')
                    if compute.dest_path.find(os.sep) >= 0:
                        continue
                    if not os.path.isdir(os.path.join(self.dest_path_prefix, compute.dest_path)):
                        break
                else:
                    logger.warning('Failed to create unique dest_path: %s', compute.dest_path)
                    resp = 'NAK'
            compute.dest_path = os.path.join(self.dest_path_prefix, compute.dest_path)
            logger.debug('dest_path for "%s": %s', compute.name, compute.dest_path)
            if not os.path.isdir(compute.dest_path):
                try:
                    os.makedirs(compute.dest_path)
                    os.chmod(compute.dest_path, stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)
                except:
                    try:
                        yield conn.send_msg('NAK (Invalid dest_path)')
                    except:
                        logger.warning('Failed to send reply to %s', str(addr))
                    raise StopIteration
            if compute.id in self.computations:
                logger.warning('Computation "%s" (%s) is being replaced',
                               compute.name, compute.id)
            setattr(compute, 'last_pulse', time.time())
            setattr(compute, 'pending_jobs', 0)
            setattr(compute, 'pending_results', 0)
            setattr(compute, 'zombie', False)
            # logger.debug('xfer_files given: %s', ','.join(xf.name for xf in compute.xfer_files))
            if compute.type == _Compute.func_type:
                try:
                    code = compile(compute.code, '<string>', 'exec')
                except:
                    logger.warning('Computation "%s" could not be compiled', compute.name)
                    if os.path.isdir(compute.dest_path):
                        os.rmdir(compute.dest_path)
                    try:
                        yield conn.send_msg('NAK (Compilation failed)')
                    except:
                        logger.warning('Failed to send reply to %s', str(addr))
                    raise StopIteration
                compute.code = marshal.dumps(code)
            elif compute.type == _Compute.prog_type:
                assert not compute.code
                compute.name = os.path.join(compute.dest_path, os.path.basename(compute.name))

            xfer_files = []
            for xf in compute.xfer_files:
                tgt = os.path.join(compute.dest_path, os.path.basename(xf.name))
                try:
                    if _same_file(tgt, xf):
                        logger.debug('Ignoring file "%s" / "%s"', xf.name, tgt)
                        if tgt not in self.file_uses:
                            self.file_uses[tgt] = 0
                        self.file_uses[tgt] += 1
                        continue
                except:
                    pass
                if self.max_file_size and xf.stat_buf.st_size > self.max_file_size:
                    resp = 'NAK (file "%s" too big)' % xf.name
                else:
                    xfer_files.append(xf)
            if resp == 'ACK' and \
                   not ((self.scheduler['ip'] == None) or \
                        (self.scheduler['ip'] == compute.scheduler_ip_addr and \
                         self.scheduler['port'] == compute.scheduler_port and \
                         self.scheduler['auth'] == compute.scheduler_auth)):
                resp = 'NAK (busy)'
            if resp == 'ACK':
                self.computations[compute.id] = compute
                self.scheduler['ip'] = compute.scheduler_ip_addr
                self.scheduler['port'] = compute.scheduler_port
                self.scheduler['auth'] = compute.scheduler_auth
                self.pulse_interval = compute.pulse_interval
                if self.zombie_interval:
                    if self.pulse_interval:
                        self.pulse_interval = min(self.pulse_interval, self.zombie_interval / 5.0)
                    else:
                        self.pulse_interval = self.zombie_interval / 5.0
                if xfer_files:
                    resp += ':XFER_FILES:' + serialize(xfer_files)
                try:
                    yield conn.send_msg(resp)
                except:
                    del self.computations[compute.id]
                    self.scheduler['ip'] = None
                    self.scheduler['auth'] = None
                    self.pulse_interval = None
                else:
                    self.timer_coro.resume(True)
            else:
                if os.path.isdir(compute.dest_path):
                    os.rmdir(compute.dest_path)
                try:
                    yield conn.send_msg(resp)
                except:
                    pass

        def xfer_file_task(msg):
            try:
                xf = unserialize(msg)
            except:
                logger.debug('Ignoring file trasnfer request from %s', addr[0])
                raise StopIteration
            resp = ''
            if xf.compute_id not in self.computations:
                logger.error('computation "%s" is invalid' % xf.compute_id)
                raise StopIteration
            tgt = os.path.join(self.computations[xf.compute_id].dest_path,
                               os.path.basename(xf.name))
            if os.path.isfile(tgt):
                if _same_file(tgt, xf):
                    if tgt in self.file_uses:
                        self.file_uses[tgt] += 1
                    else:
                        self.file_uses[tgt] = 1
                    resp = 'ACK'
                else:
                    logger.warning('File "%s" already exists with different status as "%s"',
                                   xf.name, tgt)
            if not resp:
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
                        if self.max_file_size and n > self.max_file_size:
                            logger.warning('File "%s" is too big (%s); it is truncated', tgt, n)
                            break
                    fd.close()
                    if n < xf.stat_buf.st_size:
                        resp = 'NAK (read only %s bytes)' % n
                    else:
                        resp = 'ACK'
                        logger.debug('Copied file %s, %s', tgt, resp)
                        os.utime(tgt, (xf.stat_buf.st_atime, xf.stat_buf.st_mtime))
                        os.chmod(tgt, stat.S_IMODE(xf.stat_buf.st_mode))
                        self.file_uses[tgt] = 1
                except:
                    logger.warning('Copying file "%s" failed with "%s"',
                                   xf.name, traceback.format_exc())
                    resp = 'NAK'
                try:
                    yield conn.send_msg(resp)
                except:
                    logger.debug('Could not send reply for "%s"', xf.name)
            raise StopIteration # xfer_file_task

        def setup_computation(msg):
            try:
                compute_id = unserialize(msg)
                compute = self.computations[compute_id]
                assert isinstance(compute.setup, str)
                os.chdir(compute.dest_path)
                exec(marshal.loads(compute.code)) in globals(), locals()
                _dispy_setup_func = locals()[compute.setup]
                assert _dispy_setup_func() == 0
            except:
                logger.debug('setup "%s" failed' % compute.setup)
                logger.debug(traceback.format_exc())
                resp = 'NAK (setup failed)'
            else:
                resp = 'ACK'
            if resp != 'ACK':
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
                job_info = self.job_infos.pop(_job.uid, None)
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
            reply_addr = (compute.scheduler_ip_addr, compute.job_result_port)
            job_info.job_reply.end_time = time.time()
            job_info.job_reply.status = DispyJob.Terminated
            yield self._send_job_reply(job_info, resending=False, coro=coro)

        def ping_job_task(conn, msg, addr):
            # if self.num_cpus == self.avail_cpus:
            #     raise StopIteration
            # logger.debug('tcp ping from %s:%s' % addr)
            if self.num_cpus != self.avail_cpus or self.scheduler['ip'] != None:
                logger.debug('Busy (%s/%s); ignoring ping message from %s:%s',
                             self.num_cpus, self.avail_cpus, addr[0], addr[1])
                raise StopIteration
            pong_msg = {'ip_addr':self.ext_ip_addr, 'port':self.port, 'name':self.name,
                        'cpus':self.num_cpus, 'sign':self.signature}
            try:
                info = unserialize(msg)
                assert info['version'] == _dispy_version
                if info['ip_addr']:
                    pong_msg['scheduler_ip_addr'] = info['ip_addr']
                else:
                    pong_msg['scheduler_ip_addr'] = addr[0]
                pong_msg['auth_code'] = hashlib.sha1(info['sign'] + self.secret).hexdigest()
            except:
                # logger.debug(traceback.format_exc())
                raise StopIteration

            sock = AsyncSocket(socket.socket(socket.AF_INET, socket.SOCK_STREAM))
            sock.settimeout(5)
            try:
                yield sock.connect((pong_msg['scheduler_ip_addr'], info['port']))
                yield sock.send_msg('PONG:' + serialize(pong_msg))
            except:
                # logger.debug(traceback.format_exc())
                pass
            finally:
                sock.close()

        def poll_job_task(conn):
            done = []
            for uid, job_info in self.job_infos.iteritems():
                if job_info.job_reply.status in (DispyJob.Finished, DispyJob.Terminated):
                    done.append(uid)
            if done:
                yield conn.send_msg('done:' + serialize(done))

        def retrieve_job_task(msg):
            try:
                req = unserialize(msg)
                assert req['uid'] is not None
                assert req['hash'] is not None
                assert req['compute_id'] is not None
            except:
                # logger.debug(traceback.format_exc())
                resp = serialize('Invalid job')
                try:
                    yield conn.send_msg(resp)
                except:
                    pass
                raise StopIteration

            self.thread_lock.acquire()
            job_info = self.job_infos.get(req['uid'], None)
            self.thread_lock.release()
            resp = None
            if job_info is not None:
                try:
                    yield conn.send_msg(serialize(job_info.job_reply))
                    ack = yield conn.recv_msg()
                    assert ack == 'ACK'
                    self.thread_lock.acquire()
                    self.job_infos.pop(req['uid'], None)
                    self.thread_lock.release()
                    info_file = os.path.join(self.dest_path_prefix, job_info.compute_dest_path,
                                             '_dispy_job_reply_%s' % req['uid'])
                    if os.path.isfile(info_file):
                        try:
                            os.remove(info_file)
                        except:
                            # logger.debug(traceback.format_exc())
                            logger.debug('Could not remove "%s"', info_file)
                except:
                    # logger.debug(traceback.format_exc())
                    logger.debug('Could not send reply for job %s', req['uid'])
                raise StopIteration

            for d in os.listdir(self.dest_path_prefix):
                info_file = os.path.join(self.dest_path_prefix, d,
                                         '_dispy_job_reply_%s' % req['uid'])
                if os.path.isfile(info_file):
                    try:
                        fd = open(info_file, 'rb')
                        job_reply = pickle.load(fd)
                        fd.close()
                    except:
                        job_reply = None
                    if hasattr(job_reply, 'hash') and job_reply.hash == req['hash']:
                        try:
                            yield conn.send_msg(serialize(job_reply))
                            ack = yield conn.recv_msg()
                            assert ack == 'ACK'
                            self.thread_lock.acquire()
                            self.job_infos.pop(req['uid'], None)
                            self.thread_lock.release()
                        except:
                            logger.debug('Could not send reply for job %s', req['uid'])
                            raise StopIteration
                        try:
                            os.remove(info_file)
                            compute = self.computations.get(req['compute_id'], None)
                            if compute is not None:
                                compute.pending_results -= 1
                                if compute.pending_results == 0:
                                    compute.zombie = True
                                    self.cleanup_computation(compute)
                        except:
                            logger.debug('Could not remove "%s"', info_file)
                        raise StopIteration
            else:
                resp = serialize('Invalid job: %s' % req['uid'])

            if resp:
                try:
                    yield conn.send_msg(resp)
                except:
                    pass

        # tcp_serve_task starts
        try:
            req = yield conn.recvall(len(self.auth_code))
        except:
            logger.warning('Ignoring request; invalid client authentication?')
            conn.close()
            raise StopIteration
        if req != self.auth_code:
            msg = yield conn.recv_msg()
            if msg.startswith('PING:'):
                yield ping_job_task(conn, msg[len('PING:'):], addr)
            elif msg.startswith('RECOVER:'):
                try:
                    req = unserialize(msg[len('RECOVER:'):])
                    if req['version'] == _dispy_version:
                        reply = serialize({'ip_addr':self.ext_ip_addr, 'port':self.port,
                                           'sign':self.signature})
                        yield conn.send_msg(reply)
                except:
                    # logger.debug(traceback.format_exc())
                    pass
            else:
                logger.warning('Ignoring request; invalid client authentication?')
            conn.close()
            raise StopIteration
        msg = yield conn.recv_msg()
        if not msg:
            conn.close()
            raise StopIteration
        if msg.startswith('JOB:'):
            msg = msg[len('JOB:'):]
            yield job_request_task(msg)
            conn.close()
        elif msg.startswith('COMPUTE:'):
            msg = msg[len('COMPUTE:'):]
            yield add_computation_task(msg)
            conn.close()
        elif msg.startswith('POLL_JOB:'):
            yield poll_job_task(conn)
            conn.close()
        elif msg.startswith('FILEXFER:'):
            msg = msg[len('FILEXFER:'):]
            yield xfer_file_task(msg)
            conn.close()
        elif msg.startswith('SETUP:'):
            msg = msg[len('SETUP:'):]
            yield setup_computation(msg)
            conn.close()
        elif msg.startswith('DEL_COMPUTE:'):
            msg = msg[len('DEL_COMPUTE:'):]
            try:
                info = unserialize(msg)
                compute_id = info['ID']
                compute = self.computations.get(compute_id, None)
                if compute is None:
                    logger.warning('Computation "%s" is not valid', compute_id)
                else:
                    compute.zombie = True
                    self.cleanup_computation(compute)
            except:
                logger.debug('Deleting computation failed with %s', traceback.format_exc())
                # raise
            conn.close()
        elif msg.startswith('TERMINATE_JOB:'):
            msg = msg[len('TERMINATE_JOB:'):]
            yield terminate_job_task(msg)
            conn.close()
        elif msg.startswith('RETRIEVE_JOB:'):
            msg = msg[len('RETRIEVE_JOB:'):]
            yield retrieve_job_task(msg)
            conn.close()
        elif msg.startswith('PONG:'):
            try:
                info = unserialize(msg[len('PONG:'):])
                pong_msg = {'ip_addr':self.ext_ip_addr, 'port':self.port, 'sign':self.signature,
                            'version':_dispy_version, 'name':self.name, 'cpus':self.num_cpus}
                pong_msg['auth_code'] = hashlib.sha1(info['sign'] + self.secret).hexdigest()
                if info['ip_addr']:
                    pong_msg['scheduler_ip_addr'] = info['ip_addr']
                else:
                    pong_msg['scheduler_ip_addr'] = addr[0]
                yield conn.send_msg(serialize(pong_msg))
            except:
                # logger.debug(traceback.format_exc())
                pass
            conn.close()
        else:
            logger.warning('Invalid request "%s" from %s',
                           msg[:min(10, len(msg))], addr[0])
            resp = 'NAK (invalid command: %s)' % (msg[:min(10, len(msg))])
            try:
                yield conn.send_msg(resp)
            except:
                logger.warning('Failed to send reply to %s', str(addr))
            conn.close()

    def timer_task(self, coro=None):
        coro.set_daemon()
        last_pulse_time = last_zombie_time = time.time()
        while True:
            reset = yield coro.suspend(self.pulse_interval)
            if reset:
                continue

            now = time.time()
            if self.pulse_interval and (now - last_pulse_time) >= self.pulse_interval:
                n = self.num_cpus - self.avail_cpus
                assert n >= 0
                if n > 0 and self.scheduler['ip']:
                    last_pulse_time = now
                    msg = 'PULSE:' + serialize({'ip_addr':self.ext_ip_addr, 'port':self.port,
                                                'cpus':n, 'scheduler_ip_addr':self.scheduler['ip']})
                    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                    sock = AsyncSocket(sock, blocking=False)
                    sock.settimeout(1)
                    yield sock.sendto(msg, (self.scheduler['ip'], self.scheduler['port']))
                    sock.close()

                resend = [compute for compute in self.computations.itervalues() \
                          if not compute.zombie and compute.pending_results and \
                          ((now - compute.last_pulse) < (10 * self.pulse_interval))]
                for compute in resend:
                    files = [f for f in os.listdir(compute.dest_path) \
                             if f.startswith('_dispy_job_reply_')]
                    # limit number queued so as not to take up too much time
                    files = files[:min(len(files), 64)]
                    for f in files:
                        result_file = os.path.join(compute.dest_path, f)
                        try:
                            fd = open(result_file, 'rb')
                            job_result = pickle.load(fd)
                            fd.close()
                        except:
                            logger.debug('Could not load "%s"', result_file)
                            # logger.debug(traceback.format_exc())
                            continue
                        try:
                            os.remove(result_file)
                        except:
                            logger.debug('Could not remove "%s"', result_file)
                        compute.pending_results -= 1
                        job_info = _DispyJobInfo(job_result, (compute.scheduler_ip_addr,
                                                              compute.job_result_port), compute)
                        Coro(self._send_job_reply, job_info, resending=True)

            if self.zombie_interval and (now - last_zombie_time) >= self.zombie_interval:
                last_zombie_time = now
                for compute in self.computations.itervalues():
                    if (now - compute.last_pulse) > self.zombie_interval:
                        logger.warning('computation "%s" is marked as zombie' % compute.name)
                        compute.zombie = True
                zombies = [compute for compute in self.computations.itervalues() \
                           if compute.zombie and compute.pending_jobs == 0]
                for compute in zombies:
                    logger.warning('Deleting zombie computation "%s"', compute.name)
                    self.cleanup_computation(compute)
                for compute in zombies:
                    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                    sock = AsyncSocket(sock, blocking=False)
                    sock.settimeout(1)
                    logger.debug('Sending TERMINATE to %s', compute.scheduler_ip_addr)
                    data = serialize({'ip_addr':self.ext_ip_addr, 'port':self.port,
                                      'sign':self.signature})
                    yield sock.sendto('TERMINATED:%s' % data, (compute.scheduler_ip_addr,
                                                               compute.scheduler_port))
                    sock.close()
                if self.scheduler['ip'] is None and self.avail_cpus == self.num_cpus:
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
            self.thread_lock.release()
            if job_info is not None:
                if job_info.proc is not None:
                    if isinstance(job_info.proc, multiprocessing.Process):
                        job_info.proc.join(2)
                    else:
                        job_info.proc.wait()
                job_info.job_reply = job_reply
                Coro(self._send_job_reply, job_info, resending=False)

    def _send_job_reply(self, job_info, resending=False, coro=None):
        """Internal use only.
        """
        job_reply = job_info.job_reply
        logger.debug('Sending result for job %s (%s) to %s',
                     job_reply.uid, job_reply.status, str(job_info.reply_addr))
        if not resending:
            self.avail_cpus += 1
            assert self.avail_cpus <= self.num_cpus
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock = AsyncSocket(sock, blocking=False, certfile=self.certfile, keyfile=self.keyfile)
        sock.settimeout(5)
        try:
            yield sock.connect(job_info.reply_addr)
            yield sock.send_msg(serialize(job_reply))
            ack = yield sock.recv_msg()
            assert ack == 'ACK'
            self.thread_lock.acquire()
            self.job_infos.pop(job_reply.uid, None)
            self.thread_lock.release()
            compute = self.computations.get(job_info.compute_id, None)
            if compute is not None:
                compute.last_pulse = time.time()
        except:
            if job_reply.uid in self.job_infos:
                logger.error("Couldn't send results for %s to %s : %s",
                             job_reply.uid, str(job_info.reply_addr), traceback.format_exc())
                # store job result even if computation has not enabled
                # fault recovery; user may be able to access node and
                # retrieve result manually
                f = os.path.join(job_info.compute_dest_path, '_dispy_job_reply_%s' % job_reply.uid)
                logger.debug('storing results for job %s', job_reply.uid)
                try:
                    fd = open(f, 'wb')
                    pickle.dump(job_reply, fd)
                    fd.close()
                except:
                    logger.debug('Could not save results for job %s', job_reply.uid)
                else:
                    compute = self.computations.get(job_info.compute_id, None)
                    if compute is not None:
                        compute.pending_results += 1
        finally:
            sock.close()
            if not resending:
                compute = self.computations.get(job_info.compute_id, None)
                if compute is not None:
                    compute.pending_jobs -= 1
                    if compute.pending_jobs == 0 and compute.zombie:
                        self.cleanup_computation(compute)

    def cleanup_computation(self, compute):
        if not compute.zombie:
            return
        if compute.pending_jobs != 0:
            logger.debug('pending jobs for computation "%s"/%s: %s',
                         compute.name, compute.id, compute.pending_jobs)
            if compute.pending_jobs > 0:
                return

        del self.computations[compute.id]
        if compute.scheduler_ip_addr == self.scheduler['ip'] and \
           compute.scheduler_port == self.scheduler['port'] and \
           compute.scheduler_auth == self.scheduler['auth'] and \
               all(c.scheduler_ip_addr != self.scheduler['ip'] \
                   for c in self.computations.itervalues()):
            assert self.avail_cpus == self.num_cpus
            self.scheduler['ip'] = None
            self.scheduler['auth'] = None
            self.pulse_interval = None

        if self.scheduler['ip'] is None and self.avail_cpus == self.num_cpus:
            self.timer_coro.resume(None)
            Coro(self.broadcast_ping_msg)
        if not compute.cleanup:
            return
        os.chdir(self.dest_path_prefix)
        if isinstance(compute.cleanup, str):
            try:
                exec(marshal.loads(compute.code)) in globals(), locals()
                _dispy_cleanup_func = locals()[compute.cleanup]
                _dispy_cleanup_func()
            except:
                logger.debug('cleanup "%s" failed' % compute.cleanup)
                logger.debug(traceback.format_exc())

        for xf in compute.xfer_files:
            tgt = os.path.join(compute.dest_path, os.path.basename(xf.name))
            if tgt not in self.file_uses:
                logger.debug('File "%s" is unknown', tgt)
                continue
            self.file_uses[tgt] -= 1
            if self.file_uses[tgt] == 0:
                del self.file_uses[tgt]
                if tgt == xf:
                    logger.debug('Not removing file "%s"', xf.name)
                else:
                    logger.debug('Removing file "%s"', tgt)
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
            computations = self.computations.items()
            self.computations = {}
            if self.reply_Q:
                self.reply_Q.put(None)
            self.thread_lock.release()
            for uid, job_info in job_infos.iteritems():
                job_info.proc.terminate()
                logger.debug('process for %s is killed', uid)
                if isinstance(job_info.proc, multiprocessing.Process):
                    job_info.proc.join(2)
                else:
                    job_info.proc.wait()
            for cid, compute in computations:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock = AsyncSocket(sock, blocking=False)
                sock.settimeout(2)
                logger.debug('Sending TERMINATE to %s', compute.scheduler_ip_addr)
                info = {'ip_addr':self.ext_ip_addr, 'port':self.port, 'sign':self.signature}
                try:
                    yield sock.connect((compute.scheduler_ip_addr, compute.scheduler_port))
                    yield sock.send_msg('TERMINATED:' + serialize(info))
                except:
                    pass
                sock.close()

        if hasattr(self, 'job_infos'):
            Coro(_shutdown, self).value()
            # self.asyncoro.join()
            self.asyncoro.finish()

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--cpus', dest='cpus', type=int, default=0,
                        help='number of cpus used by dispy; if negative, that many cpus are not used')
    parser.add_argument('-d', '--debug', action='store_true', dest='loglevel', default=False,
                        help='if given, debug messages are printed')
    parser.add_argument('-i', '--ip_addr', dest='ip_addr', default=None,
                        help='IP address to use (may be needed in case of multiple interfaces)')
    parser.add_argument('--ext_ip_addr', dest='ext_ip_addr', default=None,
                        help='External IP address to use (needed in case of NAT firewall/gateway)')
    parser.add_argument('-p', '--node_port', dest='node_port', type=int, default=51348,
                        help='port number to use')
    parser.add_argument('--dest_path_prefix', dest='dest_path_prefix', default=None,
                        help='path prefix where files sent by dispy are stored')
    parser.add_argument('--scheduler_node', dest='scheduler_node', default=None,
                        help='name or IP address of scheduler to announce when starting')
    parser.add_argument('--scheduler_port', dest='scheduler_port', type=int, default=51347,
                        help='port number used by scheduler')
    parser.add_argument('--max_file_size', dest='max_file_size', default=None, type=int,
                        help='maximum file size of any file transferred')
    parser.add_argument('--zombie_interval', dest='zombie_interval', default=60, type=float,
                        help='interval in minutes to presume unresponsive scheduler is zombie')
    parser.add_argument('-s', '--secret', dest='secret', default='',
                        help='authentication secret for handshake with dispy clients')
    parser.add_argument('--certfile', dest='certfile', default=None,
                        help='file containing SSL certificate')
    parser.add_argument('--keyfile', dest='keyfile', default=None,
                        help='file containing SSL key')
    config = vars(parser.parse_args(sys.argv[1:]))

    if config['loglevel']:
        logger.setLevel(logging.DEBUG)
        asyncoro.logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
    del config['loglevel']

    cpus = multiprocessing.cpu_count()
    if config['cpus']:
        if config['cpus'] > 0:
            if config['cpus'] > cpus:
                raise Exception('CPU count must be <= %s' % cpus)
        else:
            if config['cpus'] <= -cpus:
                raise Exception('CPU count must be > -%s' % cpus)
            cpus += config['cpus']
            config['cpus'] = cpus
    else:
        config['cpus'] = cpus

    if config['zombie_interval']:
        config['zombie_interval'] = float(config['zombie_interval'])
        if config['zombie_interval'] < 1:
            raise Exception('zombie_interval must be at least 1')

    node = _DispyNode(**config)

    while True:
        try:
            conn, addr = node.tcp_sock.accept()
        except KeyboardInterrupt:
            # TODO: terminate even if jobs are scheduled?
            logger.info('Interrupted; terminating')
            node.shutdown()
            break
        except:
            logger.debug(traceback.format_exc())
            continue
        conn = AsyncSocket(conn, blocking=False, keyfile=node.keyfile, certfile=node.certfile)
        Coro(node.tcp_serve_task, conn, addr)
