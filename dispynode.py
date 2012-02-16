#!/usr/bin/env python

# dispynode: Execute computations on behalf of dispy clients;
# see accompanying 'dispy' for more details.

# Copyright (C) 2011 Giridhar Pemmasani (pgiri@yahoo.com)

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

import os
import sys
import time
import stat
import socket
import multiprocessing
import threading
import cPickle
import subprocess
import signal
import cStringIO
import traceback
import base64
import hashlib
import atexit
import logging
import marshal
import shelve

from dispy import _DispyJob_, _JobReply, DispyJob, \
     _Compute, _XferFile, _xor_string, _node_name_ipaddr, _dispy_version

from asyncoro import Coro, CoroLock, AsynCoro, AsynCoroSocket, MetaSingleton

MaxFileSize = 102400000

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
    logging.debug('Sending provisional result for job %s to %s',
                  __dispy_job_reply.uid, __dispy_job_info.reply_addr)
    __dispy_job_reply.status = DispyJob.ProvisionalResult
    __dispy_job_reply.result = result
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock = AsynCoroSocket(sock, blocking=True, timeout=2,
                          certfile=__dispy_job_certfile, keyfile=__dispy_job_keyfile)
    try:
        sock.connect(__dispy_job_info.reply_addr)
        sock.write_msg(__dispy_job_reply.uid, cPickle.dumps(__dispy_job_reply))
    except:
        logging.warning("Couldn't send provisional results %s:\n%s",
                        str(result), traceback.format_exc())
    finally:
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
                    __dispy_job_env, __dispy_job_name, __dispy_job_code,
                    __dispy_path, __dispy_job_files=[]):
    """Internal use only.
    """
    os.chdir(__dispy_path)
    sys.stdout = cStringIO.StringIO()
    sys.stderr = cStringIO.StringIO()
    __dispy_job_reply = __dispy_job_info.job_reply
    if __dispy_job_env and isinstance(__dispy_job_env, list):
        sys.path = __dispy_job_env + sys.path
    sys.path = [__dispy_path] + sys.path
    try:
        exec marshal.loads(__dispy_job_code)
        globals().update(locals())
        __dispy_job_args = cPickle.loads(__dispy_job_args)
        __dispy_job_kwargs = cPickle.loads(__dispy_job_kwargs)
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
                logging.debug('Could not remove "%s"', f)
    __dispy_job_reply.stdout = sys.stdout.getvalue()
    __dispy_job_reply.stderr = sys.stderr.getvalue()
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    __dispy_reply_Q.put(__dispy_job_reply)

class _DispyNode(object):
    """Internal use only.
    """
    def __init__(self, cpus, ip_addr='', node_port=51348, dest_path_prefix='',
                 scheduler_node=None, scheduler_port=51347,
                 secret='', keyfile=None, certfile=None, max_file_size=None,
                 zombie_interval=60):
        self.cpus = cpus
        self.fqdn = socket.getfqdn()
        if ip_addr:
            ip_addr = _node_name_ipaddr(ip_addr)[1]
        else:
            ip_addr = socket.gethostbyname(socket.gethostname())
        self.ip_addr = ip_addr
        self.scheduler_port = scheduler_port
        self.pulse_interval = None

        self.asyncoro = AsynCoro()

        self.tcp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.tcp_sock.bind((ip_addr, 0))
        self.address = self.tcp_sock.getsockname()
        self.tcp_sock.listen(30)
        self.tcp_sock = AsynCoroSocket(self.tcp_sock, blocking=False, timeout=False)

        if dest_path_prefix:
            self.dest_path_prefix = dest_path_prefix.strip().rstrip(os.sep)
        else:
            self.dest_path_prefix = os.path.join(os.sep, 'tmp', 'dispy')
        if not os.path.isdir(self.dest_path_prefix):
            os.makedirs(self.dest_path_prefix)
            os.chmod(self.dest_path_prefix, stat.S_IWUSR | stat.S_IXUSR)
        if max_file_size is None:
            max_file_size = MaxFileSize
        self.max_file_size = max_file_size

        self.avail_cpus = self.cpus
        self.computations = {}
        self.scheduler_ip_addr = None
        self.file_uses = {}
        self.job_infos = {}
        self.lock = CoroLock()
        self.terminate = False
        self.signature = os.urandom(20).encode('hex')
        self.auth_code = hashlib.sha1(_xor_string(self.signature, secret)).hexdigest()
        self.keyfile = keyfile
        self.certfile = certfile
        self.zombie_interval = 60 * zombie_interval

        logging.debug('auth_code for %s: %s', ip_addr, self.auth_code)
        logging.info('Serving %s cpus at %s:%s',
                     self.cpus, self.address[0], self.address[1])

        self.udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.udp_sock.bind(('', node_port))
        logging.info('node running at %s:%s', self.address[0], node_port)
        self.udp_sock = AsynCoroSocket(self.udp_sock, blocking=False, timeout=False)

        scheduler_ip_addr = _node_name_ipaddr(scheduler_node)[1]

        self.reply_Q = multiprocessing.Queue()
        self.reply_Q_thread = threading.Thread(target=self.__reply_Q)
        self.reply_Q_thread.start()

        self.timer_coro = Coro(self.timer_task)
        self.tcp_coro = Coro(self.tcp_server)
        self.udp_coro = Coro(self.udp_server, scheduler_ip_addr)

    def send_pong_msg(self, coro=None):
        ping_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        ping_sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        ping_sock = AsynCoroSocket(ping_sock, blocking=False)
        pong_msg = {'ip_addr':self.ip_addr, 'fqdn':self.fqdn, 'port':self.address[1],
                    'cpus':self.cpus, 'sign':self.signature, 'version':_dispy_version}
        pong_msg = 'PONG:' + cPickle.dumps(pong_msg)
        yield ping_sock.sendto(pong_msg, ('<broadcast>', self.scheduler_port))
        ping_sock.close()

    def udp_server(self, scheduler_ip_addr, coro=None):
        assert coro is not None
        
        if self.avail_cpus == self.cpus:
            yield self.send_pong_msg(coro=coro)
        pong_msg = {'ip_addr':self.ip_addr, 'fqdn':self.fqdn, 'port':self.address[1],
                    'cpus':self.cpus, 'sign':self.signature, 'version':_dispy_version}
        pong_msg = 'PONG:' + cPickle.dumps(pong_msg)

        if scheduler_ip_addr:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            try:
                sock.sendto(pong_msg, (scheduler_ip_addr, self.scheduler_port))
            except:
                logging.warning("Couldn't send ping message to %s:%s",
                                scheduler_ip_addr, self.scheduler_port)
            finally:
                sock.close()

        while True:
            msg, addr = yield self.udp_sock.recvfrom(1024)
            # TODO: process each message as separate Coro, so
            # exceptions are contained?
            if msg.startswith('PING:'):
                if self.cpus != self.avail_cpus:
                    logging.debug('Busy (%s/%s); ignoring ping message from %s',
                                  self.cpus, self.avail_cpus, addr[0])
                    continue
                try:
                    info = cPickle.loads(msg[len('PING:'):])
                    socket.inet_aton(info['scheduler_ip_addr'])
                    assert isinstance(info['scheduler_port'], int)
                    assert info['version'] == _dispy_version
                    addr = (info['scheduler_ip_addr'], info['scheduler_port'])
                except:
                    # raise
                    logging.debug('Ignoring ping message from %s (%s)',
                                  addr[0], addr[1])
                    continue
                yield self.udp_sock.sendto(pong_msg, addr)
            elif msg.startswith('PULSE:'):
                try:
                    info = cPickle.loads(msg[len('PULSE:'):])
                    assert info['ip_addr'] == self.scheduler_ip_addr
                    self.lock.acquire()
                    for compute in self.computations.itervalues():
                        compute.last_pulse = time.time()
                    self.lock.release()
                except:
                    logging.warning('Ignoring PULSE from %s', addr[0])
            elif msg.startswith('SERVERPORT:'):
                try:
                    req = cPickle.loads(msg[len('SERVERPORT:'):])
                    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                    reply = {'ip_addr':self.address[0], 'port':self.address[1],
                             'sign':self.signature, 'version':_dispy_version}
                    sock = AsynCoroSocket(sock, blocking=False)
                    yield sock.sendto(cPickle.dumps(reply), (req['ip_addr'], req['port']))
                    sock.close()
                except:
                    logging.debug(traceback.format_exc())
                    # pass
            else:
                logging.warning('Ignoring ping message from %s', addr[0])

    def tcp_server(self, coro=None):
        while True:
            conn, addr = yield self.tcp_sock.accept()
            logging.debug('new tcp request from %s', str(addr))
            Coro(self.tcp_serve_task, conn, addr)

    def tcp_serve_task(self, conn, addr, coro=None):
        def job_request_task(conn, uid, msg, addr, coro):
            assert coro is not None
            try:
                _job = cPickle.loads(msg)
            except:
                logging.debug('Ignoring job request from %s', addr[0])
                logging.debug(traceback.format_exc())
                raise StopIteration
            self.lock.acquire()
            compute = self.computations.get(_job.compute_id, None)
            if compute is not None and compute.scheduler_ip_addr != self.scheduler_ip_addr:
                compute = None
            elif addr[0] != compute.scheduler_ip_addr:
                compute = None
            self.lock.release()
            _job.uid = uid
            if self.avail_cpus == 0:
                logging.warning('All cpus busy')
                resp = 'NAK (all cpus busy)'
                try:
                    yield conn.write_msg(_job.uid, cPickle.dumps(resp))
                except:
                    pass
                raise StopIteration
            elif compute is None:
                logging.warning('Invalid computation %s', _job.compute_id)
                resp = 'NAK (invalid computation %s)' % _job.compute_id
                try:
                    yield conn.write_msg(_job.uid, cPickle.dumps(resp))
                except:
                    pass
                raise StopIteration

            reply_addr = (addr[0], self.computations[_job.compute_id].job_result_port)
            logging.debug('New job id %s from %s', _job.uid, addr[0])
            files = []
            for f in _job.files:
                tgt = os.path.join(self.computations[compute.id].dest_path,
                                   os.path.basename(f['name']))
                try:
                    fd = open(tgt, 'wb')
                    fd.write(f['data'])
                    fd.close()
                except:
                    logging.warning('Could not save file "%s"', tgt)
                    continue
                try:
                    os.utime(tgt, (f['stat'].st_atime, f['stat'].st_mtime))
                    os.chmod(tgt, stat.S_IMODE(f['stat'].st_mode))
                except:
                    logging.debug('Could not set modes for "%s"', tgt)
                files.append(tgt)
            _job.files = files

            if compute.type == _Compute.func_type:
                reply = _JobReply(_job, self.ip_addr)
                job_info = _DispyJobInfo(reply, reply_addr, compute)
                args = (job_info, self.certfile, self.keyfile,
                        _job.args, _job.kwargs, self.reply_Q,
                        compute.env, compute.name, compute.code, compute.dest_path, _job.files)
                try:
                    yield conn.write_msg(_job.uid, cPickle.dumps(_job.uid))
                except:
                    logging.warning('Failed to send response for new job to %s',
                                    str(addr))
                    raise StopIteration
                job_info.job_reply.status = DispyJob.Running
                job_info.proc = multiprocessing.Process(target=_dispy_job_func, args=args)
                self.lock.acquire()
                self.avail_cpus -= 1
                compute.pending_jobs += 1
                self.job_infos[_job.uid] = job_info
                self.lock.release()
                job_info.proc.start()
                raise StopIteration
            elif compute.type == _Compute.prog_type:
                try:
                    yield conn.write_msg(_job.uid, cPickle.dumps(_job.uid))
                except:
                    logging.warning('Failed to send response for new job to %s',
                                    str(addr))
                    raise StopIteration
                reply = _JobReply(_job, self.ip_addr)
                job_info = _DispyJobInfo(reply, reply_addr, compute)
                job_info.job_reply.status = DispyJob.Running
                self.lock.acquire()
                self.job_infos[_job.uid] = job_info
                self.avail_cpus -= 1
                compute.pending_jobs += 1
                self.lock.release()
                prog_thread = threading.Thread(target=self.__job_program, args=(_job, job_info))
                prog_thread.start()
                raise StopIteration
            else:
                resp = 'NAK (invalid computation type "%s")' % compute.type
                try:
                    yield conn.write_msg(_job.uid, cPickle.dumps(resp))
                except:
                    logging.warning('Failed to send response for new job to %s',
                                    str(addr))

        def add_computation_task(conn, uid, msg, addr, coro):
            assert coro is not None
            try:
                compute = cPickle.loads(msg)
            except:
                logging.debug('Ignoring computation request from %s', addr[0])
                resp = 'Invalid computation request'
                try:
                    yield conn.write_msg(uid, resp)
                except:
                    logging.warning('Failed to send reply to %s', str(addr))
                raise StopIteration
            self.lock.acquire()
            if not ((self.scheduler_ip_addr is None) or
                    (self.scheduler_ip_addr == compute.scheduler_ip_addr and \
                     self.scheduler_port == compute.scheduler_port)):
                logging.debug('Ignoring computation request from %s: %s, %s, %s',
                              compute.scheduler_ip_addr, self.scheduler_ip_addr,
                              self.avail_cpus, self.cpus)
                self.lock.release()
                resp = 'Busy'
                try:
                    yield conn.write_msg(uid, resp)
                except:
                    pass
                raise StopIteration

            if compute.dest_path and isinstance(compute.dest_path, str):
                compute.dest_path = compute.dest_path.strip(os.sep)
            else:
                for x in xrange(20):
                    compute.dest_path = os.urandom(8).encode('hex')
                    if compute.dest_path.find(os.sep) >= 0:
                        continue
                    if not os.path.isdir(os.path.join(self.dest_path_prefix, compute.dest_path)):
                        break
                else:
                    logging.warning('Failed to create unique dest_path: %s', compute.dest_path)
                    resp = 'NACK'
            compute.dest_path = os.path.join(self.dest_path_prefix, compute.dest_path)
            try:
                os.makedirs(compute.dest_path)
                os.chmod(compute.dest_path, stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)
                logging.debug('dest_path for "%s": %s', compute.name, compute.dest_path)
            except:
                logging.warning('Invalid destination path: "%s"', compute.dest_path)
                resp = 'NACK (Invalid dest_path)'
                if os.path.isdir(compute.dest_path):
                    os.rmdir(compute.dest_path)
                self.lock.release()
                try:
                    yield conn.write_msg(uid, resp)
                except:
                    logging.warning('Failed to send reply to %s', str(addr))
                raise StopIteration
            if compute.id in self.computations:
                logging.warning('Computation "%s" (%s) is being replaced',
                                compute.name, compute.id)
            setattr(compute, 'last_pulse', time.time())
            setattr(compute, 'pending_jobs', 0)
            setattr(compute, 'pending_results', 0)
            setattr(compute, 'zombie', False)
            logging.debug('xfer_files given: %s',
                          ','.join(xf.name for xf in compute.xfer_files))
            if compute.type == _Compute.func_type:
                if compute.env and 'PYTHONPATH' in compute.env:
                    self.computations[compute.id].env = compute.env['PYTHONPATH']
                try:
                    code = compile(base64.b64decode(compute.code), '<string>', 'exec')
                except:
                    logging.warning('Computation "%s" could not be compiled', compute.name)
                    resp = 'NACK (Compilation failed)'
                    if os.path.isdir(compute.dest_path):
                        os.rmdir(compute.dest_path)
                    self.lock.release()
                    try:
                        yield conn.write_msg(uid, resp)
                    except:
                        logging.warning('Failed to send reply to %s', str(addr))
                    raise StopIteration
                compute.code = marshal.dumps(code)
            elif compute.type == _Compute.prog_type:
                assert not compute.code
            xfer_files = []
            for xf in compute.xfer_files:
                tgt = os.path.join(compute.dest_path, os.path.basename(xf.name))
                try:
                    if _same_file(tgt, xf):
                        logging.debug('Ignoring file "%s" / "%s"', xf.name, tgt)
                        if tgt not in self.file_uses:
                            self.file_uses[tgt] = 0
                        self.file_uses[tgt] += 1
                        continue
                except:
                    pass
                xfer_files.append(xf)
            self.lock.release()
            if (self.scheduler_ip_addr is None) or \
                   (self.scheduler_ip_addr == compute.scheduler_ip_addr):
                self.computations[compute.id] = compute
                self.scheduler_ip_addr = compute.scheduler_ip_addr
                self.scheduler_port = compute.scheduler_port
                self.pulse_interval = compute.pulse_interval
                resp = 'ACK'
                if xfer_files:
                    resp += ':XFER_FILES:' + cPickle.dumps(xfer_files)
                self.timer_coro.resume(True)
            else:
                resp = 'Busy'
                if os.path.isdir(compute.dest_path):
                    os.rmdir(compute.dest_path)
            if resp:
                try:
                    yield conn.write_msg(uid, resp)
                except:
                    pass

        def xfer_file_task(conn, uid, msg, addr, coro):
            assert coro is not None
            try:
                xf = cPickle.loads(msg)
            except:
                logging.debug('Ignoring file trasnfer request from %s', addr[0])
                raise StopIteration
            resp = ''
            if xf.compute_id not in self.computations:
                logging.error('computation "%s" is invalid' % xf.compute_id)
                raise StopIteration
            tgt = os.path.join(self.computations[xf.compute_id].dest_path,
                               os.path.basename(xf.name))
            if os.path.isfile(tgt):
                if _same_file(tgt, xf):
                    self.lock.acquire()
                    if tgt in self.file_uses:
                        self.file_uses[tgt] += 1
                    else:
                        self.file_uses[tgt] = 1
                    self.lock.release()
                    resp = 'ACK'
                else:
                    logging.warning('File "%s" already exists with different status as "%s"',
                                    xf.name, tgt)
            if not resp:
                logging.debug('Copying file %s to %s (%s)', xf.name,
                              tgt, xf.stat_buf.st_size)
                try:
                    fd = open(tgt, 'wb')
                    n = 0
                    while n < xf.stat_buf.st_size:
                        data = yield conn.read(min(xf.stat_buf.st_size-n, 10240000))
                        if not data:
                            break
                        fd.write(data)
                        n += len(data)
                        if self.max_file_size and n > self.max_file_size:
                            logging.warning('File "%s" is too big (%s); it is truncated',
                                            tgt, n)
                            break
                    fd.close()
                    if n < xf.stat_buf.st_size:
                        resp = 'NAK (read only %s bytes)' % n
                    else:
                        resp = 'ACK'
                        logging.debug('Copied file %s, %s', tgt, resp)
                        os.utime(tgt, (xf.stat_buf.st_atime, xf.stat_buf.st_mtime))
                        os.chmod(tgt, stat.S_IMODE(xf.stat_buf.st_mode))
                        self.file_uses[tgt] = 1
                except:
                    logging.warning('Copying file "%s" failed with "%s"',
                                    xf.name, traceback.format_exc())
                    resp = 'NACK'
                try:
                    yield conn.write_msg(0, resp)
                except:
                    logging.debug('Could not send reply for "%s"', xf.name)
            raise StopIteration # xfer_file_task

        def terminate_job_task(uid, msg, addr, coro):
            assert coro is not None
            self.lock.acquire()
            try:
                _job = cPickle.loads(msg)
                compute = self.computations[_job.compute_id]
                assert addr[0] == compute.scheduler_ip_addr
                job_info = self.job_infos.pop(uid, None)
            except:
                logging.debug('Ignoring job request from %s', addr[0])
                raise StopIteration
            finally:
                self.lock.release()
            if job_info is None:
                logging.debug('Job %s completed; ignoring cancel request from %s',
                              uid, addr[0])
                raise StopIteration
            try:
                logging.debug('Killing job %s', uid)
                job_info.proc.terminate()
                # TODO: make sure process is really killed?
                # called from coroutine, so not good to wait
                if isinstance(job_info.proc, multiprocessing.Process):
                    job_info.proc.join(1)
                else:
                    job_info.proc.wait()
            except:
                logging.debug(traceback.format_exc())
            logging.debug('Killed process for job %s', uid)
            reply_addr = (addr[0], compute.job_result_port)
            reply = _JobReply(_job, self.ip_addr)
            job_info = _DispyJobInfo(reply, reply_addr, compute)
            reply.status = DispyJob.Terminated
            Coro(self._send_job_reply, job_info, resending=False)

        def retrieve_job_task(conn, uid, msg, addr, coro):
            assert coro is not None
            try:
                req = cPickle.loads(msg)
                assert req['uid'] is not None
                assert req['hash'] is not None
                assert req['compute_id'] is not None
            except:
                resp = cPickle.dumps('Invalid job')
                try:
                    yield conn.write_msg(0, resp)
                except:
                    pass
                raise StopIteration

            job_info = self.job_infos.get(req['uid'], None)
            resp = None
            if job_info is not None:
                try:
                    yield conn.write_msg(req['uid'], cPickle.dumps(job_info.job_reply))
                    ruid, ack = yield conn.read_msg()
                    # no need to check ack
                except:
                    logging.debug('Could not send reply for job %s', req['uid'])
                raise StopIteration

            for d in os.listdir(self.dest_path_prefix):
                info_file = os.path.join(self.dest_path_prefix, d,
                                         '_dispy_job_reply_%s' % req['uid'])
                if os.path.isfile(info_file):
                    try:
                        fd = open(info_file, 'rb')
                        job_reply = cPickle.load(fd)
                        fd.close()
                    except:
                        job_reply = None
                    if hasattr(job_reply, 'hash') and job_reply.hash == req['hash']:
                        try:
                            yield conn.write_msg(req['uid'], cPickle.dumps(job_reply))
                            ruid, ack = yield conn.read_msg()
                            assert ack == 'ACK'
                            assert ruid == req['uid']
                        except:
                            logging.debug('Could not send reply for job %s', req['uid'])
                            raise StopIteration
                        try:
                            os.remove(info_file)
                            self.lock.acquire()
                            compute = self.computations.get(req['compute_id'], None)
                            if compute is None:
                                p = os.path.dirname(info_file)
                                if p.startswith(self.dest_path_prefix) and \
                                       len(p) > len(self.dest_path_prefix) and \
                                       len(os.listdir(p)) == 0:
                                    os.rmdir(p)
                            else:
                                compute.pending_results -= 1
                            self.lock.release()
                        except:
                            logging.debug('Could not remove "%s"', info_file)
                        raise StopIteration
            else:
                resp = cPickle.dumps('Invalid job: %s' % req['uid'])

            if resp:
                try:
                    yield conn.write_msg(0, resp)
                except:
                    pass

        # tcp_serve_task starts
        conn = AsynCoroSocket(conn, blocking=False, server=True,
                              certfile=self.certfile, keyfile=self.keyfile)
        try:
            req = yield conn.read(len(self.auth_code))
            assert req == self.auth_code
        except:
            logging.warning('Invalid client authentication? %s', req)
            conn.close()
            raise StopIteration
        uid, msg = yield conn.read_msg()
        if not msg:
            conn.close()
            raise StopIteration
        if msg.startswith('JOB:'):
            msg = msg[len('JOB:'):]
            yield job_request_task(conn, uid, msg, addr, coro)
            conn.close()
        elif msg.startswith('COMPUTE:'):
            msg = msg[len('COMPUTE:'):]
            yield add_computation_task(conn, uid, msg, addr, coro)
            conn.close()
        elif msg.startswith('FILEXFER:'):
            msg = msg[len('FILEXFER:'):]
            yield xfer_file_task(conn, uid, msg, addr, coro)
            conn.close()
        elif msg.startswith('DEL_COMPUTE:'):
            msg = msg[len('DEL_COMPUTE:'):]
            try:
                info = cPickle.loads(msg)
                compute_id = info['ID']
                self.lock.acquire()
                compute = self.computations.get(compute_id, None)
                if compute is None:
                    logging.warning('Computation "%s" is not valid', compute_id)
                else:
                    compute.zombie = True
                    self.cleanup_computation(compute)
                self.lock.release()
            except:
                logging.debug('Deleting computation failed with %s', traceback.format_exc())
                # raise
            conn.close()
        elif msg.startswith('TERMINATE_JOB:'):
            msg = msg[len('TERMINATE_JOB:'):]
            yield terminate_job_task(uid, msg, addr, coro)
            conn.close()
        elif msg.startswith('RETRIEVE_JOB:'):
            msg = msg[len('RETRIEVE_JOB:'):]
            yield retrieve_job_task(conn, uid, msg, addr, coro)
            conn.close()
        else:
            logging.warning('Invalid request "%s" from %s',
                            msg[:min(10, len(msg))], addr[0])
            resp = 'NAK (invalid command: %s)' % (msg[:min(10, len(msg))])
            try:
                yield conn.write_msg(uid, resp)
            except:
                logging.warning('Failed to send reply to %s', str(addr))
            conn.close()

    def timer_task(self, coro=None):
        reset = True
        last_pulse_time = last_zombie_time = time.time()
        while True:
            if reset:
                if self.pulse_interval and self.zombie_interval:
                    timeout = min(self.pulse_interval, self.zombie_interval)
                    self.zombie_interval = max(5 * self.pulse_interval, self.zombie_interval)
                else:
                    timeout = max(self.pulse_interval, self.zombie_interval)
                    self.zombie_interval = self.zombie_interval

            reset = yield coro.suspend(timeout)

            now = time.time()
            if self.pulse_interval and (now - last_pulse_time) >= self.pulse_interval:
                n = self.cpus - self.avail_cpus
                assert n >= 0
                if n > 0 and self.scheduler_ip_addr:
                    last_pulse_time = now
                    msg = 'PULSE:' + cPickle.dumps({'ip_addr':self.ip_addr,
                                                    'port':self.udp_sock.getsockname()[1], 'cpus':n})
                    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                    sock = AsynCoroSocket(sock, blocking=False)
                    yield sock.sendto(msg, (self.scheduler_ip_addr, self.scheduler_port))
                    sock.close()
            if self.zombie_interval and (now - last_zombie_time) >= self.zombie_interval:
                last_zombie_time = now
                self.lock.acquire()
                for compute in self.computations.itervalues():
                    if (now - compute.last_pulse) > self.zombie_interval:
                        compute.zombie = True
                zombies = [compute for compute in self.computations.itervalues() \
                           if compute.zombie and compute.pending_jobs == 0]
                for compute in zombies:
                    logging.debug('Deleting zombie computation "%s"', compute.name)
                    self.cleanup_computation(compute)
                phoenix = [compute for compute in self.computations.itervalues() \
                           if not compute.zombie and compute.pending_results]
                for compute in phoenix:
                    files = [f for f in os.listdir(compute.dest_path) \
                             if f.startswith('_dispy_job_reply_')]
                    # limit number queued so as not to take up too much time
                    files = files[:min(len(files), 128)]
                    for f in files:
                        result_file = os.path.join(compute.dest_path, f)
                        try:
                            fd = open(result_file, 'rb')
                            job_result = cPickle.load(fd)
                            fd.close()
                        except:
                            logging.debug('Could not load "%s"', result_file)
                            logging.debug(traceback.format_exc())
                            continue
                        try:
                            os.remove(result_file)
                        except:
                            logging.debug('Could not remove "%s"', result_file)
                        compute.pending_results -= 1
                        job_info = _DispyJobInfo(job_result, (compute.scheduler_ip_addr,
                                                              compute.job_result_port), compute)
                        Coro(self._send_job_reply, job_info, resending=True)
                self.lock.release()
                for compute in zombies:
                    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                    sock = AsynCoroSocket(sock, blocking=False)
                    logging.debug('Sending TERMINATE to %s', compute.scheduler_ip_addr)
                    data = cPickle.dumps({'ip_addr':self.address[0], 'port':self.address[1],
                                          'sign':self.signature})
                    yield sock.sendto('TERMINATED:%s' % data, (compute.scheduler_ip_addr,
                                                               compute.scheduler_port))
                    sock.close()
                if self.scheduler_ip_addr is None and self.avail_cpus == self.cpus:
                    self.pulse_interval = None
                    reset = True
                    yield self.send_pong_msg(coro=coro)

    def __job_program(self, _job, job_info):
        compute = self.computations[_job.compute_id]
        program = [compute.name]
        args = cPickle.loads(_job.args)
        program.extend(args)
        logging.debug('Executing "%s"', str(program))
        reply = job_info.job_reply
        try:
            os.chdir(compute.dest_path)
            job_info.proc = subprocess.Popen(program, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                             env={'PATH':compute.dest_path + ':' + compute.env.get('PATH')})
            assert isinstance(job_info.proc, subprocess.Popen)
            reply.stdout, reply.stderr = job_info.proc.communicate()
            reply.result = job_info.proc.returncode
            reply.status = DispyJob.Finished
        except:
            logging.debug('Executing %s failed with %s', str(program), str(sys.exc_info()))
            reply.exception = traceback.format_exc()
            reply.status = DispyJob.Terminated
        self.reply_Q.put(reply)

    def __reply_Q(self):
        while True:
            job_reply = self.reply_Q.get()
            if job_reply is None:
                break
            job_info = self.job_infos.pop(job_reply.uid, None)
            if job_info is not None:
                if job_info.proc is not None:
                    if isinstance(job_info.proc, multiprocessing.Process):
                        job_info.proc.join(2)
                    else:
                        job_info.proc.wait()
                job_info.job_reply = job_reply
                Coro(self._send_job_reply, job_info, resending=False).value()

    def _send_job_reply(self, job_info, resending=False, coro=None):
        """Internal use only.
        """
        assert coro is not None
        job_reply = job_info.job_reply
        logging.debug('Sending result for job %s (%s) to %s',
                      job_reply.uid, job_reply.status, job_info.reply_addr[0])
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock = AsynCoroSocket(sock, blocking=False,
                              certfile=self.certfile, keyfile=self.keyfile)
        try:
            yield sock.connect(job_info.reply_addr)
            yield sock.write_msg(job_reply.uid, cPickle.dumps(job_reply))
            uid, ack = yield sock.read_msg()
            assert ack == 'ACK'
            assert uid == job_reply.uid
        except:
            logging.error("Couldn't send results for %s to %s",
                          job_reply.uid, str(job_info.reply_addr))
            # store job result even if computation has not enabled
            # fault recovery; user may be able to access node and
            # retrieve result manually
            f = os.path.join(job_info.compute_dest_path, '_dispy_job_reply_%s' % job_reply.uid)
            logging.debug('storing results for job %s', job_reply.uid)
            try:
                fd = open(f, 'wb')
                cPickle.dump(job_reply, fd)
                fd.close()
            except:
                logging.debug('Could not save results for job %s', job_reply.uid)
            else:
                self.lock.acquire()
                compute = self.computations.get(job_info.compute_id, None)
                if compute is not None:
                    compute.pending_results += 1
                self.lock.release()
        finally:
            sock.close()
            if not resending:
                self.lock.acquire()
                self.avail_cpus += 1
                compute = self.computations.get(job_info.compute_id, None)
                if compute is None:
                    logging.warning('Computation for %s / %s is invalid!',
                                    job_reply.uid, job_info.compute_id)
                else:
                    # technically last_pulse should be updated only
                    # when successfully sent reply, but no harm if done
                    # otherwise, too
                    compute.last_pulse = time.time()
                    compute.pending_jobs -= 1
                    if compute.pending_jobs == 0 and compute.zombie:
                        self.cleanup_computation(compute)
                self.lock.release()

    def cleanup_computation(self, compute):
        # called with lock held
        if not compute.zombie:
            return
        if compute.pending_jobs != 0:
            logging.debug('pending jobs for computation "%s"/%s: %s',
                          compute.name, compute.id, compute.pending_jobs)
            if compute.pending_jobs > 0:
                return

        del self.computations[compute.id]
        if compute.scheduler_ip_addr == self.scheduler_ip_addr and \
               all(c.scheduler_ip_addr != self.scheduler_ip_addr \
                   for c in self.computations.itervalues()):
            assert self.avail_cpus == self.cpus
            self.scheduler_ip_addr = None
            self.pulse_interval = None

        if self.scheduler_ip_addr is None and self.avail_cpus == self.cpus:
            self.timer_coro.resume(True)
            Coro(self.send_pong_msg)
        if compute.cleanup is False:
            return
        for xf in compute.xfer_files:
            tgt = os.path.join(compute.dest_path, os.path.basename(xf.name))
            if tgt not in self.file_uses:
                logging.debug('File "%s" is unknown', tgt)
                continue
            self.file_uses[tgt] -= 1
            if self.file_uses[tgt] == 0:
                del self.file_uses[tgt]
                if tgt == xf:
                    logging.debug('Not removing file "%s"', xf.name)
                else:
                    logging.debug('Removing file "%s"', tgt)
                    try:
                        os.remove(tgt)
                        if os.path.splitext(tgt)[1] == '.py' and os.path.isfile(tgt + 'c'):
                            os.remove(tgt + 'c')
                    except:
                        logging.warning('Could not remove file "%s"', tgt)

        if os.path.isdir(compute.dest_path) and \
               compute.dest_path.startswith(self.dest_path_prefix) and \
               len(compute.dest_path) > len(self.dest_path_prefix) and \
               len(os.listdir(compute.dest_path)) == 0:
            logging.debug('Removing "%s"', compute.dest_path)
            try:
                os.rmdir(compute.dest_path)
            except:
                logging.warning('Could not remove directory "%s"', compute.dest_path)

    def shutdown(self):
        def _shutdown(self, coro=None):
            assert coro is not None
            self.lock.acquire()
            job_infos = self.job_infos
            self.job_infos = {}
            computations = self.computations.items()
            self.computations = {}
            if self.reply_Q:
                self.reply_Q.put(None)
            self.lock.release()
            for uid, job_info in job_infos.iteritems():
                job_info.proc.terminate()
                logging.debug('process for %s is killed', uid)
                if isinstance(job_info.proc, multiprocessing.Process):
                    job_info.proc.join(2)
                else:
                    job_info.proc.wait()
            for cid, compute in computations:
                sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                logging.debug('Sending TERMINATE to %s', compute.scheduler_ip_addr)
                data = cPickle.dumps({'ip_addr':self.address[0], 'port':self.address[1],
                                      'sign':self.signature})
                sock.sendto('TERMINATED:' + data, (compute.scheduler_ip_addr,
                                                   compute.scheduler_port))
                sock.close()

            self.tcp_sock.close()
            self.udp_sock.close()
            self.timer.terminate()
        Coro(_shutdown, self).value()
        self.asyncoro.terminate()

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--cpus', dest='cpus', type=int, default=0,
                        help='number of cpus used by dispy; if negative, that many cpus are not used')
    parser.add_argument('-d', action='store_true', dest='loglevel', default=False,
                        help='if True, debug messages are printed')
    parser.add_argument('-i', '--ip_addr', dest='ip_addr', default='',
                        help='IP address to use (may be needed in case of multiple interfaces)')
    parser.add_argument('-p', '--node_port', dest='node_port', type=int, default=51348,
                        help='port number to use')
    parser.add_argument('--dest_path_prefix', dest='dest_path_prefix',
                        default=os.path.join(os.sep, 'tmp', 'dispy'),
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
        config['loglevel'] = logging.DEBUG
    else:
        config['loglevel'] = logging.INFO

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

    logging.basicConfig(format='%(asctime)s %(message)s', level=config['loglevel'])
    del config['loglevel']

    node = _DispyNode(**config)

    while True:
        try:
            node.asyncoro.join()
        except KeyboardInterrupt:
            logging.info('Interrupted; terminating')
            try:
                Coro(node.shutdown).value()
            except:
                logging.debug(traceback.format_exc())
                pass
            time.sleep(1)
            break
        except:
            logging.warning(traceback.print_exc())
            logging.warning('Server terminated (possibly due to an error); restarting')
            time.sleep(2)
            break
