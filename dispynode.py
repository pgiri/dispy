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
import datetime
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
import types
import struct
import base64
import hashlib
import select
import atexit
import logging
import shutil
import getopt
import marshal

from dispy import _DispySocket, _DispyJob_, _JobReply, DispyJob, \
     _Compute, _XferFile, _xor_string, _node_name_ipaddr

MaxFileSize = 10240000

class _DispyJobInfo():
    def __init__(self, job_reply, reply_addr, certfile=None, keyfile=None):
        self.job_reply = job_reply
        self.reply_addr = reply_addr
        self.proc = None
        self.certfile = None
        self.keyfile = None

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

def _dispy_job_func(__dispy_job_info, __dispy_job_args, __dispy_job_kwargs,
                    __dispy_reply_Q, __dispy_job_env, __dispy_job_name, __dispy_job_code,
                    __dispy_job_files=[]):
    """Internal use only.
    """
    sys.stdout = cStringIO.StringIO()
    sys.stderr = cStringIO.StringIO()
    __dispy_job_reply = __dispy_job_info.job_reply
    if __dispy_job_env and isinstance(__dispy_job_env, list):
        sys.path = __dispy_job_env + sys.path
    try:
        exec marshal.loads(__dispy_job_code)
        globals().update(locals())
        __dispy_job_args = cPickle.loads(__dispy_job_args)
        __dispy_job_kwargs = cPickle.loads(__dispy_job_kwargs)
        __func = globals()[__dispy_job_name]
        __dispy_job_reply.result = __func(*__dispy_job_args, **__dispy_job_kwargs)
        __dispy_job_reply.status = DispyJob.Finished
    except:
        raise
        __dispy_job_reply.exception = traceback.format_exc()
        __dispy_job_reply.status = DispyJob.Terminated
    for f in __dispy_job_files:
        if os.path.isfile(f):
            os.remove(f)
    __dispy_job_reply.stdout = sys.stdout.getvalue()
    __dispy_job_reply.stderr = sys.stderr.getvalue()
    __dispy_reply_Q.put(__dispy_job_reply)

def dispy_provisional_result(result):
    """Sends provisional result of computation back to the client.

    In some cases, such as optimizations, computations may send best
    answer so far back to the client so that the client may decide to
    terminate computations based on the results or alter computations
    if necessary. The computations can use this function in such cases
    with the current result of computation as argument.
    """
    sock = None
    try:
        __dispy_job_reply = __dispy_job_info.job_reply
        logging.debug('Sending provisional result for job %s to %s',
                      __dispy_job_reply.job.uid, __dispy_job_reply.job.reply_addr)
        __dispy_job_reply.status = DispyJob.ProvisionalResult
        sock = _DispySocket(socket.socket(socket.AF_INET, socket.SOCK_STREAM),
                            certfile=__dispy_job_info.certfile,
                            keyfile=__dispy_job_info.keyfile)
        sock.settimeout(5)
        sock.connect(__dispy_job_info.reply_addr)
        sock.write_msg(__dispy_job_reply.uid, cPickle.dumps(__dispy_job_reply))
    except:
        logging.error("Couldn't send provisional results %s (%s)",
                      str(result), str(traceback.format_exc()))
    if sock is not None:
        sock.close()

def _send_job_reply(job_info):
    """Internal use only.
    """
    job_reply = job_info.job_reply
    logging.debug('Sending result for job %s (%s) to %s',
                  job_reply.uid, job_reply.status, job_info.reply_addr[0])
    sock = _DispySocket(socket.socket(socket.AF_INET, socket.SOCK_STREAM),
                        certfile=job_info.certfile, keyfile=job_info.keyfile)
    sock.settimeout(5)
    try:
        sock.connect(job_info.reply_addr)
        sock.write_msg(job_reply.uid, cPickle.dumps(job_reply))
    except:
        logging.error("Couldn't send results for %s to %s (%s)",
                      job_reply.uid, str(job_info.reply_addr), str(traceback.format_exc()))
    sock.close()

class _DispyNode():
    """Internal use only.
    """
    def __init__(self, cpus, ip_addr='', node_port=51348, dest_path_prefix='',
                 scheduler_node=None, scheduler_port=51347,
                 secret='', keyfile=None, certfile=None, max_file_size=None):
        self.cpus = cpus
        self.srv_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.fqdn = socket.getfqdn()
        if ip_addr:
            ip_addr = _node_name_ipaddr(ip_addr)[1]
        else:
            ip_addr = socket.gethostbyname(socket.gethostname())
        self.ip_addr = ip_addr
        self.scheduler_port = scheduler_port
        self.pulse_interval = None

        self.srv_sock.bind((ip_addr, 0))
        self.address = self.srv_sock.getsockname()
        self.srv_sock.listen(2)

        if dest_path_prefix:
            self.dest_path_prefix = dest_path_prefix.strip().rstrip(os.sep)
        else:
            self.dest_path_prefix = os.path.join(os.sep, 'tmp', 'dispy')
        if not os.path.isdir(self.dest_path_prefix):
            os.makedirs(self.dest_path_prefix)
        if max_file_size is None:
            max_file_size = MaxFileSize
        self.max_file_size = max_file_size

        self.avail_cpus = self.cpus
        self.computations = {}
        self.scheduler_ip_addr = None
        self.file_uses = {}
        self.job_infos = {}
        self.lock = threading.Lock()
        self.terminate = False
        self.signature = os.urandom(20).encode('hex')
        self.auth_code = hashlib.sha1(_xor_string(self.signature, secret)).hexdigest()
        self.keyfile = keyfile
        self.certfile = certfile
        logging.debug('auth_code for %s: %s', ip_addr, self.auth_code)
        self.server_started = threading.Event()
        self.cmd_sock = _DispySocket(socket.socket(socket.AF_INET, socket.SOCK_STREAM),
                                     auth_code=self.auth_code)
        self.cmd_sock.bind((self.ip_addr, 0))
        self.cmd_sock.listen(1)
        logging.info('Serving %s cpus at %s:%s',
                     self.cpus, self.address[0], self.address[1])

        scheduler_ip_addr = _node_name_ipaddr(scheduler_node)[1]

        self.reply_Q = multiprocessing.Queue()
        self.reply_Q_thread = threading.Thread(target=self.__reply_Q)
        self.reply_Q_thread.start()

        self.ping_thread = threading.Thread(target=self.__ping_pong,
                                            args=(node_port, scheduler_ip_addr))
        self.ping_thread.daemon = True
        self.ping_thread.start()

        atexit.register(self.shutdown)

    def send_pong_msg(self, reset_interval=True):
        ping_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        ping_sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        pong_msg = {'ip_addr':self.ip_addr, 'fqdn':self.fqdn, 'port':self.address[1],
                    'cpus':self.cpus, 'sign':self.signature}
        pong_msg = 'PONG:' + cPickle.dumps(pong_msg)
        ping_sock.sendto(pong_msg, ('<broadcast>', self.scheduler_port))
        ping_sock.close()
        if reset_interval:
            self.pulse_interval = None
            sock = _DispySocket(socket.socket(socket.AF_INET, socket.SOCK_STREAM),
                                auth_code=self.auth_code)
            sock.settimeout(5)
            sock.connect((self.ip_addr, self.cmd_sock.sock.getsockname()[1]))
            sock.write_msg(0, 'reset_interval')
            sock.close()

    def __ping_pong(self, node_port, scheduler_ip_addr):
        self.server_started.wait()

        if self.avail_cpus == self.cpus:
            self.send_pong_msg(reset_interval=False)
        pong_msg = {'ip_addr':self.ip_addr, 'fqdn':self.fqdn, 'port':self.address[1],
                    'cpus':self.cpus, 'sign':self.signature}
        pong_msg = 'PONG:' + cPickle.dumps(pong_msg)

        if scheduler_ip_addr:
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                sock.sendto(pong_msg, (scheduler_ip_addr, self.scheduler_port))
                sock.close()
            except:
                logging.warning("Couldn't send ping message to %s:%s",
                                scheduler_ip_addr, self.scheduler_port)

        ping_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        ping_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        ping_sock.bind(('', node_port))
        logging.info('Listening at %s:%s', self.address[0], node_port)
        while True:
            ready = select.select([ping_sock, self.cmd_sock.sock], [], [], self.pulse_interval)[0]
            for sock in ready:
                if sock == ping_sock:
                    msg, addr = ping_sock.recvfrom(1024)
                    if msg.startswith('PING:'):
                        logging.debug('Ping message from %s (%s)', addr[0], addr[1])
                        if self.cpus != self.avail_cpus:
                            logging.debug('Busy (%s/%s); ignoring ping message from %s',
                                          self.cpus, self.avail_cpus, addr[0])
                            continue
                        try:
                            info = cPickle.loads(msg[len('PING:'):])
                            logging.debug('ping from "%s", "%s"', info['scheduler_ip_addr'],
                                          info['scheduler_port'])
                            socket.inet_aton(info['scheduler_ip_addr'])
                            assert isinstance(info['scheduler_port'], int)
                            addr = (info['scheduler_ip_addr'], info['scheduler_port'])
                        except:
                            # raise
                            logging.debug('Ignoring ping message from %s (%s)',
                                          addr[0], addr[1])
                            continue
                        logging.debug('Sending pong to %s:%s', addr[0], addr[1])
                        ping_sock.sendto(pong_msg, addr)
                    else:
                        logging.warning('Ignoring ping message from %s', addr[0])
                elif sock == self.cmd_sock.sock:
                    conn, addr = self.cmd_sock.accept()
                    conn = _DispySocket(conn)
                    req = conn.read(len(self.auth_code))
                    if req != self.auth_code:
                        logging.debug('invalid auth for cmd')
                        conn.close()
                        continue
                    uid, msg = conn.read_msg()
                    conn.close()
                    if msg == 'terminate':
                        logging.debug('Ping thread terminating')
                        ping_sock.close()
                        self.cmd_sock.close()
                        self.cmd_sock = None
                        return
                    elif msg == 'reset_interval':
                        pass
                    else:
                        logging.debug('Ignoring terminate message: %s', msg)
            if not ready and self.pulse_interval:
                n = self.cpus - self.avail_cpus
                assert n >= 0
                if n > 0 and self.scheduler_ip_addr:
                    logging.debug('Sending PULSE to %s', self.scheduler_ip_addr)
                    msg = 'PULSE:' + cPickle.dumps({'ip_addr':self.ip_addr, 'cpus':n})
                    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                    sock.sendto(msg, (self.scheduler_ip_addr, self.scheduler_port))
                    sock.close()

    def _serve(self):
        self.server_started.set()
        while True:
            conn, addr = self.srv_sock.accept()
            try:
                conn = _DispySocket(conn, certfile=self.certfile, keyfile=self.keyfile, server=True)
            except:
                logging.warning('Invalid client authentication?')
                conn.close()
                continue
            req = conn.read(len(self.auth_code))
            if req != self.auth_code:
                logging.warning('Invalid / unauthorized request ignored (%s, %s)',
                                req, self.auth_code)
                conn.close()
                continue
            uid, msg = conn.read_msg()
            if not msg:
                conn.close()
                continue
            if msg.startswith('JOB:'):
                msg = msg[len('JOB:'):]
                try:
                    _job = cPickle.loads(msg)
                except:
                    logging.debug('Ignoring job request from %s', addr[0])
                    logging.debug(traceback.format_exc())
                    continue
                _job.uid = uid
                compute = self.computations.get(_job.compute_id, None)
                if self.avail_cpus == 0:
                    logging.warning('All cpus busy')
                    resp = 'NAK (all cpus busy)'
                elif compute is None:
                    logging.warning('Invalid computation %s', _job.compute_id)
                    resp = 'NAK (invalid computation %s)' % _job.compute_id
                else:
                    reply_addr = (addr[0], self.computations[_job.compute_id].job_result_port)
                    logging.debug('New job id %s from %s', _job.uid, addr[0])
                    files = []
                    for f in _job.files:
                        tgt = os.path.join(self.computations[compute.id].dest_path,
                                           os.path.basename(f['name']))
                        fd = open(tgt, 'wb')
                        fd.write(f['data'])
                        fd.close()
                        os.utime(tgt, (f['stat'].st_atime, f['stat'].st_mtime))
                        os.chmod(tgt, stat.S_IMODE(f['stat'].st_mode))
                        files.append(tgt)
                    _job.files = files

                    if compute.type == _Compute.func_type:
                        reply = _JobReply(_job, self.ip_addr)
                        job_info = _DispyJobInfo(reply, reply_addr, certfile=self.certfile,
                                                 keyfile=self.keyfile)
                        args = (job_info, _job.args, _job.kwargs, self.reply_Q,
                                compute.env, compute.name, compute.code, _job.files)
                        try:
                            conn.write_msg(_job.uid, cPickle.dumps(_job.uid))
                            conn.close()
                        except:
                            logging.warning('Failed to send response for new job to %s',
                                            str(addr))
                            continue
                        self.lock.acquire()
                        job_info.proc = multiprocessing.Process(target=_dispy_job_func, args=args)
                        self.avail_cpus -= 1
                        self.job_infos[_job.uid] = job_info
                        self.lock.release()
                        job_info.proc.start()
                        continue
                    elif compute.type == _Compute.prog_type:
                        prog_thread = threading.Thread(target=self.__job_program,
                                                       args=(_job, reply_addr,))
                        try:
                            conn.write_msg(_job.uid, cPickle.dumps(_job.uid))
                            conn.close()
                        except:
                            logging.warning('Failed to send response for new job to %s',
                                            str(addr))
                            continue
                        self.lock.acquire()
                        self.avail_cpus -= 1
                        self.lock.release()
                        prog_thread.start()
                        continue
                    else:
                        resp = 'NAK (invalid computation type "%s")' % compute.type
                try:
                    conn.write_msg(_job.uid, cPickle.dumps(resp))
                    conn.close()
                except:
                    logging.warning('Failed to send response for new job to %s',
                                    str(addr))
                    continue
                continue
            elif msg.startswith('COMPUTE:'):
                msg = msg[len('COMPUTE:'):]
                try:
                    compute = cPickle.loads(msg)
                except:
                    logging.debug('Ignoring computation request from %s', addr[0])
                    resp = 'Invalid computation request'
                    try:
                        conn.write_msg(uid, resp)
                        conn.close()
                    except:
                        logging.warning('Failed to send reply to %s', str(addr))
                        continue
                    continue
                resp = 'ACK'
                compute.dest_path = compute.dest_path.strip().rstrip(os.sep)
                if compute.dest_path.startswith(os.sep):
                    logging.warning('Invalid destination path: "%s"', compute.dest_path)
                    resp = 'NACK (Invalid dest_path)'
                    try:
                        conn.write_msg(uid, resp)
                        conn.close()
                    except:
                        logging.warning('Failed to send reply to %s', str(addr))
                        continue
                    continue
                logging.debug('Adding computation %s', compute.name)
                compute.dest_path = os.path.join(self.dest_path_prefix, compute.dest_path)
                if not os.path.isdir(compute.dest_path):
                    try:
                        os.makedirs(compute.dest_path)
                    except:
                        logging.warning('Invalid destination path: "%s"', compute.dest_path)
                        resp = 'NACK (Invalid dest_path)'
                        try:
                            conn.write_msg(uid, resp)
                            conn.close()
                        except:
                            logging.warning('Failed to send reply to %s', str(addr))
                            continue
                        continue
                if compute.id in self.computations:
                    logging.warning('Computation "%s" (%s) is being replaced',
                                    compute.name, compute.id)
                self.computations[compute.id] = _Compute(compute.type, compute.name)
                self.computations[compute.id].id = compute.id
                self.computations[compute.id].job_result_port = compute.job_result_port
                self.computations[compute.id].env = compute.env
                self.computations[compute.id].xfer_files = compute.xfer_files
                self.computations[compute.id].dest_path = compute.dest_path
                logging.debug('xfer_files given: %s',
                              ','.join(xf.name for xf in compute.xfer_files))
                if compute.type == _Compute.func_type:
                    if compute.env and 'PYTHONPATH' in compute.env:
                        self.computations[compute.id].env = compute.env['PYTHONPATH']
                    if compute.dest_path:
                        self.computations[compute.id].env.append(compute.dest_path)
                    try:
                        code = compile(base64.b64decode(compute.code), '<string>', 'exec')
                    except:
                        logging.warning('Computation "%s" could not be compiled', compute.name)
                        resp = 'NACK (Compilation failed)'
                        try:
                            conn.write_msg(uid, resp)
                            conn.close()
                        except:
                            logging.warning('Failed to send reply to %s', str(addr))
                            continue
                        continue
                    self.computations[compute.id].code = marshal.dumps(code)
                elif compute.type == _Compute.prog_type:
                    assert not compute.code
                    if compute.xfer_files:
                        compute.name = os.path.join(compute.dest_path,
                                                    os.path.basename(compute.name))
                    self.computations[compute.id].name = compute.name
                xfer_files = []
                for xf in compute.xfer_files:
                    tgt = os.path.join(compute.dest_path, os.path.basename(xf.name))
                    try:
                        if _same_file(tgt, xf):
                            if tgt not in self.file_uses:
                                self.file_uses[tgt] = 0
                            self.file_uses[tgt] += 1
                            continue
                    except:
                        pass
                    xfer_files.append(xf)
                if xfer_files:
                    logging.debug('xfer_files needed: %s',
                                  ','.join(xf.name for xf in compute.xfer_files))
                    resp += ':XFER_FILES:' + cPickle.dumps(xfer_files)
            elif msg.startswith('FILEXFER:'):
                msg = msg[len('FILEXFER:'):]
                try:
                    xf = cPickle.loads(msg)
                except:
                    logging.debug('Ignoring file trasnfer request from %s', addr[0])
                    continue
                resp = ''
                if xf.compute_id not in self.computations:
                    logging.error('computation "%s" is invalid' % xf.compute_id)
                    resp = 'NAK (invalid computation)'
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
                        logging.warning('File "%s" already exists with different status as "%s"',
                                        xf.name, tgt)
                if not resp:
                    logging.debug('Copying file %s to %s (%s)', xf.name,
                                  tgt, xf.stat_buf.st_size)
                    try:
                        fd = open(tgt, 'wb')
                        n = 0
                        while n < xf.stat_buf.st_size:
                            data = conn.read(min(xf.stat_buf.st_size-n, 1024000))
                            if not data:
                                break
                            fd.write(data)
                            n += len(data)
                            if n > self.max_file_size:
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
            elif msg.startswith('RESERVE:'):
                msg = msg[len('RESERVE:'):]
                try:
                    data = cPickle.loads(msg)
                    self.lock.acquire()
                    if (self.avail_cpus == self.cpus) and (self.cpus >= data['cpus']):
                        self.scheduler_ip_addr = data['ip_addr']
                        self.scheduler_port = data['port']
                        # self.cpus -= data['cpus']
                        resp = 'ACK'
                        logging.debug('Reserved %s cpus for %s', data['cpus'], data['ip_addr'])
                        pulse_interval = data.get('pulse_interval', None)
                        if pulse_interval is None:
                            self.pulse_interval = None
                        elif isinstance(pulse_interval, float) and 1 <= pulse_interval <= 600:
                            self.pulse_interval = pulse_interval
                        sock = _DispySocket(socket.socket(socket.AF_INET, socket.SOCK_STREAM),
                                            auth_code=self.auth_code)
                        sock.settimeout(5)
                        sock.connect((self.ip_addr, self.cmd_sock.sock.getsockname()[1]))
                        sock.write_msg(0, 'reset_interval')
                        sock.close()
                    else:
                        logging.warning('Rejecting RESERVE from %s', str(addr))
                        resp = 'NACK'
                    self.lock.release()
                except:
                    resp = 'NACK'
                    logging.debug('Rejecting RESERVE from %s', str(addr))
            elif msg.startswith('DEL_COMPUTE:'):
                msg = msg[len('DEL_COMPUTE:'):]
                try:
                    info = cPickle.loads(msg)
                    compute_id = info['_compute_id']
                    compute = self.computations.pop(compute_id, None)
                    if compute is None:
                        logging.warning('Computation "%s" is not valid', compute_id)
                    else:
                        logging.debug('Deleting computation "%s"', compute.name)
                        #assert compute.dest_path.startswith(self.dest_path_prefix)
                    if compute is not None and info['cleanup']:
                        for xf in compute.xfer_files:
                            tgt = os.path.join(compute.dest_path, os.path.basename(xf.name))
                            self.file_uses[tgt] -= 1
                            if self.file_uses[tgt] == 0:
                                del self.file_uses[tgt]
                                if tgt == xf:
                                    logging.debug('Not removing file "%s"', xf.name)
                                else:
                                    logging.debug('Removing file "%s"', tgt)
                                    try:
                                        os.remove(tgt)
                                    except:
                                        logging.warning('Could not remove file "%s"', tgt)
                        if compute.dest_path != self.dest_path_prefix:
                            path = compute.dest_path[len(self.dest_path_prefix):]
                            if path.startswith(os.sep):
                                path = path[1:]
                            path = os.path.join(self.dest_path_prefix, os.path.basename(path))
                            path = path.rstrip(os.sep)
                            # assert path != self.dest_path_prefix
                            if path != self.dest_path_prefix and len(os.listdir(path)) == 0:
                                logging.debug('Removing "%s"', path)
                                os.rmdir(path)
                except:
                    logging.debug('Deleting computation failed with %s', traceback.format_exc())
                    # raise
                resp = None
            elif msg.startswith('TERMINATE_JOB:'):
                msg = msg[len('TERMINATE_JOB:'):]
                try:
                    _job = cPickle.loads(msg)
                    compute = self.computations[_job.compute_id]
                except:
                    logging.debug('Ignoring job request from %s', addr[0])
                    continue
                self.lock.acquire()
                job_info = self.job_infos.pop(uid, None)
                self.lock.release()
                if job_info.proc is None:
                    logging.debug('Job %s completed; ignoring cancel request from %s',
                                  uid, addr[0])
                    continue
                try:
                    logging.debug('Killing job %s', uid)
                    job_info.proc.terminate()
                    if isinstance(job_info.proc, multiprocessing.Process):
                        job_info.proc.join(2)
                    else:
                        job_info.proc.wait()
                    self.lock.acquire()
                    self.avail_cpus += 1
                    self.lock.release()
                except:
                    print traceback.format_exc()
                logging.debug('Killed process for job %s', uid)
                reply_addr = (addr[0], compute.job_result_port)
                reply = _JobReply(_job, self.ip_addr)
                job_info = _DispyJobInfo(reply, reply_addr, certfile=self.certfile,
                                         keyfile=self.keyfile)
                reply.status = DispyJob.Terminated
                _send_job_reply(job_info)
                if self.avail_cpus == self.cpus:
                    self.send_pong_msg()
            else:
                logging.warning('Invalid request "%s" from %s',
                                msg[:min(10, len(msg))], addr[0])
                resp = 'NAK (invalid command: %s)' % (msg[:min(10, len(msg))])
            if resp:
                try:
                    conn.write_msg(uid, resp)
                    conn.close()
                except:
                    logging.warning('Failed to send reply to %s', str(addr))

    def __reply_Q(self):
        while True:
            job_reply = self.reply_Q.get()
            if job_reply is None:
                break
            self.lock.acquire()
            job_info = self.job_infos.pop(job_reply.uid, None)
            if job_info is not None:
                self.avail_cpus += 1
            self.lock.release()
            if job_info is not None:
                job_info.proc.join()
                job_info.job_reply = job_reply
                _send_job_reply(job_info)
            if self.avail_cpus == self.cpus:
                self.send_pong_msg()
        self.reply_Q = None

    def __job_program(self, _job, reply_addr):
        program = [self.computations[_job.compute_id].name]
        args = cPickle.loads(_job.args)
        program.extend(args)
        logging.debug('Executing "%s"', str(program))
        reply = _JobReply(_job, self.ip_addr)
        job_info = _DispyJobInfo(reply, reply_addr,
                                 certfile=self.certfile, keyfile=self.keyfile)
        try:
            self.lock.acquire()
            job_info.proc = subprocess.Popen(program, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                             env={'PATH':self.computations[_job.compute_id].env.get('PATH')})
            assert isinstance(job_info.proc, subprocess.Popen)
            self.job_infos[_job.uid] = job_info
            self.lock.release()
            reply.stdout, reply.stderr = job_info.proc.communicate()
            reply.result = job_info.proc.returncode
            reply.status = DispyJob.Finished
        except Exception:
            logging.debug('Executing %s failed with %s', str(program), str(sys.exc_info()))
            reply.exception = traceback.format_exc()
            reply.status = DispyJob.Terminated
        self.lock.acquire()
        if self.job_infos.pop(_job.uid, None) != job_info:
            self.lock.release()
            return
        self.avail_cpus += 1
        self.lock.release()
        for f in _job.files:
            # logging.debug('Removing job file "%s"', f)
            if os.path.isfile(f):
                os.remove(f)
        _send_job_reply(reply)
        if self.avail_cpus == self.cpus:
            self.send_pong_msg()

    def shutdown(self):
        self.lock.acquire()
        for uid, job_info in self.job_infos.iteritems():
            if isinstance(job_info.proc, multiprocessing.Process):
                logging.debug('process %s for %s is alive: %s',
                              job_info.proc.pid, uid, job_info.proc.is_alive())
                job_info.proc.terminate()
                job_info.proc.join()
        self.job_infos = {}
        ip_addr = self.scheduler_ip_addr
        self.scheduler_ip_addr = None
        self.lock.release()
        if self.reply_Q:
            self.reply_Q.put(None)
        if ip_addr:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            logging.debug('Sending TERMINATE to %s', ip_addr)
            data = cPickle.dumps({'ip_addr':self.address[0], 'port':self.address[1],
                                  'sign':self.signature})
            sock.sendto('TERMINATED:%s' % data, (ip_addr, self.scheduler_port))
            sock.close()
            if self.cmd_sock:
                sock = _DispySocket(socket.socket(socket.AF_INET, socket.SOCK_STREAM),
                                    auth_code=self.auth_code)
                sock.settimeout(5)
                sock.connect((self.ip_addr, self.cmd_sock.sock.getsockname()[1]))
                sock.write_msg(0, 'terminate')
                sock.close()

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
    parser.add_argument('--max_file_size', dest='max_file_size', default=None,
                        help='maximum file size of any file transferred')
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

    logging.basicConfig(format='%(asctime)s %(message)s', level=config['loglevel'])
    del config['loglevel']

    node = _DispyNode(**config)

    while True:
        try:
            node._serve()
        except KeyboardInterrupt:
            logging.info('Interrupted; terminating')
            node.shutdown()
            break
        except:
            logging.warning(traceback.print_exc())
            logging.warning('Server terminated (possibly due to an error); restarting')
