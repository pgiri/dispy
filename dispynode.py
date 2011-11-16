#!/usr/bin/env python

# dispynode: Execute computations on behalf of dispy clients;
# see accompanying 'dispy' for more details.

# Copyright (C) 2011 Giridhar Pemmasani (pgiri@yahoo.com)

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

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

from dispy import _Compute, _XferFile, _xor_string, _DispySocket, _Job, _node_name_ipaddr

MaxFileSize = 10240000

def _same_file(tgt, xf):
    # TODO: compare checksum?
    try:
        stat_buf = os.stat(tgt)
        if abs(stat_buf.st_mtime - xf.stat_buf.st_mtime) <= 1 and \
               stat_buf.st_size == xf.stat_buf.st_size and \
               stat.S_IMODE(stat_buf.st_mode) == stat.S_IMODE(xf.stat_buf.st_mode):
            return True
    except:
        return False

def _job_func(__job, __proc_Q, __compute_env, __compute_name, __compute_code):
    import cStringIO, sys, traceback, os, cPickle

    sys.stdout = cStringIO.StringIO()
    sys.stderr = cStringIO.StringIO()
    __exception = None
    __ret_val = None
    if __compute_env and isinstance(__compute_env, list):
        sys.path = __compute_env + sys.path
    try:
        exec __compute_code
        if __job._code:
            exec __job._code
        globals().update(locals())
        __args = cPickle.loads(__job._args)
        __kwargs = cPickle.loads(__job._kwargs)
        __func = globals()[__compute_name]
        __ret_val = __func(*__args, **__kwargs)
    except:
        __exception = traceback.format_exc()
    for f in __job._files:
        if os.path.isfile(f):
            os.remove(f)
    reply = {'uid':__job._uid, 'reply_addr':__job.reply_addr,
             'result':__ret_val, 'stdout':sys.stdout.getvalue(),
             'stderr':sys.stderr.getvalue(), 'exception':__exception, 'hash':__job._hash}
    __proc_Q.put(reply)

def _send_job_result(uid, result, addr, certfile, keyfile):
    """Internal use only.
    """
    logging.debug('Sending result for job %s to %s, %s', uid, addr[0], certfile)
    sock = _DispySocket(socket.socket(socket.AF_INET, socket.SOCK_STREAM),
                        certfile=certfile, keyfile=keyfile)
    sock.settimeout(10)
    try:
        sock.connect(addr)
        sock.write_msg(uid, result)
    except Exception:
        logging.error("Couldn't send results for %s to %s (%s)",
                      uid, str(addr), str(sys.exc_info()))
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
        self.clients = {}
        self.file_uses = {}
        self.procs = {}
        self.lock = threading.Lock()
        self.terminate = False
        self.signature = os.urandom(20).encode('hex')
        self.auth_code = hashlib.sha1(_xor_string(self.signature, secret)).hexdigest()
        self.keyfile = keyfile
        self.certfile = certfile
        self.pulse_interval = None
        logging.debug('auth_code for %s: %s', ip_addr, self.auth_code)
        self.server_started = threading.Event()
        self.cmd_sock = _DispySocket(socket.socket(socket.AF_INET, socket.SOCK_STREAM),
                                     auth_code=self.auth_code)
        self.cmd_sock.bind((self.ip_addr, 0))
        self.cmd_sock.listen(1)
        logging.info('Serving %s cpus at %s:%s',
                     self.cpus, self.address[0], self.address[1])

        scheduler_ip_addr = _node_name_ipaddr(scheduler_node)[1]

        self.proc_Q = multiprocessing.Queue()
        self.proc_Q_thread = threading.Thread(target=self.__proc_Q_process)
        self.proc_Q_thread.start()

        self.ping_thread = threading.Thread(target=self.__ping_pong,
                                            args=(node_port, scheduler_ip_addr))
        self.ping_thread.daemon = True
        self.ping_thread.start()

        atexit.register(self.shutdown)

    def send_pong_msg(self):
        ping_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        ping_sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        pong_msg = {'ip_addr':self.ip_addr, 'fqdn':self.fqdn, 'port':self.address[1],
                    'cpus':self.cpus, 'sign':self.signature}
        pong_msg = 'PONG:' + cPickle.dumps(pong_msg)
        ping_sock.sendto(pong_msg, ('<broadcast>', self.scheduler_port))
        ping_sock.close()

    def __ping_pong(self, node_port, scheduler_ip_addr):
        self.server_started.wait()

        if self.avail_cpus == self.cpus:
            self.send_pong_msg()
        pong_msg = {'ip_addr':self.ip_addr, 'fqdn':self.fqdn, 'port':self.address[1],
                    'cpus':self.cpus, 'sign':self.signature}
        pong_msg = 'PONG:' + cPickle.dumps(pong_msg)

        if scheduler_ip_addr:
            try:
                ping_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                ping_sock.sendto(pong_msg, (scheduler_ip_addr, self.scheduler_port))
                ping_sock.close()
            except:
                logging.warning("Couldn't send ping message to %s:%s",
                                scheduler_ip_addr, self.scheduler_port)

        ping_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        ping_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        ping_sock.bind(('', node_port))
        logging.info('Listening at %s:%s', self.address[0], node_port)
        while True:
            ready = select.select([ping_sock, self.cmd_sock.sock], [], [],
                                  self.pulse_interval)[0]
            for sock in ready:
                if sock == ping_sock:
                    msg, addr = ping_sock.recvfrom(1024)
                    if msg.startswith('PING:'):
                        logging.debug('Ping message from %s (%s)', addr[0], addr[1])
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

                        self.clients[addr[0]] = addr[1]
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
                    elif msg == 'restart':
                        pass
                    else:
                        logging.debug('Ignoring terminate message: %s', msg)
            if self.pulse_interval:
                n = self.cpus - self.avail_cpus
                if n > 0:
                    clients = dict(self.clients)
                    msg = 'PULSE:' + cPickle.dumps({'ip_addr':self.ip_addr, 'cpus':n})
                    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                    for ip_addr, port in clients.iteritems():
                        logging.debug('Sending pulse to %s (%s)', self.ip_addr, n)
                        sock.sendto(msg, (ip_addr, port))
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
                    job = cPickle.loads(msg)
                except:
                    logging.debug('Ignoring job request from %s', addr[0])
                    logging.debug(traceback.format_exc())
                    continue
                job._uid = uid
                compute = self.computations.get(job._compute_id, None)
                if self.avail_cpus == 0:
                    logging.warning('All cpus busy')
                    resp = 'NAK (all cpus busy)'
                elif compute is None:
                    logging.warning('Invalid computation %s', job._compute_id)
                    resp = 'NAK (invalid computation %s)' % job._compute_id
                else:
                    reply_addr = (addr[0], self.computations[job._compute_id].job_result_port)
                    setattr(job, 'reply_addr', reply_addr)
                    logging.debug('New job id %s from %s', job._uid, addr[0])
                    files = []
                    for f in job._files:
                        tgt = os.path.join(self.computations[compute.id].dest_path,
                                           os.path.basename(f['name']))
                        fd = open(tgt, 'wb')
                        fd.write(f['data'])
                        fd.close()
                        os.utime(tgt, (f['stat'].st_atime, f['stat'].st_mtime))
                        os.chmod(tgt, stat.S_IMODE(f['stat'].st_mode))
                        files.append(tgt)
                    job._files = files

                    if compute.type == _Compute.func_type:
                        args = (job, self.proc_Q, compute.env, compute.name, compute.code)
                        self.lock.acquire()
                        func_proc = multiprocessing.Process(target=_job_func, args=args)
                        self.avail_cpus -= 1
                        self.procs[job._uid] = func_proc
                        self.lock.release()
                        conn.write_msg(job._uid, cPickle.dumps(job._uid))
                        conn.close()
                        func_proc.start()
                        continue
                    elif compute.type == _Compute.prog_type:
                        prog_thread = threading.Thread(target=self.__job_program, args=(job,))
                        self.lock.acquire()
                        self.avail_cpus -= 1
                        self.lock.release()
                        conn.write_msg(job._uid, cPickle.dumps(job._uid))
                        conn.close()
                        prog_thread.start()
                        continue
                    else:
                        resp = 'NAK (invalid computation type "%s")' % compute.type
                conn.write_msg(job._uid, cPickle.dumps(resp))
                conn.close()
                continue
            elif msg.startswith('COMPUTE:'):
                msg = msg[len('COMPUTE:'):]
                try:
                    compute = cPickle.loads(msg)
                except:
                    logging.debug('Ignoring computation request from %s', addr[0])
                    resp = 'Invalid computation request'
                    conn.write_msg(uid, resp)
                    conn.close()
                    continue
                resp = 'ACK'
                compute.dest_path = compute.dest_path.strip().rstrip(os.sep)
                if compute.dest_path.startswith(os.sep):
                    logging.warning('Invalid destination path: "%s"', compute.dest_path)
                    resp = 'NACK (Invalid dest_path)'
                    conn.write_msg(uid, resp)
                    conn.close()
                    continue
                logging.debug('Adding computation %s', compute.name)
                compute.dest_path = os.path.join(self.dest_path_prefix, compute.dest_path)
                if not os.path.isdir(compute.dest_path):
                    try:
                        os.makedirs(compute.dest_path)
                    except:
                        logging.warning('Invalid destination path: "%s"', compute.dest_path)
                        resp = 'NACK (Invalid dest_path)'
                        conn.write_msg(uid, resp)
                        conn.close()
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
                    code = base64.b64decode(compute.code)
                    self.computations[compute.id].code = compile(code, '<string>', 'exec')
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
                        self.clients[data['ip_addr']] = data['port']
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
                        sock.write_msg(0, 'restart')
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
                    if compute:
                        logging.debug('Deleting computation "%s"', compute.name)
                        assert compute.dest_path.startswith(self.dest_path_prefix)
                    else:
                        logging.warning('Computation "%s" is not valid', compute_id)
                    if compute and info['cleanup']:
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
            elif msg.startswith('CANCEL_JOB:'):
                msg = msg[len('CANCEL_JOB:'):]
                try:
                    job = cPickle.loads(msg)
                    compute = self.computations[job._compute_id]
                except:
                    logging.debug('Ignoring job request from %s', addr[0])
                    continue
                self.lock.acquire()
                proc = self.procs.pop(uid, None)
                self.lock.release()
                if proc is None:
                    logging.debug('Job %s completed; ignoring cancel request from %s',
                                  uid, addr[0])
                    continue
                try:
                    logging.debug('Killing job %s', uid)
                    proc.terminate()
                    if isinstance(proc, multiprocessing.Process):
                        proc.join(2)
                    else:
                        proc.wait()
                    self.lock.acquire()
                    self.avail_cpus += 1
                    self.lock.release()
                except:
                    print traceback.format_exc()
                logging.debug('Killed process for job %s', uid)
                reply = {'result':None, 'stdout':None, 'stderr':None, 'exception':'Cancelled',
                         'hash':job._hash}
                reply_addr = (addr[0], compute.job_result_port)
                _send_job_result(uid, cPickle.dumps(reply), reply_addr, self.certfile, self.keyfile)
                if self.avail_cpus == self.cpus:
                    self.send_pong_msg()
            else:
                logging.warning('Invalid request "%s" from %s',
                                msg[:min(10, len(msg))], addr[0])
                resp = 'NAK (invalid command: %s)' % (msg[:min(10, len(msg))])
            if resp:
                conn.write_msg(uid, resp)
            conn.close()

    def __proc_Q_process(self):
        while True:
            info = self.proc_Q.get()
            if info is None:
                break
            self.lock.acquire()
            proc = self.procs.pop(info['uid'], None)
            if proc is not None:
                self.avail_cpus += 1
            self.lock.release()
            if proc is not None:
                proc.join()
                reply = {'result':info['result'], 'stdout':info['stdout'],
                         'stderr':info['stderr'], 'exception':info['exception'],
                         'hash':info['hash']}
                _send_job_result(info['uid'], cPickle.dumps(reply), info['reply_addr'],
                                 self.certfile, self.keyfile)
            if self.avail_cpus == self.cpus:
                self.send_pong_msg()
        self.proc_Q = None

    def __job_program(self, job):
        program = [self.computations[job._compute_id].name]
        args = cPickle.loads(job._args)
        program.extend(args)
        logging.debug('Executing "%s"', str(program))
        stdout = stderr = ret_val = exception = None
        proc = None
        try:
            proc = subprocess.Popen(program, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                    env={'PATH':self.computations[job._compute_id].env.get('PATH')})
            assert isinstance(proc, subprocess.Popen)
            self.lock.acquire()
            self.procs[job._uid] = proc
            self.lock.release()
            stdout, stderr = proc.communicate()
            ret_val = proc.returncode
        except Exception:
            logging.debug('Executing %s failed with %s', str(program), str(sys.exc_info()))
            exception = traceback.format_exc()
        self.lock.acquire()
        if self.procs.pop(job._uid, None) != proc:
            self.lock.release()
            return
        self.avail_cpus += 1
        self.lock.release()
        result = cPickle.dumps({'result':ret_val, 'stdout':stdout, 'stderr':stderr,
                                'exception':exception, 'hash':job._hash})
        for f in job._files:
            # logging.debug('Removing job file "%s"', f)
            if os.path.isfile(f):
                os.remove(f)
        _send_job_result(job._uid, result, job.reply_addr, self.certfile, self.keyfile)
        if self.avail_cpus == self.cpus:
            self.send_pong_msg()

    def shutdown(self):
        self.lock.acquire()
        for uid, proc in self.procs.iteritems():
            if isinstance(proc, multiprocessing.Process):
                logging.debug('process %s for %s is alive: %s', proc.pid, uid, proc.is_alive())
                proc.terminate()
                proc.join()
        self.procs = {}
        clients = dict(self.clients)
        self.clients = {}
        self.lock.release()
        if self.proc_Q:
            self.proc_Q.put(None)
        if clients:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            for ip_addr, port in clients.iteritems():
                logging.debug('Sending TERMINATE to %s:%s', ip_addr, port)
                data = cPickle.dumps({'ip_addr':self.address[0], 'port':self.address[1],
                                      'sign':self.signature})
                sock.sendto('TERMINATED:%s' % data, (ip_addr, port))
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
    parser.add_argument('--secret', dest='secret', default='',
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
