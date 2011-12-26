#!/usr/bin/env python

# dispy: Distribute computations among CPUs/cores on a single machine or
# machines in cluster(s), grid, cloud etc. for parallel execution.

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
import socket
import inspect
import stat
import cPickle
import threading
import select
import struct
import base64
import logging
import weakref
import re
import ssl
import hashlib
import atexit
import traceback
import functools
import types
import itertools
import Queue

class DispyJob():
    """Job scheduled for execution with dispy.

    Once a job is scheduled (with a tuple of arguments), the __call__
    method can be invoked. This will wait until the job is
    complete. The result of the call (either the return value in case
    of python methods or the exit value in case of programs) will be
    returned; the result is also available as .result member if
    needed.  In addition, any output, error, exception messages from
    the job will be available as .stdout, .stderr and .exception
    members. The time when the job was submitted for execution on a
    node will be available as .start_time and when the job results
    became available as .end_time.

    .id field is initially set to None and may be assigned to any
    value that is appropriate.  This may be useful, for example, to
    distinguish one job from another.

    .state is read-only field; it is set to one of Created, Running,
    Finished, Cancelled, Terminated and ProvisionalResult, indicating
    current state of job.  If job is created for SharedJobCluster,
    state is not updated to Running when job is actually running.

    .ip_addr is read-inly field; it is set to IP address of node that
    executed job.

    .finish is a read-only event that is set when a job's results are
    available.

    """
    Created = 1
    Running = 2
    Finished = 3
    Cancelled = 4
    Terminated = 5
    ProvisionalResult = 6
    def __init__(self):
        # id can be assigned by user as appropriate (e.g., to distinguish jobs)
        self.id = None
        # rest are read-only
        self.result = None
        self.stdout = None
        self.stderr = None
        self.exception = None
        self.start_time = None
        self.state = DispyJob.Created
        self.ip_addr = None
        self.finish = threading.Event()

    def __call__(self, clear=True):
        self.finish.wait()
        if clear:
            self.finish.clear()
        return self.result

class _DispySocket():
    """Internal use only.
    """
    def __init__(self, sock, auth_code=None, certfile=None, keyfile=None, server=False):
        if certfile:
            self.sock = ssl.wrap_socket(sock, server_side=server, keyfile=keyfile,
                                        certfile=certfile, ssl_version=ssl.PROTOCOL_TLSv1)
        else:
            self.sock = sock
        self.auth_code = auth_code
        for method in ['connect', 'settimeout', 'close', 'bind', 'listen', 'accept']:
            setattr(self, method, getattr(self.sock, method))

    def read(self, data_len=4096):
        data = ''
        while len(data) < data_len:
            chunk = self.sock.recv(data_len - len(data))
            if not chunk:
                logging.error('Socket disconnected?')
                return data
            data += chunk
        return data

    def read_msg(self):
        try:
            info_len = struct.calcsize('>LL')
            info = self.read(info_len)
            if len(info) < info_len:
                logging.error('Socket disconnected?(%s, %s)', len(info), info_len)
                return (None, None)
            (uid, msg_len) = struct.unpack('>LL', info)
            assert msg_len > 0
            msg = self.read(msg_len)
            if len(msg) < msg_len:
                logging.error('Socket disconnected?(%s, %s)', len(msg), msg_len)
                return (None, None)
            return (uid, msg)
        except socket.timeout:
            logging.error('Socket disconnected(timeout)?')
            return (None, None)

    def write(self, data, auth=True):
        if auth and self.auth_code:
            self.sock.sendall(self.auth_code)
        self.sock.sendall(data)

    def write_msg(self, uid, data):
        if self.auth_code:
            self.sock.sendall(self.auth_code)
        self.sock.sendall(struct.pack('>LL', uid, len(data)) + data)

def _xor_string(data, key):
    """Internal use only.
    """
    if not key:
        return data
    return ''.join(hex(ord(x) ^ ord(y))[2:] for (x, y) in itertools.izip(data, itertools.cycle(key)))

def _node_name_ipaddr(node):
    """Internal use only.
    """
    if not node:
        return (None, None)
    if node.find('*') >= 0:
        return (node, node)
    try:
        socket.inet_pton(socket.AF_INET, node)
        try:
            name = socket.gethostbyaddr(node)[0]
        except:
            name = node
        return (name, node)
    except:
        try:
            ip_addr = socket.gethostbyname(node)
            return (node, ip_addr)
        except:
            return (None, None)

def _parse_nodes(nodes):
    """Internal use only.
    """
    node_spec = {}
    for node in nodes:
        node_port = None
        if isinstance(node, str):
            name, ip_addr = _node_name_ipaddr(node)
            if not ip_addr:
                if node.find('*') >= 0:
                    name, ip_addr = node, node
                else:
                    logging.warning('Node "%s" is invalid; ignoring it.', str(node))
                    continue
            match_re = ip_addr.replace('.', '\\.').replace('*', '.*')
            node_port = None
        elif isinstance(node, tuple):
            name, ip_addr = _node_name_ipaddr(node[0])
            if not ip_addr:
                if node[0].find('*') >= 0:
                    name, ip_addr = node[0], node[0]
                else:
                    logging.warning('Node "%s" is invalid; ignoring it.')
                    continue
            match_re = ip_addr.replace('.', '\\.').replace('*', '.*')
            if len(node) == 2:
                node_port = node[1]
            else:
                logging.warning('Node "%s" is invalid; ignoring it', str(node))
                continue
        else:
            logging.warning('Node "%s" is invalid; ignoring it', str(node))
            continue
        node_spec[match_re] = {'port':node_port, 'ip_addr':ip_addr, 'name':name}
    return node_spec

class _Compute():
    """Internal use only.
    """
    func_type = 1
    prog_type = 2
    def __init__(self, compute_type, name):
        assert compute_type == _Compute.func_type or compute_type == _Compute.prog_type
        self.type = compute_type
        self.name = name
        self.id = None
        self.code = None
        self.dest_path = ''
        self.job_result_port = None
        self.env = None
        self.xfer_files = []
        self.cleanup = True
        self.auth_code = None
        self.node_ip = None
        self.node_job_result_port = None
        self.nodes = {}
        self.node_spec = None
        self.resubmit = False

    def __getstate__(self):
        state = dict(self.__dict__)
        if 'auth_code' in state:
            del state['auth_code']
            del state['nodes']
        return state

class _XferFile():
    """Internal use only.
    """
    def __init__(self, name, stat_buf, compute_id=None):
        self.name = name
        self.stat_buf = stat_buf
        self.compute_id = compute_id

class _Node():
    """Internal use only.
    """
    def __init__(self, ip_addr, port, cpus, sign, secret, keyfile=None, certfile=None):
        self.ip_addr = ip_addr
        self.port = port
        self.cpus = cpus
        self.name = None
        self.jobs = 0
        self.clusters = []
        self.cpu_time = 0
        self.busy = 0
        self.files_xferred = {}
        self.compute_files_xferred = {}
        self.auth_code = hashlib.sha1(_xor_string(sign, secret)).hexdigest()
        self.sign = sign
        self.secret = secret
        self.keyfile = keyfile
        self.certfile = certfile
        self.last_pulse = None
        logging.debug('Auth for %s: %s, %s, %s', ip_addr, self.auth_code,
                      self.keyfile, self.certfile)

    def setup(self, compute):
        logging.debug('Sending computation "%s" to %s', compute.name, self.ip_addr)
        resp = self.send(0, 'COMPUTE:' + cPickle.dumps(compute))

        if isinstance(resp, str) and resp.startswith('ACK'):
            resp = resp[len('ACK'):]
            if resp.startswith(':XFER_FILES:'):
                resp = resp[len(':XFER_FILES:'):]
                try:
                    xfer_files = cPickle.loads(resp)
                except:
                    logging.error("Couldn't transfer file '%s'", xf.name)
                    return -1
                for xf in xfer_files:
                    resp = self.xfer_file(xf)
                    if resp != 'ACK':
                        logging.error("Couldn't transfer file '%s'", xf.name)
                        return -1
            elif not resp:
                pass
            else:
                logging.error('Invalid response to computation: %s', resp)
                return -1
        else:
            logging.warning('Got "%s": Ignoring node %s', resp, self.ip_addr)
            return -1
        return 0

    def send(self, uid, msg, reply=True):
        logging.debug('Sending to %s:%s', self.ip_addr, self.port)
        sock = _DispySocket(socket.socket(socket.AF_INET, socket.SOCK_STREAM),
                            auth_code=self.auth_code, certfile=self.certfile, keyfile=self.keyfile)
        sock.settimeout(5)
        try:
            sock.connect((self.ip_addr, self.port))
            # logging.debug('uid: %s, msg len: %s', uid, len(msg))
            sock.write_msg(uid, msg)
            if reply:
                ruid, resp = sock.read_msg()
                # assert ruid == uid
            else:
                resp = None
        except Exception:
            logging.error("Couldn't connect to %s:%s, %s",
                          self.ip_addr, self.port, str(sys.exc_ifo()))
            raise
            # TODO: mark this node down, reschedule on different node?
            resp = None
        sock.close()
        return resp

    def xfer_file(self, xf):
        logging.debug('xfer_file: %s to %s', xf.name, self.ip_addr)
        sock = _DispySocket(socket.socket(socket.AF_INET, socket.SOCK_STREAM),
                            auth_code=self.auth_code, certfile=self.certfile, keyfile=self.keyfile)
        sock.settimeout(10)
        try:
            sock.connect((self.ip_addr, self.port))
            msg = 'FILEXFER:' + cPickle.dumps(xf)
            sock.write_msg(0, msg)
            fd = open(xf.name, 'rb')
            while True:
                data = fd.read(10240000)
                if not data:
                    break
                sock.write(data, auth=False)
            fd.close()
            ruid, resp = sock.read_msg()
        except:
            logging.error("Couldn't transfer %s to %s", xf.name, self.ip_addr)
            raise
            # TODO: mark this node down, reschedule on different node?
            resp = None
        sock.close()
        return resp

    def close(self, compute):
        logging.debug('Closing node %s for %s', self.ip_addr, compute.name)
        msg = 'DEL_COMPUTE:' + cPickle.dumps({'_compute_id':compute.id, 'cleanup':compute.cleanup})
        try:
            self.send(0, msg, False)
            self.clusters.remove(compute.id)
        except:
            pass

class _DispyJob_():
    """Internal use only.
    """
    lock = threading.Lock()
    jobs = {}
    def __init__(self, compute_id, args, kwargs):
        self.job = DispyJob()
        _DispyJob_.lock.acquire()
        _DispyJob_.jobs[self.job] = self
        _DispyJob_.lock.release()
        # fields below are for internal use only
        self._uid = None
        self._node = None
        self._compute_id = compute_id
        self._files = []
        self._hash = os.urandom(20).encode('hex')
        job_deps = kwargs.pop('dispy_job_depends', [])
        if not isinstance(job_deps, list):
            job_deps = list(job_deps)
        self._args = cPickle.dumps(args)
        self._kwargs = cPickle.dumps(kwargs)
        depend_ids = {}
        for dep in job_deps:
            if isinstance(dep, str) or inspect.ismodule(dep):
                if inspect.ismodule(dep):
                    dep = dep.__file__
                    if dep.endswith('.pyc'):
                        dep = dep[:-1]
                    if not dep.endswith('.py'):
                        raise Exception('Invalid module "%s" - must be python source.' % dep)
                if dep in depend_ids:
                    continue
                try:
                    fd = open(dep, 'r')
                    fd.close()
                except:
                    raise Exception('File "%s" is not valid' % dep)
                sbuf = os.stat(dep)
                fd = open(dep, 'rb')
                data = fd.read()
                fd.close()
                self._files.append({'name':dep, 'stat':sbuf, 'data':data})
                depend_ids[dep] = dep
            elif inspect.isfunction(dep) or inspect.ismethod(dep) or \
                     inspect.isclass(dep) or hasattr(dep, '__class__'):
                if inspect.isfunction(dep) or inspect.isclass(dep):
                    pass
                elif inspect.ismethod(dep):
                    dep = dep.im_class
                elif hasattr(dep, '__class__') and inspect.isclass(dep.__class__):
                    dep = dep.__class__
                if id(dep) in depend_ids:
                    continue
                lines = inspect.getsourcelines(dep)[0]
                lines[0] = lines[0].lstrip()
                self._code += '\n' + ''.join(lines)
                depend_ids[id(dep)] = id(dep)
            else:
                logging.warning('Invalid job depends element "%s"; ignoring it.', dep)

    def __getstate__(self):
        state = {'_args':self._args, '_kwargs':self._kwargs, '_files':self._files,
                 '_compute_id':self._compute_id, '_uid':self._uid,
                 '_hash':self._hash}
        return state

    def _run(self):
        if hasattr(self, 'job') and not self.job.state == DispyJob.Created:
            logging.warning('invalid job state %s for %s', self.job.state, self._uid)
        logging.debug('running job %s on %s', self._uid, self._node.ip_addr)
        resp = self._node.send(self._uid, 'JOB:' + cPickle.dumps(self))
        resp = cPickle.loads(resp)
        # TODO: deal with NAKs (reschedule?)
        if not isinstance(resp, int):
            logging.warning('Failed to run %s on %s', self._uid, self._node.ip_addr)
            raise Exception(str(resp))
        assert resp == self._uid
        if hasattr(self, 'job'):
            self.job.state = DispyJob.Running
        else:
            self.state = DispyJob.Running
        return resp

    def finish(self, state):
        self.job.state = state
        self.job.finish.set()
        if state != DispyJob.ProvisionalResult:
            _DispyJob_.lock.acquire()
            del _DispyJob_.jobs[self.job]
            _DispyJob_.lock.release()

class MetaSingleton(type):
    __instance = None
    def __call__(cls, *args, **kw):
        if cls.__instance is None:
            cls.__instance = super(MetaSingleton, cls).__call__(*args, **kw)
        return cls.__instance

class _Cluster(object):
    """Internal use only.
    """
    __metaclass__ = MetaSingleton

    def __init__(self, loglevel, ip_addr=None, port=None, node_port=None,
                 secret='', keyfile=None, certfile=None, shared=False):
        if not hasattr(self, 'ip_addr'):
            atexit.register(self.shutdown)
            if not loglevel:
                loglevel = logging.WARNING
            logging.basicConfig(format='%(asctime)s %(message)s', level=loglevel)
            if ip_addr:
                ip_addr = _node_name_ipaddr(ip_addr)[1]
            else:
                ip_addr = socket.gethostbyname(socket.gethostname())
            if port is None:
                port = 51347
            if not node_port:
                node_port = 51348

            self.ip_addr = ip_addr
            self.port = port
            self.node_port = node_port
            self._nodes = {}
            self._secret = secret
            self.keyfile = keyfile
            self.certfile = certfile
            self.shared = shared
            self.pulse_interval = None
            self.ping_interval = None

            self._clusters = {}
            self.num_jobs = 0
            self.job_uid = 1
            self._sched_jobs = {}
            self._sched_cv = threading.Condition()
            self.terminate_scheduler = False
            self.auth_code = os.urandom(20).encode('hex')
            self.cmd_sock = _DispySocket(socket.socket(socket.AF_INET, socket.SOCK_STREAM),
                                         auth_code=self.auth_code)
            self.cmd_sock.bind((self.ip_addr, 0))
            self.cmd_sock.listen(1)

            self._ready = threading.Event()
            self._listener = threading.Thread(target=self.__listen)
            self._listener.daemon = True
            self._listener.start()

            self._callbacks_Q = Queue.Queue()
            self._callback_thread = threading.Thread(target=self._call_callbacks)
            self._callback_thread.daemon = True
            self._callback_thread.start()

            # self.select_job_node = self.fast_node_schedule
            self.select_job_node = self.load_balance_schedule
            self._scheduler = threading.Thread(target=self.__schedule)
            self._scheduler.daemon = True
            self._scheduler.start()
            self.start_time = time.time()
            self.cluster_id = 1
        self._ready.wait()

    def send_ping_cluster(self, cluster):
        ping_request = cPickle.dumps({'scheduler_ip_addr':self.ip_addr,
                                      'scheduler_port':self.port})
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        for node_spec, node_info in cluster._compute.node_spec.iteritems():
            if node_spec.find('*') >= 0:
                port = node_info['port']
                if not port:
                    port = self.node_port
                bc_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                bc_sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
                bc_sock.sendto('PING:%s' % ping_request, ('<broadcast>', port))
                bc_sock.close()
            else:
                ip_addr = node_info['ip_addr']
                if ip_addr in cluster._compute.nodes:
                    continue
                port = node_info['port']
                if not port:
                    port = self.node_port
                sock.sendto('PING:%s' % ping_request, (ip_addr, port))
        sock.close()

    def add_cluster(self, cluster):
        compute = cluster._compute
        self._sched_cv.acquire()
        if self.shared:
            assert cluster._compute.id not in self._clusters
            self._clusters[compute.id] = cluster
            for xf in compute.xfer_files:
                xf.compute_id = compute.id
            self._sched_cv.release()
            return
        else:
            self._clusters[self.cluster_id] = cluster
            cluster._id = self.cluster_id
            compute.id = cluster._id
            for xf in compute.xfer_files:
                xf.compute_id = compute.id
            self.cluster_id += 1
        self._sched_cv.release()

        if cluster.pulse_interval:
            self.pulse_interval = max(self.pulse_interval, cluster.pulse_interval)
        if cluster.ping_interval:
            self.ping_interval = max(self.ping_interval, cluster.ping_interval)
        if cluster.pulse_interval or cluster.ping_interval:
            sock = _DispySocket(socket.socket(socket.AF_INET, socket.SOCK_STREAM),
                                auth_code=self.auth_code)
            sock.settimeout(5)
            sock.connect((self.ip_addr, self.cmd_sock.sock.getsockname()[1]))
            sock.write_msg(0, 'reset_interval')
            sock.close()

        self.send_ping_cluster(cluster)
        self._sched_cv.acquire()
        compute_nodes = []
        for node_spec, host in compute.node_spec.iteritems():
            for ip_addr, node in self._nodes.iteritems():
                if ip_addr in compute.nodes:
                    continue
                if re.match(node_spec, ip_addr):
                    compute_nodes.append(node)
                    break
        # self._sched_cv.notify()
        self._sched_cv.release()

        for node in compute_nodes:
            if node.setup(compute):
                logging.warning('Failed to setup %s for compute "%s"',
                                node.ip_addr, compute)
            else:
                self._sched_cv.acquire()
                if node.ip_addr not in compute.nodes:
                    compute.nodes[node.ip_addr] = node
                    node.clusters.append(compute.id)
                    self._sched_cv.notify()
                self._sched_cv.release()

    def _setup_node(self, node, computations):
        for compute in computations:
            if node.setup(compute):
                logging.warning('Failed to setup %s for compute "%s"',
                                node.ip_addr, compute)
            else:
                self._sched_cv.acquire()
                if node.ip_addr not in compute.nodes:
                    compute.nodes[node.ip_addr] = node
                    node.clusters.append(compute.id)
                    self._sched_cv.notify()
                self._sched_cv.release()

    def _call_callbacks(self):
        while True:
            item = self._callbacks_Q.get(block=True)
            cb, job = item
            cb(job)


    def __listen(self):
        def cancel_jobs(dead_jobs):
            # called with _sched_cv locked
            for _job in dead_jobs:
                cluster = self._clusters[_job._compute_id]
                del self._sched_jobs[_job._uid]
                if cluster._compute.resubmit:
                    logging.debug('Rescheduling job %s from %s',
                                  _job._uid, _job._node.ip_addr)
                    _job.job.state = DispyJob.Created
                    cluster._jobs.append(_job)
                    self.num_jobs += 1
                else:
                    logging.debug('Cancelling job %s scheduled on %s',
                                  _job._uid, _job._node.ip_addr)
                    _job.job.result = _job.job.stdout = _job.job.stderr = None
                    _job.exception = 'Cancelled'
                    cluster._pending_jobs -= 1
                    if cluster._pending_jobs == 0:
                        cluster._complete.set()
                        cluster.end_time = time.time()
                    _job.finish(DispyJob.Cancelled)

        job_result_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        job_result_sock.bind((self.ip_addr, 0))
        job_result_sock.listen(2)
        self.job_result_port = job_result_sock.getsockname()[1]
        logging.info('Listening at %s, %s, %s',
                     self.ip_addr, self.port, self.job_result_port)

        socks = [self.cmd_sock.sock, job_result_sock]
        if not self.shared:
            ping_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            ping_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            ping_sock.bind(('', self.port))
            socks.append(ping_sock)
        else:
            ping_sock = None
        self._ready.set()

        if self.pulse_interval:
            pulse_timeout = 5.0 * self.pulse_interval
        else:
            pulse_timeout = None
        if pulse_timeout and self.ping_interval:
            timeout = min(pulse_timeout, self.ping_interval)
        else:
            timeout = max(pulse_timeout, self.ping_interval)

        last_pulse_time = time.time()
        last_ping_time = last_pulse_time
        while True:
            ready = select.select(socks, [], [], timeout)[0]
            for sock in ready:
                if sock == job_result_sock:
                    conn, addr = job_result_sock.accept()
                    conn = _DispySocket(conn, certfile=self.certfile,
                                        keyfile=self.keyfile, server=True)
                    uid, msg = conn.read_msg()
                    conn.close()
                    logging.debug('Received reply for job %s from %s', uid, addr[0])
                    self._sched_cv.acquire()
                    node = self._nodes.get(addr[0], None)
                    if node is None:
                        job_ip = None
                    else:
                        job_ip = node.ip_addr
                        node.last_pulse = time.time()
                        # logging.debug('Node %s busy: %s', node.ip_addr, node.busy)
                    _job = self._sched_jobs.get(uid, None)
                    if _job is None:
                        self._sched_cv.release()
                        logging.warning('Ignoring invalid reply for job %s from %s',
                                        uid, addr[0])
                        continue
                    job = _job.job
                    # if job.state not in [DispyJob.Running, DispyJob.Cancelled, DispyJob.Created]:
                    #     logging.warning('invalid job state: %s', job.state)
                    job.end_time = time.time()
                    try:
                        result = cPickle.loads(msg)
                        assert result['hash'] == _job._hash
                        job.result = result['result']
                        job.stdout = result['stdout']
                        job.stderr = result['stderr']
                        job.exception = result['exception']
                        if 'provisional' in result:
                            logging.debug('Receveid provisional result for %s', job.id)
                            _job.finish(DispyJob.ProvisionalResult)
                            cluster = self._clusters[_job._compute_id]
                            self._sched_cv.release()
                            if cluster.callback:
                                self._callbacks_Q.put((cluster.callback, job))
                            continue
                        if 'start_time' in result:
                            # this came from shared scheduler
                            job.start_time = result['start_time']
                            job.end_time = result['end_time']
                            job_ip = result['ip_addr']
                    except:
                        self._sched_cv.release()
                        logging.warning('Invalid job result for %s from %s',
                                        uid, addr[0])
                        logging.debug(traceback.format_exc())
                        continue

                    if job_ip is None:
                        self._sched_cv.release()
                        logging.warning('Ignoring invalid reply for job %s from %s',
                                        uid, addr[0])
                        continue

                    del self._sched_jobs[uid]
                    cluster = self._clusters[_job._compute_id]
                    compute = cluster._compute
                    node = compute.nodes.get(job_ip, None)
                    if node is None:
                        node = self._nodes.get(job_ip, None)
                        if node is None:
                            # this came from dispyscheduler
                            node = type('_Node', (),
                                        {'ip_addr':job_ip, 'cpus':'', 'jobs':0,
                                         'cpu_time':0, 'busy':0})
                            self._nodes[job_ip] = node
                        compute.nodes[job_ip] = node
                        if not node.cpus and 'cpus' in result:
                            node.cpus = result['cpus']
                    if job.state == DispyJob.Cancelled:
                        logging.debug('Cancelled job: %s, %s', _job._uid, job.exception)
                        if _job._node is not None:
                            node.busy -= 1
                    elif job.exception == 'Cancelled':
                        # shared scheduler cancelled this job (e.g.,
                        # due to a node not responding)
                        assert _job._node is None
                        if job.state != DispyJob.Cancelled:
                            _job.finish(DispyJob.Cancelled)
                            cluster._pending_jobs -= 1
                            if cluster._pending_jobs == 0:
                                cluster._complete.set()
                                cluster.end_time = time.time()
                    elif job.state == DispyJob.Finished:
                        logging.debug('Ignoring job %s - it is already done by %s',
                                      _job._uid, job.ip_addr)
                    else:
                        job.ip_addr = job_ip
                        node.jobs += 1
                        node.cpu_time += job.end_time - job.start_time
                        cluster._pending_jobs -= 1
                        if cluster._pending_jobs == 0:
                            cluster._complete.set()
                            cluster.end_time = time.time()
                        _job.finish(DispyJob.Finished)
                        if _job._node is not None:
                            node.busy -= 1
                    self._sched_cv.notify()
                    self._sched_cv.release()
                    if cluster.callback:
                        self._callbacks_Q.put((cluster.callback, job))
                elif sock == ping_sock:
                    msg, addr = ping_sock.recvfrom(1024)
                    if msg.startswith('PULSE:'):
                        msg = msg[len('PULSE:'):]
                        try:
                            info = cPickle.loads(msg)
                            node = self._nodes[info['ip_addr']]
                            assert 0 <= info['cpus'] <= node.cpus
                            node.last_pulse = time.time()
                            logging.debug('pulse from %s at %s',
                                          info['ip_addr'], node.last_pulse)
                        except:
                            logging.warning('Ignoring pulse message from %s', addr[0])
                            #logging.debug(traceback.format_exc())
                            continue
                    elif msg.startswith('PONG:'):
                        try:
                            status = cPickle.loads(msg[len('PONG:'):])
                            assert status['port'] > 0 and status['cpus'] > 0
                            assert len(status['ip_addr']) > 0
                        except:
                            logging.debug('Ignoring node %s', addr[0])
                            continue
                        logging.debug('Discovered %s:%s with %s cpus',
                                      status['ip_addr'], status['port'], status['cpus'])
                        if status['cpus'] <= 0:
                            logging.debug('Ignoring node %s', status['ip_addr'])
                            continue
                        self._sched_cv.acquire()
                        node = self._nodes.get(status['ip_addr'], None)
                        if node is None:
                            self._sched_cv.release()
                            try:
                                node = _Node(status['ip_addr'], status['port'],
                                             status['cpus'], status['sign'],
                                             self._secret, self.keyfile, self.certfile)
                                data = {'ip_addr':self.ip_addr, 'port':self.port,
                                        'cpus':node.cpus, 'pulse_interval':self.pulse_interval}
                                resp = node.send(0, 'RESERVE:' + cPickle.dumps(data))
                            except:
                                logging.warning("Couldn't setup node %s; ignoring it.",
                                                status['ip_addr'])
                                del node
                                continue
                            if resp != 'ACK':
                                logging.warning('Ignoring node %s', status['ip_addr'])
                                del node
                                continue
                            self._sched_cv.acquire()
                            self._nodes[node.ip_addr] = node
                        else:
                            node.port = status['port']
                            node.last_pulse = time.time()
                            h = _xor_string(status['sign'], self._secret)
                            node.auth_code = hashlib.sha1(h).hexdigest()
                        self._nodes[node.ip_addr] = node
                        node_computations = []
                        for cid, cluster in self._clusters.iteritems():
                            compute = cluster._compute
                            if node.ip_addr in compute.nodes:
                                continue
                            for node_spec, host in compute.node_spec.iteritems():
                                if re.match(node_spec, node.ip_addr):
                                    if host['name'].find('*') >= 0:
                                        node.name = node.ip_addr
                                    else:
                                        node.name = host['name']
                                    node_computations.append(compute)
                                    break
                        self._sched_cv.release()
                        if node_computations:
                            setup_thread = threading.Thread(target=self._setup_node,
                                                            args=(node, node_computations))
                            setup_thread.start()
                    elif msg.startswith('TERMINATED:'):
                        try:
                            data = cPickle.loads(msg[len('TERMINATED:'):])
                            self._sched_cv.acquire()
                            node = self._nodes.pop(data['ip_addr'], None)
                            if not node:
                                self._sched_cv.release()
                                continue
                            logging.debug('Removing node %s', node.ip_addr)
                            h = _xor_string(data['sign'], self._secret)
                            auth_code = hashlib.sha1(h).hexdigest()
                            if auth_code != node.auth_code:
                                logging.warning('Invalid signature from %s', node.ip_addr)
                            dead_jobs = [_job for _job in self._sched_jobs.itervalues() \
                                         if _job._node is not None and _job._node.ip_addr == node.ip_addr]
                            cancel_jobs(dead_jobs)
                            for cid, cluster in self._clusters.iteritems():
                                cluster._compute.nodes.pop(node.ip_addr, None)
                            self._sched_cv.release()
                            del node
                        except Exception:
                            logging.debug('Removing node failed: %s',
                                          str(sys.exc_info()))
                            pass
                    else:
                        logging.debug('Ignoring PONG message %s from: %s',
                                      msg[:min(5, len(msg))], addr[0])
                        continue
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
                    if msg == 'reset_interval':
                        if self.pulse_interval:
                            pulse_timeout = 5.0 * self.pulse_interval
                        else:
                            pulse_timeout = None
                        if pulse_timeout and self.ping_interval:
                            timeout = min(pulse_timeout, self.ping_interval)
                        else:
                            timeout = max(pulse_timeout, self.ping_interval)
                    elif msg == 'terminate':
                        logging.debug('Terminating all running jobs')
                        self._sched_cv.acquire()
                        self.terminate_scheduler = True
                        for uid, _job in self._sched_jobs.iteritems():
                            _job.job.result = _job.job.stdout = _job.job.stderr = None
                            _job.job.exception = 'Terminated'
                            _job.job.end_time = None
                            _job.finish(DispyJob.Terminated)
                        self._sched_jobs = {}
                        self._sched_cv.notify()
                        self._sched_cv.release()
                        logging.debug('listener is terminated')
                        self.cmd_sock.close()
                        self.cmd_sock = None
                        return
                    else:
                        logging.warning('Invalid command: %s', msg)

            if timeout:
                now = time.time()
                if pulse_timeout and (now - last_pulse_time) >= pulse_timeout:
                    last_pulse_time = now
                    self._sched_cv.acquire()
                    dead_nodes = {}
                    for node in self._nodes.itervalues():
                        if node.busy and (node.last_pulse + pulse_timeout) <= now:
                            logging.warning('Node %s is not responding; removing it (%s, %s, %s)',
                                            node.ip_addr, node.busy, node.last_pulse, now)
                            dead_nodes[node.ip_addr] = node
                    for ip_addr in dead_nodes:
                        del self._nodes[ip_addr]
                        for cluster in self._clusters.itervalues():
                            cluster._compute.nodes.pop(ip_addr, None)
                    dead_jobs = [_job for _job in self._sched_jobs.itervalues() \
                                 if _job._node is not None and _job._node.ip_addr in dead_nodes]
                    cancel_jobs(dead_jobs)
                    if dead_nodes or dead_jobs:
                        self._sched_cv.notify()
                    self._sched_cv.release()
                if self.ping_interval and (now - last_ping_time) >= self.ping_interval:
                    last_ping_time = now
                    self._sched_cv.acquire()
                    for cluster in self._clusters.itervalues():
                        self.send_ping_cluster(cluster)
                    self._sched_cv.release()

    def load_balance_schedule(self):
        node = None
        load = None
        for ip_addr, host in self._nodes.iteritems():
            if host.busy >= host.cpus:
                continue
            if all(not self._clusters[cluster_id]._jobs for cluster_id in host.clusters):
                continue
            # logging.debug('load: %s, %s, %s' % (host.ip_addr, host.busy, host.cpus))
            if (load is None) or ((float(host.busy) / host.cpus) < load):
                node = host
                load = float(node.busy) / node.cpus
        return node

    def fast_node_schedule(self):
        # as we eagerly schedule, this has limited advantages
        # (useful only when  we have data about all the nodes and more than one node
        # is currently available)
        # in addition, we assume all jobs take equal time to execute
        node = None
        secs_per_job = None
        for ip_addr, host in self._nodes.iteritems():
            if host.busy >= host.cpus:
                continue
            if all(not self._clusters[cluster_id]._jobs for cluster_id in host.clusters):
                continue
            # logging.debug('load: %s, %s, %s' % (host.ip_addr, host.jobs, host.cpu_time))
            if (secs_per_job is None) or (host.jobs == 0) or \
                   ((host.cpu_time / host.jobs) <= secs_per_job):
                node = host
                if host.jobs == 0:
                    secs_per_job = 0
                else:
                    secs_per_job = host.cpu_time / host.jobs
        return node

    def __schedule(self):
        while True:
            self._sched_cv.acquire()
            if self.terminate_scheduler:
                self._sched_cv.release()
                break
            start_time = time.time()
            while self.num_jobs:
                logging.debug('Pending jobs: %s', self.num_jobs)
                node = self.select_job_node()
                if node is None:
                    logging.debug('No nodes/jobs')
                    break
                for cid in node.clusters:
                    if self._clusters[cid]._jobs:
                        _job = self._clusters[cid]._jobs.pop(0)
                        break
                else:
                    break
                cluster = self._clusters[_job._compute_id]
                compute = cluster._compute
                if _job.job.start_time == start_time:
                    logging.warning('Job %s is rescheduled too quickly; ' \
                                    'scheduler is sleeping', _job._uid)
                    cluster._jobs.append(_job)
                    break
                _job.job.start_time = start_time
                _job._node = node
                logging.debug('Scheduling job %s on %s (load: %.3f, %s)',
                              _job._uid, node.ip_addr, float(node.busy) / node.cpus, node.busy)
                assert node.busy < node.cpus
                try:
                    _job._run()
                except EnvironmentError:
                    logging.warning('Failed to run job %s on %s for computation %s; ' \
                                    'removing this node', _job._uid, node.ip_addr, compute.name)
                    cluster._jobs.insert(0, _job)
                    # TODO: close the node properly?
                    del compute.nodes[node.ip_addr]
                    node.clusters.remove(compute.id)
                    continue
                except Exception:
                    logging.warning('Failed to run job %s on %s for computation %s; ' \
                                    'rescheduling it', _job._uid, node.ip_addr, compute.name)
                    logging.debug(traceback.format_exc())
                    # raise
                    # TODO: delay executing again for some time?
                    cluster._jobs.append(_job)
                    continue
                self._sched_jobs[_job._uid] = _job
                self.num_jobs -= 1
                node.busy += 1
            self._sched_cv.wait()
            self._sched_cv.release()
        self._sched_cv.acquire()
        logging.debug('Scheduler quitting (%s / %s)',
                      len(self._sched_jobs), self.num_jobs)
        for cid, cluster in self._clusters.iteritems():
            compute = cluster._compute
            for node in compute.nodes.itervalues():
                node.close(compute)
            for _job in cluster._jobs:
                logging.debug('Finishing job %s', _job._uid)
                _job.job.result = _job.job.stdout = _job.job.stderr = None
                _job.job.exception = 'terminated'
                _job.finish(DispyJob.Terminated)
            cluster._jobs = []

        self._nodes = {}
        logging.debug('Scheduler quit')
        self._sched_cv.release()

    def submit_job(self, _job):
        self._sched_cv.acquire()
        _job._uid = self.job_uid
        logging.debug('Created job %s', _job._uid)
        self.job_uid += 1
        if self.job_uid == sys.maxint:
            # TODO: check if it is okay to reset
            self.job_uid = 1
        self.num_jobs += 1
        cluster = self._clusters[_job._compute_id]
        cluster._jobs.append(_job)
        cluster._pending_jobs += 1
        cluster._complete.clear()
        self._sched_cv.notify()
        self._sched_cv.release()
        return True

    def cancel_job(self, _job):
        self._sched_cv.acquire()
        logging.debug('Job %s state: %s', _job._uid, _job.job.state)
        cluster = self._clusters.get(_job._compute_id, None)
        if not cluster:
            logging.debug('Invalid job %s!', _job._uid)
            self._sched_cv.release()
            return
        cancel = True
        if _job.job.state == DispyJob.Created:
            logging.debug('Cancelled (removed) job %s', _job._uid)
            if not self.shared:
                cluster._jobs.remove(_job)
            cancel = False
        elif not (_job.job.state == DispyJob.Running or _job.job.state == DispyJob.ProvisionalResult):
            logging.warning('Job %s is not valid for cancel (%s)', _job._uid, _job.job.state)
            self._sched_cv.release()
            return
        assert cluster._pending_jobs > 0
        cluster._pending_jobs -= 1
        if cluster._pending_jobs == 0:
            cluster._complete.set()
            cluster.end_time = time.time()
        _job.finish(DispyJob.Cancelled)
        self._sched_cv.release()
        if cancel and _job._node is not None:
            _job._node.send(_job._uid, 'CANCEL_JOB:' + cPickle.dumps(_job), reply=False)
            logging.debug('Job %s is cancelled', _job._uid)

    def close(self, cluster):
        self._sched_cv.acquire()
        compute = cluster._compute
        for ip_addr in compute.nodes:
            node = self._nodes.get(ip_addr, None)
            if node is not None:
                node.close(compute)
        del self._clusters[compute.id]
        self._sched_cv.release()

    def shutdown(self):
        # TODO: make sure JobCluster instances are done
        if self._scheduler:
            logging.debug('Shutting down scheduler ...')
            self._sched_cv.acquire()
            self.terminate_scheduler = True
            self._sched_cv.notify()
            self._sched_cv.release()
            self._scheduler.join()
            self._scheduler = None
            self.num_jobs = 0
            self._sched_jobs = {}
        if self._listener and self.cmd_sock:
            logging.debug('Shutting down listener')
            sock = _DispySocket(socket.socket(socket.AF_INET, socket.SOCK_STREAM),
                                auth_code=self.auth_code)
            sock.settimeout(5)
            sock.connect((self.ip_addr, self.cmd_sock.sock.getsockname()[1]))
            sock.write_msg(0, 'terminate')
            sock.close()
            self._listener.join()
            self._listener = None
        if self.select_job_node:
            self.select_job_node = None

    def stats(self, compute, wall_time=None):
        print
        heading = '%020s  |  %05s  |  %05s  |  %010s  |  %13s' % \
                  ('Node', 'CPUs', 'Jobs', 'Sec/Job', 'Node Time Sec')
        print heading
        print '-' * len(heading)
        cpu_time = 0.0
        for ip_addr in sorted(compute.nodes, key=lambda addr: compute.nodes[addr].cpu_time,
                              reverse=True):
            node = compute.nodes[ip_addr]
            if node.jobs:
                secs_per_job = node.cpu_time / node.jobs
            else:
                secs_per_job = 0
            cpu_time += node.cpu_time
            print '%020s  |  %05s  |  %05s  |  %10.3f  |  %13.3f' % \
                  (ip_addr, node.cpus, node.jobs, secs_per_job, node.cpu_time)
        print
        msg = 'Total job time: %.3f sec' % cpu_time
        if wall_time:
            msg += ', wall time: %.3f sec, speedup: %.3f' % (wall_time, cpu_time / wall_time)
        print msg
        print

class JobCluster():
    """Create an instance of cluster for a specific job.
    """

    def __init__(self, computation, nodes=['*'], depends=[], callback=None,
                 ip_addr=None, port=None, node_port=None, dest_path='',
                 loglevel=None, cleanup=True, ping_interval=None, pulse_interval=None,
                 resubmit=False, secret='', keyfile=None, certfile=None):
        """Create an instance of cluster for a specific computation.

        @computation is either a string (which is name of program, possibly
        with full path) or a python object.

        @nodes is a list. Each element of @nodes is either a string
          (which must be either IP address or name of server node), or
          a tuple with up to 3 elements.  The tuple's first element
          must be IP address or name of server node, second element,
          if present, must be port number where that node is listening
          for ping from clients, the third element, if present, must
          be number of CPUs to use on that node.

        @depends is a list. Each element of @depends is either
          a string or a python object. If the element is a string,
          it must be a file which will be transferred to the node
          executing a job for this cluster.
          If the element is a python object (a function name, class name etc.),
          then the code for that object is transferred to the node executing
          a job for this cluster.

       @callback is a function. When a job's results become available,
          dispy will call provided callback function with that job as the
          argument. If a job sends provisional results with
          'dispy_provisional_result' multiple times, then dispy will call
          provided callback each such time. The (provisional) results of
          computation can be retrieved with 'result' field of job, etc.

        @ip_addr and @port indicate the address where the cluster will bind to.
          If multiple instances of JobCluster are used, these arguments are used
          only in the case of first instance.
          If no value for @ip_addr is given (default), IP address associated
          with the 'hostname' is used.
          If no value for @port is given (default), number 51347 is used.

        @node_port indicates port on which node servers are listening
          for ping messages. The client (JobCluster instance) broadcasts
          ping requests to this port.
          If no value for @node_port is given (default), number 51348 is used.

        @dest_path indicates path of directory to which files are
          transferred to a server node when executing a job.
          If @computation is a string, indicating a program, then that program is
          also transferred to @dest_path.

        @loglevel indicates message priority for logging module.

        @cleanup indicates if the files transferred should be removed when
          shutting down.

        @ping_interval is number of seconds between 1 and
        1000. Normally dispy can find nodes running 'dispynode' by
        broadcasting 'ping' messages that nodes respond to. However,
        these packets may get lost. If ping_interval is set, then
        every ping_interval seconds, dispy sends ping messages to find
        nodes that may have missed earlier ping messages.

        @pulse_interval is number of seconds between 1 and 1000. If
        pulse_interval is set, dispy directs nodes to send 'pulse'
        messages to indicate they are computing submitted jobs. A node
        is presumed dead if 5*pulse_interval elapses without a pulse
        message. See 'resubmit' below.

        @resubmit must be either True or False. This value is used
        only if 'pulse_interval' is set for any of the clusters. If
        pulse_interval is given and resubmit is False (default), jobs
        scheduled for a dead node are automatically cancelled; if
        resubmit is True, then jobs scheduled for a dead node are
        resubmitted to other eligible nodes.
        """

        if not isinstance(nodes, list):
            if isinstance(nodes, str):
                nodes = [nodes]
            else:
                raise Exception('"nodes" must be list of IP addresses or host names')
        if resubmit != True and resubmit != False:
            logging.warning('Invalid value for resubmit (%s) is ignored; ' \
                            'it must be either True or False' % resubmit)
            resubmit = False
        if ping_interval is not None:
            try:
                ping_interval = float(ping_interval)
                assert 1.0 <= ping_interval <= 1000
            except:
                raise Exception('Invalid ping_interval; must be between 1 and 1000')
        self.ping_interval = ping_interval
        if pulse_interval is not None:
            try:
                pulse_interval = float(pulse_interval)
                assert 1.0 <= pulse_interval <= 1000
            except:
                raise Exception('Invalid pulse_interval; must be between 1 and 1000')
        self.pulse_interval = pulse_interval
        atexit.register(self.close)
        if callback:
            assert inspect.isfunction(callback) or inspect.ismethod(callback)
        self.callback = callback
        if hasattr(self, 'scheduler_port'):
            shared = True
        else:
            shared = False
        self._cluster = _Cluster(loglevel, ip_addr=ip_addr, port=port, node_port=node_port,
                                 secret=secret, keyfile=keyfile, certfile=certfile,
                                 shared=shared)
        self.ip_addr = self._cluster.ip_addr
        self.job_result_port = self._cluster.job_result_port

        if inspect.isfunction(computation) or inspect.ismethod(computation):
            func = computation
            compute = _Compute(_Compute.func_type, func.func_name)
            # compute.env = {'PYTHONPATH':[os.getcwd()] + sys.path}
            compute.env = {'PYTHONPATH':[os.getcwd()]}
            lines = inspect.getsourcelines(func)[0]
            lines[0] = lines[0].lstrip()
            compute.code = ''.join(lines)
        elif isinstance(computation, str):
            compute = _Compute(_Compute.prog_type, computation)
            compute.env = {'PATH':os.getenv('PATH')}
            depends.append(computation)
        else:
            raise Exception('Invalid computation type: %s' % type(compute))
        if dest_path:
            compute.dest_path = dest_path
        depend_ids = {}
        for dep in depends:
            if isinstance(dep, str) or inspect.ismodule(dep):
                if inspect.ismodule(dep):
                    dep = dep.__file__
                    if dep.endswith('.pyc'):
                        dep = dep[:-1]
                    if not (dep.endswith('.py') and os.path.isfile(dep)):
                        raise Exception('Invalid module "%s" - must be python source.' % dep)
                if dep in depend_ids:
                    continue
                if compute.type == _Compute.prog_type and not os.path.isfile(dep):
                    for p in compute.env['PATH'].split(':'):
                        f = os.path.join(p, dep)
                        if os.path.isfile(f):
                            logging.debug('Assuming "%s" is program "%s"', dep, f)
                            dep = f
                            break
                    else:
                        raise Exception('Program "%s" is not valid', dep)
                try:
                    fd = open(dep, 'r')
                    fd.close()
                    xf = _XferFile(dep, os.stat(dep), compute.id)
                    compute.xfer_files.append(xf)
                    depend_ids[dep] = dep
                except:
                    raise Exception('File "%s" is not valid' % dep)
            elif inspect.isfunction(dep) or inspect.ismethod(dep) or \
                     inspect.isclass(dep) or hasattr(dep, '__class__'):
                if inspect.isfunction(dep) or inspect.isclass(dep):
                    pass
                elif inspect.ismethod(dep):
                    dep = dep.im_class
                elif hasattr(dep, '__class__') and inspect.isclass(dep.__class__):
                    dep = dep.__class__
                if id(dep) in depend_ids:
                    continue
                lines = inspect.getsourcelines(dep)[0]
                lines[0] = lines[0].lstrip()
                compute.code += '\n' + ''.join(lines)
                depend_ids[id(dep)] = id(dep)
            else:
                raise Exception('Invalid function/method: %s' % dep)
        if compute.code:
            # make sure code can be compiled
            code = compile(compute.code, '<string>', 'exec')
            del code
            compute.code = base64.b64encode(compute.code)
        compute.job_result_port = self._cluster.job_result_port
        compute.cleanup = cleanup
        compute.node_ip = self.ip_addr
        compute.node_job_result_port = self.job_result_port
        compute.resubmit = resubmit

        if not shared:
            compute.node_spec = _parse_nodes(nodes)
            if not compute.node_spec:
                raise Exception('"nodes" argument is invalid')

        self._compute = compute
        self._pending_jobs = 0
        self._jobs = []
        self._complete = threading.Event()
        self.cpu_time = 0
        self.start_time = time.time()
        self.end_time = None
        if not shared:
            self._cluster.add_cluster(self)

    def submit(self, *args, **kwargs):
        """Submit a job for execution with the given arguments.

        Arguments should be serializable and should correspond to
        arguments for computation used when cluster is created.
        """
        if self._compute.type == _Compute.prog_type:
            if kwargs:
                logging.warning("Programs can't have keyword arguments")
                return None
            args = [str(arg) for arg in args]
        try:
            _job = _DispyJob_(self._compute.id, args, kwargs)
        except Exception:
            logging.warning('Creating job for "%s", "%s" failed with "%s"',
                            str(args), str(kwargs), str(sys.exc_info()))
            return None
        if self._cluster.submit_job(_job):
            return _job.job
        else:
            return None

    def cancel(self, job):
        _job = _DispyJob_.jobs[job]
        self._cluster.cancel_job(_job)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.wait()

    def stats(self):
        """Show statistics for cluster(s).

        Prints various statistics, such as number of nodes, CPUs in each node,
        jobs performed by each node and time taken for each job and all jobs.
        """
        if self.start_time is None or self.end_time is None:
            time = None
        else:
            time = self.end_time - self.start_time

        self._cluster.stats(self._compute, time)

    def wait(self):
        """Wait for scheduled jobs to complete.
        """
        self._complete.wait()

    def __call__(self):
        """Wait for scheduled jobs to complete.
        """
        self.wait()

    def close(self):
        if hasattr(self, '_compute'):
            if self._pending_jobs > 0:
                logging.warning('Waiting for pending jobs to finish...')
                self._complete.wait()
            self._cluster.close(self)
            logging.debug('Cluster "%s" deleted', self._compute.name)
            del self._compute

    def __del__(self):
        self.close()

class SharedJobCluster(JobCluster):
    """SharedJobCluster should be used (instead of JobCluster) if two
    or more processes can simultaneously use dispy. In this case,
    'dispyscheduler' must be running on a node and 'scheduler_node'
    parameter should be set to that node's IP address or host name.

    @pulse_interval for SharedJobCluster is not used; instead,
    dispyscheduler must be called with appropriate pulse_interval.
    The behaviour is same as for JobCluster.
    """
    def __init__(self, computation, nodes=['*'], depends=[], callback=None,
                 ip_addr=None, port=None, scheduler_node=None, scheduler_port=None,
                 dest_path='', loglevel=logging.WARNING, cleanup=True,
                 pulse_interval=None, ping_interval=None, resubmit=False,
                 secret='', keyfile=None, certfile=None):
        if not isinstance(nodes, list):
            if isinstance(nodes, str):
                nodes = [nodes]
            else:
                raise Exception('"nodes" must be list of IP addresses or host names')
        if not scheduler_port:
            scheduler_port = 51349
        self.scheduler_port = scheduler_port
        JobCluster.__init__(self, computation, nodes='dummy', depends=depends,
                            callback=callback, ip_addr=ip_addr, port=port,
                            dest_path=dest_path, cleanup=cleanup, pulse_interval=None,
                            resubmit=resubmit, loglevel=loglevel)
        if pulse_interval is not None:
            logging.warning('pulse_interval is not used in SharedJobCluster; ' \
                            'dispyscheduler should be started appropriately.')
        self.pulse_interval = None

        self.certfile = certfile
        self.keyfile = keyfile
        self._cluster.terminate_scheduler = True
        self._cluster._sched_cv.acquire()
        self._cluster._sched_cv.notify()
        self._cluster._sched_cv.release()
        self._compute.node_spec = nodes
        if scheduler_node:
            self.scheduler_node = _node_name_ipaddr(scheduler_node)[1]
        else:
            self.scheduler_node = self.ip_addr

        scheduler_sock = _DispySocket(socket.socket(socket.AF_INET, socket.SOCK_STREAM),
                                      certfile=certfile, keyfile=keyfile)
        try:
            scheduler_sock.connect((self.scheduler_node, self.scheduler_port))
        except:
            raise Exception("Couldn't connect to scheduler at %s:%s" % \
                            (self.scheduler_node, self.scheduler_port))
        req = ' ' * len(hashlib.sha1('').hexdigest()) + 'CLUSTER'
        scheduler_sock.write(req)
        uid, msg = scheduler_sock.read_msg()
        signature = cPickle.loads(msg)
        self.auth_code = hashlib.sha1(_xor_string(signature['sign'], secret)).hexdigest()
        logging.debug('auth_code: %s', self.auth_code)
        scheduler_sock.close()

        scheduler_sock = _DispySocket(socket.socket(socket.AF_INET, socket.SOCK_STREAM),
                                      auth_code=self.auth_code, certfile=certfile, keyfile=keyfile)
        scheduler_sock.connect((self.scheduler_node, self.scheduler_port))
        req = 'COMPUTE:' + cPickle.dumps(self._compute)
        scheduler_sock.write_msg(0, req)
        uid, msg = scheduler_sock.read_msg()
        self._compute.id = cPickle.loads(msg)
        logging.debug('Computation %s created with %s', self._compute.name, self._compute.id)
        assert self._compute.id is not None
        scheduler_sock.close()
        self._cluster.job_uid = None
        self._cluster.add_cluster(self)

        node = type('_Node', (),
                    {'ip_addr':self.scheduler_node, 'cpus':'',
                     'jobs':0, 'cpu_time':0, 'busy':0})
        self._cluster._nodes[node.ip_addr] = node

    def submit(self, *args, **kwargs):
        """Submit a job for execution with the given arguments.

        Arguments should be serializable and should correspond to
        arguments for computation used when cluster is created.
        """
        if self._compute.type == _Compute.prog_type:
            if kwargs:
                logging.warning("Programs can't have keyword arguments")
                return None
            args = [str(arg) for arg in args]
        try:
            _job = _DispyJob_(self._compute.id, args, kwargs)
        except Exception:
            logging.warning('Creating job for "%s", "%s" failed with "%s"',
                            str(args), str(kwargs), str(sys.exc_info()))
            return None

        scheduler_sock = _DispySocket(socket.socket(socket.AF_INET, socket.SOCK_STREAM),
                                      auth_code=self.auth_code,
                                      certfile=self.certfile, keyfile=self.keyfile)
        scheduler_sock.connect((self.scheduler_node, self.scheduler_port))
        req = 'JOB:' + cPickle.dumps(_job)
        scheduler_sock.write_msg(self._compute.id, req)
        uid, msg = scheduler_sock.read_msg()
        try:
            _job._uid = cPickle.loads(msg)
        except Exception:
            logging.warning('Creating job for "%s", "%s" failed with "%s"',
                            str(args), str(kwargs), str(sys.exc_info()))
            scheduler_sock.close()
            del _DispyJob_.jobs[_job.job]
            return None
        scheduler_sock.close()
        self._cluster._sched_cv.acquire()
        self._cluster._sched_jobs[_job._uid] = _job
        self._pending_jobs += 1
        self._complete.clear()
        self._cluster._sched_cv.release()
        assert isinstance(_job._uid, int), type(_job._uid)
        logging.debug('Job %s created for %s', _job._uid, _job._compute_id)
        return _job.job

    def cancel(self, job):
        _job = _DispyJob_.jobs[job]
        self._cluster._sched_cv.acquire()
        if self._cluster._clusters.get(_job._compute_id, None) != self:
            logging.debug('Invalid job %s!', _job._uid)
            self._sched_cv.release()
            return
        if job.state != DispyJob.Created:
            logging.warning('Job %s is not valid for cancel (%s)', job._uid, job.state)
            self._cluster._sched_cv.release()
            return

        assert self._pending_jobs >= 1
        self._pending_jobs -= 1
        if self._pending_jobs == 0:
            self._complete.set()
            self.end_time = time.time()
        _job.finish(DispyJob.Cancelled)
        self._cluster._sched_cv.release()
        scheduler_sock = _DispySocket(socket.socket(socket.AF_INET, socket.SOCK_STREAM),
                                      auth_code=self.auth_code,
                                      certfile=self.certfile, keyfile=self.keyfile)
        scheduler_sock.connect((self.scheduler_node, self.scheduler_port))
        req = 'CANCEL_JOB:' + cPickle.dumps(_job)
        scheduler_sock.write_msg(self._compute.id, req)
        scheduler_sock.close()
        logging.debug('Job %s is cancelled', _job._uid)

    def close(self):
        if hasattr(self, '_compute'):
            scheduler_sock = _DispySocket(socket.socket(socket.AF_INET, socket.SOCK_STREAM),
                                          auth_code=self.auth_code,
                                          certfile=self.certfile, keyfile=self.keyfile)
            scheduler_sock.connect((self.scheduler_node, self.scheduler_port))
            req = 'DEL_COMPUTE:' + cPickle.dumps(self._compute.id)
            scheduler_sock.write_msg(self._compute.id, req)
            scheduler_sock.close()
            del self._compute

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('computation', help='program to distribute and parallelize')
    parser.add_argument('-c', action='store_false', dest='cleanup', default=True,
                        help='if True, nodes will remove any files transferred when this computation is over')
    parser.add_argument('-d', action='store_true', dest='loglevel', default=False,
                        help='if True, debug messages are printed')
    parser.add_argument('-a', action='append', dest='args', default=[],
                        help='argument(s) to program; repeat for multiple instances')
    parser.add_argument('-f', action='append', dest='depends', default=[],
                        help='dependencies (files) needed by program')
    parser.add_argument('-t', dest='dest_path', default='',
                        help='path on remote nodes where files for this computation are stored')
    parser.add_argument('-n', '--nodes', action='append', dest='nodes', default=[],
                        help='list of nodes (names or IP address) acceptable for this computation')
    parser.add_argument('--secret', dest='secret', default='',
                        help='authentication secret for handshake with nodes')
    parser.add_argument('--certfile', dest='certfile', default=None,
                        help='file containing SSL certificate')
    parser.add_argument('--keyfile', dest='keyfile', default=None,
                        help='file containing SSL key')
    config = vars(parser.parse_args(sys.argv[1:]))
    # print config

    if config['loglevel']:
        config['loglevel'] = logging.DEBUG
    else:
        config['loglevel'] = logging.INFO

    args = config.pop('args')
    cluster = JobCluster(**config)
    for arg in args:
        cluster.submit(*arg)
    cluster.wait()
    cluster.stats()
    exit(0)
