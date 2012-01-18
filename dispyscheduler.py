#!/usr/bin/env python

# dispyscheduler: Schedule jobs to nodes running 'dispynode';
# needed when multiple processes may use same nodes simultaneously
# in which case SharedJobCluster should be used;
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
import itertools
import Queue
import collections
import copy

from dispy import _DispySocket, _Compute, DispyJob, _DispyJob_, _Node, _JobReply, \
     MetaSingleton, _xor_string, _parse_nodes, _node_name_ipaddr, _XferFile
from dispynode import _same_file

_dispy_version = '1.0'
MaxFileSize = 10240000

class _Scheduler(object):
    """Internal use only.
    """
    __metaclass__ = MetaSingleton

    def __init__(self, loglevel, nodes=[], ip_addr=None, port=None, node_port=None,
                 scheduler_port=None, pulse_interval=None, ping_interval=None,
                 node_secret='', node_keyfile=None, node_certfile=None,
                 cluster_secret='', cluster_keyfile=None, cluster_certfile=None,
                 dest_path_prefix=None, max_file_size=None):
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
            if scheduler_port is None:
                scheduler_port = 51349
            if not nodes:
                nodes = ['*']

            self.ip_addr = ip_addr
            self.port = port
            self.node_port = node_port
            self.scheduler_port = scheduler_port
            self.node_spec = nodes
            self._nodes = {}
            self.node_secret = node_secret
            self.node_keyfile = node_keyfile
            self.node_certfile = node_certfile
            self.cluster_secret = cluster_secret
            self.cluster_keyfile = cluster_keyfile
            self.cluster_certfile = cluster_certfile
            if not dest_path_prefix:
                dest_path_prefix = os.path.join(os.sep, 'tmp', 'dispyscheduler')
            self.dest_path_prefix = dest_path_prefix
            if not os.path.isdir(self.dest_path_prefix):
                os.makedirs(self.dest_path_prefix)
                os.chmod(self.dest_path_prefix, stat.S_IWUSR | stat.S_IXUSR)
            if max_file_size is None:
                max_file_size = MaxFileSize
            self.max_file_size = max_file_size

            if pulse_interval:
                try:
                    self.pulse_interval = float(pulse_interval)
                    assert 1.0 <= self.pulse_interval <= 1000
                except:
                    raise Exception('Invalid pulse_interval; must be between 1 and 1000')
            else:
                self.pulse_interval = None

            if ping_interval:
                try:
                    self.ping_interval = float(ping_interval)
                    assert 1.0 <= self.ping_interval <= 1000
                except:
                    raise Exception('Invalid ping_interval; must be between 1 and 1000')
            else:
                self.ping_interval = None

            self._clusters = {}
            self.cluster_id = 1
            self.unsched_jobs = 0
            self.job_uid = 1
            self._sched_jobs = {}
            self._sched_cv = threading.Condition()
            self._terminate_scheduler = False
            self.sign = os.urandom(20).encode('hex')
            self.auth_code = hashlib.sha1(_xor_string(self.sign, self.cluster_secret)).hexdigest()
            logging.debug('auth_code: %s', self.auth_code)

            # TODO: for better performance, it may be better to
            # implement one worker_Q per cluster (i.e., make this part
            # of _Cluster) so each instance of _Cluster runs tasks for
            # it separately from other instances

            self.worker_Qs = {}

            self.cmd_sock = _DispySocket(socket.socket(socket.AF_INET, socket.SOCK_STREAM),
                                         auth_code=self.auth_code)
            self.cmd_sock.bind((self.ip_addr, 0))
            self.cmd_sock.listen(2)

            #self.select_job_node = self.fast_node_schedule
            self.select_job_node = self.load_balance_schedule
            self._scheduler = threading.Thread(target=self.__schedule)
            self._scheduler.daemon = True
            self._scheduler.start()
            self.start_time = time.time()

            self.main_worker_Q = Queue.PriorityQueue()
            worker_thread = threading.Thread(target=self.worker, args=(self.main_worker_Q,))
            worker_thread.daemon = True
            worker_thread.start()

            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            bc_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            bc_sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

            ping_request = cPickle.dumps({'scheduler_ip_addr':self.ip_addr,
                                          'scheduler_port':self.port,
                                          'version':_dispy_version})
            node_spec = _parse_nodes(nodes)
            for node_spec, node_info in node_spec.iteritems():
                logging.debug('Node: %s, %s', node_spec, str(node_info))
                # TODO: broadcast only if node_spec is wildcard that
                # matches local network and only in that case, or if
                # node_spec is '.*'
                if node_spec.find('*') >= 0:
                    port = node_info['port']
                    if not port:
                        port = self.node_port
                    logging.debug('Broadcasting to %s', port)
                    bc_sock.sendto('PING:%s' % ping_request, ('<broadcast>', port))
                    continue
                ip_addr = node_info['ip_addr']
                port = node_info['port']
                if not port:
                    port = self.node_port
                sock.sendto('PING:%s' % ping_request, (ip_addr, port))
            bc_sock.close()
            sock.close()

    def send_ping_cluster(self, cluster):
        # called with _sched_cv locked
        ping_request = cPickle.dumps({'scheduler_ip_addr':self.ip_addr,
                                      'scheduler_port':self.port, 'version':_dispy_version})
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
        compute.pulse_interval = self.pulse_interval
        # TODO: should we allow clients to add new nodes, or use only
        # the nodes initially created with command-line?
        self._sched_cv.acquire()
        self.send_ping_cluster(cluster)
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
                logging.warning('Failed to setup %s for computation "%s"',
                                node.ip_addr, compute.name)
            else:
                self._sched_cv.acquire()
                if node.ip_addr not in compute.nodes:
                    compute.nodes[node.ip_addr] = node
                    node.clusters.append(compute.id)
                    self._sched_cv.notify()
                self._sched_cv.release()

    def worker(self, worker_Q):
        while True:
            item = worker_Q.get(block=True)
            priority, func, args = item
            if func is None:
                worker_Q.task_done()
                break
            logging.debug('Calling %s', func.__name__)
            try:
                func(*args)
            except:
                logging.debug('Running %s failed: %s', func.__name__, traceback.format_exc())
            worker_Q.task_done()

    def close_work_Q(self, worker_Q):
        worker_Q.join()

    def setup_node(self, node, computes):
        # called via worker
        for compute in computes:
            if node.setup(compute):
                logging.warning('Failed to setup %s for computation "%s"',
                                node.ip_addr, compute.name)
            else:
                self._sched_cv.acquire()
                if node.ip_addr not in compute.nodes:
                    compute.nodes[node.ip_addr] = node
                    node.clusters.append(compute.id)
                    self._sched_cv.notify()
                self._sched_cv.release()

    def run_job(self, _job, cluster):
        # called via worker
        try:
            _job.run()
        except EnvironmentError:
            logging.warning('Failed to run job %s on %s for computation %s; removing this node',
                            _job.uid, _job.node.ip_addr, cluster._compute.name)
            self._sched_cv.acquire()
            if cluster._compute.nodes.pop(_job.node.ip_addr, None) is not None:
                _job.node.clusters.remove(cluster.compute.id)
                # TODO: remove the node from all clusters and globally?
            # this job might have been deleted already due to timeout
            if self._sched_jobs.pop(_job.uid, None) == _job:
                cluster._jobs.append(_job)
                self.unsched_jobs += 1
                _job.node.busy -= 1
            self._sched_cv.notify()
            self._sched_cv.release()
        except:
            logging.debug(traceback.format_exc())
            logging.warning('Failed to run job %s on %s for computation %s; rescheduling it',
                            _job.uid, _job.node.ip_addr, cluster._compute.name)
            # TODO: delay executing again for some time?
            self._sched_cv.acquire()
            # this job might have been deleted already due to timeout
            if self._sched_jobs.pop(_job.uid, None) == _job:
                cluster._jobs.append(_job)
                self.unsched_jobs += 1
                _job.node.busy -= 1
            self._sched_cv.notify()
            self._sched_cv.release()

    def send_job_result(self, uid, ip, port, result):
        if port is None:
            # when a computation is closed, port is set to None so we
            # don't send reply
            logging.debug('Ignoring result for job %s', uid)
            return
        logging.debug('Sending results for %s to %s, %s', uid, ip, port)
        sock = _DispySocket(socket.socket(socket.AF_INET, socket.SOCK_STREAM))
        sock.settimeout(2)
        try:
            sock.connect((ip, port))
            sock.write_msg(uid, cPickle.dumps(result))
        except:
            logging.warning("Couldn't send results for job %s to %s (%s)",
                            uid, ip, str(sys.exc_info()))
        sock.close()

    def terminate_jobs(self, _jobs):
        for _job in _jobs:
            try:
                _job.node.send(_job.uid, 'TERMINATE_JOB:' + cPickle.dumps(_job), reply=False)
            except:
                logging.warning('Canceling job %s failed', _job.uid)

    def close_compute_nodes(self, compute, nodes):
        for node in nodes:
            try:
                node.close(compute)
            except:
                logging.warning('Closing node %s failed', node.ip_addr)

    def run(self):
        def reschedule_jobs(dead_jobs):
            # called with _sched_cv locked
            for _job in dead_jobs:
                cluster = self._clusters[_job.compute_id]
                del self._sched_jobs[_job.uid]
                if cluster._compute.resubmit:
                    logging.debug('Rescheduling job %s from %s',
                                  _job.uid, _job.node.ip_addr)
                    _job.job.status = DispyJob.Created
                    cluster._jobs.append(_job)
                    self.unsched_jobs += 1
                else:
                    logging.debug('Terminating job %s scheduled on %s',
                                  _job.uid, _job.node.ip_addr)
                    reply = _JobReply(_job, _job.node.ip_addr, status=DispyJob.Terminated)
                    cluster._pending_jobs -= 1
                    if cluster._pending_jobs == 0:
                        cluster._complete.set()
                        cluster.end_time = time.time()
                    self.worker_Qs[cluster._compute.id].put(
                        (5, self.send_job_result,
                         (_job.uid, compute.client_scheduler_ip_addr,
                          compute.client_job_result_port, reply)))

        ping_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        ping_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        ping_sock.bind(('', self.port))

        job_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        job_sock.bind((self.ip_addr, 0))
        job_sock.listen(5)

        sched_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sched_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sched_sock.bind((self.ip_addr, self.scheduler_port))
        sched_sock.listen(5)

        logging.info('Ping port is %s', self.port)
        logging.info('Scheduler port is %s:%s', self.ip_addr, self.scheduler_port)
        logging.info('Job results port is %s:%s', self.ip_addr, job_sock.getsockname()[1])

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
            ready = select.select([sched_sock, self.cmd_sock.sock, ping_sock, job_sock],
                                  [], [], timeout)[0]
            for sock in ready:
                if sock == job_sock:
                    conn, addr = job_sock.accept()
                    if addr[0] not in self._nodes:
                        logging.warning('Ignoring results from %s', addr[0])
                        continue
                    conn = _DispySocket(conn, certfile=self.node_certfile,
                                        keyfile=self.node_keyfile, server=True)
                    try:
                        uid, msg = conn.read_msg()
                        conn.write_msg(uid, 'ACK')
                        conn.close()
                    except:
                        logging.warning('Failed to read job results from %s: %s',
                                        str(addr), traceback.format_exc())
                        continue
                    logging.debug('Received reply for job %s from %s' % (uid, addr[0]))
                    self._sched_cv.acquire()
                    node = self._nodes.get(addr[0], None)
                    if node is None:
                        self._sched_cv.release()
                        logging.warning('Ignoring invalid reply for job %s from %s',
                                        uid, addr[0])
                        continue
                    node.last_pulse = time.time()
                    _job = self._sched_jobs.get(uid, None)
                    if _job is None:
                        self._sched_cv.release()
                        logging.warning('Ignoring invalid job %s from %s', uid, addr[0])
                        continue
                    _job.job.end_time = time.time()
                    cluster = self._clusters.get(_job.compute_id, None)
                    if cluster is None:
                        self._sched_cv.release()
                        logging.warning('Invalid cluster for job %s from %s', uid, addr[0])
                        continue
                    compute = cluster._compute
                    try:
                        reply = cPickle.loads(msg)
                        assert reply.uid == _job.uid
                        assert reply.hash == _job.hash
                        assert _job.job.status not in [DispyJob.Created, DispyJob.Finished]
                        setattr(reply, 'cpus', node.cpus)
                        setattr(reply, 'start_time', _job.job.start_time)
                        setattr(reply, 'end_time', _job.job.end_time)
                        if reply.status == DispyJob.ProvisionalResult:
                            logging.debug('Receveid provisional result for %s', uid)
                            self.worker_Qs[compute.id].put(
                                (5, self.send_job_result,
                                 (_job.uid, compute.client_scheduler_ip_addr,
                                  compute.client_job_result_port, reply)))
                            self._sched_cv.release()
                            continue
                        else:
                            del self._sched_jobs[uid]
                    except:
                        self._sched_cv.release()
                        logging.warning('Invalid job result for %s from %s', uid, addr[0])
                        logging.debug(traceback.format_exc())
                        continue

                    _job.node.busy -= 1
                    assert compute.nodes[addr[0]] == _job.node
                    if reply.status != DispyJob.Terminated:
                        _job.node.jobs += 1
                    _job.node.cpu_time += _job.job.end_time - _job.job.start_time
                    cluster._pending_jobs -= 1
                    if cluster._pending_jobs == 0:
                        cluster._complete.set()
                        cluster.end_time = time.time()
                    self.worker_Qs[compute.id].put(
                        (5, self.send_job_result,
                         (_job.uid, compute.client_scheduler_ip_addr,
                          compute.client_job_result_port, reply)))
                    self._sched_cv.notify()
                    self._sched_cv.release()
                elif sock == sched_sock:
                    conn, addr = sched_sock.accept()
                    conn = _DispySocket(conn, certfile=self.cluster_certfile,
                                        keyfile=self.cluster_keyfile, server=True)
                    try:
                        req = conn.read(len(self.auth_code))
                        if req != self.auth_code:
                            req = conn.read(len('CLUSTER'))
                            if req == 'CLUSTER':
                                resp = cPickle.dumps({'sign':self.sign,'version':_dispy_version})
                                conn.write_msg(0, resp)
                            else:
                                logging.warning('Invalid/unauthorized request ignored')
                            conn.close()
                            continue
                        uid, msg = conn.read_msg()
                        if not msg:
                            logging.info('Closing connection')
                            conn.close()
                            continue
                    except:
                        logging.warning('Failed to read message from %s: %s',
                                        str(addr), traceback.format_exc())
                        conn.close()
                        continue
                    if msg.startswith('JOB:'):
                        msg = msg[len('JOB:'):]
                        try:
                            _job = cPickle.loads(msg)
                        except:
                            logging.debug('Ignoring job request from %s', addr[0])
                            conn.close()
                            continue
                        self._sched_cv.acquire()
                        cluster = self._clusters[_job.compute_id]
                        _job.uid = self.job_uid
                        self.job_uid += 1
                        if self.job_uid == sys.maxint:
                            # TODO: check if it is okay to reset
                            self.job_uid = 1
                        setattr(_job, 'node', None)
                        job = type('DispyJob', (), {'status':DispyJob.Created,
                                                    'start_time':None, 'end_time':None})
                        setattr(_job, 'job', job)
                        cluster._jobs.append(_job)
                        self.unsched_jobs += 1
                        cluster._pending_jobs += 1
                        self._sched_cv.notify()
                        self._sched_cv.release()
                        resp = _job.uid
                        resp = cPickle.dumps(resp)
                    elif msg.startswith('COMPUTE:'):
                        msg = msg[len('COMPUTE:'):]
                        try:
                            compute = cPickle.loads(msg)
                        except:
                            logging.debug('Ignoring compute request from %s', addr[0])
                            conn.close()
                            continue
                        setattr(compute, 'client_job_result_port', compute.job_result_port)
                        setattr(compute, 'client_scheduler_ip_addr', compute.scheduler_ip_addr)
                        setattr(compute, 'client_scheduler_port', compute.scheduler_port)
                        compute.job_result_port = job_sock.getsockname()[1]
                        compute.scheduler_ip_addr = self.ip_addr
                        compute.scheduler_port = self.port
                        setattr(compute, 'nodes', {})
                        cluster = _Cluster(self, compute)
                        compute = cluster._compute
                        self._sched_cv.acquire()
                        compute.id = cluster.id = self.cluster_id
                        self._clusters[cluster.id] = cluster
                        self.cluster_id += 1
                        dest_path = os.path.join(self.dest_path_prefix, str(compute.id))
                        for xf in compute.xfer_files:
                            xf.compute_id = compute.id
                            tgt = os.path.join(dest_path, os.path.basename(xf.name))
                            xf.name = tgt
                        self._sched_cv.release()
                        resp = {'ID':compute.id}
                        resp = cPickle.dumps(resp)
                        logging.debug('New computation %s: %s, %s', compute.id, compute.name,
                                      len(compute.xfer_files))
                    elif msg.startswith('ADD_COMPUTE:'):
                        msg = msg[len('ADD_COMPUTE:'):]
                        self._sched_cv.acquire()
                        try:
                            req = cPickle.loads(msg)
                            cluster = self._clusters[req['ID']]
                            for xf in cluster._compute.xfer_files:
                                assert os.path.isfile(xf.name)
                            resp = cPickle.dumps(cluster._compute.id)
                            worker_Q = Queue.PriorityQueue()
                            self.worker_Qs[cluster._compute.id] = worker_Q
                            worker_thread = threading.Thread(target=self.worker, args=(worker_Q,))
                            worker_thread.daemon = True
                            worker_thread.start()
                            worker_Q.put((50, self.add_cluster, (cluster,)))
                        except:
                            logging.debug('Ignoring compute request from %s', addr[0])
                            resp = None
                        finally:
                            self._sched_cv.release()
                    elif msg.startswith('DEL_COMPUTE:'):
                        conn.close()
                        msg = msg[len('DEL_COMPUTE:'):]
                        try:
                            req = cPickle.loads(msg)
                            assert isinstance(req['ID'], int)
                        except:
                            logging.warning('Invalid compuation for deleting')
                            continue
                        self._sched_cv.acquire()
                        cluster = self._clusters.get(req['ID'], None)
                        if cluster is None:
                            # this cluster is closed
                            self._sched_cv.release()
                            continue
                        compute = cluster._compute
                        logging.debug('Deleting computation "%s"/%s', compute.name, compute.id)
                        _jobs = [_job for _job in self._sched_jobs.itervalues() \
                                 if _job.compute_id == compute.id]
                        self.unsched_jobs -= len(cluster._jobs)
                        nodes = compute.nodes.values()
                        for node in nodes:
                            node.clusters.remove(compute.id)
                        compute.nodes = {}
                        for xf in compute.xfer_files:
                            logging.debug('Removing file "%s"', xf.name)
                            if os.path.isfile(xf.name):
                                os.remove(xf.name)
                        dest_path = os.path.join(self.dest_path_prefix, str(compute.id))
                        if os.path.isdir(dest_path):
                            os.rmdir(dest_path)

                        # set client_job_result_port to None so result is not sent to client
                        compute.client_job_result_port = None
                        compute.resubmit = False
                        cluster._jobs = []
                        del self._clusters[compute.id]
                        if _jobs:
                            self.worker_Qs[compute.id].put((30, self.terminate_jobs, (_jobs,)))
                        self.worker_Qs[compute.id].put(
                            (40, self.close_compute_nodes, (compute, nodes)))
                        worker_Q = self.worker_Qs[compute.id]
                        del self.worker_Qs[compute.id]
                        self.main_worker_Q.put((30, self.close_work_Q, (worker_Q,)))
                        self._sched_cv.release()
                        continue
                    elif msg.startswith('FILEXFER:'):
                        msg = msg[len('FILEXFER:'):]
                        try:
                            xf = cPickle.loads(msg)
                        except:
                            logging.debug('Ignoring file trasnfer request from %s', addr[0])
                            conn.close()
                            continue
                        resp = ''
                        if xf.compute_id not in self._clusters:
                            logging.error('computation "%s" is invalid' % xf.compute_id)
                            conn.close()
                            continue
                        compute = self._clusters[xf.compute_id]
                        dest_path = os.path.join(self.dest_path_prefix, str(compute.id))
                        if not os.path.isdir(dest_path):
                            os.makedirs(dest_path)
                        tgt = os.path.join(dest_path, os.path.basename(xf.name))
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
                        except:
                            logging.warning('Copying file "%s" failed with "%s"',
                                            xf.name, traceback.format_exc())
                            resp = 'NACK'
                    elif msg.startswith('TERMINATE_JOB:'):
                        conn.close()
                        msg = msg[len('TERMINATE_JOB:'):]
                        try:
                            job = cPickle.loads(msg)
                        except:
                            logging.warning('Invalid job cancel message')
                            continue
                        self._sched_cv.acquire()
                        cluster = self._clusters.get(job.compute_id, None)
                        if not cluster:
                            logging.debug('Invalid job %s!', job.uid)
                            self._sched_cv.release()
                            continue
                        compute = cluster._compute
                        _job = self._sched_jobs.get(job.uid, None)
                        if _job is None:
                            for i, _job in enumerate(cluster._jobs):
                                if _job.uid == job.uid:
                                    del cluster._jobs[i]
                                    self.unsched_jobs -= 1
                                    reply = _JobReply(_job, self.ip_addr, status=DispyJob.Cancelled)
                                    self.worker_Qs[compute.id].put(
                                        (5, self.send_job_result,
                                         (_job.uid, compute.client_scheduler_ip_addr,
                                          compute.client_job_result_port, reply)))
                                    break
                            else:
                                logging.debug('Invalid job %s!', job.uid)
                        else:
                            _job.job.status = DispyJob.Cancelled
                            self.worker_Qs[compute.id].put((30, self.terminate_jobs, ([_job],)))
                        self._sched_cv.release()
                        continue
                    if resp:
                        try:
                            conn.write_msg(0, resp)
                        except:
                            logging.warning('Failed to send response to %s: %s',
                                            str(addr), traceback.format_exc())
                    conn.close()
                elif sock == ping_sock:
                    msg, addr = ping_sock.recvfrom(1024)
                    if msg.startswith('PULSE:'):
                        msg = msg[len('PULSE:'):]
                        try:
                            info = cPickle.loads(msg)
                            node = self._nodes[info['ip_addr']]
                            assert 0 <= info['cpus'] <= node.cpus
                            node.last_pulse = time.time()
                            msg = 'PULSE:' + cPickle.dumps({'ip_addr':self.ip_addr})
                            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                            sock.sendto(msg, (info['ip_addr'], info['port']))
                            sock.close()
                        except:
                            logging.warning('Ignoring pulse message from %s', addr[0])
                            #logging.debug(traceback.format_exc())
                            continue
                    elif msg.startswith('PONG:'):
                        try:
                            status = cPickle.loads(msg[len('PONG:'):])
                            assert status['port'] > 0 and status['cpus'] > 0
                            assert status['version'] == _dispy_version
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
                            node = _Node(status['ip_addr'], status['port'], status['cpus'],
                                         status['sign'], self.node_secret,
                                         self.node_keyfile, self.node_certfile)
                            self._nodes[node.ip_addr] = node
                        else:
                            node.last_pulse = time.time()
                            h = _xor_string(status['sign'], self.node_secret)
                            h = hashlib.sha1(h).hexdigest()
                            if node.port == status['port'] and node.auth_code == h:
                                self._sched_cv.release()
                                logging.debug('Node %s is already known', node.ip_addr)
                                continue
                            node.port = status['port']
                            node.auth_code = h
                        node_computes = []
                        for cid, cluster in self._clusters.iteritems():
                            compute = cluster._compute
                            if node.ip_addr in compute.nodes:
                                continue
                            for node_spec, host in compute.node_spec.iteritems():
                                if re.match(node_spec, node.ip_addr):
                                    if host['name'] is None or host['name'].find('*') >= 0:
                                        node.name = node.ip_addr
                                    else:
                                        node.name = host['name']
                                    node_computes.append(compute)
                                    break
                        if node_computes:
                            self.main_worker_Q.put(
                                (10, self.setup_node, (node, node_computes)))
                        self._sched_cv.release()
                    elif msg.startswith('TERMINATED:'):
                        try:
                            data = cPickle.loads(msg[len('TERMINATED:'):])
                            self._sched_cv.acquire()
                            node = self._nodes.pop(data['ip_addr'], None)
                            if not node:
                                self._sched_cv.release()
                                continue
                            logging.debug('Removing node %s', node.ip_addr)
                            h = _xor_string(data['sign'], self.node_secret)
                            auth_code = hashlib.sha1(h).hexdigest()
                            if auth_code != node.auth_code:
                                logging.warning('Invalid signature from %s', node.ip_addr)
                            dead_jobs = [_job for _job in self._sched_jobs.itervalues() \
                                         if _job.node is not None and _job.node.ip_addr == node.ip_addr]
                            reschedule_jobs(dead_jobs)
                            for cid, cluster in self._clusters.iteritems():
                                if cluster._compute.nodes.pop(node.ip_addr, None) is not None:
                                    node.clusters.remove(cid)
                            self._sched_cv.notify()
                            self._sched_cv.release()
                            del node
                        except:
                            logging.debug('Removing node failed: %s', traceback.format_exc())
                    else:
                        logging.debug('Ignoring PONG message %s from: %s',
                                      msg[:min(5, len(msg))], addr[0])
                        continue
                elif sock == self.cmd_sock.sock:
                    logging.debug('Listener terminating ...')
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
                        logging.debug('Terminating all running jobs')
                        self._sched_cv.acquire()
                        self._terminate_scheduler = True
                        for uid, _job in self._sched_jobs.iteritems():
                            reply = _JobReply(_job, self.ip_addr, status=DispyJob.Terminated)
                            cluster = self._clusters[_job.compute_id]
                            compute = cluster._compute
                            self.worker_Qs[compute.id].put(
                                (5, self.send_job_result,
                                 (_job.uid, compute.client_scheduler_ip_addr,
                                  compute.client_job_result_port, reply)))
                        self._sched_jobs = {}
                        self._sched_cv.notify()
                        self._sched_cv.release()
                        logging.debug('Listener is terminated')
                        self.cmd_sock.close()
                        self.cmd_sock = None
                        return

            if timeout:
                now = time.time()
                if pulse_timeout and (now - last_pulse_time) >= pulse_timeout:
                    last_pulse_time = now
                    self._sched_cv.acquire()
                    dead_nodes = {}
                    for node in self._nodes.itervalues():
                        if node.busy and node.last_pulse + pulse_timeout < now:
                            logging.warning('Node %s is not responding; removing it (%s, %s, %s)',
                                            node.ip_addr, node.busy, node.last_pulse, now)
                            dead_nodes[node.ip_addr] = node
                    for ip_addr in dead_nodes:
                        del self._nodes[ip_addr]
                        for cluster in self._clusters.itervalues():
                            cluster._compute.nodes.pop(ip_addr, None)
                    dead_jobs = [_job for _job in self._sched_jobs.itervalues() \
                                 if _job.node is not None and _job.node.ip_addr in dead_nodes]
                    reschedule_jobs(dead_jobs)
                    if dead_nodes or dead_jobs:
                        self._sched_cv.notify()
                    self._sched_cv.release()
                if self.ping_interval and (now - last_ping_time) >= self.ping_interval:
                    last_ping_time = now
                    self._sched_cv.acquire()
                    for cluster in clusters.itervalues():
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
            logging.debug('load: %s, %s, %s' % (host.ip_addr, host.busy, host.cpus))
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
            # n = sum(len(cluster._jobs) for cluster in self._clusters.itervalues())
            # assert self.unsched_jobs == n, '%s != %s' % (self.unsched_jobs, n)
            if self._terminate_scheduler:
                self._sched_cv.release()
                break
            while self.unsched_jobs:
                logging.debug('Pending jobs: %s', self.unsched_jobs)
                node = self.select_job_node()
                if node is None:
                    logging.debug('No nodes/jobs')
                    break
                # TODO: strategy to pick a cluster?
                for cid in node.clusters:
                    if self._clusters[cid]._jobs:
                        _job = self._clusters[cid]._jobs.pop(0)
                        break
                else:
                    break
                cluster = self._clusters[_job.compute_id]
                _job.node = node
                logging.debug('Scheduling job %s on %s (load: %.3f)',
                              _job.uid, node.ip_addr, float(node.busy) / node.cpus)
                assert node.busy < node.cpus
                # _job.ip_addr = node.ip_addr
                self._sched_jobs[_job.uid] = _job
                self.unsched_jobs -= 1
                node.busy += 1
                self.worker_Qs[cluster._compute.id].put((1, self.run_job, (_job, cluster)))
            self._sched_cv.wait()
            self._sched_cv.release()
        self._sched_cv.acquire()
        logging.debug('Scheduler quitting (%s / %s)',
                      len(self._sched_jobs), self.unsched_jobs)
        for cid, cluster in self._clusters.iteritems():
            compute = cluster._compute
            for node in compute.nodes.itervalues():
                node.close(compute)
            for _job in cluster._jobs:
                reply = _JobReply(_job, self.ip_addr, status=DispyJob.Terminated)
                self.worker_Qs[compute.id].put(
                    (5, self.send_job_result,
                     (_job.uid, compute.client_scheduler_ip_addr,
                      compute.client_job_result_port, reply)))
            cluster._jobs = []
        logging.debug('Scheduler quit')
        self._sched_cv.release()

    def shutdown(self):
        # TODO: send shutdown notification to clients? Or wait for all
        # pending tasks to complete?
        for cid, cluster in self._clusters.iteritems():
            compute = cluster._compute
            for node in compute.nodes.itervalues():
                node.close(compute)

        if self._scheduler:
            logging.debug('Shutting down scheduler ...')
            self._sched_cv.acquire()
            self._terminate_scheduler = True
            self._sched_cv.notify()
            self._sched_cv.release()
            self._scheduler.join()
            self._scheduler = None
        if self.cmd_sock:
            sock = _DispySocket(socket.socket(socket.AF_INET, socket.SOCK_STREAM),
                                auth_code=self.auth_code)
            sock.settimeout(5)
            sock.connect((self.ip_addr, self.cmd_sock.sock.getsockname()[1]))
            sock.write_msg(0, 'terminate')
            sock.close()
        self._sched_cv.acquire()
        select_job_node = self.select_job_node
        self.select_job_node = None
        if select_job_node:
            clusters = self._clusters.itervalues()
            for cluster in clusters:
                self.worker_Qs[cluster._compute.id].put((99, None, None))
            self.main_worker_Q.put((99, None, None))
        else:
            clusters = []
        self._sched_cv.release()
        if select_job_node:
            for cluster in clusters:
                self.worker_Qs[cluster._compute.id].join()
            self.main_worker_Q.join()
            self.worker_Qs = {}
            self.clusters = {}
            self.main_worer_Q = None

    def stats(self):
        print
        heading = '%020s  |  %05s  |  %05s  |  %010s  |  %13s' % \
                  ('Node', 'CPUs', 'Jobs', 'Sec/Job', 'Node Time Sec')
        print heading
        print '-' * len(heading)
        tot_cpu_time = 0
        for ip_addr in sorted(self._nodes, key=lambda addr: self._nodes[addr].cpu_time,
                              reverse=True):
            node = self._nodes[ip_addr]
            if node.jobs:
                secs_per_job = node.cpu_time / node.jobs
            else:
                secs_per_job = 0
            tot_cpu_time += node.cpu_time
            print '%020s  |  %05s  |  %05s  |  %10.3f  |  %13.3f' % \
                  (ip_addr, node.cpus, node.jobs, secs_per_job, node.cpu_time)
        wall_time = time.time() - self.start_time
        print
        print 'Total job time: %.3f sec, wall time: %.3f sec, speedup: %.3f' % \
              (tot_cpu_time, wall_time, tot_cpu_time / wall_time)
        print

    def close(self, compute_id):
        cluster = self._clusters.get(compute_id, None)
        if compute is not None:
            for ip_addr, node in self._nodes.iteritems():
                node.close(cluster._compute)
            del self._clusters[compute_id]

class _Cluster():
    """Internal use only.
    """
    def __init__(self, scheduler, compute):
        self._compute = compute
        compute.node_spec = _parse_nodes(compute.node_spec)
        logging.debug('node_spec: %s', str(compute.node_spec))
        self._lock = threading.Lock()
        self._pending_jobs = 0
        self._jobs = []
        self._complete = threading.Event()
        self.cpu_time = 0
        self.start_time = time.time()
        self.end_time = None
        self.scheduler = scheduler

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('-d', action='store_true', dest='loglevel', default=False,
                        help='if True, debug messages are printed')
    parser.add_argument('-n', '--nodes', action='append', dest='nodes', default=[],
                        help='name or IP address used for all computations; repeat for multiple nodes')
    parser.add_argument('-i', '--ip_addr', dest='ip_addr', default=None,
                        help='IP address to use (may be needed in case of multiple interfaces)')
    parser.add_argument('-p', '--port', dest='port', type=int, default=51347,
                        help='port number to use')
    parser.add_argument('--node_port', dest='node_port', type=int, default=51348,
                        help='port number used by nodes')
    parser.add_argument('--node_secret', dest='node_secret', default='',
                        help='authentication secret for handshake with dispy clients')
    parser.add_argument('--node_keyfile', dest='node_keyfile', default=None,
                        help='file containing SSL key to be used with nodes')
    parser.add_argument('--node_certfile', dest='node_certfile', default=None,
                        help='file containing SSL certificate to be used with nodes')
    parser.add_argument('--cluster_secret', dest='cluster_secret', default='',
                        help='file containing SSL certificate to be used with dispy clients')
    parser.add_argument('--cluster_certfile', dest='cluster_certfile', default=None,
                        help='file containing SSL certificate to be used with dispy clients')
    parser.add_argument('--cluster_keyfile', dest='cluster_keyfile', default=None,
                        help='file containing SSL key to be used with dispy clients')
    parser.add_argument('--pulse_interval', dest='pulse_interval', type=float, default=None,
                        help='number of seconds between pulse messages to indicate whether node is alive')
    parser.add_argument('--ping_interval', dest='ping_interval', type=float, default=None,
                        help='number of seconds between ping messages to discover nodes')
    parser.add_argument('--dest_path_prefix', dest='dest_path_prefix',
                        default=os.path.join(os.sep, 'tmp', 'dispyscheduler'),
                        help='path prefix where files sent by dispy are stored')

    config = vars(parser.parse_args(sys.argv[1:]))
    if config['loglevel']:
        config['loglevel'] = logging.DEBUG
    else:
        config['loglevel'] = logging.INFO

    scheduler = _Scheduler(**config)
    while True:
        try:
            scheduler.run()
        except KeyboardInterrupt:
            logging.info('Interrupted; terminating')
            scheduler.shutdown()
            break
        except:
            logging.warning(traceback.format_exc())
            logging.warning('Scheduler terminated (possibly due to an error); restarting')
            time.sleep(2)
    scheduler.stats()
    exit(0)
