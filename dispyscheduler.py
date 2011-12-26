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

from dispy import _DispySocket, _Compute, DispyJob, _DispyJob_, _Node, MetaSingleton, \
     _xor_string, _parse_nodes, _node_name_ipaddr

class _Scheduler(object):
    """Internal use only.
    """
    __metaclass__ = MetaSingleton

    def __init__(self, loglevel, nodes=[], ip_addr=None, port=None, node_port=None,
                 scheduler_port=None, pulse_interval=None, ping_interval=None,
                 node_secret='', node_keyfile=None, node_certfile=None,
                 cluster_secret='', cluster_keyfile=None, cluster_certfile=None):
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
            self.num_jobs = 0
            self.job_uid = 1
            self._sched_jobs = {}
            self._sched_cv = threading.Condition()
            self._terminate_scheduler = False
            self.sign = os.urandom(20).encode('hex')
            self.auth_code = hashlib.sha1(_xor_string(self.sign, self.cluster_secret)).hexdigest()
            logging.debug('auth_code: %s', self.auth_code)

            self.cmd_sock = _DispySocket(socket.socket(socket.AF_INET, socket.SOCK_STREAM),
                                         auth_code=self.auth_code)
            self.cmd_sock.bind((self.ip_addr, 0))
            self.cmd_sock.listen(1)

            #self.select_job_node = self.fast_node_schedule
            self.select_job_node = self.load_balance_schedule
            self._scheduler = threading.Thread(target=self.__schedule)
            self._scheduler.daemon = True
            self._scheduler.start()
            self.start_time = time.time()
            self._job_results_Q = Queue.Queue()
            self._job_results_reaper = threading.Thread(target=self.reap_job_results)
            self._job_results_reaper.daemon = True
            self._job_results_reaper.start()

            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            bc_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            bc_sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

            ping_request = cPickle.dumps({'scheduler_ip_addr':self.ip_addr,
                                          'scheduler_port':self.port})
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
        compute.id = cluster.id = self.cluster_id
        for xf in compute.xfer_files:
            xf.compute_id = compute.id
        self._clusters[cluster.id] = cluster
        self.cluster_id += 1
        self._sched_cv.release()
        logging.debug('Adding computation %s with nodes %s', compute.name, compute.node_spec)

        # TODO: should we allow clients to add new nodes, or use only
        # the nodes initially created with command-line?
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
                logging.warning('Failed to setup %s for computation "%s"',
                                node.ip_addr, compute.name)
            else:
                self._sched_cv.acquire()
                if node.ip_addr not in compute.nodes:
                    compute.nodes[node.ip_addr] = node
                    node.clusters.append(compute.id)
                    self._sched_cv.notify()
                self._sched_cv.release()

    def _setup_node(self, node, computes):
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

    def reap_job_results(self):
        while True:
            item = self._job_results_Q.get(block=True)
            uid, ip, port, result = item
            logging.debug('Sending results for %s to %s, %s', uid, ip, port)
            sock = _DispySocket(socket.socket(socket.AF_INET, socket.SOCK_STREAM))
            sock.settimeout(2)
            try:
                sock.connect((ip, port))
                sock.write_msg(uid, cPickle.dumps(result))
            except Exception:
                logging.warning("Couldn't send results for job %s to %s (%s)",
                                uid, ip, str(sys.exc_info()))
            sock.close()

    def run(self):
        def cancel_jobs(dead_jobs):
            # called with _sched_cv locked
            for _job in dead_jobs:
                cluster = self._clusters[_job._compute_id]
                del self._sched_jobs[_job._uid]
                if cluster._compute.resubmit:
                    logging.debug('Rescheduling job %s from %s',
                                  _job._uid, _job._node.ip_addr)
                    _job.state = DispyJob.Created
                    cluster._jobs.append(_job)
                    self.num_jobs += 1
                else:
                    logging.debug('Cancelling job %s scheduled on %s',
                                  _job._uid, _job._node.ip_addr)
                    result = {'result':None, 'stdout':None, 'stderr':None,
                              'exception':'Cancelled'}
                    result['hash'] = _job._hash
                    result['ip_addr'] = _job._node.ip_addr
                    result['cpus'] = _job._node.cpus
                    result['start_time'] = _job.start_time
                    result['end_time'] = None
                    cluster._pending_jobs -= 1
                    if cluster._pending_jobs == 0:
                        cluster._complete.set()
                        cluster.end_time = time.time()
                    self._job_results_Q.put((_job._uid, compute.node_ip,
                                             compute.node_job_result_port, result))

        ping_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        ping_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        ping_sock.bind(('', self.port))

        job_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        job_sock.bind((self.ip_addr, 0))
        job_sock.listen(2)

        sched_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sched_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sched_sock.bind((self.ip_addr, self.scheduler_port))
        sched_sock.listen(2)

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
                    uid, msg = conn.read_msg()
                    conn.close()
                    logging.debug('Received reply for job %s from %s' % (uid, addr[0]))
                    self._sched_cv.acquire()
                    node = self._nodes.get(addr[0], None)
                    if node is None:
                        self._sched_cv.release()
                        logging.warning('Ignoring invalid reply for job %s from %s',
                                        uid, addr[0])
                        continue
                    job_ip = node.ip_addr
                    node.last_pulse = time.time()
                    # logging.debug('Node %s busy: %s', node.ip_addr, node.busy)
                    _job = self._sched_jobs.get(uid, None)
                    if _job is None:
                        self._sched_cv.release()
                        logging.warning('Ignoring invalid job %s from %s',
                                        uid, addr[0])
                        continue
                    try:
                        result = cPickle.loads(msg)
                        assert result['hash'] == _job._hash
                        if 'provisional' in result:
                            logging.debug('Receveid provisional result for %s', uid)
                            self._job_results_Q.put((_job._uid, compute.node_ip,
                                                     compute.node_job_result_port, result))
                            self._sched_cv.release()
                            continue
                        result['ip_addr'] = node.ip_addr
                        result['cpus'] = node.cpus
                        result['start_time'] = _job.start_time
                        result['end_time'] = time.time()
                    except:
                        self._sched_cv.release()
                        logging.debug(traceback.format_exc())
                        logging.warning('Invalid job result for %s from %s', uid, addr[0])
                        # raise
                        continue

                    cluster = self._clusters.get(_job._compute_id, None)
                    if cluster is None:
                        self._sched_cv.release()
                        logging.warning('Invalid cluster for job %s from %s', uid, addr[0])
                        continue
                    compute = cluster._compute
                    assert compute.nodes[addr[0]] == _job._node
                    _job._node.busy -= 1
                    del self._sched_jobs[uid]
                    if _job.state == DispyJob.Cancelled:
                        logging.debug('Cancelled job: %s', _job._uid)
                    else:
                        _job._node.jobs += 1
                        _job._node.cpu_time += time.time() - _job.start_time
                        cluster._pending_jobs -= 1
                        if cluster._pending_jobs == 0:
                            cluster._complete.set()
                            cluster.end_time = time.time()
                        self._job_results_Q.put((_job._uid, compute.node_ip,
                                                 compute.node_job_result_port, result))
                    self._sched_cv.notify()
                    self._sched_cv.release()
                elif sock == sched_sock:
                    conn, addr = sched_sock.accept()
                    conn = _DispySocket(conn, certfile=self.cluster_certfile,
                                        keyfile=self.cluster_keyfile, server=True)
                    req = conn.read(len(self.auth_code))
                    if req != self.auth_code:
                        req = conn.read(len('CLUSTER'))
                        if req == 'CLUSTER':
                            resp = cPickle.dumps({'sign':self.sign})
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
                    if msg.startswith('JOB:'):
                        msg = msg[len('JOB:'):]
                        try:
                            _job = cPickle.loads(msg)
                            self._sched_cv.acquire()
                            cluster = self._clusters[_job._compute_id]
                            _job._uid = self.job_uid
                            self.job_uid += 1
                            if self.job_uid == sys.maxint:
                                # TODO: check if it is okay to reset
                                self.job_uid = 1
                            self.num_jobs += 1
                            setattr(_job, '_node', None)
                            setattr(_job, 'start_time', None)
                            setattr(_job, 'state', DispyJob.Created)
                            cluster._jobs.append(_job)
                            cluster._pending_jobs += 1
                            self._sched_cv.notify()
                            self._sched_cv.release()
                            resp = _job._uid
                        except:
                            logging.debug('Ignoring job request from %s', addr[0])
                            resp = None
                        resp = cPickle.dumps(resp)
                    elif msg.startswith('COMPUTE:'):
                        msg = msg[len('COMPUTE:'):]
                        try:
                            compute = cPickle.loads(msg)
                            compute.job_result_port = job_sock.getsockname()[1]
                            setattr(compute, 'nodes', {})
                            cluster = _Cluster(self, compute)
                            self.add_cluster(cluster)
                            resp = compute.id
                            logging.debug('New computation %s: %s', compute.id, compute.name)
                        except:
                            logging.debug('Ignoring compute request from %s', addr[0])
                            resp = None
                        resp = cPickle.dumps(resp)
                    elif msg.startswith('DEL_COMPUTE:'):
                        msg = msg[len('DEL_COMPUTE:'):]
                        try:
                            compute_id = cPickle.loads(msg)
                        except:
                            logging.warning('Failed to load compuation for deleting')
                        else:
                            cluster = self._clusters.get(compute_id, None)
                            if cluster is None:
                                logging.warning('Computation ID %s is not valid', compute_id)
                            else:
                                compute = cluster._compute
                                for ip_addr, node in self._nodes.iteritems():
                                    node.close(compute)
                                del self._clusters[compute_id]
                        resp = None
                    elif msg.startswith('CANCEL_JOB:'):
                        msg = msg[len('CANCEL_JOB:'):]
                        try:
                            job = cPickle.loads(msg)
                        except:
                            logging.warning('Invalid job cancel message')
                            conn.close()
                            continue
                        logging.debug('Cancel job: %s', job._uid)
                        self._sched_cv.acquire()
                        cluster = self._clusters.get(_job._compute_id, None)
                        if not cluster:
                            logging.debug('Invalid job %s!', _job._uid)
                            self._sched_cv.release()
                            conn.close()
                            continue
                        _job = self._sched_jobs.get(job._uid, None)
                        if _job is not None:
                            _job.state = DispyJob.Cancelled
                            node = _job._node
                            assert node is not None
                            assert cluster._pending_jobs >= 1
                            cluster._pending_jobs -= 1
                            if cluster._pending_jobs == 0:
                                cluster._complete.set()
                                cluster.end_time = time.time()
                            self._sched_cv.release()
                            node.send(_job._uid, 'CANCEL_JOB:' + cPickle.dumps(_job), reply=False)
                            logging.debug('Job %s is cancelled', _job._uid)
                        else:
                            for i, _job in enumerate(cluster._jobs):
                                if _job._uid == job._uid:
                                    del cluster._jobs[i]
                                    break
                            else:
                                logging.debug('Invalid job %s!', job._uid)
                            self._sched_cv.release()
                            conn.close()
                            continue
                    if resp:
                        conn.write_msg(0, resp)
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
                            logging.debug('pulse from %s at %s', info['ip_addr'], node.last_pulse)
                        except:
                            logging.warning('Ignoring pulse message from %s', addr[0])
                            #logging.debug(traceback.format_exc())
                            continue
                    elif msg.startswith('PONG:'):
                        try:
                            status = cPickle.loads(msg[len('PONG:'):])
                            assert status['port'] > 0 and status['cpus'] > 0
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
                                node = _Node(status['ip_addr'], status['port'], status['cpus'],
                                             status['sign'], self.node_secret,
                                             self.node_keyfile, self.node_certfile)
                                data = {'ip_addr':self.ip_addr, 'port':self.port,
                                        'cpus':node.cpus, 'pulse_interval':self.pulse_interval}
                                resp = node.send(0, 'RESERVE:' + cPickle.dumps(data))
                            except:
                                logging.warning("Couldn't setup node %s; ignoring it.",
                                                status['ip_addr'])
                                logging.debug(traceback.format_exc())
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
                            h = _xor_string(status['sign'], self.node_secret)
                            node.auth_code = hashlib.sha1(h).hexdigest()
                            node.last_pulse = time.time()
                        self._nodes[node.ip_addr] = node
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
                        self._sched_cv.release()
                        if node_computes:
                            setup_thread = threading.Thread(target=self._setup_node,
                                                            args=(node, node_computes))
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
                            h = _xor_string(data['sign'], self.node_secret)
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
                            result = {'hash':_job._hash, 'node':None, 'ip_addr':None, 'cpus':None,
                                      'result':None, 'stdout':None, 'stderr':'terminated',
                                      'exception':'terminated', 'start_time':None, 'end_time':None}
                            cluster = self._clusters[_job._compute_id]
                            compute = cluster._compute
                            self._job_results_Q.put((_job._uid, compute.node_ip,
                                                     compute.node_job_result_port, result))

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
            if self._terminate_scheduler:
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
                if _job.start_time == start_time:
                    logging.warning('Job %s is rescheduled too quickly; ' \
                                    'scheduler is sleeping', _job._uid)
                    cluster._jobs.append(_job)
                    break
                _job.start_time = start_time
                _job._node = node
                logging.debug('Scheduling job %s on %s (load: %.3f)',
                              _job._uid, node.ip_addr, float(node.busy) / node.cpus)
                assert node.busy < node.cpus
                # _job.ip_addr = node.ip_addr
                try:
                    _job._run()
                except EnvironmentError:
                    logging.warning('Failed to run job %s on %s for computation %s; ' \
                                    'removing this node', _job._uid, node.ip_addr, compute.name)
                    cluster._jobs.append(_job)
                    # TODO: close the node properly?
                    del compute.nodes[node.ip_addr]
                    node.clusters.remove(compute.id)
                    continue
                except Exception:
                    logging.debug(traceback.format_exc())
                    logging.warning('Failed to run job %s on %s for computation %s; ' \
                                    'rescheduling it', _job._uid, node.ip_addr, compute.name)
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
                result = {'hash':_job._hash, 'node':None, 'ip_addr':None, 'cpus':'', 'result':None,
                          'stdout':None, 'stderr':None, 'exception':'terminated',
                          'start_time':None, 'end_time':None}
                self._job_results_Q.put((_job._uid, compute.node_ip,
                                         compute.node_job_result_port, result))
            cluster._jobs = []
        logging.debug('Scheduler quit')
        self._sched_cv.release()

    def shutdown(self):
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
        self.select_job_node = None

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
                  (node.name, node.cpus, node.jobs, secs_per_job, node.cpu_time)
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
            logging.warning(traceback.print_exc())
            logging.warning('Scheduler terminated (possibly due to an error); restarting')
            time.sleep(2)
    scheduler.stats()
    exit(0)
