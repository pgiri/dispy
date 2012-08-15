#!/usr/bin/env python

"""
dispyscheduler: Schedule jobs to nodes running 'dispynode'; needed
when multiple processes may use same nodes simultaneously with
SharedJobCluster; see accompanying 'dispy' for more details.
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
import socket
import stat
import struct
import logging
import re
import ssl
import hashlib
import atexit
import traceback
import cPickle as pickle

from dispy import _Compute, DispyJob, _DispyJob_, _Node, _JobReply, \
     _parse_nodes, _node_ipaddr, _XferFile, _dispy_version

import asyncoro
from asyncoro import Coro, AsynCoro, AsynCoroSocket, MetaSingleton, serialize, unserialize

from dispynode import _same_file

__version__ = _dispy_version
__all__ = []

MaxFileSize = 10240000

logger = logging.getLogger('dispyscheduler')
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(message)s'))
logger.addHandler(handler)
del handler

class _Cluster(object):
    """Internal use only.
    """
    def __init__(self, compute):
        self._compute = compute
        compute.node_spec = _parse_nodes(compute.node_spec)
        logger.debug('node_spec: %s', str(compute.node_spec))
        self._pending_jobs = 0
        self._jobs = []
        self.cpu_time = 0
        self.start_time = time.time()
        self.end_time = None
        self.zombie = False
        self.last_pulse = time.time()
        self.client_job_result_port = None
        self.client_scheduler_ip_addr = None
        self.client_scheduler_port = None
        self.pending_results = 0

class _Scheduler(object):
    """Internal use only.

    See dispy's JobCluster and SharedJobCluster for documentation.
    """
    __metaclass__ = MetaSingleton

    def __init__(self, nodes=[], ip_addr=None, ext_ip_addr=None,
                 port=None, node_port=None, scheduler_port=None,
                 pulse_interval=None, ping_interval=None,
                 node_secret='', node_keyfile=None, node_certfile=None,
                 cluster_secret='', cluster_keyfile=None, cluster_certfile=None,
                 dest_path_prefix=None, max_file_size=None, zombie_interval=60):
        if not hasattr(self, 'ip_addr'):
            atexit.register(self.shutdown)
            if ip_addr:
                ip_addr = _node_ipaddr(ip_addr)
                if not ip_addr:
                    raise Exception('invalid ip_addr')
            else:
                ip_addr = socket.gethostbyname(socket.gethostname())
            if ext_ip_addr:
                ext_ip_addr = _node_ipaddr(ext_ip_addr)
                if not ext_ip_addr:
                    raise Exception('invalid ext_ip_addr')
            else:
                ext_ip_addr = ip_addr
            if not port:
                port = 51347
            if not node_port:
                node_port = 51348
            if not scheduler_port:
                scheduler_port = 51349
            if not nodes:
                nodes = ['*']

            self.ip_addr = ip_addr
            self.ext_ip_addr = ext_ip_addr
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
            if zombie_interval:
                self.zombie_interval = 60 * zombie_interval
            else:
                self.zombie_interval = None

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

            self.asyncoro = AsynCoro()

            self._clusters = {}
            self.cluster_id = 1
            self.unsched_jobs = 0
            self.job_uid = 1
            self._sched_jobs = {}
            self._sched_cv = asyncoro.Condition()
            self._terminate_scheduler = False
            self.sign = os.urandom(20).encode('hex')
            self.auth_code = hashlib.sha1(self.sign + self.cluster_secret).hexdigest()
            logger.debug('auth_code: %s', self.auth_code)

            #self.select_job_node = self.fast_node_schedule
            self.select_job_node = self.load_balance_schedule
            self.start_time = time.time()

            self.timer_coro = Coro(self.timer_task)

            self.scheduler_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            if self.cluster_certfile:
                self.scheduler_sock = ssl.wrap_socket(self.scheduler_sock,
                                                      keyfile=self.cluster_keyfile,
                                                      certfile=self.cluster_certfile)
            self.scheduler_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.scheduler_sock.bind((self.ip_addr, self.scheduler_port))
            self.scheduler_port = self.scheduler_sock.getsockname()[1]
            self.scheduler_sock.listen(32)

            self.job_result_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.job_result_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.job_result_sock.bind((self.ip_addr, self.port))
            self.port = self.job_result_sock.getsockname()[1]
            self.job_result_sock.listen(50)
            self.job_result_sock = AsynCoroSocket(self.job_result_sock, blocking=False,
                                                  keyfile=self.node_keyfile,
                                                  certfile=self.node_certfile)
            self.job_result_coro = Coro(self.job_result_server)

            self.udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.udp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.udp_sock.bind(('', self.port))
            self.udp_sock = AsynCoroSocket(self.udp_sock, blocking=False)
            self.udp_coro = Coro(self.udp_server)

            self.scheduler_coro = Coro(self._schedule_jobs)

            logger.info('UDP server at %s:%s', self.ip_addr, self.port)
            logger.info('Job scheduler is at %s:%s', self.ip_addr, self.scheduler_port)
            logger.info('Job result server is at %s:%s', self.ip_addr, self.port)

            # TODO: wait for scheduler and udp servers to start
            bc_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            bc_sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

            ping_request = serialize({'scheduler_ip_addr':self.ext_ip_addr,
                                      'scheduler_port':self.port, 'version':_dispy_version})
            node_spec = _parse_nodes(nodes)
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            for node_spec, node_info in node_spec.iteritems():
                logger.debug('Node: %s, %s', node_spec, str(node_info))
                # TODO: broadcast only if node_spec is wildcard that
                # matches local network and only in that case, or if
                # node_spec is '.*'
                if node_spec.find('*') >= 0:
                    port = node_info['port']
                    if not port:
                        port = self.node_port
                    logger.debug('Broadcasting to %s', port)
                    bc_sock.sendto('PING:%s' % ping_request, ('<broadcast>', port))
                    continue
                ip_addr = node_info['ip_addr']
                port = node_info['port']
                if not port:
                    port = self.node_port
                sock.sendto('PING:%s' % ping_request, (ip_addr, port))
            bc_sock.close()
            sock.close()

    def udp_server(self, coro=None):
        # generator
        assert coro is not None
        coro.set_daemon()
        while True:
            msg, addr = yield self.udp_sock.recvfrom(1024)
            # no need to create coros to process these requests
            if msg.startswith('PULSE:'):
                msg = msg[len('PULSE:'):]
                try:
                    info = unserialize(msg)
                except:
                    logger.warning('Ignoring pulse message from %s', addr[0])
                    continue
                if 'client_scheduler_ip_addr' in info:
                    yield self._sched_cv.acquire()
                    for cluster in self._clusters.itervalues():
                        if cluster.client_scheduler_ip_addr == info['client_scheduler_ip_addr'] and \
                               cluster.client_scheduler_port == info['client_scheduler_port']:
                            cluster.last_pulse = time.time()
                    yield self._sched_cv.release()
                else:
                    node = self._nodes.get(info['ip_addr'], None)
                    if node is not None:
                        # assert 0 <= info['cpus'] <= node.cpus
                        node.last_pulse = time.time()
                        msg = 'PULSE:' + serialize({'ip_addr':self.ext_ip_addr})
                        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                        sock = AsynCoroSocket(sock, blocking=False)
                        sock.settimeout(1)
                        yield sock.sendto(msg, (info['ip_addr'], info['port']))
                        sock.close()
            elif msg.startswith('PONG:'):
                try:
                    status = unserialize(msg[len('PONG:'):])
                    assert status['port'] > 0 and status['cpus'] > 0
                    assert status['version'] == _dispy_version
                except:
                    logger.debug('Ignoring node %s', addr[0])
                    continue
                logger.debug('Discovered %s:%s with %s cpus',
                             status['ip_addr'], status['port'], status['cpus'])
                if status['cpus'] <= 0:
                    logger.debug('Ignoring node %s', status['ip_addr'])
                    continue
                yield self._sched_cv.acquire()
                node = self._nodes.get(status['ip_addr'], None)
                if node is None:
                    node = _Node(status['ip_addr'], status['port'], status['cpus'],
                                 status['sign'], self.node_secret,
                                 keyfile=self.node_keyfile, certfile=self.node_certfile)
                    node.name = status['name']
                    self._nodes[node.ip_addr] = node
                else:
                    node.last_pulse = time.time()
                    h = hashlib.sha1(status['sign'] + self.node_secret).hexdigest()
                    if node.port == status['port'] and node.auth_code == h:
                        yield self._sched_cv.release()
                        logger.debug('Node %s is already known', node.ip_addr)
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
                            node_computes.append(compute)
                            break
                if node_computes:
                    Coro(self.setup_node, node, node_computes)
                yield self._sched_cv.release()
            elif msg.startswith('TERMINATED:'):
                try:
                    data = unserialize(msg[len('TERMINATED:'):])
                    yield self._sched_cv.acquire()
                    node = self._nodes.pop(data['ip_addr'], None)
                    if not node:
                        yield self._sched_cv.release()
                        continue
                    logger.debug('Removing node %s', node.ip_addr)
                    auth_code = hashlib.sha1(data['sign'] + self.node_secret).hexdigest()
                    if auth_code != node.auth_code:
                        logger.warning('Invalid signature from %s', node.ip_addr)
                    dead_jobs = [_job for _job in self._sched_jobs.itervalues() \
                                 if _job.node is not None and \
                                 _job.node.ip_addr == node.ip_addr]
                    self.reschedule_jobs(dead_jobs)
                    for cid, cluster in self._clusters.iteritems():
                        if cluster._compute.nodes.pop(node.ip_addr, None) is not None:
                            try:
                                node.clusters.discard(cid)
                            except:
                                logger.debug('Cluster %s/%s is already removed from %s',
                                             cluster._compute.name, cluster._compute.id,
                                             node.ip_addr)
                                pass
                    self._sched_cv.notify()
                    yield self._sched_cv.release()
                    del node
                except:
                    logger.debug('Removing node failed: %s', traceback.format_exc())
            elif msg.startswith('SERVERPORT:'):
                try:
                    req = unserialize(msg[len('SERVERPORT:'):])
                    logger.debug('Sending %s:%s to %s:%s', self.ext_ip_addr, self.scheduler_port,
                                 req['ip_addr'], req['port'])
                    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                    sock = AsynCoroSocket(sock, blocking=False)
                    sock.settimeout(1)
                    reply = {'ip_addr':self.ext_ip_addr, 'port':self.scheduler_port,
                             'sign':self.sign, 'version':_dispy_version}
                    yield sock.sendto(serialize(reply), (req['ip_addr'], req['port']))
                    sock.close()
                except:
                    logger.debug(traceback.format_exc())
                    # pass
            else:
                logger.debug('Ignoring UDP message %s from: %s', msg[:min(5, len(msg))], addr[0])

    def send_ping_cluster(self, cluster, coro=None):
        # generator
        ping_request = serialize({'scheduler_ip_addr':self.ext_ip_addr,
                                  'scheduler_port':self.port, 'version':_dispy_version})
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock = AsynCoroSocket(sock, blocking=False)
        sock.settimeout(1)
        for node_spec, node_info in cluster._compute.node_spec.iteritems():
            if node_spec.find('*') >= 0:
                port = node_info['port']
                if not port:
                    port = self.node_port
                bc_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                bc_sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
                bc_osck = AsynCoroSocket(bc_sock, blocking=False)
                try:
                    yield bc_sock.sendto('PING:%s' % ping_request, ('<broadcast>', port))
                except:
                    pass
                bc_sock.close()
            else:
                ip_addr = node_info['ip_addr']
                if ip_addr in cluster._compute.nodes:
                    continue
                port = node_info['port']
                if not port:
                    port = self.node_port
                try:
                    yield sock.sendto('PING:%s' % ping_request, (ip_addr, port))
                except:
                    pass
        sock.close()

    def add_cluster(self, cluster, coro=None):
        # generator
        assert coro is not None
        compute = cluster._compute
        compute.pulse_interval = self.pulse_interval
        # TODO: should we allow clients to add new nodes, or use only
        # the nodes initially created with command-line?
        yield self.send_ping_cluster(cluster, coro)
        yield self._sched_cv.acquire()
        compute_nodes = []
        for node_spec, host in compute.node_spec.iteritems():
            for ip_addr, node in self._nodes.iteritems():
                if ip_addr in compute.nodes:
                    continue
                if re.match(node_spec, ip_addr):
                    compute_nodes.append(node)
        # self._sched_cv.notify()
        yield self._sched_cv.release()

        for node in compute_nodes:
            r = yield node.setup(compute, coro=coro)
            if r:
                logger.warning('Failed to setup %s for computation "%s"',
                               node.ip_addr, compute.name)
            else:
                yield self._sched_cv.acquire()
                if node.ip_addr not in compute.nodes:
                    compute.nodes[node.ip_addr] = node
                    node.clusters.add(compute.id)
                    self._sched_cv.notify()
                self._sched_cv.release()

    def setup_node(self, node, computes, coro=None):
        # generator
        assert coro is not None
        for compute in computes:
            r = yield node.setup(compute, coro=coro)
            if r:
                logger.warning('Failed to setup %s for computation "%s"',
                               node.ip_addr, compute.name)
            else:
                yield self._sched_cv.acquire()
                if node.ip_addr not in compute.nodes:
                    compute.nodes[node.ip_addr] = node
                    node.clusters.add(compute.id)
                    self._sched_cv.notify()
                yield self._sched_cv.release()

    def send_job_result(self, uid, cid, ip, port, result, coro=None):
        # generator
        assert coro is not None
        logger.debug('Sending results for %s to %s, %s', uid, ip, port)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock = AsynCoroSocket(sock, blocking=False)
        sock.settimeout(2)
        try:
            yield sock.connect((ip, port))
            yield sock.send_msg(serialize(result))
            ack = yield sock.recv_msg()
            assert ack == 'ACK'
        except:
            f = os.path.join(self.dest_path_prefix, str(cid), '_dispy_job_reply_%s' % uid)
            logger.debug('storing results for job %s', uid)
            try:
                # TODO: file operations should be done asynchronously with coros
                fd = open(f, 'wb')
                pickle.dump(result, fd)
                fd.close()
            except:
                logger.debug('Could not save results for job %s', uid)
                logger.debug(traceback.format_exc())

            yield self._sched_cv.acquire()
            cluster = self._clusters.get(cid, None)
            if cluster:
                cluster.pending_results += 1
            self._sched_cv.release()
        else:
            cluster = self._clusters.get(cid, None)
            if cluster:
                cluster.last_pulse = time.time()
        finally:
            sock.close()

    def reschedule_jobs(self, dead_jobs):
        # non-generator called with _sched_cv locked
        for _job in dead_jobs:
            cluster = self._clusters[_job.compute_id]
            del self._sched_jobs[_job.uid]
            if cluster._compute.reentrant:
                logger.debug('Rescheduling job %s from %s', _job.uid, _job.node.ip_addr)
                _job.job.status = DispyJob.Created
                cluster._jobs.append(_job)
                self.unsched_jobs += 1
            else:
                logger.debug('Terminating job %s scheduled on %s',
                             _job.uid, _job.node.ip_addr)
                reply = _JobReply(_job, _job.node.ip_addr, status=DispyJob.Terminated)
                cluster._pending_jobs -= 1
                if cluster._pending_jobs == 0:
                    cluster.end_time = time.time()
                Coro(self.send_job_result, _job.uid, cluster._compute.id,
                     cluster.client_scheduler_ip_addr, cluster.client_job_result_port, reply)

    def _request_task(self, conn, addr, coro=None):
        # generator
        def _job_request_task(self, msg):
            # generator
            try:
                _job = unserialize(msg)
            except:
                logger.debug('Ignoring job request from %s', addr[0])
                raise StopIteration
            yield self._sched_cv.acquire()
            _job.uid = self.job_uid
            self.job_uid += 1
            yield self._sched_cv.release()
            setattr(_job, 'node', None)
            job = type('DispyJob', (), {'status':DispyJob.Created,
                                        'start_time':None, 'end_time':None})
            setattr(_job, 'job', job)
            resp = serialize(_job.uid)
            try:
                yield conn.send_msg(resp)
            except:
                logger.warning('Failed to send response to %s: %s',
                               str(addr), traceback.format_exc())
                raise StopIteration
            try:
                msg = yield conn.recv_msg()
                assert msg == 'ACK'
            except:
                logger.warning('Invalid reply for job: %s, %s',
                               _job.uid, traceback.format_exc())
                raise StopIteration
            yield self._sched_cv.acquire()
            cluster = self._clusters.get(_job.compute_id, None)
            if cluster is None:
                logger.debug('cluster %s is not valid anymore for job %s',
                             _job.compute_id, _job.uid)
                self._sched_cv.release()
                raise StopIteration
            cluster._jobs.append(_job)
            self.unsched_jobs += 1
            cluster._pending_jobs += 1
            cluster.last_pulse = time.time()
            self._sched_cv.notify()
            self._sched_cv.release()

        def _compute_task(self, msg):
            # generator
            try:
                compute = unserialize(msg)
            except:
                logger.debug('Ignoring compute request from %s', addr[0])
                raise StopIteration(None)
            setattr(compute, 'nodes', {})
            cluster = _Cluster(compute)
            compute = cluster._compute
            cluster.client_job_result_port = compute.job_result_port
            cluster.client_scheduler_ip_addr = compute.scheduler_ip_addr
            cluster.client_scheduler_port = compute.scheduler_port
            compute.job_result_port = self.job_result_sock.getsockname()[1]
            compute.scheduler_ip_addr = self.ext_ip_addr
            compute.scheduler_port = self.port
            yield self._sched_cv.acquire()
            compute.id = cluster.id = self.cluster_id
            self._clusters[cluster.id] = cluster
            self.cluster_id += 1
            dest_path = os.path.join(self.dest_path_prefix, str(compute.id))
            if not os.path.isdir(dest_path):
                try:
                    os.makedirs(dest_path)
                except:
                    logger.warning('Could not create directory "%s"', dest_path)
                    if compute.xfer_files:
                        self._sched_cv.release()
                        conn.close()
                        raise StopIteration
            for xf in compute.xfer_files:
                if self.max_file_size and xf.stat_buf.st_size > self.max_file_size:
                    logger.warning('transfer file "%s" is too big (%s)',
                                   xf.name, xf.stat_buf.st_size)
                    self._sched_cv.release()
                    conn.close()
                    raise StopIteration
                xf.compute_id = compute.id
                xf.name = os.path.join(dest_path, os.path.basename(xf.name))
            self._sched_cv.release()
            resp = {'ID':compute.id,
                    'pulse_interval':(self.zombie_interval / 5.0) if self.zombie_interval else None}
            resp = serialize(resp)
            logger.debug('New computation %s: %s, %s', compute.id, compute.name,
                         len(compute.xfer_files))
            yield resp

        def _xfer_file_task(self, msg):
            # generator
            try:
                xf = unserialize(msg)
            except:
                logger.debug('Ignoring file trasnfer request from %s', addr[0])
                raise StopIteration
            resp = ''
            if xf.compute_id not in self._clusters:
                logger.error('computation "%s" is invalid' % xf.compute_id)
                raise StopIteration
            compute = self._clusters[xf.compute_id]
            dest_path = os.path.join(self.dest_path_prefix, str(compute.id))
            if not os.path.isdir(dest_path):
                try:
                    os.makedirs(dest_path)
                except:
                    raise StopIteration
            tgt = os.path.join(dest_path, os.path.basename(xf.name))
            logger.debug('Copying file %s to %s (%s)', xf.name,
                         tgt, xf.stat_buf.st_size)
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
            except:
                logger.warning('Copying file "%s" failed with "%s"',
                               xf.name, traceback.format_exc())
                resp = 'NACK'
            if resp:
                try:
                    yield conn.send_msg(resp)
                except:
                    logger.debug('Could not send reply for "%s"', xf.name)

        # _request_task begins here
        conn = AsynCoroSocket(conn, blocking=False,
                              keyfile=self.cluster_keyfile, certfile=self.cluster_certfile)
        conn.settimeout(2)
        resp = None
        try:
            req = yield conn.recvall(len(self.auth_code))
            if req != self.auth_code:
                conn.close()
                logger.warning('Invalid/unauthorized request ignored')
                raise StopIteration
            msg = yield conn.recv_msg()
            if not msg:
                conn.close()
                logger.info('Closing connection')
                raise StopIteration
        except:
            logger.warning('Failed to read message from %s: %s',
                           str(addr), traceback.format_exc())
            conn.close()
            raise StopIteration
        if msg.startswith('JOB:'):
            msg = msg[len('JOB:'):]
            yield _job_request_task(self, msg)
            conn.close()
        elif msg.startswith('COMPUTE:'):
            msg = msg[len('COMPUTE:'):]
            resp = yield _compute_task(self, msg)
        elif msg.startswith('ADD_COMPUTE:'):
            msg = msg[len('ADD_COMPUTE:'):]
            yield self._sched_cv.acquire()
            try:
                req = unserialize(msg)
                cluster = self._clusters[req['ID']]
                for xf in cluster._compute.xfer_files:
                    assert os.path.isfile(xf.name)
                resp = serialize(cluster._compute.id)
                Coro(self.add_cluster, cluster)
            except:
                logger.debug('Ignoring compute request from %s', addr[0])
                conn.close()
                raise StopIteration
            finally:
                yield self._sched_cv.release()
        elif msg.startswith('DEL_COMPUTE:'):
            msg = msg[len('DEL_COMPUTE:'):]
            conn.close()
            try:
                req = unserialize(msg)
                assert isinstance(req['ID'], int)
            except:
                logger.warning('Invalid compuation for deleting')
                raise StopIteration
            yield self._sched_cv.acquire()
            cluster = self._clusters.get(req['ID'], None)
            if cluster is None:
                # this cluster is closed
                self._sched_cv.release()
                raise StopIteration
            cluster.zombie = True
            self.cleanup_computation(cluster)
            yield self._sched_cv.release()
        elif msg.startswith('FILEXFER:'):
            msg = msg[len('FILEXFER:'):]
            yield _xfer_file_task(self, msg)
        elif msg.startswith('TERMINATE_JOB:'):
            msg = msg[len('TERMINATE_JOB:'):]
            conn.close()
            try:
                job = unserialize(msg)
            except:
                logger.warning('Invalid job cancel message')
                raise StopIteration
            yield self._sched_cv.acquire()
            cluster = self._clusters.get(job.compute_id, None)
            if not cluster:
                logger.debug('Invalid job %s!', job.uid)
                self._sched_cv.release()
                raise StopIteration
            compute = cluster._compute
            cluster.last_pulse = time.time()
            _job = self._sched_jobs.get(job.uid, None)
            if _job is None:
                for i, _job in enumerate(cluster._jobs):
                    if _job.uid == job.uid:
                        del cluster._jobs[i]
                        self.unsched_jobs -= 1
                        reply = _JobReply(_job, self.ext_ip_addr, status=DispyJob.Cancelled)
                        Coro(self.send_job_result, _job.uid, compute.id,
                             cluster.client_scheduler_ip_addr, cluster.client_job_result_port,
                             reply)
                        break
                else:
                    logger.debug('Invalid job %s!', job.uid)
            else:
                _job.job.status = DispyJob.Cancelled
                Coro(_job.node.send, _job.uid, 'TERMINATE_JOB:' + serialize(_job),
                     reply=False)
            yield self._sched_cv.release()
        elif msg.startswith('RETRIEVE_JOB:'):
            req = msg[len('RETRIEVE_JOB:'):]
            try:
                req = unserialize(req)
                assert req['uid'] is not None
                assert req['hash'] is not None
                assert req['compute_id'] is not None
                result_file = os.path.join(self.dest_path_prefix, str(req['compute_id']),
                                           '_dispy_job_reply_%s' % req['uid'])
                if os.path.isfile(result_file):
                    fd = open(result_file, 'rb')
                    job_reply = pickle.load(fd)
                    fd.close()
                    if job_reply.hash == req['hash']:
                        yield conn.send_msg(serialize(job_reply))
                        ack = yield conn.recv_msg()
                        assert ack == 'ACK'
                        try:
                            os.remove(result_file)
                            yield self._sched_cv.acquire()
                            cluster = self._clusters.get(req['compute_id'], None)
                            if cluster is None:
                                p = os.path.dirname(result_file)
                                if len(os.listdir(p)) == 0:
                                    os.rmdir(p)
                            else:
                                cluster.pending_results -= 1
                            yield self._sched_cv.release()
                        except:
                            logger.debug('Could not remove "%s"', result_file)
                        raise StopIteration
                    else:
                        resp = serialize('Invalid job')
            except:
                resp = serialize('Invalid job')
                # logger.debug(traceback.format_exc())
        else:
            logger.debug('Ignoring invalid command')

        if resp is not None:
            try:
                yield conn.send_msg(resp)
            except:
                logger.warning('Failed to send response to %s: %s',
                               str(addr), traceback.format_exc())
            conn.close()
        # end of _request_task

    def timer_task(self, coro=None):
        coro.set_daemon()
        reset = True
        last_ping_time = last_pulse_time = last_zombie_time = time.time()
        while True:
            if reset:
                if self.pulse_interval:
                    self.pulse_timeout = 5.0 * self.pulse_interval
                else:
                    self.pulse_timeout = None

                if self.pulse_timeout and self.ping_interval:
                    timeout = min(self.pulse_timeout, self.ping_interval)
                else:
                    timeout = max(self.pulse_timeout, self.ping_interval)

                if timeout and self.zombie_interval:
                    timeout = min(timeout, self.zombie_interval)
                    self.zombie_interval = max(5 * timeout, self.zombie_interval)
                else:
                    timeout = max(timeout, self.zombie_interval)

            reset = yield coro.suspend(timeout)

            now = time.time()
            if self.pulse_timeout and (now - last_pulse_time) >= self.pulse_timeout:
                last_pulse_time = now
                yield self._sched_cv.acquire()
                dead_nodes = {}
                for node in self._nodes.itervalues():
                    if node.busy and node.last_pulse + self.pulse_timeout < now:
                        logger.warning('Node %s is not responding; removing it (%s, %s, %s)',
                                       node.ip_addr, node.busy, node.last_pulse, now)
                        dead_nodes[node.ip_addr] = node
                for ip_addr in dead_nodes:
                    del self._nodes[ip_addr]
                    for cluster in self._clusters.itervalues():
                        cluster._compute.nodes.pop(ip_addr, None)
                dead_jobs = [_job for _job in self._sched_jobs.itervalues() \
                             if _job.node is not None and _job.node.ip_addr in dead_nodes]
                self.reschedule_jobs(dead_jobs)
                if dead_nodes or dead_jobs:
                    self._sched_cv.notify()
                yield self._sched_cv.release()
            if self.ping_interval and (now - last_ping_time) >= self.ping_interval:
                last_ping_time = now
                yield self._sched_cv.acquire()
                for cluster in self.clusters.itervalues():
                    Coro(self.send_ping_cluster, cluster)
                yield self._sched_cv.release()
            if self.zombie_interval and (now - last_zombie_time) >= self.zombie_interval:
                last_zombie_time = now
                yield self._sched_cv.acquire()
                for cluster in self._clusters.itervalues():
                    if (now - cluster.last_pulse) > self.zombie_interval:
                        cluster.zombie = True
                zombies = [cluster for cluster in self._clusters.itervalues() \
                           if cluster.zombie and cluster._pending_jobs == 0]
                for cluster in zombies:
                    logger.debug('Deleting zombie computation "%s" / %s',
                                 cluster._compute.name, cluster._compute.id)
                    self.cleanup_computation(cluster)
                phoenix = [cluster for cluster in self._clusters.itervalues() \
                           if not cluster.zombie and cluster.pending_results]
                for cluster in phoenix:
                    compute = cluster._compute
                    result_dir = os.path.join(self.dest_path_prefix, str(compute.id))
                    files = [f for f in os.listdir(result_dir) \
                             if f.startswith('_dispy_job_reply_')]
                    # limit number queued so as not to take up too much time
                    files = files[:min(len(files), 128)]
                    for f in files:
                        result_file = os.path.join(result_dir, f)
                        try:
                            fd = open(result_file, 'rb')
                            result = pickle.load(fd)
                            fd.close()
                            Coro(self.send_job_result, result.uid, compute.id,
                                 cluster.client_scheduler_ip_addr, cluster.client_job_result_port,
                                 result)
                        except:
                            logger.debug('Could not load "%s"', result_file)
                        else:
                            cluster.pending_results -= 1
                        try:
                            os.remove(result_file)
                        except:
                            logger.debug('Could not remove "%s"', result_file)
                yield self._sched_cv.release()

    def job_result_task(self, conn, addr, coro=None):
        # generator
        assert coro is not None

        try:
            msg = yield conn.recv_msg()
            yield conn.send_msg('ACK')
            reply = unserialize(msg)
        except:
            logger.warning('Failed to read job results from %s', str(addr))
            raise StopIteration
        finally:
            conn.close()

        logger.debug('Received reply for job %s from %s' % (reply.uid, addr[0]))
        yield self._sched_cv.acquire()
        node = self._nodes.get(reply.ip_addr, None)
        if node is None:
            self._sched_cv.release()
            logger.warning('Ignoring invalid reply for job %s from %s', reply.uid, addr[0])
            raise StopIteration
        node.last_pulse = time.time()
        _job = self._sched_jobs.get(reply.uid, None)
        if _job is None:
            self._sched_cv.release()
            logger.warning('Ignoring invalid job %s from %s', reply.uid, addr[0])
            raise StopIteration
        _job.job.end_time = time.time()
        cluster = self._clusters.get(_job.compute_id, None)
        if cluster is None:
            self._sched_cv.release()
            logger.warning('Invalid cluster for job %s from %s', reply.uid, addr[0])
            raise StopIteration
        compute = cluster._compute
        try:
            assert reply.uid == _job.uid
            assert reply.hash == _job.hash
            assert _job.job.status not in [DispyJob.Created, DispyJob.Finished]
            setattr(reply, 'cpus', node.cpus)
            setattr(reply, 'start_time', _job.job.start_time)
            setattr(reply, 'end_time', _job.job.end_time)
        except:
            self._sched_cv.release()
            logger.warning('Invalid job result for %s from %s', reply.uid, addr[0])
            logger.debug(traceback.format_exc())
            raise StopIteration

        if reply.status != DispyJob.ProvisionalResult:
            del self._sched_jobs[_job.uid]
            _job.node.busy -= 1
            assert compute.nodes[reply.ip_addr] == _job.node
            if reply.status != DispyJob.Terminated:
                _job.node.jobs += 1
            _job.node.cpu_time += _job.job.end_time - _job.job.start_time
            cluster._pending_jobs -= 1
            if cluster._pending_jobs == 0:
                cluster.end_time = time.time()
            self._sched_cv.notify()
        self._sched_cv.release()
        Coro(self.send_job_result, _job.uid, compute.id, cluster.client_scheduler_ip_addr,
             cluster.client_job_result_port, reply)

    def job_result_server(self, coro=None):
        # generator
        assert coro is not None
        coro.set_daemon()
        logger.debug('Job results port is %s:%s',
                     self.ip_addr, self.job_result_sock.getsockname()[1])
        while True:
            try:
                conn, addr = yield self.job_result_sock.accept()
            except ssl.SSLError as err:
                logger.debug('SSL connection failed: %s', str(err))
                continue
            except GeneratorExit:
                break
            except:
                logger.debug(traceback.format_exc())
                continue
            conn.settimeout(2)
            Coro(self.job_result_task, conn, addr)

    def fast_node_schedule(self):
        # as we eagerly schedule, this has limited advantages
        # (useful only when  we have data about all the nodes and more than one node
        # is currently available)
        # in addition, we assume all jobs take equal time to execute
        host = None
        secs_per_job = None
        for ip_addr, node in self._nodes.iteritems():
            if node.busy >= node.cpus or not node.clusters:
                continue
            if all(not self._clusters[cluster_id]._jobs for cluster_id in node.clusters):
                continue
            if (secs_per_job is None) or (node.jobs == 0) or \
                   ((node.cpu_time / node.jobs) <= secs_per_job):
                host = node
                if node.jobs == 0:
                    secs_per_job = 0
                else:
                    secs_per_job = node.cpu_time / node.jobs
        return host

    def load_balance_schedule(self):
        # TODO: maintain "available" sequence of nodes for better performance
        host = None
        load = None
        for ip_addr, node in self._nodes.iteritems():
            if node.busy >= node.cpus or not node.clusters:
                continue
            if all((not self._clusters[cluster_id]._jobs or \
                    node.ip_addr not in self._clusters[cluster_id]._compute.nodes) \
                   for cluster_id in node.clusters):
                continue
            logger.debug('load: %s, %s, %s' % (node.ip_addr, node.busy, node.cpus))
            if (load is None) or ((float(node.busy) / node.cpus) < load):
                host = node
                load = float(node.busy) / node.cpus
        return host

    def run_job(self, _job, cluster, coro=None):
        # generator
        assert coro is not None
        try:
            yield _job.run(coro=coro)
        except EnvironmentError:
            logger.warning('Failed to run job %s on %s for computation %s; removing this node',
                           _job.uid, _job.node.ip_addr, cluster._compute.name)
            yield self._sched_cv.acquire()
            if cluster._compute.nodes.pop(_job.node.ip_addr, None) is not None:
                _job.node.clusters.discard(cluster._compute.id)
                # TODO: remove the node from all clusters and globally?
            # this job might have been deleted already due to timeout
            if self._sched_jobs.pop(_job.uid, None) == _job:
                cluster._jobs.append(_job)
                _job.job.status = DispyJob.Created
                self.unsched_jobs += 1
                _job.node.busy -= 1
            self._sched_cv.notify()
            self._sched_cv.release()
        except:
            logger.debug(traceback.format_exc())
            logger.warning('Failed to run job %s on %s for computation %s; rescheduling it',
                           _job.uid, _job.node.ip_addr, cluster._compute.name)
            # TODO: delay executing again for some time?
            yield self._sched_cv.acquire()
            # this job might have been deleted already due to timeout
            if self._sched_jobs.pop(_job.uid, None) == _job:
                cluster._jobs.append(_job)
                _job.job.status = DispyJob.Created
                self.unsched_jobs += 1
                _job.node.busy -= 1
            self._sched_cv.notify()
            self._sched_cv.release()

    def _schedule_jobs(self, coro=None):
        # generator
        assert coro is not None
        while True:
            yield self._sched_cv.acquire()
            # n = sum(len(cluster._jobs) for cluster in self._clusters.itervalues())
            # assert self.unsched_jobs == n, '%s != %s' % (self.unsched_jobs, n)
            while not self._terminate_scheduler:
                logger.debug('Pending jobs: %s', self.unsched_jobs)
                node = self.select_job_node()
                if node:
                    break
                logger.debug('No nodes/jobs')
                yield self._sched_cv.wait()
            if self._terminate_scheduler:
                yield self._sched_cv.release()
                break

            # TODO: strategy to pick a cluster?
            for cid in node.clusters:
                if self._clusters[cid]._jobs:
                    _job = self._clusters[cid]._jobs.pop(0)
                    break
            else:
                yield self._sched_cv.release()
                continue
            cluster = self._clusters[_job.compute_id]
            _job.node = node
            logger.debug('Scheduling job %s on %s (load: %.3f)',
                         _job.uid, node.ip_addr, float(node.busy) / node.cpus)
            assert node.busy < node.cpus
            # _job.ip_addr = node.ip_addr
            self._sched_jobs[_job.uid] = _job
            _job.job.status = DispyJob.Running
            self.unsched_jobs -= 1
            node.busy += 1
            Coro(self.run_job, _job, cluster)
            yield self._sched_cv.release()

        yield self._sched_cv.acquire()
        logger.debug('scheduler quitting (%s / %s)', len(self._sched_jobs), self.unsched_jobs)
        for uid, _job in self._sched_jobs.iteritems():
            reply = _JobReply(_job, self.ext_ip_addr, status=DispyJob.Terminated)
            cluster = self._clusters[_job.compute_id]
            compute = cluster._compute
            Coro(self.send_job_result, _job.uid, compute.id,
                 cluster.client_scheduler_ip_addr, cluster.client_job_result_port, reply)
        for cid, cluster in self._clusters.iteritems():
            for _job in cluster._jobs:
                reply = _JobReply(_job, self.ext_ip_addr, status=DispyJob.Terminated)
                Coro(self.send_job_result, _job.uid, compute.id, cluster.client_scheduler_ip_addr,
                     cluster.client_job_result_port, reply)
            cluster._jobs = []
        clusters = self._clusters.values()
        self._clusters = {}
        self._sched_jobs = {}
        yield self._sched_cv.release()
        for cluster in clusters:
            compute = cluster._compute
            for node in compute.nodes.itervalues():
                yield node.close(compute, coro=coro)
        logger.debug('scheduler quit')

    def shutdown(self):
        def _shutdown(self, coro=None):
            # generator
            assert coro is not None
            # TODO: send shutdown notification to clients? Or wait for all
            # pending tasks to complete?
            yield self._sched_cv.acquire()
            if self._terminate_scheduler is False:
                logger.debug('shutting down scheduler ...')
                self._terminate_scheduler = True
                self._sched_cv.notify()
                self._sched_cv.release()
            else:
                self._sched_cv.release()

        if self._terminate_scheduler is False:
            Coro(_shutdown, self).value()
            self.scheduler_coro.value()
            self.asyncoro.join()
            self.asyncoro.terminate()

    def stats(self):
        print()
        heading = ' %30s | %5s | %7s | %13s' % ('Node', 'CPUs', 'Jobs', 'Node Time Sec')
        print(heading)
        print('-' * len(heading))
        tot_cpu_time = 0
        for ip_addr in sorted(self._nodes, key=lambda addr: self._nodes[addr].cpu_time,
                              reverse=True):
            node = self._nodes[ip_addr]
            tot_cpu_time += node.cpu_time
            if node.name:
                name = ip_addr + ' (' + node.name + ')'
            else:
                name = ip_addr
            print(' %-30.30s | %5s | %7s | %13.3f' % \
                  (name, node.cpus, node.jobs, node.cpu_time))
        wall_time = time.time() - self.start_time
        print()
        print('Total job time: %.3f sec' % (tot_cpu_time))
        print()

    def close(self, compute_id):
        cluster = self._clusters.get(compute_id, None)
        if compute is not None:
            for ip_addr, node in self._nodes.iteritems():
                node.close(cluster._compute)
            del self._clusters[compute_id]

    def cleanup_computation(self, cluster):
        # non-generator, called with _sched_cv held
        if not cluster.zombie:
            return
        if cluster._pending_jobs:
            logger.debug('pedning jobs for "%s" / %s: %s', cluster._compute.name,
                         cluster._compute.id, cluster._pending_jobs)
            if cluster._pending_jobs > 0:
                return

        compute = cluster._compute
        nodes = compute.nodes.values()
        for node in nodes:
            node.clusters.discard(compute.id)
        compute.nodes = {}
        for xf in compute.xfer_files:
            logger.debug('Removing file "%s"', xf.name)
            if os.path.isfile(xf.name):
                try:
                    os.remove(xf.name)
                except:
                    logger.warning('Could not remove file "%s"', xf.name)
                    
        dest_path = os.path.join(self.dest_path_prefix, str(compute.id))
        if os.path.isdir(dest_path) and len(os.listdir(dest_path)) == 0:
            try:
                os.rmdir(dest_path)
            except:
                logger.warning('Could not remove directory "%s"', dest_path)

        del self._clusters[compute.id]
        for node in nodes:
            try:
                Coro(node.close, compute)
            except:
                logger.warning('Closing node %s failed', node.ip_addr)

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--debug', action='store_true', dest='loglevel', default=False,
                        help='if given, debug messages are printed')
    parser.add_argument('-n', '--nodes', action='append', dest='nodes', default=[],
                        help='name or IP address used for all computations; repeat for multiple nodes')
    parser.add_argument('-i', '--ip_addr', dest='ip_addr', default=None,
                        help='IP address to use (may be needed in case of multiple interfaces)')
    parser.add_argument('--ext_ip_addr', dest='ext_ip_addr', default=None,
                        help='External IP address to use (needed in case of NAT firewall/gateway)')
    parser.add_argument('-p', '--port', dest='port', type=int, default=51347,
                        help='port number for UDP data and job results')
    parser.add_argument('--node_port', dest='node_port', type=int, default=51348,
                        help='port number used by nodes')
    parser.add_argument('--scheduler_port', dest='scheduler_port', type=int, default=51349,
                        help='port number for scheduler')
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
    parser.add_argument('--zombie_interval', dest='zombie_interval', default=60, type=float,
                        help='interval in minutes to presume unresponsive scheduler is zombie')
    parser.add_argument('--dest_path_prefix', dest='dest_path_prefix',
                        default=os.path.join(os.sep, 'tmp', 'dispyscheduler'),
                        help='path prefix where files sent by dispy are stored')
    parser.add_argument('--max_file_size', dest='max_file_size', default=None, type=int,
                        help='maximum file size of any file transferred')

    config = vars(parser.parse_args(sys.argv[1:]))
    if config['loglevel']:
        logger.setLevel(logging.DEBUG)
        asyncoro.logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
    del config['loglevel']

    if config['zombie_interval']:
        config['zombie_interval'] = float(config['zombie_interval'])
        if config['zombie_interval'] < 1:
            raise Exception('zombie_interval must be at least 1')

    scheduler = _Scheduler(**config)
    while True:
        try:
            conn, addr = scheduler.scheduler_sock.accept()
        except KeyboardInterrupt:
            # TODO: terminate even if jobs are scheduled?
            logger.info('Interrupted; terminating')
            scheduler.shutdown()
            break
        except:
            logger.debug(traceback.format_exc())
            continue
        Coro(scheduler._request_task, conn, addr)
    scheduler.stats()
    exit(0)
