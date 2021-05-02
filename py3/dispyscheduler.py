#!/usr/bin/python3


"""
dispyscheduler: Schedule jobs to nodes running 'dispynode'; needed
when multiple processes may use same nodes simultaneously with
SharedJobCluster; see accompanying 'dispy' for more details.
"""

import os
import sys
import time
import socket
import stat
import re
import ssl
import atexit
import traceback
import tempfile
import shutil
import glob
import pickle
import hashlib
import struct
import platform
try:
    import netifaces
except ImportError:
    netifaces = None

import pycos
from pycos import Task, Pycos, AsyncSocket, Singleton, serialize, deserialize
import dispy
import dispy.httpd
import dispy.config
from dispy.config import MsgTimeout, MaxFileSize
from dispy import _Compute, DispyJob, _DispyJob_, _Node, DispyNode, NodeAllocate, \
    _JobReply, auth_code, num_min, _parse_node_allocs, _XferFile, _dispy_version, \
    _same_file, _node_ipaddr, logger

__author__ = "Giridhar Pemmasani (pgiri@yahoo.com)"
__email__ = "pgiri@yahoo.com"
__copyright__ = "Copyright 2011, Giridhar Pemmasani"
__contributors__ = []
__maintainer__ = "Giridhar Pemmasani (pgiri@yahoo.com)"
__license__ = "Apache 2.0"
__url__ = "https://dispy.org"
__status__ = "Production"
__version__ = _dispy_version
__all__ = []

# PyPI / pip packaging adjusts assertion below for Python 3.7+
assert sys.version_info.major == 3 and sys.version_info.minor < 7, \
    ('\n\n  This file is not suitable for Python version %s.%s directly;\n'
     '  see installation instructions at %s\n' %
     (sys.version_info.major, sys.version_info.minor, __url__))


class _Cluster(object):
    """Internal use only.
    """
    def __init__(self, compute, node_allocs, scheduler):
        self._compute = compute
        # self.name = compute.name
        self.name = '%s @ %s' % (compute.name, compute.scheduler_ip_addr)
        self._node_allocs = _parse_node_allocs(node_allocs)
        self._node_allocs = sorted(self._node_allocs, key=lambda na: na.ip_rex, reverse=True)
        for na in self._node_allocs:
            for dep in na.depends:
                dep.compute_id = compute.id
        self.scheduler = scheduler
        self.status_callback = None
        self.pending_jobs = 0
        self.pending_results = 0
        self._jobs = []
        self._dispy_nodes = {}
        self.cpu_time = 0
        self.start_time = time.time()
        self.end_time = None
        self.job_sched_time = 0
        self.zombie = False
        self.exclusive = False
        self.last_pulse = time.time()
        self.client_ip_addr = None
        self.client_port = None
        self.client_sock_family = None
        self.client_job_result_port = None
        self.client_auth = None
        self.ip_addr = None
        self.dest_path = None
        self.file_uses = {}

    def __getstate__(self):
        state = dict(self.__dict__)
        for var in ('_node_allocs', 'scheduler', 'status_callback', '_jobs', '_dispy_nodes'):
            state.pop(var, None)
        return state

    def node_jobs(self, node, from_node=False, task=None):
        jobs = Task(self.scheduler.node_jobs, self, node, from_node, get_uids=False).value()
        return jobs

    def cancel(self, job):
        return self.scheduler.cancel_job(self, job.id)

    def allocate_node(self, node_alloc):
        if not isinstance(node_alloc, list):
            node_alloc = [node_alloc]
        node_allocs = _parse_node_allocs(node_alloc)
        Task(self.scheduler.allocate_node, self, node_allocs)

    def set_node_cpus(self, node, cpus):
        return Task(self.scheduler.set_node_cpus, node, cpus).value()


class _Scheduler(object, metaclass=Singleton):
    """Internal use only.

    See dispy's JobCluster and SharedJobCluster for documentation.
    """

    def __init__(self, nodes=[], hosts=[], ext_hosts=[], ipv4_udp_multicast=False,
                 pulse_interval=None, ping_interval=None, cooperative=False, cleanup_nodes=False,
                 node_secret='', cluster_secret='', node_keyfile=None, node_certfile=None,
                 cluster_keyfile=None, cluster_certfile=None, dest_path_prefix=None, clean=False,
                 scheduler_alg=None, zombie_interval=60, http_server=False):
        self.ipv4_udp_multicast = bool(ipv4_udp_multicast)
        self.addrinfos = []
        if not hosts:
            hosts = [None]
        for i in range(len(hosts)):
            addrinfo = dispy.host_addrinfo(host=hosts[i], ipv4_multicast=self.ipv4_udp_multicast)
            if not addrinfo:
                logger.warning('Ignoring invalid host %s', hosts[i])
                continue
            if i < len(ext_hosts):
                ext_host = dispy._node_ipaddr(ext_hosts[i])
                if ext_host:
                    addrinfo.ext_ip = ext_host
                else:
                    logger.warning('Ignoring invalid ext_host %s', ext_hosts[i])

            self.addrinfos.append(addrinfo)
        if not self.addrinfos:
            raise Exception('No valid IP address found')

        self.ip_addrs = []
        self.port = eval(dispy.config.ClientPort)
        self.node_port = eval(dispy.config.NodePort)
        self.scheduler_port = eval(dispy.config.SharedSchedulerPort)

        if not nodes:
            nodes = ['*']
        self._node_allocs = _parse_node_allocs(nodes)
        self._nodes = {}
        self.node_secret = node_secret
        self.node_keyfile = node_keyfile
        self.node_certfile = node_certfile
        self.cluster_secret = cluster_secret
        self.cluster_keyfile = cluster_keyfile
        self.cluster_certfile = cluster_certfile
        if not dest_path_prefix:
            dest_path_prefix = os.path.join(tempfile.gettempdir(), 'dispy', 'scheduler')
        self.dest_path_prefix = os.path.abspath(dest_path_prefix.strip()).rstrip(os.sep)
        if clean:
            shutil.rmtree(self.dest_path_prefix, ignore_errors=True)
        if not os.path.isdir(self.dest_path_prefix):
            os.makedirs(self.dest_path_prefix)
            os.chmod(self.dest_path_prefix, stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)

        self.cooperative = bool(cooperative)
        self.cleanup_nodes = bool(cleanup_nodes)
        if pulse_interval:
            try:
                self.pulse_interval = float(pulse_interval)
                assert 0.1 <= self.pulse_interval <= 1000
            except Exception:
                raise Exception('Invalid pulse_interval; must be between 0.1 and 1000')
        else:
            self.pulse_interval = None

        if ping_interval:
            try:
                self.ping_interval = float(ping_interval)
                assert 1.0 <= self.ping_interval <= 1000
            except Exception:
                raise Exception('Invalid ping_interval; must be between 1 and 1000')
        else:
            self.ping_interval = None

        if zombie_interval:
            self.zombie_interval = 60 * zombie_interval
            if self.pulse_interval:
                self.pulse_interval = min(self.pulse_interval, self.zombie_interval / 5.0)
            else:
                self.pulse_interval = self.zombie_interval / 5.0
        else:
            self.zombie_interval = None

        self.pycos = Pycos()
        atexit.register(self.shutdown)

        self._clusters = {}
        self.unsched_clusters = []
        self.pending_clusters = {}
        self._sched_jobs = {}
        self._sched_event = pycos.Event()
        # once a _job is done (i.e., final result for it is
        # received from node), it is added to done_jobs, so same
        # object is not reused by Python (when a new job is
        # submitted) until the result is sent back to client
        # (otherwise, 'id' may be duplicate)
        self.done_jobs = {}
        self.terminate = False
        self.sign = hashlib.sha1(os.urandom(20))
        for addrinfo in self.addrinfos:
            self.sign.update(addrinfo.ip.encode())
        self.sign = self.sign.hexdigest()
        self.cluster_auth = auth_code(self.cluster_secret, self.sign)
        self.node_auth = auth_code(self.node_secret, self.sign)

        with open(os.path.join(self.dest_path_prefix, 'config'), 'wb') as fd:
            config = {
                'hosts': [ai.ip for ai in self.addrinfos],
                'ext_hosts': [ai.ext_ip for ai in self.addrinfos],
                'port': self.port, 'sign': self.sign,
                'cluster_secret': self.cluster_secret, 'cluster_auth': self.cluster_auth,
                'node_secret': self.node_secret, 'node_auth': self.node_auth
                }
            pickle.dump(config, fd)

        if scheduler_alg == 'fair_cluster':
            self.select_job_node_cluster = self.fair_cluster_schedule
        elif scheduler_alg == 'fcfs_cluster':
            self.select_job_node_cluster = self.fcfs_cluster_schedule
        else:
            self.select_job_node_cluster = self.fsfs_job_schedule

        self.start_time = time.time()
        if http_server:
            self.httpd = dispy.httpd.DispyHTTPServer(None)
        else:
            self.httpd = None

        self.timer_task = Task(self.timer_proc)
        self.job_scheduler_task = Task(self._schedule_jobs)

        self.tcp_tasks = []
        self.udp_tasks = []
        self.scheduler_tasks = []
        udp_addrinfos = {}
        for addrinfo in self.addrinfos:
            self.tcp_tasks.append(Task(self.tcp_server, addrinfo))
            self.scheduler_tasks.append(Task(self.scheduler_server, addrinfo))
            if os.name == 'nt':
                bind_addr = addrinfo.ip
            elif sys.platform == 'darwin':
                if addrinfo.family == socket.AF_INET and (not self.ipv4_udp_multicast):
                    bind_addr = ''
                else:
                    bind_addr = addrinfo.broadcast
            else:
                bind_addr = addrinfo.broadcast
            udp_addrinfos[bind_addr] = addrinfo

        for bind_addr, addrinfo in udp_addrinfos.items():
            self.udp_tasks.append(Task(self.udp_server, bind_addr, addrinfo))

    def udp_server(self, bind_addr, addrinfo, task=None):
        task.set_daemon()

        udp_sock = AsyncSocket(socket.socket(addrinfo.family, socket.SOCK_DGRAM))
        udp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if hasattr(socket, 'SO_REUSEPORT'):
            try:
                udp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
            except Exception:
                pass

        while not self.port:
            yield task.sleep(0.2)
        udp_sock.bind((bind_addr, self.port))
        if addrinfo.family == socket.AF_INET:
            if self.ipv4_udp_multicast:
                mreq = socket.inet_aton(addrinfo.broadcast) + socket.inet_aton(addrinfo.ip)
                udp_sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
        else:  # addrinfo.family == socket.AF_INET6:
            mreq = socket.inet_pton(addrinfo.family, addrinfo.broadcast)
            mreq += struct.pack('@I', addrinfo.ifn)
            udp_sock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_JOIN_GROUP, mreq)
            try:
                udp_sock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 1)
            except Exception:
                pass

        Task(self.broadcast_ping, addrinfos=[addrinfo])
        self.send_ping_cluster(self._node_allocs, set())

        while 1:
            msg, addr = yield udp_sock.recvfrom(1000)
            if msg.startswith(b'PING:'):
                try:
                    info = deserialize(msg[len(b'PING:'):])
                    if info['version'] != _dispy_version:
                        logger.warning('Ignoring %s due to version mismatch', addr[0])
                        continue
                    assert info['port'] > 0
                    assert info['ip_addr']
                    # socket.inet_aton(status['ip_addr'])
                except Exception:
                    logger.debug('Ignoring node %s', addr[0])
                    logger.debug(traceback.format_exc())
                    continue
                if info['port'] == self.port:
                    continue
                auth = auth_code(self.node_secret, info['sign'])
                node = self._nodes.get(info['ip_addr'], None)
                if node:
                    if node.auth == auth:
                        continue
                sock = AsyncSocket(socket.socket(addrinfo.family, socket.SOCK_STREAM),
                                   keyfile=self.node_keyfile, certfile=self.node_certfile)
                sock.settimeout(MsgTimeout)
                msg = {'port': self.port, 'sign': self.sign, 'version': _dispy_version}
                msg['ip_addrs'] = self.ip_addrs
                try:
                    yield sock.connect((info['ip_addr'], info['port']))
                    yield sock.sendall(auth)
                    yield sock.send_msg(b'PING:' + serialize(msg))
                except Exception:
                    logger.debug(traceback.format_exc())
                finally:
                    sock.close()

            elif msg.startswith(b'TERMINATED:'):
                try:
                    info = deserialize(msg[len(b'TERMINATED:'):])
                    node = self._nodes[info['ip_addr']]
                    assert node.auth == auth_code(self.secret, info['sign'])
                except Exception:
                    # logger.debug(traceback.format_exc())
                    pass
                else:
                    self.delete_node(node)

            else:
                pass

    def tcp_server(self, addrinfo, task=None):
        # generator
        task.set_daemon()
        sock = socket.socket(addrinfo.family, socket.SOCK_STREAM)
        sock = AsyncSocket(sock, keyfile=self.node_keyfile, certfile=self.node_certfile)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind((addrinfo.ip, self.port))
        except Exception:
            logger.debug('Could not bind TCP to %s:%s', addrinfo.ip, self.port)
            raise StopIteration
        logger.debug('TCP server at %s:%s', addrinfo.ip, self.port)
        self.ip_addrs.append(addrinfo.ip)
        sock.listen(32)

        while 1:
            try:
                conn, addr = yield sock.accept()
            except ssl.SSLError as err:
                logger.debug('SSL connection failed: %s', str(err))
                continue
            except GeneratorExit:
                break
            except Exception:
                logger.debug(traceback.format_exc())
                continue
            Task(self.tcp_req, conn, addr)

    def tcp_req(self, conn, addr, task=None):
        # generator
        conn.settimeout(MsgTimeout)
        msg = yield conn.recv_msg()
        if msg.startswith(b'JOB_REPLY:'):
            try:
                info = deserialize(msg[len(b'JOB_REPLY:'):])
            except Exception:
                logger.warning('Invalid job reply from %s:%s ignored', addr[0], addr[1])
            else:
                yield self.job_reply_process(info, len(msg), conn, addr)
            conn.close()

        elif msg.startswith(b'PULSE:'):
            msg = msg[len(b'PULSE:'):]
            try:
                info = deserialize(msg)
            except Exception:
                logger.warning('Ignoring pulse message from %s', addr[0])
                conn.close()
                raise StopIteration
            node = self._nodes.get(info['ip_addr'], None)
            if node:
                # assert 0 <= info['cpus'] <= node.cpus
                node.last_pulse = time.time()
                yield conn.send_msg(b'PULSE')
                if info['avail_info']:
                    node.avail_info = info['avail_info']
                    for cluster in node.clusters:
                        dispy_node = cluster._dispy_nodes.get(node.ip_addr, None)
                        if dispy_node:
                            dispy_node.avail_info = info['avail_info']
                            dispy_node.update_time = node.last_pulse
                            Task(self.send_node_status, cluster, dispy_node, DispyNode.AvailInfo)
                            if cluster.status_callback:
                                cluster.status_callback(DispyNode.AvailInfo, dispy_node, None)
            conn.close()

        elif msg.startswith(b'PONG:'):
            conn.close()
            try:
                info = deserialize(msg[len(b'PONG:'):])
                assert info['auth'] == self.node_auth
            except Exception:
                logger.warning('Ignoring node %s due to "secret" mismatch', addr[0])
            else:
                self.add_node(info)

        elif msg.startswith(b'PING:'):
            sock_family = conn.family
            conn.close()
            try:
                info = deserialize(msg[len(b'PING:'):])
                if info['version'] != _dispy_version:
                    logger.warning('Ignoring node %s due to version mismatch', addr[0])
                    raise Exception('')
                assert info['port'] > 0
                assert info['ip_addr']
            except Exception:
                logger.debug('Ignoring node %s', addr[0])
                logger.debug(traceback.format_exc())
                raise StopIteration
            if info['port'] != self.port:
                auth = auth_code(self.node_secret, info['sign'])
                node = self._nodes.get(info['ip_addr'], None)
                if node:
                    if node.auth == auth:
                        raise StopIteration
                sock = AsyncSocket(socket.socket(sock_family, socket.SOCK_STREAM),
                                   keyfile=self.node_keyfile, certfile=self.node_certfile)
                sock.settimeout(MsgTimeout)
                msg = {'port': self.port, 'sign': self.sign, 'version': _dispy_version}
                msg['ip_addrs'] = self.ip_addrs
                try:
                    yield sock.connect((info['ip_addr'], info['port']))
                    yield sock.sendall(auth)
                    yield sock.send_msg(b'PING:' + serialize(msg))
                except Exception:
                    logger.debug(traceback.format_exc())
                finally:
                    sock.close()

        elif msg.startswith(b'FILEXFER:'):
            try:
                xf = deserialize(msg[len(b'FILEXFER:'):])
                msg = yield conn.recv_msg()
                job_reply = deserialize(msg)
                yield self.xfer_to_client(job_reply, xf, conn, addr)
            except Exception:
                logger.debug(traceback.format_exc())
            conn.close()

        elif msg.startswith(b'TERMINATED:'):
            conn.close()
            try:
                info = deserialize(msg[len(b'TERMINATED:'):])
                node = self._nodes[info['ip_addr']]
                assert node.auth == auth_code(self.secret, info['sign'])
            except Exception:
                # logger.debug(traceback.format_exc())
                pass
            else:
                self.delete_node(node)

        elif msg.startswith(b'NODE_CPUS:'):
            conn.close()
            try:
                info = deserialize(msg[len(b'NODE_CPUS:'):])
                node = self._nodes.get(info['ip_addr'], None)
                if not node:
                    raise StopIteration
                auth = auth_code(self.node_secret, info['sign'])
                if auth != node.auth:
                    logger.warning('Invalid signature from %s', node.ip_addr)
                    raise StopIteration
                cpus = info['cpus']
            except Exception:
                logger.debug(traceback.format_exc())
                raise StopIteration
            if cpus < 0:
                logger.warning('Node requested using %s CPUs, disabling it', node.ip_addr, cpus)
                cpus = 0
            logger.debug('Setting cpus for %s to %s', node.ip_addr, cpus)
            # TODO: set node.cpus to min(cpus, node.cpus)?
            node.cpus = cpus
            if cpus > node.avail_cpus:
                node.avail_cpus = cpus
                setup_computations = []
                for cluster in self._clusters.values():
                    if cluster in node.clusters:
                        continue
                    compute = cluster._compute
                    for node_alloc in cluster._node_allocs:
                        cpus = node_alloc.allocate(cluster, node.ip_addr, node.name,
                                                   node.avail_cpus)
                        if cpus <= 0:
                            continue
                        if cluster.exclusive or self.cooperative:
                            node.cpus = min(node.avail_cpus, cpus)
                        setup_computations.append((node_alloc.depends, node_alloc.setup_args,
                                                   compute))
                        break
                if setup_computations:
                    Task(self.setup_node, node, setup_computations)
                yield self._sched_event.set()
            else:
                node.avail_cpus = cpus
            for cluster in node.clusters:
                dispy_node = cluster._dispy_nodes.get(node.ip_addr, None)
                if dispy_node:
                    dispy_node.cpus = cpus

        elif msg.startswith(b'RELAY_INFO:'):
            try:
                info = deserialize(msg[len(b'RELAY_INFO:'):])
                assert info['version'] == _dispy_version
                msg = {'sign': self.sign, 'ip_addrs': [info['scheduler_ip_addr']],
                       'port': self.port}
                if 'auth' in info and info['auth'] != self.node_auth:
                    msg = None
            except Exception:
                msg = None
            yield conn.send_msg(serialize(msg))
            conn.close()

        else:
            logger.warning('Invalid message from %s:%s ignored', addr[0], addr[1])
            conn.close()

    def schedule_cluster(self, task=None):
        while self.unsched_clusters:
            cluster = self.unsched_clusters[0]
            if self._clusters:
                if cluster.exclusive:
                    raise StopIteration
                for cur_cluster in self._clusters.values():
                    if cur_cluster.exclusive:
                        raise StopIteration
                    break
            self.unsched_clusters.pop(0)
            reply_sock = socket.socket(cluster.client_sock_family, socket.SOCK_STREAM)
            reply_sock = AsyncSocket(reply_sock, keyfile=self.cluster_keyfile,
                                     certfile=self.cluster_certfile)
            reply_sock.settimeout(MsgTimeout)
            reply = {'compute_id': cluster._compute.id, 'pulse_interval': self.pulse_interval}
            self._clusters[cluster._compute.id] = cluster
            try:
                yield reply_sock.connect((cluster.client_ip_addr, cluster.client_job_result_port))
                yield reply_sock.send_msg('SCHEDULED:'.encode() + serialize(reply))
                msg = yield reply_sock.recv_msg()
                assert msg == 'ACK'.encode()
                self.add_cluster(cluster)
            except Exception:
                self._clusters.pop(cluster._compute.id, None)
                logger.debug('Ignoring computation %s / %s from %s:%s',
                             cluster._compute.name, cluster._compute.id,
                             cluster.client_ip_addr, cluster.client_job_result_port)
                continue
            finally:
                reply_sock.close()

    def scheduler_server(self, addrinfo, task=None):
        task.set_daemon()
        sock = socket.socket(addrinfo.family, socket.SOCK_STREAM)
        sock = AsyncSocket(sock, keyfile=self.cluster_keyfile, certfile=self.cluster_certfile)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind((addrinfo.ip, self.scheduler_port))
        except Exception:
            logger.warning('Could not bind scheduler server to %s:%s',
                           addrinfo.ip, self.scheduler_port)
            raise StopIteration
        logger.debug('Scheduler at %s:%s', addrinfo.ip, self.scheduler_port)
        sock.listen(32)
        while 1:
            conn, addr = yield sock.accept()
            Task(self.scheduler_req, conn, addr)

    def scheduler_req(self, conn, addr, task=None):
        # generator
        def _job_request(self, cluster, node, _job):
            # generator
            _job.uid = id(_job)
            for xf in _job.xfer_files:
                xf.name = os.path.join(cluster.dest_path, xf.dest_path.replace(xf.sep, os.sep),
                                       xf.name.split(xf.sep)[-1])
                xf.sep = os.sep

            job = DispyJob(None, (), {})
            job.id = _job.uid
            _job.job = job
            yield conn.send_msg(serialize(_job.uid))
            ack = yield conn.recv_msg()
            if ack != b'ACK':
                raise StopIteration
            if node:
                _job.pinned = node
                node.pending_jobs.append(_job)
            else:
                _job.pinned = None
                cluster._jobs.append(_job)
            logger.debug('Submitted job %s / %s', _job.uid, job.submit_time)
            cluster.pending_jobs += 1
            cluster.last_pulse = job.submit_time
            self._sched_event.set()
            if cluster.status_callback:
                cluster.status_callback(DispyJob.Created, None, job)

        def _compute_req(self, msg):
            # function
            try:
                req = deserialize(msg)
                compute = req['compute']
                node_allocs = req['node_allocs']
                exclusive = req['exclusive']
            except Exception:
                return serialize(('Invalid computation').encode())
            compute.id = id(compute)
            for xf in compute.xfer_files:
                if MaxFileSize and xf.stat_buf.st_size > MaxFileSize:
                    return serialize(('File "%s" is too big; limit is %s' %
                                      (xf.name, MaxFileSize)).encode())
                xf.compute_id = compute.id
            if self.terminate:
                return serialize(('Scheduler is closing').encode())
            if self.cleanup_nodes and not compute.cleanup:
                compute.cleanup = True
            cluster = _Cluster(compute, node_allocs, self)
            cluster.ip_addr = conn.getsockname()[0]
            cluster.exclusive = exclusive
            dest = compute.scheduler_ip_addr
            if os.name == 'nt':
                dest = dest.replace(':', '_')
            dest = os.path.join(self.dest_path_prefix, dest)
            if not os.path.isdir(dest):
                try:
                    os.mkdir(dest)
                except Exception:
                    return serialize(('Could not create destination directory').encode())
            if compute.dest_path and isinstance(compute.dest_path, str):
                # TODO: get os.sep from client and convert (in case of mixed environments)?
                if compute.dest_path.startswith(os.sep):
                    cluster.dest_path = compute.dest_path
                else:
                    cluster.dest_path = os.path.join(dest, compute.dest_path)
                if not os.path.isdir(cluster.dest_path):
                    try:
                        os.makedirs(cluster.dest_path)
                    except Exception:
                        return serialize(('Could not create destination directory').encode())
            else:
                cluster.dest_path = tempfile.mkdtemp(prefix=compute.name + '_', dir=dest)

            cluster.client_job_result_port = compute.job_result_port
            cluster.client_ip_addr = compute.scheduler_ip_addr
            cluster.client_port = compute.scheduler_port
            cluster.client_sock_family = conn.family
            cluster.client_auth = compute.auth
            compute.job_result_port = self.port
            compute.scheduler_port = self.port
            compute.auth = hashlib.sha1(os.urandom(10)).hexdigest()
            cluster.last_pulse = time.time()
            for xf in compute.xfer_files:
                xf.compute_id = compute.id
                xf.name = os.path.join(cluster.dest_path, xf.dest_path.replace(xf.sep, os.sep),
                                       xf.name.split(xf.sep)[-1])
                xf.sep = os.sep

            with open(os.path.join(self.dest_path_prefix,
                                   '%s_%s' % (compute.id, cluster.client_auth)), 'wb') as fd:
                pickle.dump(cluster, fd)
            self.pending_clusters[cluster._compute.id] = cluster
            logger.debug('New computation %s: %s, %s',
                         compute.id, compute.name, cluster.dest_path)
            return serialize({'compute_id': cluster._compute.id, 'auth': cluster.client_auth})

        def xfer_from_client(self, msg):
            # generator
            try:
                xf = deserialize(msg)
            except Exception:
                logger.debug('Ignoring file trasnfer request from %s', addr[0])
                raise StopIteration(serialize(-1))
            cluster = self.pending_clusters.get(xf.compute_id, None)
            if not cluster:
                # if file is transfered for 'dispy_job_depends', cluster would be active
                cluster = self._clusters.get(xf.compute_id, None)
                if not cluster:
                    logger.error('Computation "%s" is invalid', xf.compute_id)
                    raise StopIteration(serialize(-1))
            tgt = os.path.join(cluster.dest_path, xf.dest_path.replace(xf.sep, os.sep),
                               xf.name.split(xf.sep)[-1])
            if os.path.isfile(tgt) and _same_file(tgt, xf):
                if tgt in cluster.file_uses:
                    cluster.file_uses[tgt] += 1
                else:
                    cluster.file_uses[tgt] = 2
                raise StopIteration(serialize(xf.stat_buf.st_size))
            logger.debug('Copying file %s to %s (%s)', xf.name, tgt, xf.stat_buf.st_size)
            try:
                if not os.path.isdir(os.path.dirname(tgt)):
                    os.makedirs(os.path.dirname(tgt))
                with open(tgt, 'wb') as fd:
                    recvd = 0
                    while recvd < xf.stat_buf.st_size:
                        yield conn.send_msg(serialize(recvd))
                        data = yield conn.recvall(min(xf.stat_buf.st_size-recvd, 1024000))
                        if not data:
                            break
                        fd.write(data)
                        recvd += len(data)
                assert recvd == xf.stat_buf.st_size
                os.utime(tgt, (xf.stat_buf.st_atime, xf.stat_buf.st_mtime))
                os.chmod(tgt, stat.S_IMODE(xf.stat_buf.st_mode))
                if tgt in cluster.file_uses:
                    cluster.file_uses[tgt] += 1
                else:
                    cluster.file_uses[tgt] = 1
                logger.debug('Copied file %s', tgt)
            except Exception:
                logger.warning('Copying file "%s" failed with "%s"',
                               xf.name, traceback.format_exc())
                recvd = -1
                try:
                    os.remove(tgt)
                    if len(os.listdir(cluster.dest_path)) == 0:
                        os.rmdir(cluster.dest_path)
                except Exception:
                    pass
            raise StopIteration(serialize(recvd))

        def send_file(self, msg):
            # generator
            try:
                msg = deserialize(msg)
                node = self._nodes.get(msg['node'], None)
                xf = msg['xf']
            except Exception:
                logger.debug('Ignoring file trasnfer request from %s', addr[0])
                raise StopIteration(serialize(-1))
            cluster = self._clusters.get(xf.compute_id, None)
            if not cluster or not node:
                logger.error('send_file "%s" is invalid', xf.name)
                raise StopIteration(serialize(-1))
            dispy_node = cluster._dispy_nodes.get(node.ip_addr)
            if not dispy_node:
                logger.error('send_file "%s" is invalid', xf.name)
                raise StopIteration(serialize(-1))
            if _same_file(xf.name, xf):
                resp = yield node.xfer_file(xf)
                if resp < 0:
                    raise StopIteration(serialize(-1))
                else:
                    node.tx += resp
                    dispy_node.tx += resp
                    raise StopIteration(serialize(xf.stat_buf.st_size))

            node_sock = AsyncSocket(socket.socket(node.sock_family, socket.SOCK_STREAM),
                                    keyfile=self.node_keyfile, certfile=self.node_certfile)
            node_sock.settimeout(MsgTimeout)
            try:
                yield node_sock.connect((node.ip_addr, node.port))
                yield node_sock.sendall(node.auth)
                yield node_sock.send_msg('FILEXFER:'.encode() + serialize(xf))
                recvd = yield node_sock.recv_msg()
                recvd = deserialize(recvd)
                while recvd < xf.stat_buf.st_size:
                    yield conn.send_msg(serialize(recvd))
                    data = yield conn.recvall(min(xf.stat_buf.st_size-recvd, 1024000))
                    if not data:
                        break
                    yield node_sock.sendall(data)
                    recvd = yield node_sock.recv_msg()
                    recvd = deserialize(recvd)
                node.tx += recvd
                dispy_node.tx += recvd
            except Exception:
                logger.error('Could not transfer %s to %s: %s', xf.name, node.ip_addr, recvd)
                logger.debug(traceback.format_exc())
                # TODO: mark this node down, reschedule on different node?
                recvd = -1
            finally:
                node_sock.close()
            raise StopIteration(serialize(recvd))

        # scheduler_req begins here
        conn.settimeout(MsgTimeout)
        resp = None
        try:
            req = yield conn.recvall(len(self.cluster_auth))
        except Exception:
            logger.warning('Failed to read message from %s: %s', str(addr), traceback.format_exc())
            conn.close()
            raise StopIteration

        if req != self.cluster_auth:
            msg = yield conn.recv_msg()
            if msg.startswith(b'CLIENT:'):
                try:
                    req = deserialize(msg[len(b'CLIENT:'):])
                    if req['version'] != _dispy_version:
                        logger.warning('Ignoring %s due to version mismatch', addr[0])
                        raise Exception('')
                    if not req['ip_addr']:
                        req['ip_addr'] = addr[0]
                    reply = {'ip_addr': req['ip_addr'], 'port': self.scheduler_port,
                             'sign': self.sign, 'version': _dispy_version}
                    yield conn.send_msg(serialize(reply))
                except Exception:
                    pass
            else:
                logger.warning('Invalid/unauthorized request ignored')
            conn.close()
            raise StopIteration
        msg = yield conn.recv_msg()
        if not msg:
            logger.info('Closing connection')
            conn.close()
            raise StopIteration

        if msg.startswith(b'JOB:'):
            msg = msg[len(b'JOB:'):]
            try:
                req = deserialize(msg)
                _job = req['job']
                cluster = self._clusters[_job.compute_id]
                assert cluster.client_auth == req['auth']
                node = req['node']
                if node:
                    node = self._nodes[node]
            except Exception:
                pass
            else:
                yield _job_request(self, cluster, node, _job)
            resp = None

        elif msg.startswith(b'PULSE:'):
            msg = msg[len(b'PULSE:'):]
            try:
                info = deserialize(msg)
            except Exception:
                logger.warning('Ignoring pulse message from %s', addr[0])
                conn.close()
                raise StopIteration
            if 'client_port' in info:
                for cluster in self._clusters.values():
                    if (cluster.client_ip_addr == addr[0] and
                        cluster.client_port == info['client_port']):
                        cluster.last_pulse = time.time()
            conn.close()

        elif msg.startswith(b'COMPUTE:'):
            msg = msg[len(b'COMPUTE:'):]
            resp = _compute_req(self, msg)

        elif msg.startswith(b'SCHEDULE:'):
            msg = msg[len(b'SCHEDULE:'):]
            try:
                req = deserialize(msg)
                cluster = self.pending_clusters[req['compute_id']]
                assert cluster.client_auth == req['auth']
                for xf in cluster._compute.xfer_files:
                    assert os.path.isfile(xf.name)
                self.unsched_clusters.append(cluster)
                self.pending_clusters.pop(cluster._compute.id)
            except Exception:
                logger.debug('Ignoring schedule request from %s', addr[0])
                resp = 'NAK'.encode()
            else:
                resp = 'ACK'.encode()
                Task(self.schedule_cluster)

        elif msg.startswith(b'CLOSE:'):
            msg = msg[len(b'CLOSE:'):]
            try:
                req = deserialize(msg)
                auth = req['auth']
            except Exception:
                logger.warning('Invalid compuation for deleting')
                conn.close()
                raise StopIteration
            cluster = self._clusters.get(req['compute_id'], None)
            if cluster is None or cluster.client_auth != auth:
                # this cluster is closed
                conn.close()
                raise StopIteration
            cluster.zombie = True
            terminate_pending = req.get('terminate_pending', False)
            Task(self.cleanup_computation, cluster, terminate_pending=bool(terminate_pending))

        elif msg.startswith(b'FILEXFER:'):
            msg = msg[len(b'FILEXFER:'):]
            resp = yield xfer_from_client(self, msg)

        elif msg.startswith(b'SENDFILE:'):
            msg = msg[len(b'SENDFILE:'):]
            resp = yield send_file(self, msg)

        elif msg.startswith(b'NODE_JOBS:'):
            msg = msg[len(b'NODE_JOBS:'):]
            try:
                req = deserialize(msg)
                cluster = self._clusters.get(req['compute_id'], None)
                if cluster is None or cluster.client_auth != req['auth']:
                    job_uids = []
                else:
                    node = req['node']
                    from_node = req['from_node']
                    # assert req['get_uids'] == True
                    job_uids = yield self.node_jobs(cluster, node, from_node,
                                                    get_uids=True, task=task)
            except Exception:
                job_uids = []
            resp = serialize(job_uids)

        elif msg.startswith(b'TERMINATE_JOB:'):
            msg = msg[len(b'TERMINATE_JOB:'):]
            try:
                req = deserialize(msg)
                uid = req['uid']
                cluster = self._clusters[req['compute_id']]
                assert cluster.client_auth == req['auth']
            except Exception:
                logger.warning('Invalid job cancel message')
                conn.close()
                raise StopIteration
            self.cancel_job(cluster, uid)

        elif msg.startswith(b'RESEND_JOB_RESULTS:'):
            msg = msg[len(b'RESEND_JOB_RESULTS:'):]
            try:
                info = deserialize(msg)
                compute_id = info['compute_id']
                auth = info['auth']
            except Exception:
                resp = serialize(0)
            else:
                cluster = self._clusters.get(compute_id, None)
                if cluster is None or cluster.client_auth != auth:
                    try:
                        with open(os.path.join(self.dest_path_prefix,
                                               '%s_%s' % (compute_id, auth)), 'rb') as fd:
                            cluster = pickle.load(fd)
                    except Exception:
                        pass
                if cluster is None:
                    resp = 0
                else:
                    resp = cluster.pending_results + cluster.pending_jobs
            yield conn.send_msg(serialize(resp))
            conn.close()
            if resp > 0:
                yield self.resend_job_results(cluster, task=task)
            raise StopIteration

        elif msg.startswith(b'PENDING_JOBS:'):
            msg = msg[len(b'PENDING_JOBS:'):]
            reply = {'done': [], 'pending': 0}
            try:
                info = deserialize(msg)
                compute_id = info['compute_id']
                auth = info['auth']
            except Exception:
                pass
            else:
                cluster = self._clusters.get(compute_id, None)
                if cluster is None or cluster.client_auth != auth:
                    with open(os.path.join(self.dest_path_prefix,
                                           '%s_%s' % (compute_id, auth)), 'rb') as fd:
                        cluster = pickle.load(fd)
                if cluster is not None and cluster.client_auth == auth:
                    done = []
                    if cluster.pending_results:
                        for result_file in glob.glob(os.path.join(cluster.dest_path,
                                                                  '_dispy_job_reply_*')):
                            result_file = os.path.basename(result_file)
                            try:
                                uid = int(result_file[len('_dispy_job_reply_'):])
                            except Exception:
                                pass
                            else:
                                done.append(uid)
                                # limit so as not to take up too much time
                                if len(done) > 50:
                                    break
                    reply['done'] = done
                    reply['pending'] = cluster.pending_jobs
            resp = serialize(reply)

        elif msg.startswith(b'RETRIEVE_JOB:'):
            msg = msg[len(b'RETRIEVE_JOB:'):]
            yield self.retrieve_job_req(conn, msg)

        elif msg.startswith(b'ALLOCATE_NODE:'):
            req = msg[len(b'ALLOCATE_NODE:'):]
            try:
                req = deserialize(req)
                cluster = self._clusters[req['compute_id']]
                assert cluster.client_auth == req['auth']
                resp = yield self.allocate_node(cluster, req['node_alloc'], task=task)
                resp = serialize(resp)
            except Exception:
                resp = serialize(-1)

        elif msg.startswith(b'DEALLOCATE_NODE:'):
            req = msg[len(b'DEALLOCATE_NODE:'):]
            try:
                req = deserialize(req)
                cluster = self._clusters[req['compute_id']]
                assert cluster.client_auth == req['auth']
                node = self._nodes.get(_node_ipaddr(req['node']), None)
                if node:
                    node.clusters.discard(cluster)
                    resp = 0
                else:
                    resp = -1
                resp = serialize(resp)
            except Exception:
                resp = serialize(-1)

        elif msg.startswith(b'CLOSE_NODE:'):
            req = msg[len(b'CLOSE_NODE:'):]
            try:
                req = deserialize(req)
                cluster = self._clusters[req['compute_id']]
                assert cluster.client_auth == req['auth']
                resp = yield self.close_node(cluster, req['node'],
                                             terminate_pending=req['terminate_pending'], task=task)
                resp = serialize(resp)
            except Exception:
                resp = serialize(-1)

        elif msg.startswith(b'SET_NODE_CPUS:'):
            req = msg[len(b'SET_NODE_CPUS:'):]
            try:
                req = deserialize(req)
                cluster = self._clusters[req['compute_id']]
                assert cluster.client_auth == req['auth']
                if cluster.exclusive or self.cooperative:
                    resp = yield self.set_node_cpus(req['node'], req['cpus'])
                else:
                    resp = -1
            except Exception:
                logger.debug(traceback.format_exc())
                resp = -1
            resp = serialize(resp)

        else:
            logger.debug('Ignoring invalid command')

        if resp is not None:
            try:
                yield conn.send_msg(resp)
            except Exception:
                logger.warning('Failed to send response to %s: %s',
                               str(addr), traceback.format_exc())
        conn.close()
        # end of scheduler_req

    def resend_job_results(self, cluster, task=None):
        # TODO: limit number queued so as not to take up too much space/time
        result_files = [f for f in os.listdir(cluster.dest_path)
                        if f.startswith('_dispy_job_reply_')]
        result_files = result_files[:min(len(result_files), 64)]
        for result_file in result_files:
            result_file = os.path.join(cluster.dest_path, result_file)
            try:
                with open(result_file, 'rb') as fd:
                    result = pickle.load(fd)
            except Exception:
                logger.debug('Could not load "%s"', result_file)
            else:
                status = yield self.send_job_result(
                    result.uid, cluster, result, resending=True, task=task)
                if status:
                    break

    def timer_proc(self, task=None):
        task.set_daemon()
        reset = True
        last_ping_time = last_pulse_time = last_zombie_time = time.time()
        while 1:
            if reset:
                timeout = num_min(self.pulse_interval, self.ping_interval, self.zombie_interval)

            reset = yield task.suspend(timeout)
            if reset:
                continue

            now = time.time()
            if self.pulse_interval and (now - last_pulse_time) >= self.pulse_interval:
                last_pulse_time = now
                dead_nodes = {}
                for node in self._nodes.values():
                    if node.busy and (node.last_pulse + (5 * self.pulse_interval)) < now:
                        logger.warning('Node %s is not responding; removing it (%s, %s, %s)',
                                       node.ip_addr, node.busy, node.last_pulse, now)
                        dead_nodes[node.ip_addr] = node
                for ip_addr in dead_nodes:
                    node = self._nodes.pop(ip_addr, None)
                    clusters = list(node.clusters)
                    node.clusters.clear()
                    for cluster in clusters:
                        dispy_node = cluster._dispy_nodes.pop(node.ip_addr, None)
                        if not dispy_node:
                            continue
                        Task(self.send_node_status, cluster, dispy_node, DispyNode.Closed)

                dead_jobs = [_job for _job in self._sched_jobs.values()
                             if _job.node is not None and _job.node.ip_addr in dead_nodes]
                self.reschedule_jobs(dead_jobs)
                resend = [resend_cluster for resend_cluster in self._clusters.values()
                          if resend_cluster.pending_results and not resend_cluster.zombie]
                for cluster in resend:
                    Task(self.resend_job_results, cluster)

            if self.ping_interval and (now - last_ping_time) >= self.ping_interval:
                last_ping_time = now
                for cluster in self._clusters.values():
                    self.send_ping_cluster(cluster._node_allocs,
                                           set(cluster._dispy_nodes.keys()))
                self.send_ping_cluster(self._node_allocs, set())

            if self.zombie_interval and (now - last_zombie_time) >= self.zombie_interval:
                last_zombie_time = now
                for cluster in self._clusters.values():
                    if (now - cluster.last_pulse) > self.zombie_interval:
                        cluster.zombie = True
                zombies = [cluster for cluster in self._clusters.values()
                           if cluster.zombie and cluster.pending_jobs == 0]
                for cluster in zombies:
                    logger.debug('Deleting zombie computation "%s" / %s',
                                 cluster._compute.name, cluster._compute.id)
                    Task(self.cleanup_computation, cluster)
                zombies = [cluster for cluster in self.pending_clusters.values()
                           if (now - cluster.last_pulse) > self.zombie_interval]
                for cluster in zombies:
                    logger.debug('Deleting zombie computation "%s" / %s',
                                 cluster._compute.name, cluster._compute.id)
                    path = os.path.join(self.dest_path_prefix,
                                        '%s_%s' % (cluster._compute.id, cluster.client_auth))
                    if os.path.isfile(path):
                        os.remove(path)
                    try:
                        shutil.rmtree(cluster.dest_path)
                    except Exception:
                        logger.debug(traceback.format_exc())
                    self.pending_clusters.pop(cluster._compute.id, None)

    def xfer_to_client(self, job_reply, xf, conn, addr):
        _job = self._sched_jobs.get(job_reply.uid, None)
        if _job is None or _job.hash != job_reply.hash:
            logger.warning('Ignoring invalid file transfer from job %s at %s',
                           job_reply.uid, addr[0])
            yield conn.send_msg(serialize(-1))
            raise StopIteration
        node = self._nodes.get(job_reply.ip_addr, None)
        cluster = self._clusters.get(_job.compute_id, None)
        if not node or not cluster:
            logger.warning('Ignoring invalid file transfer from job %s at %s',
                           job_reply.uid, addr[0])
            yield conn.send_msg(serialize(-1))
            raise StopIteration
        node.last_pulse = time.time()
        client_sock = AsyncSocket(socket.socket(node.sock_family, socket.SOCK_STREAM),
                                  keyfile=self.cluster_keyfile, certfile=self.cluster_certfile)
        client_sock.settimeout(MsgTimeout)
        try:
            yield client_sock.connect((cluster.client_ip_addr, cluster.client_job_result_port))
            yield client_sock.send_msg('FILEXFER:'.encode() + serialize(xf))
            yield client_sock.send_msg(serialize(job_reply))

            recvd = yield client_sock.recv_msg()
            recvd = deserialize(recvd)
            while recvd < xf.stat_buf.st_size:
                yield conn.send_msg(serialize(recvd))
                data = yield conn.recvall(min(xf.stat_buf.st_size-recvd, 1024000))
                if not data:
                    break
                yield client_sock.sendall(data)
                recvd = yield client_sock.recv_msg()
                recvd = deserialize(recvd)
            yield conn.send_msg(serialize(recvd))
        except Exception:
            yield conn.send_msg(serialize(-1))
        finally:
            client_sock.close()
            conn.close()

    def send_ping_node(self, ip_addr, port=None, task=None):
        ping_msg = {'version': _dispy_version, 'sign': self.sign, 'port': self.port,
                    'node_ip_addr': ip_addr}
        ping_msg['ip_addrs'] = self.ip_addrs
        if not port:
            port = self.node_port
        if re.match(r'\d+\.', ip_addr):
            sock_family = socket.AF_INET
        else:
            sock_family = socket.AF_INET6
        tcp_sock = AsyncSocket(socket.socket(sock_family, socket.SOCK_STREAM),
                               keyfile=self.node_keyfile, certfile=self.node_certfile)
        tcp_sock.settimeout(MsgTimeout)
        try:
            yield tcp_sock.connect((ip_addr, port))
            yield tcp_sock.sendall(b'x' * len(self.node_auth))
            yield tcp_sock.send_msg(b'PING:' + serialize(ping_msg))
        except Exception:
            pass
        tcp_sock.close()

    def broadcast_ping(self, addrinfos=[], port=None, task=None):
        # generator
        if not port:
            port = self.node_port
        ping_msg = {'version': _dispy_version, 'sign': self.sign, 'port': self.port}
        ping_msg['ip_addrs'] = self.ip_addrs
        if not addrinfos:
            addrinfos = self.addrinfos
        for addrinfo in addrinfos:
            bc_sock = AsyncSocket(socket.socket(addrinfo.family, socket.SOCK_DGRAM))
            bc_sock.settimeout(MsgTimeout)
            ttl_bin = struct.pack('@i', 1)
            if addrinfo.family == socket.AF_INET:
                if self.ipv4_udp_multicast:
                    bc_sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, ttl_bin)
                else:
                    bc_sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
            else:  # addrinfo.family == socket.AF_INET6
                bc_sock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_MULTICAST_HOPS, ttl_bin)
                bc_sock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_MULTICAST_IF, addrinfo.ifn)
            bc_sock.bind((addrinfo.ip, 0))
            try:
                yield bc_sock.sendto(b'PING:' + serialize(ping_msg), (addrinfo.broadcast, port))
            except Exception:
                pass
            bc_sock.close()

    def send_ping_cluster(self, node_allocs, present_ip_addrs, task=None):
        for node_alloc in node_allocs:
            # TODO: we assume subnets are indicated by '*', instead of
            # subnet mask; this is a limitation, but specifying with
            # subnet mask a bit cumbersome.
            if node_alloc.ip_rex.find('*') >= 0:
                Task(self.broadcast_ping, addrinfos=[], port=node_alloc.port)
            else:
                ip_addr = node_alloc.ip_addr
                if ip_addr in present_ip_addrs:
                    continue
                port = node_alloc.port
                Task(self.send_ping_node, ip_addr, port)

    def add_cluster(self, cluster):
        compute = cluster._compute
        compute.pulse_interval = self.pulse_interval
        if self.httpd and cluster.status_callback is None:
            self.httpd.add_cluster(cluster)
        # TODO: should we allow clients to add new nodes, or use only
        # the nodes initially created with command-line?
        self.send_ping_cluster(cluster._node_allocs, set(cluster._dispy_nodes.keys()))
        for ip_addr, node in self._nodes.items():
            if cluster in node.clusters:
                continue
            for node_alloc in cluster._node_allocs:
                cpus = node_alloc.allocate(cluster, node.ip_addr, node.name, node.avail_cpus)
                if cpus <= 0:
                    continue
                if cluster.exclusive or self.cooperative:
                    node.cpus = min(node.avail_cpus, cpus)
                Task(self.setup_node, node, [(node_alloc.depends, node_alloc.setup_args, compute)])
                break

    def cleanup_computation(self, cluster, terminate_pending=False, task=None):
        # generator
        if not cluster.zombie:
            raise StopIteration

        compute = cluster._compute
        cid = compute.id
        pkl_path = os.path.join(self.dest_path_prefix,
                                '%s_%s' % (cid, cluster.client_auth))
        if self._clusters.pop(cid, None) is None:
            if not cluster.pending_results:
                try:
                    os.remove(pkl_path)
                except Exception:
                    logger.debug(traceback.format_exc())
                    pass
            raise StopIteration
        cluster._jobs = []
        cluster.pending_jobs = 0

        if cluster.pending_results == 0:
            try:
                os.remove(pkl_path)
            except Exception:
                logger.warning('Could not remove "%s"', pkl_path)
        else:
            with open(pkl_path, 'wb') as fd:
                pickle.dump(cluster, fd)

        for path, use_count in cluster.file_uses.items():
            if use_count == 1:
                try:
                    os.remove(path)
                except Exception:
                    logger.warning('Could not remove "%s"', path)
        cluster.file_uses.clear()

        if os.path.isdir(cluster.dest_path):
            for dirpath, dirnames, filenames in os.walk(cluster.dest_path, topdown=False):
                if not filenames or dirpath.endswith('__pycache__'):
                    try:
                        shutil.rmtree(dirpath)
                    except Exception:
                        logger.warning('Could not remove "%s"', dirpath)
                        break

        # remove cluster from all nodes before closing (which uses
        # yield); otherwise, scheduler may access removed cluster
        # through node.clusters
        close_nodes = []
        for dispy_node in cluster._dispy_nodes.values():
            node = self._nodes.get(dispy_node.ip_addr, None)
            if not node:
                continue
            drop_jobs = [i for i, _job in enumerate(node.pending_jobs)
                         if _job.compute_id == cid]
            for i in reversed(drop_jobs):
                node.pending_jobs.remove(i)
            node.clusters.discard(cluster)
            close_nodes.append((Task(node.close, compute, terminate_pending=terminate_pending),
                                dispy_node))
        cluster._dispy_nodes.clear()
        for close_task, dispy_node in close_nodes:
            yield close_task.finish()
            yield self.send_node_status(cluster, dispy_node, DispyNode.Closed)
        if self.httpd:
            self.httpd.del_cluster(cluster)
        Task(self.schedule_cluster)

    def setup_node(self, node, setup_computations, task=None):
        # generator
        task.set_daemon()
        for depends, setup_args, compute in setup_computations:
            # NB: to avoid computation being sent multiple times, we
            # add to cluster's _dispy_nodes before sending computation
            # to node
            cluster = self._clusters[compute.id]
            dispy_node = cluster._dispy_nodes.get(node.ip_addr, None)
            if not dispy_node:
                dispy_node = DispyNode(node.ip_addr, node.name, node.cpus)
                cluster._dispy_nodes[node.ip_addr] = dispy_node
                dispy_node.tx = node.tx
                dispy_node.rx = node.rx
            dispy_node.avail_cpus = node.avail_cpus
            dispy_node.avail_info = node.avail_info
            res = yield node.setup(depends, setup_args, compute, resetup=False,
                                   exclusive=cluster.exclusive, task=task)
            if res or compute.id not in self._clusters:
                cluster._dispy_nodes.pop(node.ip_addr, None)
                logger.warning('Failed to setup %s for computation "%s": %s',
                               node.ip_addr, compute.name, res)
                Task(node.close, compute)
            else:
                dispy_node.update_time = time.time()
                node.clusters.add(cluster)
                self._sched_event.set()
                Task(self.send_node_status, cluster, dispy_node, DispyNode.Initialized)

    def add_node(self, info):
        try:
            # assert info['version'] == _dispy_version
            assert info['port'] > 0 and info['cpus'] > 0
            # TODO: check if it is one of ext_ip_addr?
        except Exception:
            # logger.debug(traceback.format_exc())
            return
        node = self._nodes.get(info['ip_addr'], None)
        if node is None:
            logger.debug('Discovered %s:%s (%s) with %s cpus',
                         info['ip_addr'], info['port'], info['name'], info['cpus'])
            node = _Node(info['ip_addr'], info['port'], info['cpus'], info['sign'],
                         self.node_secret, platform=info['platform'],
                         keyfile=self.node_keyfile, certfile=self.node_certfile)
            node.name = info['name']
            node.avail_info = info['avail_info']
            self._nodes[node.ip_addr] = node
        else:
            node.last_pulse = time.time()
            auth = auth_code(self.node_secret, info['sign'])
            if info['cpus'] > 0:
                node.avail_cpus = info['cpus']
                node.cpus = min(node.cpus, node.avail_cpus)
            else:
                logger.warning('Invalid "cpus" %s from %s ignored', info['cpus'], info['ip_addr'])
            if node.port == info['port'] and node.auth == auth:
                return
            logger.debug('Node %s rediscovered', info['ip_addr'])
            node.port = info['port']
            if node.auth is not None:
                dead_jobs = [_job for _job in self._sched_jobs.values()
                             if _job.node is not None and _job.node.ip_addr == node.ip_addr]
                node.busy = 0
                node.auth = auth
                clusters = list(node.clusters)
                node.clusters.clear()
                for cluster in clusters:
                    dispy_node = cluster._dispy_nodes.pop(node.ip_addr, None)
                    if not dispy_node:
                        continue
                    Task(self.send_node_status, cluster, dispy_node, DispyNode.Closed)
                self.reschedule_jobs(dead_jobs)
            node.auth = auth

        setup_computations = []
        node.name = info['name']
        node.scheduler_ip_addr = info['scheduler_ip_addr']
        for cluster in self._clusters.values():
            if cluster in node.clusters:
                continue
            compute = cluster._compute
            for node_alloc in cluster._node_allocs:
                cpus = node_alloc.allocate(cluster, node.ip_addr, node.name, node.avail_cpus)
                if cpus <= 0:
                    continue
                if cluster.exclusive or self.cooperative:
                    node.cpus = min(node.avail_cpus, cpus)
                setup_computations.append((node_alloc.depends, node_alloc.setup_args, compute))
                break
        if setup_computations:
            Task(self.setup_node, node, setup_computations)

    def send_job_result(self, uid, cluster, result, resending=False, task=None):
        # generator
        sock = socket.socket(cluster.client_sock_family, socket.SOCK_STREAM)
        sock = AsyncSocket(sock, keyfile=self.cluster_keyfile, certfile=self.cluster_certfile)
        sock.settimeout(MsgTimeout)
        try:
            yield sock.connect((cluster.client_ip_addr, cluster.client_job_result_port))
            yield sock.send_msg(b'JOB_REPLY:' + serialize(result))
            ack = yield sock.recv_msg()
            assert ack == b'ACK'
        except Exception:
            status = -1
            if not resending:
                # store job result even if computation has not enabled
                # fault recovery; user may be able to access node and
                # retrieve result manually
                f = os.path.join(cluster.dest_path, '_dispy_job_reply_%s' % uid)
                logger.error('Could not send reply for job %s to %s:%s; saving it in "%s"',
                             uid, cluster.client_ip_addr, cluster.client_job_result_port, f)
                try:
                    with open(f, 'wb') as fd:
                        pickle.dump(result, fd)
                except Exception:
                    logger.debug('Could not save reply for job %s', uid)
                else:
                    cluster.pending_results += 1
                    cluster.file_uses[f] = 2
        else:
            status = 0
            cluster.last_pulse = time.time()
            if result.status != DispyJob.ProvisionalResult:
                if resending:
                    cluster.pending_results -= 1
                    f = os.path.join(cluster.dest_path, '_dispy_job_reply_%s' % uid)
                    if os.path.isfile(f):
                        cluster.file_uses.pop(f, None)
                        try:
                            os.remove(f)
                        except Exception:
                            logger.warning('Could not remove "%s"', f)
                else:
                    self.done_jobs.pop(uid, None)
                    if cluster.pending_results:
                        Task(self.resend_job_results, cluster)
            if cluster.pending_jobs == 0 and cluster.pending_results == 0 and cluster.zombie:
                Task(self.cleanup_computation, cluster)
            dispy_node = cluster._dispy_nodes.get(result.ip_addr, None)
            if dispy_node:
                Task(self.send_node_status, cluster, dispy_node, DispyNode.AvailInfo)
        finally:
            sock.close()

        raise StopIteration(status)

    def delete_node(self, node):
        if node.clusters:
            dead_jobs = [_job for _job in self._sched_jobs.values()
                         if _job.node is not None and _job.node.ip_addr == node.ip_addr]
            clusters = list(node.clusters)
            node.clusters.clear()
            for cluster in clusters:
                dispy_node = cluster._dispy_nodes.pop(node.ip_addr, None)
                if not dispy_node:
                    continue
                dispy_node.avail_cpus = dispy_node.cpus = dispy_node.busy = 0
                if cluster.cluster_status:
                    self.worker_Q.put((cluster.cluster_status,
                                       (DispyNode.Closed, dispy_node, None)))
            self.reschedule_jobs(dead_jobs)

        for _job in node.pending_jobs:
            cluster = self._clusters[_job.compute_id]
            self.finish_job(cluster, _job, DispyJob.Cancelled)
            if cluster.cluster_status:
                dispy_node = cluster._dispy_nodes.get(node.ip_addr, None)
                self.worker_Q.put((cluster.cluster_status,
                                   (DispyJob.Cancelled, dispy_node, _job.job)))
        node.pending_jobs = []
        self._nodes.pop(node.ip_addr, None)

    def send_job_status(self, cluster, _job, task=None):
        if cluster.status_callback:
            dispy_node = cluster._dispy_nodes.get(_job.node.ip_addr, None)
            # assert _job.job.status == DispyJob.Running
            if dispy_node:
                dispy_node.busy += 1
                dispy_node.update_time = time.time()
                cluster.status_callback(_job.job.status, dispy_node, _job.job)
        sock = socket.socket(cluster.client_sock_family, socket.SOCK_STREAM)
        sock = AsyncSocket(sock, keyfile=self.cluster_keyfile, certfile=self.cluster_certfile)
        sock.settimeout(MsgTimeout)
        try:
            yield sock.connect((cluster.client_ip_addr, cluster.client_job_result_port))
            status = {'uid': _job.uid, 'status': _job.job.status, 'node': _job.node.ip_addr,
                      'hash': _job.hash}
            status['start_time'] = _job.job.start_time
            yield sock.send_msg(b'JOB_STATUS:' + serialize(status))
        except Exception:
            logger.warning('Could not send job status to %s:%s',
                           cluster.client_ip_addr, cluster.client_job_result_port)
        sock.close()

    def send_node_status(self, cluster, dispy_node, status, task=None):
        status_info = {'compute_id': cluster._compute.id, 'ip_addr': dispy_node.ip_addr,
                       'status': status, 'auth': cluster.client_auth}
        if status == DispyNode.AvailInfo:
            status_info['avail_info'] = dispy_node.avail_info
            status_info['tx'] = dispy_node.tx
            status_info['rx'] = dispy_node.rx
        elif status == DispyNode.Initialized:
            status_info['dispy_node'] = dispy_node
            node = self._nodes.get(dispy_node.ip_addr, None)
            if node:
                status_info['node_auth'] = node.auth
                status_info['node_port'] = node.port
        elif status == DispyNode.Closed:
            status_info['tx'] = dispy_node.tx
            status_info['rx'] = dispy_node.rx
        elif status == 'node_cpus':
            status_info['node_cpus'] = dispy_node.cpus
            status = None
        else:
            logger.warning('Ignoring invalid node status %s to %s', status, dispy_node.ip_addr)
            raise StopIteration

        if cluster.status_callback:
            dispy_node.update_time = time.time()
            cluster.status_callback(status, dispy_node, None)

        sock = socket.socket(cluster.client_sock_family, socket.SOCK_STREAM)
        sock = AsyncSocket(sock, keyfile=self.cluster_keyfile, certfile=self.cluster_certfile)
        sock.settimeout(MsgTimeout)
        try:
            yield sock.connect((cluster.client_ip_addr, cluster.client_job_result_port))
            yield sock.send_msg(b'NODE_STATUS:' + serialize(status_info))
        except Exception:
            logger.debug('Could not send node status to %s:%s',
                         cluster.client_ip_addr, cluster.client_job_result_port)
        sock.close()

    def job_reply_process(self, reply, msg_len, sock, addr):
        _job = self._sched_jobs.get(reply.uid, None)
        if not _job or reply.hash != _job.hash:
            logger.warning('Ignoring invalid reply for job %s from %s', reply.uid, addr[0])
            yield sock.send_msg(b'ACK')
            raise StopIteration
        job = _job.job
        _job._args = _job._kwargs = None
        node = self._nodes.get(reply.ip_addr, None)
        cluster = self._clusters.get(_job.compute_id, None)
        if not cluster:
            # job cancelled while/after closing computation
            if node:
                node.rx += msg_len
                if node.busy > 0:
                    node.busy -= 1
                    node.cpu_time += reply.end_time - reply.start_time
                    node.last_pulse = time.time()
                    self._sched_event.set()
            yield sock.send_msg(b'ACK')
            raise StopIteration
        if not node:
            logger.warning('Ignoring invalid reply for job %s from %s', reply.uid, addr[0])
            yield sock.send_msg(b'ACK')
            raise StopIteration
        # assert reply.ip_addr == node.ip_addr
        node.last_pulse = time.time()
        logger.debug('Received reply for job %s from %s', _job.uid, addr[0])
        # assert _job.job.status not in [DispyJob.Created, DispyJob.Finished]
        setattr(reply, 'cpus', node.cpus)
        dispy_node = cluster._dispy_nodes.get(_job.node.ip_addr, None)
        if dispy_node:
            dispy_node.rx += msg_len

        yield sock.send_msg(b'ACK')
        job.start_time = reply.start_time
        job.end_time = reply.end_time
        if reply.status != DispyJob.ProvisionalResult:
            self.done_jobs[_job.uid] = _job
            del self._sched_jobs[_job.uid]
            node.busy -= 1
            node.cpu_time += reply.end_time - reply.start_time
            if cluster.status_callback:
                if dispy_node:
                    dispy_node.busy -= 1
                    dispy_node.jobs_done += 1
                    dispy_node.cpu_time += reply.end_time - reply.start_time
                    dispy_node.update_time = time.time()
                    cluster.status_callback(reply.status, dispy_node, job)

            cluster.pending_jobs -= 1
            if cluster.pending_jobs == 0:
                cluster.end_time = time.time()
                if cluster.zombie:
                    Task(self.cleanup_computation, cluster)
            self._sched_event.set()
            for xf in _job.xfer_files:
                try:
                    cluster.file_uses[xf.name] -= 1
                    if cluster.file_uses[xf.name] == 0:
                        cluster.file_uses.pop(xf.name)
                        os.remove(xf.name)
                except Exception:
                    logger.warning('Could not remove "%s"', xf.name)
        Task(self.send_job_result, _job.uid, cluster, reply, resending=False)

    def reschedule_jobs(self, dead_jobs):
        if not dead_jobs:
            return
        for _job in dead_jobs:
            self._sched_jobs.pop(_job.uid, None)
            cluster = self._clusters.get(_job.compute_id, None)
            if not cluster:
                continue
            if cluster._compute.reentrant and not _job.pinned:
                logger.debug('Rescheduling job %s from %s', _job.uid, _job.node.ip_addr)
                _job.job.status = DispyJob.Created
                _job.hash = ''.join(hex(x)[2:] for x in os.urandom(10))
                cluster._jobs.append(_job)
            else:
                logger.debug('Terminating job %s scheduled on %s', _job.uid, _job.node.ip_addr)
                reply = _JobReply(_job, _job.node.ip_addr, status=DispyJob.Abandoned)
                reply.result = serialize(None)
                cluster.pending_jobs -= 1
                if cluster.pending_jobs == 0:
                    cluster.end_time = time.time()
                self.done_jobs[_job.uid] = _job
                Task(self.send_job_result, _job.uid, cluster, reply, resending=False)
        self._sched_event.set()

    def load_balance_node(self):
        """Return node with least load
        """
        # TODO: maintain "available" sequence of nodes for better performance
        node = None
        load = 1.0
        for host in self._nodes.values():
            if host.busy >= host.cpus:
                continue
            if host.pending_jobs:
                return host
            if not any(cluster._jobs for cluster in host.clusters):
                continue
            if (host.busy / host.cpus) < load:
                node = host
                load = host.busy / host.cpus
        return node

    def fsfs_job_schedule(self):
        """Return tuple (_job, node, cluster) such that _job is earliest
        submitted in all clusters.
        """
        node = self.load_balance_node()
        if not node:
            return (None, None, None)
        _job = cluster = lrs = None
        for cluster in node.clusters:
            if cluster._jobs and (not lrs or
                                  cluster._jobs[0].job.submit_time < lrs._jobs[0].job.submit_time):
                lrs = cluster
        if lrs:
            if node.pending_jobs:
                if node.pending_jobs[0].job.submit_time < lrs._jobs[0].job.submit_time:
                    _job = node.pending_jobs.pop(0)
                    cluster = self._clusters[_job.compute_id]
            if not _job:
                cluster = lrs
                _job = cluster._jobs.pop(0)
        elif node.pending_jobs:
            _job = node.pending_jobs.pop(0)
            cluster = self._clusters[_job.compute_id]
        return (_job, node, cluster)

    def fair_cluster_schedule(self):
        """Return tuple (_job, node, cluster) such that cluster is earliest
        scheduled last time.
        """
        node = self.load_balance_node()
        if not node:
            return (None, None, None)
        _job = cluster = lrs = None
        for cluster in node.clusters:
            if cluster._jobs and (not lrs or cluster.job_sched_time < lrs.job_sched_time):
                lrs = cluster
        if lrs:
            if node.pending_jobs:
                _job = node.pending_jobs[0]
                cluster = self._clusters[_job.compute_id]
                if cluster.job_sched_time < lrs.job_sched_time:
                    node.pending_jobs.pop(0)
                else:
                    cluster = lrs
                    _job = cluster._jobs.pop(0)
            if not _job:
                cluster = lrs
                _job = cluster._jobs.pop(0)
        elif node.pending_jobs:
            _job = node.pending_jobs.pop(0)
            cluster = self._clusters[_job.compute_id]
        if _job:
            cluster.job_sched_time = time.time()
        return (_job, node, cluster)

    def fcfs_cluster_schedule(self):
        """Return tuple (_job, node, cluster) such that cluster is created
        earliest.
        """
        node = self.load_balance_node()
        if not node:
            return (None, None, None)
        _job = cluster = lrs = None
        for cluster in node.clusters:
            if cluster._jobs and (not lrs or cluster.start_time < lrs.start_time):
                lrs = cluster
        if lrs:
            if node.pending_jobs:
                _job = node.pending_jobs[0]
                cluster = self._clusters[_job.compute_id]
                if cluster.start_time < lrs.start_time:
                    node.pending_jobs.pop(0)
                else:
                    cluster = lrs
                    _job = cluster._jobs.pop(0)
            if not _job:
                cluster = lrs
                _job = cluster._jobs.pop(0)
        elif node.pending_jobs:
            _job = node.pending_jobs.pop(0)
            cluster = self._clusters[_job.compute_id]
        return (_job, node, cluster)

    def run_job(self, _job, cluster, task=None):
        # generator
        # assert task is not None
        node = _job.node
        dispy_node = cluster._dispy_nodes.get(node.ip_addr, None)
        try:
            tx = yield _job.run(task=task)
            if dispy_node:
                dispy_node.tx += tx
        except (EnvironmentError, OSError):
            logger.warning('Failed to run job %s on %s for computation %s; removing this node',
                           _job.uid, node.ip_addr, cluster._compute.name)
            self.delete_node(node)
            if self._sched_jobs.pop(_job.uid, None) == _job:
                if not _job.pinned:
                    cluster._jobs.insert(0, _job)
                node.busy -= 1
            self._sched_event.set()
        except Exception:
            logger.warning('Failed to run job %s on %s for computation %s',
                           _job.uid, node.ip_addr, cluster._compute.name)
            # logger.debug(traceback.format_exc())
            # TODO: delay executing again for some time?
            # this job might have been deleted already due to timeout
            if self._sched_jobs.pop(_job.uid, None) == _job:
                if cluster.status_callback:
                    if dispy_node:
                        dispy_node.update_time = time.time()
                        cluster.status_callback(DispyJob.Cancelled, dispy_node, _job.job)
                node.busy -= 1
            self._sched_event.set()
        else:
            # job may have already finished (in which case _job.job would be None)
            if _job.job:
                logger.debug('Running job %s on %s (busy: %d / %d)',
                             _job.uid, node.ip_addr, node.busy, node.cpus)
                _job.job.status = DispyJob.Running
                _job.job.start_time = time.time()
                cluster = self._clusters[_job.compute_id]
                # TODO/Note: It is likely that this job status may arrive at
                # the client before the job is done and the node's status
                # arrives. Either use queing for messages (ideally with
                # pycos's message passing) or tag messages with timestamps
                # so recipient can use temporal ordering to ignore prior
                # messages
                Task(self.send_job_status, cluster, _job)
        if not cluster._compute.reentrant:
            _job._args = _job._kwargs = None

    def _schedule_jobs(self, task=None):
        # generator
        assert task is not None
        while not self.terminate:
            # n = sum(len(cluster._jobs) for cluster in self._clusters.values())
            _job, node, cluster = self.select_job_node_cluster()
            if not _job:
                self._sched_event.clear()
                yield self._sched_event.wait()
                continue
            _job.node = node
            # assert node.busy < node.cpus
            self._sched_jobs[_job.uid] = _job
            node.busy += 1
            Task(self.run_job, _job, cluster)

        logger.debug('Scheduler quitting: %s', len(self._sched_jobs))
        for uid, _job in self._sched_jobs.items():
            cluster = self._clusters.get(_job.compute_id, None)
            if cluster:
                reply = _JobReply(_job, cluster.ip_addr, status=DispyJob.Terminated)
                reply.result = serialize(None)
                Task(self.send_job_result, _job.uid, cluster, reply, resending=False)
        for cluster in self._clusters.values():
            for _job in cluster._jobs:
                reply = _JobReply(_job, cluster.ip_addr, status=DispyJob.Terminated)
                reply.result = serialize(None)
                Task(self.send_job_result, _job.uid, cluster, reply, resending=False)
            cluster._jobs = []

        for cluster in list(self._clusters.values()):
            cluster.pending_jobs = 0
            cluster.zombie = True
            yield self.cleanup_computation(cluster)
        self._clusters = {}
        self._sched_jobs = {}
        self.done_jobs = {}
        logger.debug('Scheduler quit')

    def retrieve_job_req(self, conn, msg):
        # generator

        def send_reply(reply):
            try:
                yield conn.send_msg(serialize(reply))
            except Exception:
                raise StopIteration(-1)
            raise StopIteration(0)

        try:
            req = deserialize(msg)
            uid = req['uid']
            compute_id = req['compute_id']
            auth = req['auth']
            job_hash = req['hash']
        except Exception:
            yield send_reply(None)
            raise StopIteration

        pkl_path = os.path.join(self.dest_path_prefix, '%s_%s' % (compute_id, auth))
        cluster = self._clusters.get(compute_id, None)
        if not cluster or cluster.client_auth != auth:
            with open(pkl_path, 'rb') as fd:
                cluster = pickle.load(fd)
        if not cluster or cluster.client_auth != auth:
            yield send_reply(None)
            raise StopIteration

        info_file = os.path.join(cluster.dest_path, '_dispy_job_reply_%s' % uid)
        if not os.path.isfile(info_file):
            yield send_reply(None)
            raise StopIteration

        try:
            with open(info_file, 'rb') as fd:
                job_reply = pickle.load(fd)
            assert job_reply.hash == job_hash
        except Exception:
            yield send_reply(None)
            raise StopIteration

        try:
            yield conn.send_msg(serialize(job_reply))
            ack = yield conn.recv_msg()
            assert ack == 'ACK'.encode()
            cluster.pending_results -= 1
            with open(pkl_path, 'wb') as fd:
                pickle.dump(cluster, fd)
        except Exception:
            pass
        else:
            cluster.file_uses.pop(info_file, None)
            try:
                os.remove(info_file)
            except Exception:
                pass

    def cancel_job(self, cluster, uid):
        # function
        cluster.last_pulse = time.time()
        _job = self._sched_jobs.get(uid, None)
        if _job:
            _job.job.status = DispyJob.Cancelled
            Task(_job.node.send, b'TERMINATE_JOB:' + serialize(_job), reply=False)
            return 0
        else:
            for i, _job in enumerate(cluster._jobs):
                if _job.uid == uid:
                    del cluster._jobs[i]
                    self.done_jobs[_job.uid] = _job
                    cluster.pending_jobs -= 1
                    reply = _JobReply(_job, cluster.ip_addr, status=DispyJob.Cancelled)
                    reply.result = serialize(None)
                    Task(self.send_job_result, _job.uid, cluster, reply, resending=False)
                    return 0

            for ip_addr in cluster._dispy_nodes:
                node = self._nodes.get(ip_addr, None)
                if not node:
                    continue
                for i, _job in enumerate(node.pending_jobs):
                    if _job.uid == uid:
                        del node.pending_jobs[i]
                        self.done_jobs[_job.uid] = _job
                        cluster.pending_jobs -= 1
                        reply = _JobReply(_job, cluster.ip_addr, status=DispyJob.Cancelled)
                        reply.result = serialize(None)
                        Task(self.send_job_result, _job.uid, cluster, reply, resending=False)
                        return 0
            logger.debug('Invalid job %s!', uid)
            return -1

    def allocate_node(self, cluster, node_alloc, task=None):
        # generator
        if not isinstance(node_alloc, list):
            node_alloc = [node_alloc]
        for i in range(len(node_alloc)-1, -1, -1):
            node = self._nodes.get(node_alloc[i].ip_addr, None)
            if node:
                dispy_node = cluster._dispy_nodes.get(node.ip_addr, None)
                if dispy_node:
                    node.clusters.add(cluster)
                    self._sched_event.set()
                    del node_alloc[i]
                    continue
        if not node_alloc:
            raise StopIteration(0)
        cluster._node_allocs.extend(node_alloc)
        cluster._node_allocs = sorted(cluster._node_allocs,
                                      key=lambda node_alloc: node_alloc.ip_rex, reverse=True)
        present = set()
        cluster._node_allocs = [na for na in cluster._node_allocs
                                if na.ip_rex not in present and not present.add(na.ip_rex)]
        del present
        yield self.add_cluster(cluster)
        raise StopIteration(0)

    def close_node(self, cluster, node, terminate_pending, task=None):
        # generator
        node = self._nodes.get(_node_ipaddr(node), None)
        if node is None:
            raise StopIteration(-1)
        node.clusters.discard(cluster)
        yield node.close(cluster._compute, terminate_pending=terminate_pending)

    def node_jobs(self, cluster, node, from_node=False, get_uids=True, task=None):
        # generator
        node = self._nodes.get(_node_ipaddr(node), None)
        if not node or cluster not in node.clusters:
            raise StopIteration([])
        if from_node:
            sock = socket.socket(node.sock_family, socket.SOCK_STREAM)
            sock = AsyncSocket(sock, keyfile=self.node_keyfile, certfile=self.node_certfile)
            sock.settimeout(MsgTimeout)
            try:
                yield sock.connect((node.ip_addr, node.port))
                yield sock.sendall(node.auth)
                req = {'compute_id': cluster._compute.id, 'auth': cluster._compute.auth}
                yield sock.send_msg(b'JOBS:' + serialize(req))
                msg = yield sock.recv_msg()
                uids = [info['uid'] for info in deserialize(msg)]
            except Exception:
                logger.debug(traceback.format_exc())
                uids = []
            sock.close()
            if get_uids:
                jobs = uids
            else:
                _jobs = [self._sched_jobs.get(uid, None) for uid in uids]
                jobs = [_job.job for _job in _jobs if _job is not None
                        and _job.compute_id == cluster._compute.id
                        ]
        else:
            if get_uids:
                jobs = [_job.uid for _job in self._sched_jobs.values() if _job.node == node
                        and _job.compute_id == cluster._compute.id]
            else:
                jobs = [_job.job for _job in self._sched_jobs.values() if _job.node == node
                        and _job.compute_id == cluster._compute.id]

        raise StopIteration(jobs)

    def set_node_cpus(self, node, cpus):
        # generator
        try:
            cpus = int(cpus)
        except ValueError:
            raise StopIteration(-1)
        node = self._nodes.get(_node_ipaddr(node), None)
        if node is None:
            raise StopIteration(-1)
        if cpus >= 0:
            node.cpus = min(node.avail_cpus, cpus)
        elif (node.avail_cpus + cpus) >= 0:
            node.cpus = node.avail_cpus + cpus
        cpus = node.cpus
        for cluster in node.clusters:
            dispy_node = cluster._dispy_nodes.get(node.ip_addr, None)
            if dispy_node:
                dispy_node.cpus = cpus
                Task(self.send_node_status, cluster, dispy_node, 'node_cpus')
        yield self._sched_event.set()
        raise StopIteration(0)

    def shutdown(self):
        def _shutdown(self, task=None):
            logger.debug('Shutting down scheduler ...')
            for cluster in list(self.pending_clusters.values()) + self.unsched_clusters:
                path = os.path.join(self.dest_path_prefix,
                                    '%s_%s' % (cluster._compute.id, cluster.client_auth))
                if os.path.isfile(path):
                    os.remove(path)
                try:
                    shutil.rmtree(cluster.dest_path)
                except Exception:
                    logger.debug(traceback.format_exc())
                # TODO: inform cluster
            self.pending_clusters.clear()
            self.unsched_clusters = []
            while (any(cluster.pending_jobs for cluster in self._clusters.values())):
                logger.warning('Waiting for %s clusters to finish', len(self._clusters))
                yield task.sleep(5)

            self._sched_event.set()
            yield self.job_scheduler_task.finish()
            yield self.print_status()

        if self.terminate:
            return
        self.terminate = True
        Task(_shutdown, self).value()
        if self.pycos:
            self.pycos.finish()
            self.pycos = None
        Singleton.discard(self.__class__)

    def print_status(self):
        print('')
        heading = ' %30s | %5s | %13s' % ('Node', 'CPUs', 'Node Time Sec')
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
            print(' %-30.30s | %5s | %13.3f' % (name, node.cpus, node.cpu_time))
        print('')
        print('Total job time: %.3f sec\n' % (tot_cpu_time))
        if self._clusters:
            print('Current clients: %s (%s)' % (len(self._clusters),
                                                ', '.join(cluster.ip_addr for cluster in
                                                          self._clusters.values())))
        if self.unsched_clusters:
            print('Pending clients: %s' % (len(self.unsched_clusters)))
        print('')
        yield 0


if __name__ == '__main__':
    import argparse

    logger.name = 'dispyscheduler'

    parser = argparse.ArgumentParser()
    parser.add_argument('--config', dest='config', default='',
                        help='use configuration in given file')
    parser.add_argument('--save_config', dest='save_config', default='',
                        help='save configuration in given file and exit')
    parser.add_argument('-d', '--debug', action='store_true', dest='loglevel', default=False,
                        help='if given, debug messages are printed')
    parser.add_argument('-n', '--node', action='append', dest='nodes', default=[],
                        help='node host name or IP address used for all computations; '
                        'repeat for multiple nodes')
    parser.add_argument('-i', '--host', action='append', dest='hosts', default=[],
                        help='host name or IP address to use; repeat for multiple interfaces')
    parser.add_argument('--ext_host', action='append', dest='ext_hosts', default=[],
                        help='external host name or IP address to use (needed in case of '
                        'NAT firewall/gateway); repeat for multiple interfaces')
    parser.add_argument('-p', '--port', dest='dispy_port', type=int, default=dispy.config.DispyPort,
                        help='dispy port number')
    parser.add_argument('--ipv4_udp_multicast', dest='ipv4_udp_multicast', action='store_true',
                        default=False, help='use multicast for IPv4 UDP instead of broadcast')
    parser.add_argument('--node_secret', dest='node_secret', default='',
                        help='authentication secret for handshake with dispy clients')
    parser.add_argument('--node_keyfile', dest='node_keyfile', default='',
                        help='file containing SSL key to be used with nodes')
    parser.add_argument('--node_certfile', dest='node_certfile', default='',
                        help='file containing SSL certificate to be used with nodes')
    parser.add_argument('--cluster_secret', dest='cluster_secret', default='',
                        help='file containing SSL certificate to be used with dispy clients')
    parser.add_argument('--cluster_certfile', dest='cluster_certfile', default='',
                        help='file containing SSL certificate to be used with dispy clients')
    parser.add_argument('--cluster_keyfile', dest='cluster_keyfile', default='',
                        help='file containing SSL key to be used with dispy clients')
    parser.add_argument('--pulse_interval', dest='pulse_interval', type=float, default=0,
                        help='number of seconds between pulse messages to indicate '
                        'whether node is alive')
    parser.add_argument('--ping_interval', dest='ping_interval', type=float, default=0,
                        help='number of seconds between ping messages to discover nodes')
    parser.add_argument('--zombie_interval', dest='zombie_interval', default=60, type=float,
                        help='interval in minutes to presume unresponsive scheduler is zombie')
    parser.add_argument('--msg_timeout', dest='msg_timeout', default=MsgTimeout, type=float,
                        help='timeout used for messages to/from client/nodes in seconds')
    parser.add_argument('--dest_path_prefix', dest='dest_path_prefix', default=None,
                        help='path prefix where files sent by dispy are stored')
    parser.add_argument('--max_file_size', dest='max_file_size', type=str, default=str(MaxFileSize),
                        help='maximum file size of any file transferred')
    parser.add_argument('--clean', action='store_true', dest='clean', default=False,
                        help='if given, files copied from or generated by clients '
                        'will be removed before scheduler starts')
    parser.add_argument('--httpd', action='store_true', dest='http_server', default=False,
                        help='if given, HTTP server is created so clusters can be '
                        'monitored and managed')
    parser.add_argument('--fair_cluster_scheduler', dest='scheduler_alg', action='store_const',
                        const='fair_cluster',
                        help='Choose job from cluster that was least recently scheduled')
    parser.add_argument('--early_cluster_scheduler', dest='scheduler_alg', action='store_const',
                        const='fcfs_cluster',
                        help='Choose job from cluster created earliest')
    parser.add_argument('--cooperative', action='store_true', dest='cooperative', default=False,
                        help='if given, clients (clusters) can update CPUs')
    parser.add_argument('--cleanup_nodes', action='store_true', dest='cleanup_nodes', default=False,
                        help='if given, nodes always remove files even if '
                        '"cleanup=False" is used by clients')
    parser.add_argument('--daemon', action='store_true', dest='daemon', default=False,
                        help='if given, input is not read from terminal')

    config = vars(parser.parse_args(sys.argv[1:]))

    if config['config']:
        import configparser
        cfg = configparser.ConfigParser()
        cfg.read(config['config'])
        cfg = dict(cfg.items('DEFAULT'))
        cfg['nodes'] = ([] if cfg['nodes'] == '[]' else
                        [_.strip() for _ in cfg['nodes'][1:-1].split(',')])
        cfg['hosts'] = ([] if cfg['hosts'] == '[]' else
                        [_.strip() for _ in cfg['hosts'][1:-1].split(',')])
        cfg['ext_hosts'] = ([] if cfg['ext_hosts'] == '[]' else
                            [_.strip() for _ in cfg['ext_hosts'][1:-1].split(',')])
        cfg['dispy_port'] = int(cfg['dispy_port'])
        cfg['ipv4_udp_multicast'] = cfg['ipv4_udp_multicast'] == 'True'
        cfg['pulse_interval'] = float(cfg['pulse_interval'])
        cfg['ping_interval'] = float(cfg['ping_interval'])
        cfg['zombie_interval'] = float(cfg['zombie_interval'])
        cfg['msg_timeout'] = float(cfg['msg_timeout'])
        cfg['loglevel'] = cfg['loglevel'] == 'True'
        cfg['clean'] = cfg['clean'] == 'True'
        cfg['http_server'] = cfg['http_server'] == 'True'
        cfg['cooperative'] = cfg['cooperative'] == 'True'
        cfg['cleanup_nodes'] = cfg['cleanup_nodes'] == 'True'
        cfg['daemon'] = cfg['daemon'] == 'True'
        if cfg['dest_path_prefix'] == 'None':
            cfg['dest_path_prefix'] = None
        if cfg['scheduler_alg'] == 'None':
            cfg['scheduler_alg'] = None
        config = cfg
    config.pop('config', None)

    cfg = config.pop('save_config', None)
    if cfg:
        import configparser
        for key, val in list(config.items()):
            if not isinstance(val, str):
                config[key] = str(val)
        config = configparser.ConfigParser(config)
        cfg = open(cfg, 'w')
        config.write(cfg)
        cfg.close()
        exit(0)
    del parser, cfg

    if config['loglevel']:
        logger.setLevel(logger.DEBUG)
        pycos.logger.setLevel(pycos.logger.DEBUG)
    else:
        logger.setLevel(logger.INFO)
    del config['loglevel']

    dispy.config.DispyPort = config.pop('dispy_port')
    if config['zombie_interval']:
        config['zombie_interval'] = float(config['zombie_interval'])
        if config['zombie_interval'] < 1:
            raise Exception('zombie_interval must be at least 1')

    MsgTimeout = config['msg_timeout']
    del config['msg_timeout']

    m = re.match(r'(\d+)([kKmMgGtT]?)', config['max_file_size'])
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
    del config['max_file_size']

    if config['node_certfile']:
        config['node_certfile'] = os.path.abspath(config['node_certfile'])
    else:
        config['node_certfile'] = None
    if config['node_keyfile']:
        config['node_keyfile'] = os.path.abspath(config['node_keyfile'])
    else:
        config['node_keyfile'] = None

    if config['cluster_certfile']:
        config['cluster_certfile'] = os.path.abspath(config['cluster_certfile'])
    else:
        config['cluster_certfile'] = None
    if config['cluster_keyfile']:
        config['cluster_keyfile'] = os.path.abspath(config['cluster_keyfile'])
    else:
        config['cluster_keyfile'] = None

    daemon = config.pop('daemon', False)
    if not daemon:
        try:
            if os.getpgrp() != os.tcgetpgrp(sys.stdin.fileno()):
                daemon = True
        except (Exception, KeyboardInterrupt):
            pass

    logger.info('version: %s (Python %s), PID: %s',
                _dispy_version, platform.python_version(), os.getpid())
    scheduler = _Scheduler(**config)
    if daemon:
        scheduler.job_scheduler_task.value()
    else:
        while 1:
            try:
                cmd = input('Enter "quit" or "exit" to terminate scheduler, '
                            'anything else to get status: ')
                cmd = cmd.strip().lower()
                if cmd == 'quit' or cmd == 'exit':
                    break
                else:
                    Task(scheduler.print_status)
                    time.sleep(0.2)
            except KeyboardInterrupt:
                # TODO: terminate even if jobs are scheduled?
                logger.info('Interrupted; terminating')
                break
            except (Exception, KeyboardInterrupt):
                logger.debug(traceback.format_exc())
    scheduler.shutdown()
    exit(0)
