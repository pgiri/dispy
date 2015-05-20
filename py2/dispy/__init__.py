#!/usr/bin/python

"""
dispy: Distribute computations among CPUs/cores on a single machine or
machines in cluster(s), grid, cloud etc. for parallel execution.
See http://dispy.sourceforge.net for details.
"""

__author__ = "Giridhar Pemmasani (pgiri@yahoo.com)"
__email__ = "pgiri@yahoo.com"
__copyright__ = "Copyright 2011, Giridhar Pemmasani"
__contributors__ = []
__maintainer__ = "Giridhar Pemmasani (pgiri@yahoo.com)"
__license__ = "MIT"
__url__ = "http://dispy.sourceforge.net"
__status__ = "Production"
__version__ = "4.3"

__all__ = ['logger', 'DispyJob', 'DispyNode', 'JobCluster', 'SharedJobCluster']

import os
import sys
import time
import socket
import inspect
import stat
import threading
import logging
import re
import ssl
import hashlib
import traceback
import shelve
import datetime
import atexit
import Queue as queue
import numbers
import collections

import asyncoro
from asyncoro import Coro, AsynCoro, AsyncSocket, MetaSingleton, serialize, unserialize

_dispy_version = __version__
MsgTimeout = 5

logger = logging.getLogger('dispy')
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(message)s'))
logger.addHandler(handler)
del handler


class DispyJob(object):
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

    .id field is initially set to None and may be assigned by user to
    any value that is appropriate.  This may be useful, for example,
    to distinguish one job from another.

    .status is read-only field; it is set to one of Created, Running,
    Finished, Cancelled, Terminated and ProvisionalResult, indicating
    current status of job.  If job is created for SharedJobCluster,
    status is not updated to Running when job is actually running.

    .ip_addr is read-inly field; it is set to IP address of node that
    executed job.

    .finish is a read-only event that is set when a job's results are
    available.

    """

    __slots__ = ('id', 'args', 'kwargs', 'result', 'stdout', 'stderr', 'exception',
                 'start_time', 'end_time', 'status', 'ip_addr', 'finish', '_dispy_job_')

    Created = 5
    Running = 6
    ProvisionalResult = 7
    # NB: Cancelled, Terminated and Finished status should have
    # values in that order, as PriorityQueue sorts data.
    # Thus, if a job with provisional result is already in the queue
    # and a job is finished, finished/terminated job is processed (in
    # callback) last.
    Cancelled = 8
    Terminated = 9
    Abandoned = 10
    Finished = 11

    def __init__(self):
        # id can be assigned by user as appropriate (e.g., to distinguish jobs)
        self.id = None
        # rest are read-only
        self.args = None
        self.kwargs = None
        self.result = None
        self.stdout = None
        self.stderr = None
        self.exception = None
        self.start_time = None
        self.end_time = None
        self.status = DispyJob.Created
        self.ip_addr = None
        self.finish = threading.Event()

        # _dispy_job_ is for dispy implementation only - it is opaque to users
        self._dispy_job_ = None

    def __call__(self, clear=False):
        self.finish.wait()
        if clear:
            self.finish.clear()
        return self.result

    def __lt__(self, other):
        if isinstance(self._dispy_job_, _DispyJob_):
            if isinstance(other._dispy_job_, _DispyJob_):
                return self._dispy_job_ < other._dispy_job_
            else:
                return True
        else:
            return False


class DispyNode(object):
    """If 'cluster_status' is used when creating cluster, that function
    is called with an instance of this class as first argument.
    See 'cluster_status' in JobCluster below.
    """

    Initialized = DispyJob.Created - 1
    Closed = DispyJob.Finished + 5

    def __init__(self, ip_addr, name, cpus):
        self.ip_addr = ip_addr
        self.name = name
        self.cpus = cpus
        self.avail_cpus = cpus
        self.busy = 0
        self.jobs_done = 0
        self.cpu_time = 0.0
        self.secs_per_job = 0
        self.update_time = 0


# a cluster's "status" function (not "cluster_status" callback)
# returns this structure; "nodes" is list of DispyNode objects and
# "jobs_pending" is number of jobs that are not done yet
ClusterStatus = collections.namedtuple('ClusterStatus', ['nodes', 'jobs_pending'])


class NodeSpec(object):
    """
    """
    def __init__(self, name_ip, port=None, cpus=0):
        self.ip_addr = _node_ipaddr(name_ip)
        self.rex = self.ip_addr.replace('.', '\\.').replace('*', '.*')
        if port:
            try:
                port = int(port)
                assert port > 0
            except:
                logger.warning('port must be > 0')
                port = None
        self.port = port
        if cpus:
            try:
                cpus = int(cpus)
                assert cpus != 0
            except:
                logger.warning('cpus must be > 0 or < 0')
                cpus = 0
        self.cpus = cpus


def num_min(*args):
    items = [arg for arg in args if isinstance(arg, numbers.Number)]
    if not items:
        return None
    return min(items)


def num_max(*args):
    items = [arg for arg in args if isinstance(arg, numbers.Number)]
    if not items:
        return None
    return max(items)


def auth_code(secret, sign):
    return hashlib.sha1(secret + sign).hexdigest()


def _node_ipaddr(node):
    """Internal use only.
    """
    if not node:
        return None
    if node.find('*') >= 0:
        return node
    try:
        ip_addr = socket.gethostbyname(node)
        return ip_addr
    except:
        return None


def _parse_nodes(nodes):
    """Internal use only.
    """
    node_specs = []
    for node in nodes:
        if isinstance(node, NodeSpec):
            node_specs.append(node)
        elif isinstance(node, str):
            node_specs.append(NodeSpec(node))
        elif isinstance(node, dict):
            node_specs.append(NodeSpec(node.get('ip_addr', node['name_ip']), node.get('port', None),
                                       node.get('cpus', 0)))
        elif isinstance(node, tuple):
            node_specs.append(NodeSpec(*node))
        elif isinstance(node, list):
            node_specs.append(NodeSpec(*tuple(node)))
    return node_specs


class _Compute(object):
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
        self.context = {}
        self.dest_path = None
        self.xfer_files = []
        self.reentrant = False
        self.setup = None
        self.cleanup = None
        self.scheduler_ip_addr = None
        self.scheduler_port = None
        self.auth = None
        self.job_result_port = None
        self.pulse_interval = None

    def __getstate__(self):
        state = dict(self.__dict__)
        return state


class _XferFile(object):
    """Internal use only.
    """
    def __init__(self, name, stat_buf, compute_id=None):
        self.name = name
        self.stat_buf = stat_buf
        self.compute_id = compute_id
        self.sep = os.sep


class _Node(object):
    """Internal use only.
    """
    def __init__(self, ip_addr, port, cpus, sign, secret, keyfile=None, certfile=None):
        self.ip_addr = ip_addr
        self.port = port
        self.name = None
        self.cpus = cpus
        self.avail_cpus = cpus
        self.busy = 0
        self.cpu_time = 0.0
        self.clusters = set()
        self.auth = auth_code(secret, sign)
        self.secret = secret
        self.keyfile = keyfile
        self.certfile = certfile
        self.last_pulse = None
        self.scheduler_ip_addr = None
        self._jobs = set()

    def setup(self, compute, coro=None):
        # generator
        compute.scheduler_ip_addr = self.scheduler_ip_addr
        resp = yield self.send('COMPUTE:' + serialize(compute), coro=coro)
        if resp != 0:
            logger.warning('Transfer of computation "%s" to %s failed: %s',
                           compute.name, self.ip_addr, resp)
            raise StopIteration(resp)
        for xf in compute.xfer_files:
            resp = yield self.xfer_file(xf, coro=coro)
            if resp != 0:
                logger.error('Could not transfer file "%s"', xf.name)
                raise StopIteration(resp)

        if isinstance(compute.setup, str):
            resp = yield self.send('SETUP:' + serialize(compute.id), coro=coro)
            if resp != 0:
                logger.warning('Setup of computation "%s" on %s failed: %s',
                               compute.name, self.ip_addr, resp)
                raise StopIteration(resp)
        raise StopIteration(0)

    def send(self, msg, reply=True, coro=None):
        # generator
        # logger.debug('Sending to %s:%s', self.ip_addr, self.port)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock = AsyncSocket(sock, keyfile=self.keyfile, certfile=self.certfile)
        sock.settimeout(MsgTimeout)
        try:
            yield sock.connect((self.ip_addr, self.port))
            yield sock.sendall(self.auth)
            yield sock.send_msg(msg)
            if reply:
                resp = yield sock.recv_msg()
            else:
                resp = 0
        except:
            logger.error('Could not connect to %s:%s, %s',
                         self.ip_addr, self.port, traceback.format_exc())
            # TODO: mark this node down, reschedule on different node?
            resp = traceback.format_exc()
        finally:
            sock.close()

        if resp == 'ACK':
            resp = 0
        raise StopIteration(resp)

    def xfer_file(self, xf, coro=None):
        # generator
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock = AsyncSocket(sock, keyfile=self.keyfile, certfile=self.certfile)
        sock.settimeout(MsgTimeout)
        try:
            yield sock.connect((self.ip_addr, self.port))
            yield sock.sendall(self.auth)
            yield sock.send_msg('FILEXFER:' + serialize(xf))
            resp = yield sock.recv_msg()
            if resp != 'ACK':
                fd = open(xf.name, 'rb')
                while True:
                    data = fd.read(1024000)
                    if not data:
                        break
                    yield sock.sendall(data)
                fd.close()
                resp = yield sock.recv_msg()
        except:
            logger.error('Could not transfer %s to %s: %s', xf.name, self.ip_addr, resp)
            # TODO: mark this node down, reschedule on different node?
            resp = traceback.format_exc()
        finally:
            sock.close()

        if resp == 'ACK':
            resp = 0
        raise StopIteration(resp)

    def close(self, compute, coro=None):
        # generator
        logger.debug('Closing node %s for %s / %s', self.ip_addr, compute.name, compute.id)
        req = {'compute_id': compute.id, 'auth': compute.auth}
        try:
            yield self.send('CLOSE:' + serialize(req), reply=False, coro=coro)
        except:
            logger.debug('Deleting computation %s/%s from %s failed',
                         compute.id, compute.name, self.ip_addr)
        self.busy = 0


class _DispyJob_(object):
    """Internal use only.
    """

    __slots__ = ('job', 'uid', 'compute_id', 'hash', 'node', 'xfer_files', 'args', 'kwargs', 'code')

    def __init__(self, compute_id, args, kwargs):
        self.job = DispyJob()
        self.job._dispy_job_ = self
        self.uid = None
        self.compute_id = compute_id
        self.hash = os.urandom(10).encode('hex')
        self.node = None
        self.xfer_files = []
        self.code = ''
        job_deps = kwargs.pop('dispy_job_depends', [])
        self.job.args = args
        self.job.kwargs = kwargs
        self.args = serialize(args)
        self.kwargs = serialize(kwargs)
        depend_ids = set()
        for dep in job_deps:
            if isinstance(dep, str) or inspect.ismodule(dep):
                if inspect.ismodule(dep):
                    dep = dep.__file__
                    if dep.endswith('.pyc'):
                        dep = dep[:-1]
                    if not dep.endswith('.py'):
                        logger.warning('Invalid module "%s" - must be python source.' % dep)
                        continue
                if dep in depend_ids:
                    continue
                self.xfer_files.append(_XferFile(dep, os.stat(dep), compute_id))
                depend_ids.add(dep)
            elif inspect.isfunction(dep) or inspect.isclass(dep) or hasattr(dep, '__class__'):
                if inspect.isfunction(dep) or inspect.isclass(dep):
                    pass
                elif hasattr(dep, '__class__') and inspect.isclass(dep.__class__):
                    dep = dep.__class__
                if id(dep) in depend_ids:
                    continue
                lines = inspect.getsourcelines(dep)[0]
                lines[0] = lines[0].lstrip()
                self.code += '\n' + ''.join(lines)
                depend_ids.add(id(dep))
            else:
                logger.warning('Invalid job depends element "%s"; ignoring it.', dep)

    def __getstate__(self):
        state = {'uid': self.uid, 'hash': self.hash, 'compute_id': self.compute_id,
                 'args': self.args, 'kwargs': self.kwargs, 'xfer_files': self.xfer_files,
                 'code': self.code}
        return state

    def __setstate__(self, state):
        for k, v in state.iteritems():
            setattr(self, k, v)

    def __lt__(self, other):
        return self.uid < other.uid

    def __eq__(self, other):
        return isinstance(other, _DispyJob_) and self.uid == other.uid

    def run(self, coro=None):
        # generator
        logger.debug('running job %s on %s', self.uid, self.node.ip_addr)
        self.job.start_time = time.time()
        for xf in self.xfer_files:
            resp = yield self.node.xfer_file(xf, coro=coro)
            if resp:
                logger.warning('Transfer of file "%s" to %s failed' % (xf.name, self.node.ip_addr))
                raise Exception(-1)
        resp = yield self.node.send('JOB:' + serialize(self), coro=coro)
        # TODO: deal with NAKs (reschedule?)
        if resp != 0:
            logger.warning('Failed to run %s on %s: %s', self.uid, self.node.ip_addr, resp)
            raise Exception(str(resp))
        raise StopIteration(resp)

    def finish(self, status):
        job = self.job
        job.status = status
        if status != DispyJob.ProvisionalResult:
            self.job._dispy_job_ = None
            self.job = None
        job.finish.set()


class _JobReply(object):
    """Internal use only.
    """
    def __init__(self, _job, ip_addr, status=None, keyfile=None, certfile=None):
        self.uid = _job.uid
        self.hash = _job.hash
        self.ip_addr = ip_addr
        self.status = status
        self.result = None
        self.stdout = None
        self.stderr = None
        self.exception = None
        self.start_time = 0
        self.end_time = 0


class _Cluster(object):
    """Internal use only.
    """
    __metaclass__ = MetaSingleton

    def __init__(self, ip_addr=None, ext_ip_addr=None, port=None, node_port=None,
                 shared=False, secret='', keyfile=None, certfile=None, recover_file=None):
        if not hasattr(self, 'asyncoro'):
            self.asyncoro = AsynCoro()
            self.ip_addrs = set()
            if ip_addr:
                if not isinstance(ip_addr, list):
                    ip_addr = [ip_addr]
                for node in ip_addr:
                    addr = _node_ipaddr(node)
                    if addr:
                        self.ip_addrs.add(addr)
                    else:
                        logger.warning('ignoring invalid ip_addr "%s"' % node)
            if not self.ip_addrs:
                self.ip_addrs.add(None)
            self.ext_ip_addrs = set(self.ip_addrs)
            if ext_ip_addr:
                if not isinstance(ext_ip_addr, list):
                    ext_ip_addr = [ext_ip_addr]
                for node in ext_ip_addr:
                    addr = _node_ipaddr(node)
                    if addr:
                        self.ext_ip_addrs.add(addr)
                    else:
                        logger.warning('ignoring invalid ext_ip_addr "%s"' % node)
            if port:
                port = int(port)
            else:
                if shared:
                    port = 0
                else:
                    port = 51347
            if node_port:
                node_port = int(node_port)
            else:
                node_port = 51348

            self.port = port
            self.node_port = node_port
            self._nodes = {}
            self.secret = secret
            self.keyfile = keyfile
            self.certfile = certfile
            self.shared = shared
            self.pulse_interval = None
            self.ping_interval = None
            self.poll_interval = None
            self.dest_path = os.getcwd()  # TODO: make it an option?

            self._clusters = {}
            self.unsched_jobs = 0
            self._sched_jobs = {}
            self._sched_event = asyncoro.Event()
            self.terminate = False
            self.sign = os.urandom(10).encode('hex')
            self.auth = auth_code(self.secret, self.sign)

            if isinstance(recover_file, str):
                self.recover_file = recover_file
            else:
                now = datetime.datetime.now()
                self.recover_file = '_dispy_%.4i%.2i%.2i%.2i%.2i%.2i' % \
                                    (now.year, now.month, now.day,
                                     now.hour, now.minute, now.second)
            try:
                self.shelf = shelve.open(self.recover_file, flag='c', writeback=True)
                self.shelf['_cluster'] = {'ip_addrs': self.ip_addrs, 'port': self.port,
                                          'sign': self.sign, 'secret': self.secret,
                                          'auth': self.auth,
                                          'keyfile': self.keyfile, 'certfile': self.certfile}
                self.shelf.sync()
            except:
                raise Exception('Could not create fault recover file "%s"' %
                                self.recover_file)
            logger.info('Storing fault recovery information in "%s"', self.recover_file)
            atexit.register(self.shutdown)
            self.timer_coro = Coro(self.timer_task)

            self.tcp_coros = []
            for ip_addr in list(self.ip_addrs):
                self.tcp_coros.append(Coro(self.tcp_server, ip_addr))

            if self.shared:
                self.udp_coro = None
            else:
                self.udp_coro = Coro(self.udp_server)

            self.select_job_node = self.load_balance_schedule
            self._scheduler = Coro(self._schedule_jobs)
            self.start_time = time.time()
            self.compute_id = int(1000 * self.start_time)

            self.worker_Q = queue.Queue()
            self.worker_thread = threading.Thread(target=self.worker)
            self.worker_thread.daemon = True
            self.worker_thread.start()

    def udp_server(self, coro=None):
        # generator
        coro.set_daemon()
        Coro(self.broadcast_ping)
        udp_sock = AsyncSocket(socket.socket(socket.AF_INET, socket.SOCK_DGRAM))
        udp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        udp_sock.bind(('', self.port))
        while True:
            msg, addr = yield udp_sock.recvfrom(1000)
            if msg.startswith('PULSE:'):
                msg = msg[len('PULSE:'):]
                try:
                    info = unserialize(msg)
                    node = self._nodes[info['ip_addr']]
                    assert 0 <= info['cpus'] <= node.cpus
                    node.last_pulse = time.time()
                    pulse_msg = {'ip_addr': info['scheduler_ip_addr'], 'port': self.port}
                except:
                    logger.warning('Ignoring pulse message from %s', addr[0])
                    # logger.debug(traceback.format_exc())
                    continue

                def _send_pulse(self, pulse_msg, addr, coro=None):
                    sock = AsyncSocket(socket.socket(socket.AF_INET, socket.SOCK_DGRAM))
                    sock.settimeout(2)
                    try:
                        yield sock.sendto('PULSE:' + serialize(pulse_msg), addr)
                    except:
                        pass
                    sock.close()

                Coro(_send_pulse, self, pulse_msg, (info['ip_addr'], info['port']))
            elif msg.startswith('PING:'):
                try:
                    info = unserialize(msg[len('PING:'):])
                    if info['version'] != _dispy_version:
                        logger.warning('Ignoring %s due to version mismatch', addr[0])
                        continue
                    assert info['port'] > 0
                    assert info['ip_addr']
                    # socket.inet_aton(status['ip_addr'])
                except:
                    # logger.debug(traceback.format_exc())
                    logger.debug('Ignoring node %s', addr[0])
                    continue
                auth = auth_code(self.secret, info['sign'])
                node = self._nodes.get(info['ip_addr'], None)
                if node:
                    if node.auth == auth:
                        continue
                sock = AsyncSocket(socket.socket(socket.AF_INET, socket.SOCK_STREAM),
                                   keyfile=self.keyfile, certfile=self.certfile)
                sock.settimeout(MsgTimeout)
                msg = {'version': _dispy_version, 'port': self.port, 'sign': self.sign}
                msg['ip_addrs'] = list(filter(lambda ip: bool(ip), self.ext_ip_addrs))
                try:
                    yield sock.connect((info['ip_addr'], info['port']))
                    yield sock.sendall(auth)
                    yield sock.send_msg('PING:' + serialize(msg))
                    info = yield sock.recv_msg()
                except:
                    logger.debug(traceback.format_exc())
                finally:
                    sock.close()
            else:
                # logger.debug('Ignoring UDP message %s from: %s', msg[:min(5, len(msg))], addr[0])
                pass

    def tcp_server(self, ip_addr, coro=None):
        # generator
        coro.set_daemon()
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock = AsyncSocket(sock, keyfile=self.keyfile, certfile=self.certfile)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if not ip_addr:
            ip_addr = ''
        try:
            sock.bind((ip_addr, self.port))
        except:
            if ip_addr == '':
                ip_addr = None
            self.ip_addrs.discard(ip_addr)
            raise StopIteration
        if self.shared and self.port == 0:
            self.port = sock.getsockname()[1]
        logger.debug('dispy client at %s:%s' % (ip_addr, self.port))
        sock.listen(128)

        while True:
            try:
                conn, addr = yield sock.accept()
            except ssl.SSLError as err:
                logger.debug('SSL connection failed: %s', str(err))
                continue
            except GeneratorExit:
                break
            except:
                logger.debug(traceback.format_exc())
                continue
            # logger.debug('received job result from %s', str(addr))
            Coro(self.tcp_task, conn, addr)

    def tcp_task(self, conn, addr, coro=None):
        # generator
        conn.settimeout(MsgTimeout)
        msg = yield conn.recv_msg()
        if msg.startswith('JOB_REPLY:'):
            try:
                info = unserialize(msg[len('JOB_REPLY:'):])
            except:
                logger.warning('invalid job reply from %s:%s ignored' % (addr[0], addr[1]))
            else:
                yield self.job_reply_process(info, conn, addr)
        elif msg.startswith('JOB_STATUS:'):
            # message from dispyscheduler
            try:
                info = unserialize(msg[len('JOB_STATUS:'):])
                _job = self._sched_jobs[info['uid']]
                assert _job.hash == info['hash']
                job = _job.job
                job.status = info['status']
                job.ip_addr = info['node']
                node = self._nodes.get(job.ip_addr, None)
                # TODO: if node is None, likely missed NODE_STATUS, so create it now?
                if node:
                    if job.status == DispyJob.Running:
                        job.start_time = info['start_time']
                        node.busy += 1
                    else:
                        logger.warning('invalid job status for shared cluster: %s' % job.status)
                    cluster = self._clusters.get(_job.compute_id, None)
                    if cluster:
                        dispy_node = cluster._dispy_nodes.get(node.ip_addr, None)
                        if dispy_node:
                            if job.status == DispyJob.Running:
                                dispy_node.busy += 1
                            dispy_node.update_time = time.time()
                            if cluster.status_callback:
                                self.worker_Q.put((cluster.status_callback,
                                                   (job.status, dispy_node, job)))
            except:
                logger.warning('invalid job status from %s:%s ignored' % (addr[0], addr[1]))
        elif msg.startswith('PONG:'):
            try:
                info = unserialize(msg[len('PONG:'):])
                assert info['auth'] == self.auth
                yield self.add_node(info, coro=coro)
            except:
                logger.warning('Ignoring node %s: %s' % (addr[0], traceback.format_exc()))
        elif msg.startswith('PING:'):
            try:
                info = unserialize(msg[len('PING:'):])
                if info['version'] != _dispy_version:
                    logger.warning('Ignoring %s due to version mismatch', addr[0])
                    raise StopIteration
                assert info['port'] > 0
                assert info['ip_addr']
                # socket.inet_aton(status['ip_addr'])
            except:
                # logger.debug(traceback.format_exc())
                logger.debug('Ignoring node %s', addr[0])
                conn.close()
                raise StopIteration
            auth = auth_code(self.secret, info['sign'])
            node = self._nodes.get(info['ip_addr'], None)
            if node:
                if node.auth == auth:
                    conn.close()
                    raise StopIteration
            sock = AsyncSocket(socket.socket(socket.AF_INET, socket.SOCK_STREAM),
                               keyfile=self.keyfile, certfile=self.certfile)
            sock.settimeout(MsgTimeout)
            msg = {'version': _dispy_version, 'port': self.port, 'sign': self.sign}
            msg['ip_addrs'] = list(filter(lambda ip: bool(ip), self.ext_ip_addrs))
            try:
                yield sock.connect((info['ip_addr'], info['port']))
                yield sock.sendall(auth)
                yield sock.send_msg('PING:' + serialize(msg))
                yield sock.recv_msg()
            except:
                logger.debug(traceback.format_exc())
            finally:
                sock.close()
        elif msg.startswith('FILEXFER:'):
            try:
                xf = unserialize(msg[len('FILEXFER:'):])
                msg = yield conn.recv_msg()
                reply = unserialize(msg)
                yield self.file_xfer_process(reply, xf, conn, addr)
            except:
                logger.debug(traceback.format_exc())
        elif msg.startswith('TERMINATED:'):
            try:
                info = unserialize(msg[len('TERMINATED:'):])
                node = self._nodes.pop(info['ip_addr'], None)
                if not node:
                    raise StopIteration
                auth = auth_code(self.secret, info['sign'])
                if auth != node.auth:
                    logger.warning('Invalid signature from %s', node.ip_addr)
                    raise StopIteration
                logger.debug('Removing node %s', node.ip_addr)
                if node.clusters:
                    dead_jobs = [_job for _job in self._sched_jobs.itervalues()
                                 if _job.node is not None and _job.node.ip_addr == node.ip_addr]
                    yield self.reschedule_jobs(dead_jobs)
                    for cid in node.clusters:
                        cluster = self._clusters[cid]
                        dispy_node = cluster._dispy_nodes[node.ip_addr]
                        dispy_node.avail_cpus = dispy_node.cpus = 0
                        if cluster.status_callback:
                            self.worker_Q.put((cluster.status_callback,
                                               (DispyNode.Closed, dispy_node, None)))
                    node.clusters = set()
            except:
                # logger.debug(traceback.format_exc())
                pass
        elif msg.startswith('NODE_STATUS:'):
            # this message is from dispyscheduler for SharedJobCluster
            try:
                info = unserialize(msg[len('NODE_STATUS:'):])
                cluster = self._clusters[info['compute_id']]
                assert info['auth'] == cluster._compute.auth
                dispy_node = info['dispy_node']
                dispy_node.update_time = time.time()
                if info['status'] == DispyNode.Initialized:
                    node = self._nodes.get(dispy_node.ip_addr, None)
                    if node:
                        node.name = dispy_node.name
                        node.cpus = dispy_node.cpus
                    else:
                        node = _Node(dispy_node.ip_addr, 0, dispy_node.cpus, '', '')
                        node.name = dispy_node.name
                        self._nodes[node.ip_addr] = node
                    cluster._dispy_nodes[dispy_node.ip_addr] = dispy_node
                    if cluster.status_callback:
                        self.worker_Q.put((cluster.status_callback,
                                           (DispyNode.Initialized, dispy_node, None)))
                elif info['status'] == DispyNode.Closed:
                    dispy_node = cluster._dispy_nodes.get(dispy_node.ip_addr, None)
                    if dispy_node:
                        dispy_node.avail_cpus = dispy_node.cpus = 0
                        if cluster.status_callback:
                            self.worker_Q.put((cluster.status_callback,
                                               (DispyNode.Closed, dispy_node, None)))
                else:
                    logger.warning('Invalid node status %s from %s:%s ignored',
                                   info['status'], addr[0], addr[1])
            except:
                logger.warning('invalid node status from %s:%s ignored' % (addr[0], addr[1]))
                logger.debug(traceback.format_exc())
        else:
            logger.warning('invalid message from %s:%s ignored' % (addr[0], addr[1]))
            # logger.debug(traceback.format_exc())
        conn.close()

    def timer_task(self, coro=None):
        coro.set_daemon()
        reset = True
        last_pulse_time = last_ping_time = last_poll_time = time.time()
        timeout = None
        while True:
            if reset:
                timeout = num_min(self.pulse_interval, self.ping_interval, self.poll_interval)

            reset = yield coro.suspend(timeout)
            if reset:
                continue

            now = time.time()
            if self.pulse_interval and (now - last_pulse_time) >= self.pulse_interval:
                last_pulse_time = now
                if self.shared:
                    clusters = self._clusters.values()
                    for cluster in clusters:
                        msg = {'client_ip_addr': cluster._compute.scheduler_ip_addr,
                               'client_port': cluster._compute.scheduler_port}
                        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                        sock = AsyncSocket(sock)
                        sock.settimeout(2)
                        yield sock.sendto('PULSE:' + serialize(msg),
                                          (cluster.scheduler_ip_addr, cluster.job_result_port))
                        sock.close()
                else:
                    dead_nodes = {}
                    for node in self._nodes.itervalues():
                        if node.busy and node.last_pulse is not None and \
                           (node.last_pulse + (5 * self.pulse_interval)) <= now:
                            logger.warning('Node %s is not responding; removing it (%s, %s, %s)',
                                           node.ip_addr, node.busy, node.last_pulse, now)
                            dead_nodes[node.ip_addr] = node
                    for node in dead_nodes.itervalues():
                        for cid in node.clusters:
                            cluster = self._clusters.get(cid, None)
                            if cluster:
                                dispy_node = cluster._dispy_nodes.get(node.ip_addr, None)
                                if dispy_node:
                                    dispy_node.cpus = dispy_node.avail_cpus = dispy_node.busy = 0
                        del self._nodes[node.ip_addr]
                    dead_jobs = [_job for _job in self._sched_jobs.itervalues()
                                 if _job.node is not None and _job.node.ip_addr in dead_nodes]
                    yield self.reschedule_jobs(dead_jobs)
                    if dead_nodes or dead_jobs:
                        self._sched_event.set()

            if self.ping_interval and (now - last_ping_time) >= self.ping_interval:
                last_ping_time = now
                for cluster in self._clusters.itervalues():
                    Coro(self.send_ping_cluster, cluster)

            if self.poll_interval and (now - last_poll_time) >= self.poll_interval:
                last_poll_time = now
                for cluster in self._clusters.itervalues():
                    Coro(self.poll_job_results, cluster)

    def file_xfer_process(self, reply, xf, sock, addr):
        _job = self._sched_jobs.get(reply.uid, None)
        if _job is None or _job.hash != reply.hash:
            logger.warning('Ignoring invalid file transfer from job %s at %s', reply.uid, addr[0])
            yield sock.send_msg('NAK')
            raise StopIteration
        node = self._nodes.get(reply.ip_addr, None)
        if node:
            node.last_pulse = time.time()
        yield sock.send_msg('ACK')
        xf.name = xf.name.replace(xf.sep, os.sep)
        if xf.name.startswith(os.sep):
            xf.name = xf.name[len(os.sep):]
        tgt = os.path.join(self.dest_path, xf.name)
        if not os.path.isdir(os.path.dirname(tgt)):
            os.makedirs(os.path.dirname(tgt))
        fd = open(tgt, 'wb')
        n = 0
        while n < xf.stat_buf.st_size:
            data = yield sock.recvall(min(xf.stat_buf.st_size-n, 1024000))
            if not data:
                break
            fd.write(data)
            n += len(data)
        fd.close()
        if n != xf.stat_buf.st_size:
            yield sock.send_msg('NAK (read only %s bytes)' % n)
        else:
            yield sock.send_msg('ACK')
        os.utime(tgt, (xf.stat_buf.st_atime, xf.stat_buf.st_mtime))
        os.chmod(tgt, stat.S_IMODE(xf.stat_buf.st_mode))

    def send_ping_node(self, ip_addr, port=None, coro=None):
        ping_msg = {'version': _dispy_version, 'sign': self.sign, 'port': self.port}
        ping_msg['ip_addrs'] = list(filter(lambda ip: bool(ip), self.ext_ip_addrs))
        udp_sock = AsyncSocket(socket.socket(socket.AF_INET, socket.SOCK_DGRAM))
        udp_sock.settimeout(2)
        if not port:
            port = self.node_port
        try:
            yield udp_sock.sendto('PING:' + serialize(ping_msg), (ip_addr, port))
        except:
            # logger.debug(traceback.format_exc())
            pass
        udp_sock.close()
        tcp_sock = AsyncSocket(socket.socket(socket.AF_INET, socket.SOCK_STREAM),
                               keyfile=self.keyfile, certfile=self.certfile)
        tcp_sock.settimeout(MsgTimeout)
        try:
            yield tcp_sock.connect((ip_addr, port))
            yield tcp_sock.sendall('x' * len(self.auth))
            yield tcp_sock.send_msg('PING:' + serialize(ping_msg))
            info = yield tcp_sock.recv_msg()
            info = unserialize(info)
            if self.poll_interval and info['version'] == _dispy_version and \
               info['auth'] == self.auth:
                Coro(self.add_node, info)
        except:
            # logger.debug(traceback.format_exc())
            pass
        tcp_sock.close()

    def broadcast_ping(self, port=None, coro=None):
        # generator
        if not port:
            port = self.node_port
        ping_msg = {'version': _dispy_version, 'sign': self.sign, 'port': self.port}
        ping_msg['ip_addrs'] = list(filter(lambda ip: bool(ip), self.ext_ip_addrs))
        bc_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        bc_sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        bc_sock = AsyncSocket(bc_sock)
        bc_sock.settimeout(2)
        try:
            yield bc_sock.sendto('PING:' + serialize(ping_msg), ('<broadcast>', port))
        except:
            pass
        bc_sock.close()

    def send_ping_cluster(self, cluster, coro=None):
        # generator
        for node_spec in cluster._node_specs:
            # TODO: we assume subnets are indicated by '*', instead of
            # subnet mask; this is a limitation, but specifying with
            # subnet mask a bit cumbersome.
            if node_spec.rex.find('*') >= 0:
                yield self.broadcast_ping(node_spec.port)
                # need to do broadcast only once
            else:
                ip_addr = node_spec.ip_addr
                if ip_addr in cluster._dispy_nodes:
                    continue
                port = node_spec.port
                Coro(self.send_ping_node, ip_addr, port)

    def poll_job_results(self, cluster, coro=None):
        # generator
        for ip_addr in cluster._dispy_nodes:
            node = self._nodes.get(ip_addr, None)
            if not node or not node.port:
                continue
            sock = AsyncSocket(socket.socket(socket.AF_INET, socket.SOCK_STREAM),
                               keyfile=self.keyfile, certfile=self.certfile)
            sock.settimeout(MsgTimeout)
            try:
                req = {'compute_id': cluster._compute.id, 'auth': self.auth}
                reply = yield node.send('PENDING_JOBS:' + serialize(req))
                reply = unserialize(reply)
            except:
                logger.debug(traceback.format_exc())
                continue
            finally:
                sock.close()

            for uid in reply['done']:
                _job = self._sched_jobs.get(uid, None)
                if _job is None:
                    continue
                conn = AsyncSocket(socket.socket(socket.AF_INET, socket.SOCK_STREAM),
                                   keyfile=self.keyfile, certfile=self.certfile)
                conn.settimeout(MsgTimeout)
                try:
                    yield conn.connect((node.ip_addr, node.port))
                    req = {'compute_id': cluster._compute.id, 'auth': self.auth,
                           'uid': uid, 'hash': _job.hash}
                    yield conn.sendall(node.auth)
                    yield conn.send_msg('RETRIEVE_JOB:' + serialize(req))
                    reply = yield conn.recv_msg()
                    reply = unserialize(reply)
                except:
                    logger.debug(traceback.format_exc())
                    continue
                else:
                    if isinstance(reply, _JobReply):
                        yield self.job_reply_process(reply, conn, (node.ip_addr, node.port))
                    else:
                        logger.debug('invalid reply for %s' % uid)
                finally:
                    conn.close()

    def add_cluster(self, cluster, coro=None):
        # generator
        compute = cluster._compute
        if self.shared:
            self._clusters[compute.id] = cluster
            for xf in compute.xfer_files:
                xf.compute_id = compute.id

            node = _Node(cluster.scheduler_ip_addr, cluster.scheduler_port, 0, '', '',
                         keyfile=self.keyfile, certfile=self.certfile)
            node.auth = cluster._scheduler_auth
            self._nodes[cluster.scheduler_ip_addr] = node
            dispy_node = DispyNode(cluster.scheduler_ip_addr, None, 0)
            cluster._dispy_nodes[dispy_node.ip_addr] = dispy_node
            info = self.shelf['_cluster']
            info['port'] = self.port
            self.shelf['_cluster'] = info
            info = {'name': compute.name, 'auth': compute.auth,
                    'nodes': [cluster.scheduler_ip_addr]}
            self.shelf['compute_%s' % compute.id] = info
            info = {'port': cluster.scheduler_port, 'auth': cluster._scheduler_auth}
            self.shelf['node_%s' % (cluster.scheduler_ip_addr)] = info
            self.shelf.sync()
            if cluster.poll_interval:
                self.poll_interval = num_min(self.poll_interval, cluster.poll_interval)
            if self.poll_interval:
                self.timer_coro.resume(True)
            raise StopIteration

        # if a node is added with 'add_nodespec', compute is already
        # initialized, so don't reinitialize it
        if compute.id is None:
            compute.id = self.compute_id
            self.compute_id += 1
            self._clusters[compute.id] = cluster
            for xf in compute.xfer_files:
                xf.compute_id = compute.id
            info = {'name': compute.name, 'auth': compute.auth, 'nodes': []}
            self.shelf['compute_%s' % compute.id] = info
            self.shelf.sync()

            if compute.pulse_interval:
                self.pulse_interval = num_min(self.pulse_interval, compute.pulse_interval)
            if cluster.ping_interval:
                self.ping_interval = num_min(self.ping_interval, cluster.ping_interval)
            if cluster.poll_interval:
                self.poll_interval = num_min(self.poll_interval, cluster.poll_interval)
            if self.pulse_interval or self.ping_interval or self.poll_interval:
                self.timer_coro.resume(True)

        yield self.send_ping_cluster(cluster, coro=coro)
        compute_nodes = []
        for ip_addr, node in self._nodes.iteritems():
            if compute.id in node.clusters:
                continue
            for node_spec in cluster._node_specs:
                if re.match(node_spec.rex, ip_addr):
                    if node_spec.cpus > 0:
                        node.cpus = min(node.avail_cpus, node_spec.cpus)
                    elif (node.avail_cpus + node_spec.cpus) > 0:
                        node.cpus = node.avail_cpus + node_spec.cpus
                    cluster._dispy_nodes.pop(node.ip_addr, None)
                    compute_nodes.append(node)
        for node in compute_nodes:
            yield self.setup_node(node, [compute], coro=coro)

    def del_cluster(self, cluster, coro=None):
        # generator
        if self._clusters.pop(cluster._compute.id, None) != cluster:
            logger.warning('cluster %s already closed?', cluster._compute.name)
            raise StopIteration

        if cluster._jobs or cluster._pending_jobs:
            logger.warning('cluster %s has pending jobs', cluster._compute.name)
            raise StopIteration

        if self.shared:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock = AsyncSocket(sock, keyfile=self.keyfile, certfile=self.certfile)
            sock.settimeout(MsgTimeout)
            yield sock.connect((cluster.scheduler_ip_addr, cluster.scheduler_port))
            yield sock.sendall(cluster._scheduler_auth)
            req = {'compute_id': cluster._compute.id, 'auth': cluster._compute.auth}
            yield sock.send_msg('CLOSE:' + serialize(req))
            sock.close()
        else:
            for ip_addr, dispy_node in cluster._dispy_nodes.iteritems():
                node = self._nodes.get(ip_addr, None)
                if not node:
                    continue
                node.clusters.discard(cluster._compute.id)
                yield node.close(cluster._compute, coro=coro)
                dispy_node.busy = 0
                dispy_node.update_time = time.time()
                if cluster.status_callback:
                    self.worker_Q.put((cluster.status_callback,
                                       (DispyNode.Closed, dispy_node, None)))
        self.shelf.pop('compute_%s' % (cluster._compute.id), None)
        # TODO: prune nodes in shelf
        self.shelf.sync()

    def setup_node(self, node, computations, coro=None):
        # generator
        for compute in computations:
            # NB: to avoid computation being sent multiple times, we
            # add to cluster's _dispy_nodes before sending computation
            # to node
            cluster = self._clusters[compute.id]
            if node.ip_addr in cluster._dispy_nodes:
                continue
            dispy_node = DispyNode(node.ip_addr, node.name, node.cpus)
            dispy_node.avail_cpus = node.avail_cpus
            dispy_node.update_time = time.time()
            cluster._dispy_nodes[node.ip_addr] = dispy_node
            self.shelf['node_%s' % (node.ip_addr)] = {'port': node.port, 'auth': node.auth}
            shelf_compute = self.shelf['compute_%s' % compute.id]
            shelf_compute['nodes'].append(node.ip_addr)
            self.shelf['compute_%s' % compute.id] = shelf_compute
            self.shelf.sync()
            r = yield node.setup(compute, coro=coro)
            if r != 0:
                cluster._dispy_nodes.pop(node.ip_addr, None)
                logger.warning('Failed to setup %s for compute "%s": %s',
                               node.ip_addr, compute.name, r)
                # TODO: delete node from shelf's cluster._dispy_nodes
                del self.shelf['node_%s' % (node.ip_addr)]
                self.shelf.sync()
                yield node.close(compute, coro=coro)
            else:
                node.clusters.add(compute.id)
                self._sched_event.set()
                if cluster.status_callback:
                    self.worker_Q.put((cluster.status_callback,
                                       (DispyNode.Initialized, dispy_node, None)))

    def add_node(self, info, coro=None):
        try:
            # assert info['version'] == _dispy_version
            assert info['port'] > 0 and info['cpus'] > 0
            socket.inet_aton(info['ip_addr'])
            # TODO: check if it is one of ext_ip_addr?
        except:
            # logger.debug(traceback.format_exc())
            # logger.debug('Ignoring node %s', addr[0])
            raise StopIteration
        node = self._nodes.get(info['ip_addr'], None)
        if node is None:
            logger.debug('Discovered %s:%s (%s) with %s cpus',
                         info['ip_addr'], info['port'], info['name'], info['cpus'])
            node = _Node(info['ip_addr'], info['port'], info['cpus'], info['sign'],
                         self.secret, keyfile=self.keyfile, certfile=self.certfile)
            node.name = info['name']
            self._nodes[node.ip_addr] = node
        else:
            node.last_pulse = time.time()
            auth = auth_code(self.secret, info['sign'])
            if info['cpus'] > 0:
                node.avail_cpus = info['cpus']
                node.cpus = min(node.cpus, node.avail_cpus)
                for cid in node.clusters:
                    cluster = self._clusters[cid]
                    dispy_node = cluster._dispy_nodes.get(node.ip_addr, None)
                    if dispy_node:
                        dispy_node.avail_cpus = node.avail_cpus
                        dispy_node.cpus = node.cpus
            else:
                logger.warning('invalid "cpus" %s from %s ignored',
                               (info['cpus'], info['ip_addr']))
            if node.port == info['port'] and node.auth == auth:
                raise StopIteration
            logger.debug('node %s rediscovered' % info['ip_addr'])
            node.port = info['port']
            if node.auth is not None:
                dead_jobs = [_job for _job in self._sched_jobs.itervalues()
                             if _job.node is not None and _job.node.ip_addr == node.ip_addr]
                node.clusters = set()
                node.auth = auth
                yield self.reschedule_jobs(dead_jobs)
            node.auth = auth
        node_computations = []
        node.name = info['name']
        node.scheduler_ip_addr = info['scheduler_ip_addr']
        for cid, cluster in self._clusters.iteritems():
            if cid in node.clusters:
                continue
            compute = cluster._compute
            for node_spec in cluster._node_specs:
                if re.match(node_spec.rex, node.ip_addr):
                    if node_spec.cpus > 0:
                        node.cpus = min(node.avail_cpus, node_spec.cpus)
                    elif (node.avail_cpus + node_spec.cpus) > 0:
                        node.cpus = node.avail_cpus + node_spec.cpus
                    cluster._dispy_nodes.pop(node.ip_addr, None)
                    node_computations.append(compute)
                    break
        if node_computations:
            Coro(self.setup_node, node, node_computations)

    def worker(self):
        # used for user callbacks only
        while True:
            item = self.worker_Q.get(block=True)
            if item is None:
                self.worker_Q.task_done()
                break
            func, args = item
            try:
                func(*args)
            except:
                logger.debug('Running %s failed: %s', func.__name__, traceback.format_exc())
            self.worker_Q.task_done()

    def finish_job(self, cluster, _job, status):
        # generator
        # assert status in (DispyJob.Finished, DispyJob.Terminated, DispyJob.Abandoned)
        job = _job.job
        _job.finish(status)
        if cluster.callback:
            self.worker_Q.put((cluster.callback, (job,)))
        if status != DispyJob.ProvisionalResult:
            assert cluster._pending_jobs > 0
            cluster._pending_jobs -= 1
            if cluster._pending_jobs == 0:
                cluster.end_time = time.time()
                cluster._complete.set()

    def job_reply_process(self, reply, sock, addr):
        _job = self._sched_jobs.get(reply.uid, None)
        if _job is None:
            logger.warning('Ignoring invalid reply for job %s from %s', reply.uid, addr[0])
            yield sock.send_msg('ACK')
            raise StopIteration
        job = _job.job
        job.ip_addr = reply.ip_addr
        node = self._nodes.get(reply.ip_addr, None)
        cluster = self._clusters.get(_job.compute_id, None)
        if cluster is None:
            # job cancelled while closing computation?
            if node:
                # assert node.busy > 0
                node.busy -= 1
            yield sock.send_msg('ACK')
            raise StopIteration
        if node is None:
            if self.shared:
                node = _Node(reply.ip_addr, 0, getattr(reply, 'cpus', 0), '', self.secret,
                             keyfile=None, certfile=None)
                self._nodes[reply.ip_addr] = node
                dispy_node = DispyNode(node.ip_addr, node.name, node.cpus)
                dispy_node.update_time = time.time()
                cluster._dispy_nodes[reply.ip_addr] = dispy_node
                if cluster.status_callback:
                    self.worker_Q.put((cluster.status_callback,
                                       (DispyNode.Initialized, dispy_node, None)))
            else:
                logger.warning('Ignoring invalid reply for job %s from %s', reply.uid, addr[0])
                yield sock.send_msg('ACK')
                raise StopIteration
        else:
            node._jobs.discard(_job.uid)
        node.last_pulse = time.time()
        try:
            assert reply.hash == _job.hash
            job.result = reply.result
            job.stdout = reply.stdout
            job.stderr = reply.stderr
            job.exception = reply.exception
            job.start_time = reply.start_time
            job.end_time = reply.end_time
        except:
            logger.warning('Invalid job result for %s from %s', _job.uid, addr[0])
            # logger.debug('%s, %s', str(reply), traceback.format_exc())
            yield sock.send_msg('ACK')
            raise StopIteration

        yield sock.send_msg('ACK')
        logger.debug('Received reply for job %s / %s from %s' % (job.id, _job.uid, job.ip_addr))
        if reply.status == DispyJob.ProvisionalResult:
            yield self.finish_job(cluster, _job, reply.status)
        else:
            del self._sched_jobs[_job.uid]
            dispy_node = cluster._dispy_nodes[node.ip_addr]
            if reply.status == DispyJob.Finished or reply.status == DispyJob.Terminated:
                node.busy -= 1
                node.cpu_time += reply.end_time - reply.start_time
                dispy_node.busy -= 1
                dispy_node.cpu_time += reply.end_time - reply.start_time
                dispy_node.jobs_done += 1
                dispy_node.secs_per_job = dispy_node.cpu_time / dispy_node.jobs_done
                dispy_node.update_time = time.time()
            elif reply.status == DispyJob.Cancelled:
                assert self.shared is True
                pass
            else:
                logger.warning('invalid reply status: %s for job %s' % (reply.status, _job.uid))
            if cluster.status_callback:
                self.worker_Q.put((cluster.status_callback,
                                   (reply.status, dispy_node, _job.job)))
            yield self.finish_job(cluster, _job, reply.status)
            self._sched_event.set()

    def reschedule_jobs(self, dead_jobs):
        # generator
        for _job in dead_jobs:
            cluster = self._clusters[_job.compute_id]
            del self._sched_jobs[_job.uid]
            _job.node._jobs.discard(_job.uid)
            dispy_node = cluster._dispy_nodes.get(_job.node.ip_addr, None)
            if dispy_node:
                dispy_node.cpus = 0
                dispy_node.busy = 0
                dispy_node.update_time = time.time()
            if cluster._compute.reentrant:
                logger.debug('Rescheduling job %s from %s', _job.uid, _job.node.ip_addr)
                _job.job.status = DispyJob.Created
                _job.hash = os.urandom(10).encode('hex')
                cluster._jobs.append(_job)
                self.unsched_jobs += 1
            else:
                logger.debug('job %s scheduled on %s abandoned', _job.uid, _job.node.ip_addr)
                # TODO: it is likely node finishes this job and sends
                # reply later; keep this in _abandoned_jobs and process reply?
                if cluster.status_callback and dispy_node:
                    self.worker_Q.put((cluster.status_callback,
                                       (DispyJob.Abandoned, dispy_node, _job.job)))
                yield self.finish_job(cluster, _job, DispyJob.Abandoned)

    def run_job(self, _job, cluster, coro=None):
        # generator
        node = _job.node
        node._jobs.add(_job.uid)
        dispy_node = cluster._dispy_nodes[node.ip_addr]
        try:
            yield _job.run(coro=coro)
        except EnvironmentError:
            logger.warning('Failed to run job %s on %s for computation %s; removing this node',
                           _job.uid, node.ip_addr, cluster._compute.name)
            logger.debug(traceback.format_exc())
            # TODO: remove the node from all clusters and globally?
            # this job might have been deleted already due to timeout
            node.clusters.discard(cluster._compute.id)
            node._jobs.discard(_job.uid)
            if self._sched_jobs.pop(_job.uid, None) == _job:
                cluster._jobs.insert(0, _job)
                self.unsched_jobs += 1
                node.busy -= 1
            self._sched_event.set()
        except:
            logger.warning('Failed to run job %s on %s for computation %s; rescheduling it',
                           _job.uid, node.ip_addr, cluster._compute.name)
            logger.debug(traceback.format_exc())
            # TODO: delay executing again for some time?
            # this job might have been deleted already due to timeout
            node._jobs.discard(_job.uid)
            if self._sched_jobs.pop(_job.uid, None) == _job:
                cluster._jobs.append(_job)
                self.unsched_jobs += 1
                node.busy -= 1
            self._sched_event.set()
        else:
            logger.debug('Running job %s / %s on %s (busy: %s / %s)',
                         _job.job.id, _job.uid, node.ip_addr, node.busy, node.cpus)
            _job.job.status = DispyJob.Running
            _job.job.start_time = time.time()
            dispy_node.busy += 1
            dispy_node.update_time = time.time()
            if cluster.status_callback:
                self.worker_Q.put((cluster.status_callback,
                                   (DispyJob.Running, dispy_node, _job.job)))

    def load_balance_schedule(self):
        host = None
        load = 1.0
        for node in self._nodes.itervalues():
            if node.busy >= node.cpus:
                continue
            if all((not self._clusters[cid]._jobs) for cid in node.clusters):
                continue
            # logger.debug('load: %s, %s, %s' % (node.ip_addr, node.busy, node.cpus))
            if (float(node.busy) / node.cpus) < load:
                load = float(node.busy) / node.cpus
                host = node
        return host

    def _schedule_jobs(self, coro=None):
        # generator
        while not self.terminate:
            # n = sum(len(cluster._jobs) for cluster in self._clusters.itervalues())
            # assert self.unsched_jobs == n, '%s != %s' % (self.unsched_jobs, n)
            logger.debug('Pending jobs: %s', self.unsched_jobs)
            node = self.select_job_node()
            if not node:
                self._sched_event.clear()
                yield self._sched_event.wait()
                continue
            # TODO: strategy to pick a cluster?
            _job = None
            for cid in node.clusters:
                if self._clusters[cid]._jobs:
                    _job = self._clusters[cid]._jobs.pop(0)
                    break
            if _job is None:
                self._sched_event.clear()
                yield self._sched_event.wait()
                continue
            cluster = self._clusters[_job.compute_id]
            _job.node = node
            assert node.busy < node.cpus
            self._sched_jobs[_job.uid] = _job
            self.unsched_jobs -= 1
            node.busy += 1
            Coro(self.run_job, _job, cluster)

        logger.debug('scheduler quitting (%s / %s)', len(self._sched_jobs), self.unsched_jobs)
        self.unsched_jobs = 0
        self._sched_jobs = {}
        for cid in self._clusters.keys():
            cluster = self._clusters[cid]
            if not hasattr(cluster, '_compute'):
                # cluster is closed
                continue
            for _job in cluster._jobs:
                if _job.job.status == DispyJob.Running:
                    status = DispyJob.Terminated
                else:
                    status = DispyJob.Cancelled
                yield self.finish_job(cluster, _job, status)
                if cluster.status_callback:
                    dispy_node = cluster._dispy_nodes.get(_job.node.ip_addr, None)
                    if dispy_node:
                        dispy_node.update_time = time.time()
                        self.worker_Q.put((cluster.status_callback,
                                           (status, dispy_node, _job.job)))
            cluster._jobs = []
            cluster._pending_jobs = []
            yield self.del_cluster(cluster, coro=coro)
        self._clusters = {}
        self._nodes = {}
        logger.debug('scheduler quit')

    def submit_job(self, _job, coro=None):
        # generator
        _job.uid = id(_job)
        cluster = self._clusters[_job.compute_id]
        cluster._jobs.append(_job)
        self.unsched_jobs += 1
        cluster._pending_jobs += 1
        cluster._complete.clear()
        if cluster.status_callback:
            self.worker_Q.put((cluster.status_callback, (DispyJob.Created, None, _job.job)))
        yield self._sched_event.set()

    def cancel_job(self, job, coro=None):
        # generator
        assert self.shared is False
        _job = job._dispy_job_
        if _job is None:
            logger.warning('Job %s is invalid for cancellation!', job.id)
            raise StopIteration(-1)
        cluster = self._clusters.get(_job.compute_id, None)
        if not cluster:
            logger.warning('Invalid job %s for cluster "%s"!',
                           _job.uid, cluster._compute.name)
            raise StopIteration(-1)
        assert cluster._pending_jobs >= 1
        if _job.job.status == DispyJob.Created:
            cluster._jobs.remove(_job)
            self.unsched_jobs -= 1
            if cluster.status_callback:
                self.worker_Q.put((cluster.status_callback, (DispyJob.Cancelled, None, _job.job)))
            yield self.finish_job(cluster, _job, DispyJob.Cancelled)
            logger.debug('Cancelled (removed) job %s', _job.uid)
            raise StopIteration(0)
        elif not (_job.job.status == DispyJob.Running or
                  _job.job.status == DispyJob.ProvisionalResult or _job.node is None):
            logger.warning('Job %s is not valid for cancel (%s)', _job.uid, _job.job.status)
            raise StopIteration(-1)
        _job.job.status = DispyJob.Cancelled
        # don't send this status - when job is terminated status/callback get called
        logger.debug('Job %s / %s is being terminated', _job.job.id, _job.uid)
        resp = yield _job.node.send('TERMINATE_JOB:' + serialize(_job), reply=False, coro=coro)
        if resp != 0:
            logger.debug('Terminating job %s / %s failed: %s', _job.job.id, _job.uid, resp)
            resp = -1
        raise StopIteration(resp)

    def add_nodespec(self, cluster, node, coro=None):
        # generator
        node_specs = _parse_nodes([node])
        if not node_specs:
            raise StopIteration(-1)
        cluster._node_specs.extend(node_specs)
        cluster._node_specs = sorted(cluster._node_specs, key=lambda node_spec: node_spec.rex)
        cluster._node_specs.reverse()
        present = set()
        cluster._node_specs = [node_spec for node_spec in cluster._node_specs
                               if node_spec.rex not in present and
                               not present.add(node_spec.rex)]
        del present
        yield self.add_cluster(cluster)
        yield self._sched_event.set()
        raise StopIteration(0)

    def set_node_cpus(self, node, cpus, coro=None):
        # generator
        try:
            cpus = int(cpus)
        except ValueError:
            raise StopIteration(-1)
        node = _node_ipaddr(node)
        node = self._nodes.get(node, None)
        if node is None:
            cpus = -1
        else:
            if cpus >= 0:
                node.cpus = min(node.avail_cpus, cpus)
            elif (node.avail_cpus + cpus) >= 0:
                node.cpus = node.avail_cpus + cpus
            cpus = node.cpus
            for cid in node.clusters:
                cluster = self._clusters[cid]
                dispy_node = cluster._dispy_nodes.get(node.ip_addr, None)
                if dispy_node:
                    dispy_node.cpus = cpus
            yield self._sched_event.set()
        raise StopIteration(cpus)

    def node_jobs(self, cluster, node, from_node, coro=None):
        # generator
        node = _node_ipaddr(node)
        if not node:
            raise StopIteration([])
        node = self._nodes.get(node, None)
        if not node or cluster._compute.id not in node.clusters:
            raise StopIteration([])
        if from_node:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock = AsyncSocket(sock, keyfile=self.keyfile, certfile=self.certfile)
            sock.settimeout(MsgTimeout)
            try:
                yield sock.connect((node.ip_addr, node.port))
                yield sock.sendall(node.auth)
                req = {'compute_id': cluster._compute.id, 'auth': self.auth}
                yield sock.send_msg('JOBS:' + serialize(req))
                msg = yield sock.recv_msg()
                _jobs = [self._sched_jobs.get(info['uid'], None) for info in unserialize(msg)]
            except:
                logger.debug(traceback.format_exc())
                _jobs = []
            sock.close()
        else:
            _jobs = [self._sched_jobs.get(uid, None) for uid in node._jobs]

        jobs = [_job.job for _job in _jobs if _job is not None]
        raise StopIteration(jobs)

    def shutdown(self):
        # non-generator
        def _shutdown(self, coro=None):
            # generator
            # TODO: make sure JobCluster instances are done
            if not hasattr(self, '_scheduler'):
                raise StopIteration
            if self.terminate is False:
                logger.debug('shutting down scheduler ...')
                self.terminate = True
                yield self._sched_event.set()
                self.worker_Q.put(None)

        if self.terminate is False:
            Coro(_shutdown, self).value()
            self._scheduler.value()
            self.worker_Q.join()
        if self.asyncoro:
            # self.asyncoro.join(show_running=True)
            self.asyncoro.finish()
            self.asyncoro = None
            logger.debug('shutdown complete')
        if self.shelf:
            # TODO: need to check all clusters are deleted?
            self.shelf.close()
            self.shelf = None
            try:
                os.remove(self.recover_file)
            except:
                pass


class JobCluster(object):
    """Create an instance of cluster for a specific computation.
    """

    def __init__(self, computation, nodes=None, depends=[], callback=None, cluster_status=None,
                 ip_addr=None, port=None, node_port=None, ext_ip_addr=None,
                 dest_path=None, loglevel=logging.INFO, setup=None, cleanup=True,
                 ping_interval=None, pulse_interval=None, poll_interval=None,
                 reentrant=False, secret='', keyfile=None, certfile=None, recover_file=None):
        """Create an instance of cluster for a specific computation.

        @computation is either a string (which is name of program, possibly
        with full path) or a python function or class method.

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

       @callback is a function or class method. When a job's results
          become available, dispy will call provided callback
          function/method with that job as the argument. If a job
          sends provisional results with 'dispy_provisional_result'
          multiple times, then dispy will call provided callback each
          such time. The (provisional) results of computation can be
          retrieved with 'result' field of job, etc. While
          computations are run on nodes in isolated environments,
          callbacks are run in the context of user programs from which
          (Shared)JobCluster is called - for example, callbacks can
          access global variables in user programs.

        @cluster_status is a function or class method. When a node
          accepts this cluster's computation, a job is submitted, a
          jos is done or node is closed, given function is called with
          three parameters: an instance of DispyNode, node/job status
          (one of DispyNode.Initialized, DispyNode.Closed, or job
          status), and an instance of DispyJob (if job submitted,
          finished etc.) or None (if node started or closed). dispy
          queues these status messages and a worker thread calls the
          functions, so it is possible that actual current status of
          node may be different from the status indicated at the time
          status function is called.

        @ip_addr and @port indicate the address where the cluster will bind to.
          If multiple instances of JobCluster are used, these arguments are used
          only in the case of first instance.
          If no value for @ip_addr is given (default), IP address associated
          with the 'hostname' is used.
          If no value for @port is given (default), number 51347 is used.

        @ext_ip_addr is the IP address of NAT firewall/gateway if
          dispy client is behind that firewall/gateway.

        @node_port indicates port on which node servers are listening
          for ping messages. The client (JobCluster instance) broadcasts
          ping requests to this port.
          If no value for @node_port is given (default), number 51348 is used.

        @dest_path indicates path of directory to which files are
          transferred to a server node when executing a job.  If
          @computation is a string, indicating a program, then that
          program is also transferred to @dest_path.

        @loglevel indicates message priority for logging module.

        @cleanup indicates if the files transferred should be removed when
          shutting down.

        @secret is a string that is (hashed and) used for handshaking
          of communication with nodes.

        @certfile is path to file containing SSL certificate (see
          Python 'ssl' module).

        @keyfile is path to file containing private key for SSL
          communication (see Python 'ssl' module). This key may be
          stored in 'certfile' itself, in which case this should be
          None.

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
        message. See 'reentrant' below.

        @poll_interval is number of seconds between 5 and 1000. If
        poll_interval is set, the client uses polling to check the
        status of jobs executed by nodes, instead of nodes connecting
        to the client to send the status of jobs, which is not
        possible if the client is behind a gateway / router which
        doesn't forward ports to where the client is running. Polling
        is not efficient, so it must be used only where necessary.

        @reentrant must be either True or False. This value is used
        only if 'pulse_interval' is set for any of the clusters. If
        pulse_interval is given and reentrant is False (default), jobs
        scheduled for a dead node are automatically cancelled; if
        reentrant is True, then jobs scheduled for a dead node are
        resubmitted to other eligible nodes.

        @recover_file must be either None (default) or file path. If
        this is None, dispy stores information about cluster in a file
        of the form '_dispy_YYYYMMDDHHMMSS' in current directory. If
        it is a path, dispy will use given path to store
        information. If user program terminates for some reason (such
        as raising an exception), it is possible to retrieve results
        of scheduled jobs later (after they are finished) by calling
        'recover' function (implemented in this file) with this file.
        """

        logger.setLevel(loglevel)
        asyncoro.logger.setLevel(loglevel)
        if reentrant is not True and reentrant is not False:
            logger.warning('Invalid value for reentrant (%s) is ignored; '
                           'it must be either True or False' % reentrant)
            reentrant = False
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

        if poll_interval is not None:
            try:
                poll_interval = float(poll_interval)
                assert 5.0 <= poll_interval <= 1000
            except:
                raise Exception('Invalid poll_interval; must be between 5 and 1000')
        self.poll_interval = poll_interval

        if callback:
            assert inspect.isfunction(callback) or inspect.ismethod(callback), \
                'callback must be a function or method'
            try:
                args = inspect.getargspec(callback)
                if inspect.isfunction(callback):
                    assert len(args.args) == 1
                else:
                    assert len(args.args) == 2
                    if args.args[0] != 'self':
                        logger.warning('First argument to callback method is not "self"')
                assert args.varargs is None
                assert args.keywords is None
                assert args.defaults is None
            except:
                raise Exception('Invalid callback function; '
                                'it must take excatly one argument - an instance of DispyJob')
        self.callback = callback

        if cluster_status:
            assert inspect.isfunction(cluster_status) or inspect.ismethod(cluster_status), \
                'cluster_status must be a function or method'
            try:
                args = inspect.getargspec(cluster_status)
                if inspect.isfunction(cluster_status):
                    assert len(args.args) == 3
                else:
                    assert len(args.args) == 4
                    if args.args[0] != 'self':
                        logger.warning('First argument to cluster_status method is not "self"')
                assert args.varargs is None
                assert args.keywords is None
                assert args.defaults is None
            except:
                raise Exception('Invalid cluster_status function; '
                                'it must take excatly 3 arguments')
        self.status_callback = cluster_status

        if hasattr(self, 'scheduler_ip_addr'):
            shared = True
        else:
            shared = False
            if not nodes:
                nodes = ['*']
            elif not isinstance(nodes, list):
                if isinstance(nodes, str):
                    nodes = [nodes]
                else:
                    raise Exception('"nodes" must be list of IP addresses or host names')
            self._node_specs = _parse_nodes(nodes)
            if not self._node_specs:
                raise Exception('"nodes" argument is invalid')
            self._node_specs = sorted(self._node_specs, key=lambda node_spec: node_spec.rex)
            self._node_specs.reverse()
        self._dispy_nodes = {}

        if setup:
            assert inspect.isfunction(setup), "setup must be Python function"
            depends.append(setup)

        if cleanup:
            if cleanup is not True:
                assert inspect.isfunction(cleanup), "cleanup must be Python function"
                depends.append(cleanup)

        self._cluster = _Cluster(ip_addr=ip_addr, port=port, node_port=node_port,
                                 ext_ip_addr=ext_ip_addr, shared=shared,
                                 secret=secret, keyfile=keyfile, certfile=certfile,
                                 recover_file=recover_file)
        atexit.register(self.shutdown)
        # self.ip_addr = self._cluster.ip_addr

        if inspect.isfunction(computation):
            func = computation
            compute = _Compute(_Compute.func_type, func.func_name)
            lines = inspect.getsourcelines(func)[0]
            lines[0] = lines[0].lstrip()
            compute.code = ''.join(lines)
        elif isinstance(computation, str):
            compute = _Compute(_Compute.prog_type, computation)
            depends.append(computation)
        else:
            raise Exception('Invalid computation type: %s' % type(compute))
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
                    for p in os.environ['PATH'].split(os.pathsep):
                        f = os.path.join(p, dep)
                        if os.path.isfile(f):
                            logger.debug('Assuming "%s" is program "%s"', dep, f)
                            dep = f
                            break
                    else:
                        raise Exception('Program "%s" is not valid' % dep)
                try:
                    fd = open(dep, 'rb')
                    fd.close()
                    xf = _XferFile(dep, os.stat(dep), compute.id)
                    compute.xfer_files.append(xf)
                    depend_ids[dep] = dep
                except:
                    raise Exception('File "%s" is not valid' % dep)
            elif inspect.isfunction(dep) or inspect.isclass(dep) or hasattr(dep, '__class__'):
                if inspect.isfunction(dep) or inspect.isclass(dep):
                    pass
                elif hasattr(dep, '__class__') and inspect.isclass(dep.__class__):
                    dep = dep.__class__
                if id(dep) in depend_ids:
                    continue
                if compute.type == _Compute.prog_type:
                    raise Exception('Program computations cannot depend on "%s"' % dep.__name__)
                lines = inspect.getsourcelines(dep)[0]
                lines[0] = lines[0].lstrip()
                compute.code += '\n' + ''.join(lines)
                depend_ids[id(dep)] = id(dep)
            else:
                raise Exception('Invalid function: %s' % dep)
        if compute.code:
            # make sure code can be compiled
            code = compile(compute.code, '<string>', 'exec')
            del code
        if dest_path:
            if not isinstance(dest_path, str):
                raise Exception('Invalid dest_path: it must be a string')
            dest_path = dest_path.strip()
            # we should check for absolute path in dispynode.py as well
            if dest_path.startswith(os.sep):
                logger.warning('dest_path must not be absolute path')
            dest_path = dest_path.lstrip(os.sep)
            compute.dest_path = dest_path

        compute.scheduler_port = self._cluster.port
        compute.auth = os.urandom(10).encode('hex')
        compute.job_result_port = self._cluster.port
        compute.reentrant = reentrant
        compute.pulse_interval = pulse_interval
        if inspect.isfunction(setup):
            compute.setup = setup.func_name
        else:
            compute.setup = None
        if inspect.isfunction(cleanup):
            compute.cleanup = cleanup.func_name
        else:
            compute.cleanup = cleanup

        self._compute = compute
        self._pending_jobs = 0
        self._jobs = []
        self._complete = threading.Event()
        self._complete.set()
        self.cpu_time = 0
        self.start_time = time.time()
        self.end_time = None
        if not shared:
            Coro(self._cluster.add_cluster, self).value()

    def submit(self, *args, **kwargs):
        """Submit a job for execution with the given arguments.

        Arguments should be serializable and should correspond to
        arguments for computation used when cluster is created.
        """
        if self._compute.type == _Compute.prog_type:
            if kwargs:
                logger.warning('Programs can not have keyword arguments')
                return None
            args = [str(arg) for arg in args]
        try:
            _job = _DispyJob_(self._compute.id, args, kwargs)
        except:
            logger.warning('Creating job for "%s", "%s" failed with "%s"',
                           str(args), str(kwargs), traceback.format_exc())
            return None
        Coro(self._cluster.submit_job, _job).value()
        return _job.job

    def cancel(self, job):
        return Coro(self._cluster.cancel_job, job).value()

    def add_node(self, node):
        return Coro(self._cluster.add_nodespec, self, node).value()

    def node_jobs(self, node, from_node=False):
        return Coro(self._cluster.node_jobs, self, node, from_node).value()

    def set_node_cpus(self, node, cpus):
        return Coro(self._cluster.set_node_cpus, node, cpus).value()

    @property
    def name(self):
        return self._compute.name

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, trace):
        self.close()
        return True

    def status(self):
        """
        Return cluster status (ClusterStatus structure).
        """
        def _status(self, coro=None):
            yield ClusterStatus(self._dispy_nodes.values(), self._pending_jobs)
        return Coro(_status, self).value()

    def print_status(self, wall_time=None):
        """
        Prints status of cluster (see 'status').
        """
        print
        heading = ' %30s | %5s | %7s | %10s | %13s' % \
                  ('Node', 'CPUs', 'Jobs', 'Sec/Job', 'Node Time Sec')
        print(heading)
        print('-' * len(heading))
        info = self.status()
        cpu_time = 0.0
        for dispy_node in info.nodes:
            cpu_time += dispy_node.cpu_time
            name = dispy_node.ip_addr
            if dispy_node.name:
                name += ' (' + dispy_node.name + ')'
            print(' %-30.30s | %5s | %7s | %10.3f | %13.3f' %
                  (name, dispy_node.cpus, dispy_node.jobs_done,
                   dispy_node.secs_per_job, dispy_node.cpu_time))
        print
        if info.jobs_pending:
            print('Jobs pending: %s' % info.jobs_pending)
        msg = 'Total job time: %.3f sec' % cpu_time
        if wall_time:
            msg += ', wall time: %.3f sec, speedup: %.3f' % (wall_time, cpu_time / wall_time)
        print(msg)
        print

    # for backward compatibility
    stats = print_status

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
            self._complete.wait()
            Coro(self._cluster.del_cluster, self).value()
            del self._compute

    def shutdown(self):
        self.close()
        if hasattr(self, '_cluster'):
            cluster = self._cluster
            del self._cluster
            cluster.shutdown()


class SharedJobCluster(JobCluster):
    """SharedJobCluster should be used (instead of JobCluster) if two
    or more processes can simultaneously use dispy. In this case,
    'dispyscheduler' must be running on a node and 'scheduler_node'
    parameter should be set to that node's IP address or host name.

    @scheduler_node is name or IP address where dispyscheduler is
      running to which jobs are submitted.

    @port is port where dispyscheduler is running at @scheduler_node.

    @pulse_interval for SharedJobCluster is not used; instead,
    dispyscheduler must be called with appropriate pulse_interval.
    The behaviour is same as for JobCluster.
    """
    def __init__(self, computation, nodes=None, depends=[], callback=None, cluster_status=None,
                 ip_addr=None, port=None, scheduler_node=None, scheduler_port=None,
                 ext_ip_addr=None, loglevel=logging.INFO, setup=None, cleanup=True, dest_path=None,
                 poll_interval=None, reentrant=False, secret='',
                 keyfile=None, certfile=None, recover_file=None):

        if scheduler_node:
            self.scheduler_ip_addr = _node_ipaddr(scheduler_node)
            if not self.scheduler_ip_addr:
                raise Exception('scheduler_node "%s" is invalid' % scheduler_node)
        else:
            self.scheduler_ip_addr = socket.gethostbyname(socket.gethostname())
        if not nodes:
            nodes = ['*']
        elif not isinstance(nodes, list):
            if isinstance(nodes, str):
                nodes = [nodes]
            else:
                raise Exception('"nodes" must be list of IP addresses or host names')
        node_specs = _parse_nodes(nodes)
        if not node_specs:
            raise Exception('"nodes" argument is invalid')

        JobCluster.__init__(self, computation, depends=depends,
                            callback=callback, cluster_status=cluster_status,
                            ip_addr=ip_addr, port=port, ext_ip_addr=ext_ip_addr,
                            loglevel=loglevel, setup=setup, cleanup=cleanup, dest_path=dest_path,
                            poll_interval=poll_interval, reentrant=reentrant,
                            secret=secret, keyfile=keyfile, certfile=certfile,
                            recover_file=recover_file)

        def _terminate_scheduler(self, coro=None):
            self._cluster.terminate = True
            yield self._cluster._sched_event.set()

        Coro(_terminate_scheduler, self)
        # wait for scheduler to terminate
        self._cluster._scheduler.value()
        self._cluster.job_uid = None
        node_specs = sorted(node_specs, key=lambda node_spec: node_spec.rex)
        node_specs.reverse()

        if not scheduler_port:
            scheduler_port = 51349

        # wait until tcp server has started
        while self._cluster.port == 0:
            time.sleep(0.1)

        ext_ip_addr = None
        for ext_ip_addr in (self._cluster.ext_ip_addrs - self._cluster.ip_addrs):
            if not ext_ip_addr:
                break
        if not ext_ip_addr:
            for ext_ip_addr in self._cluster.ext_ip_addrs:
                if not ext_ip_addr:
                    break

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock = AsyncSocket(sock, blocking=True, keyfile=keyfile, certfile=certfile)
        sock.connect((self.scheduler_ip_addr, scheduler_port))
        sock.sendall(self._cluster.auth)
        req = {'version': _dispy_version, 'ip_addr': ext_ip_addr,
               'scheduler_ip_addr': self.scheduler_ip_addr}
        sock.send_msg('CLIENT:' + serialize(req))
        reply = sock.recv_msg()
        sock.close()
        reply = unserialize(reply)
        if reply['version'] != _dispy_version:
            raise Exception('dispyscheduler version "%s" is different from dispy version "%s"' %
                            reply['version'], _dispy_version)
        ext_ip_addr = reply['ip_addr']
        self.scheduler_port = reply['port']
        self._scheduler_auth = auth_code(secret, reply['sign'])

        sock = AsyncSocket(socket.socket(socket.AF_INET, socket.SOCK_STREAM), blocking=True,
                           keyfile=keyfile, certfile=certfile)
        sock.settimeout(MsgTimeout)
        self._compute.scheduler_ip_addr = ext_ip_addr
        self._compute.scheduler_port = self._cluster.port
        self._compute.job_result_port = self._cluster.port
        try:
            sock.connect((self.scheduler_ip_addr, self.scheduler_port))
            sock.sendall(self._scheduler_auth)
            req = {'compute': self._compute, 'node_specs': node_specs}
            sock.send_msg('COMPUTE:' + serialize(req))
            msg = sock.recv_msg()
            sock.close()
            resp = unserialize(msg)
            self._compute.id = resp['compute_id']
            assert self._compute.id is not None
        except:
            logger.debug(traceback.format_exc())
            raise Exception('Could not connect to scheduler at %s:%s' %
                            (self.scheduler_ip_addr, self.scheduler_port))
        self.job_result_port = resp['job_result_port']
        self._cluster.timer_coro.resume(True)

        for xf in self._compute.xfer_files:
            xf.compute_id = self._compute.id
            logger.debug('Sending file "%s"', xf.name)
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock = AsyncSocket(sock, blocking=True, keyfile=keyfile, certfile=certfile)
            sock.settimeout(MsgTimeout)
            try:
                sock.connect((self.scheduler_ip_addr, self.scheduler_port))
                sock.sendall(self._scheduler_auth)
                sock.send_msg('FILEXFER:' + serialize(xf))
                resp = sock.recv_msg()
                if resp != 'ACK':
                    fd = open(xf.name, 'rb')
                    while True:
                        data = fd.read(1024000)
                        if not data:
                            break
                        sock.sendall(data)
                    fd.close()
                    resp = sock.recv_msg()
                    assert resp == 'ACK'
            except:
                logger.error('Could not transfer %s to %s', xf.name, self.scheduler_ip_addr)
                # TODO: delete computation?
            sock.close()

        sock = AsyncSocket(socket.socket(socket.AF_INET, socket.SOCK_STREAM), blocking=True,
                           keyfile=keyfile, certfile=certfile)
        sock.settimeout(MsgTimeout)
        sock.connect((self.scheduler_ip_addr, self.scheduler_port))
        sock.sendall(self._scheduler_auth)
        req = {'compute_id': self._compute.id, 'auth': self._compute.auth}
        sock.send_msg('ADD_CLUSTER:' + serialize(req))
        msg = sock.recv_msg()
        sock.close()
        resp = unserialize(msg)
        if resp == self._compute.id:
            logger.debug('Computation %s created with %s', self._compute.name, self._compute.id)
            Coro(self._cluster.add_cluster, self).value()
        else:
            self._cluster._clusters.pop(self._compute.id, None)
            raise Exception('Computation "%s" could not be sent to scheduler' % self._compute.name)

    def submit(self, *args, **kwargs):
        """Submit a job for execution with the given arguments.

        Arguments should be serializable and should correspond to
        arguments for computation used when cluster is created.
        """
        if self._compute.type == _Compute.prog_type:
            if kwargs:
                logger.warning('Programs can not have keyword arguments')
                return None
            args = [str(arg) for arg in args]
        try:
            _job = _DispyJob_(self._compute.id, args, kwargs)
        except:
            logger.warning('Creating job for "%s", "%s" failed with "%s"',
                           str(args), str(kwargs), traceback.format_exc())
            return None

        try:
            for xf in _job.xfer_files:
                sock = AsyncSocket(socket.socket(socket.AF_INET, socket.SOCK_STREAM), blocking=True,
                                   keyfile=self._cluster.keyfile, certfile=self._cluster.certfile)
                sock.settimeout(MsgTimeout)
                sock.connect((self.scheduler_ip_addr, self.scheduler_port))
                sock.sendall(self._scheduler_auth)
                sock.send_msg('FILEXFER:' + serialize(xf))
                resp = sock.recv_msg()
                if resp != 'ACK':
                    fd = open(xf.name, 'rb')
                    while True:
                        data = fd.read(1024000)
                        if not data:
                            break
                        sock.sendall(data)
                    fd.close()
                    resp = sock.recv_msg()
                    assert resp == 'ACK'
                sock.close()

            sock = AsyncSocket(socket.socket(socket.AF_INET, socket.SOCK_STREAM), blocking=True,
                               keyfile=self._cluster.keyfile, certfile=self._cluster.certfile)
            sock.settimeout(MsgTimeout)
            sock.connect((self.scheduler_ip_addr, self.scheduler_port))
            sock.sendall(self._scheduler_auth)
            req = {'job': _job, 'auth': self._compute.auth}
            sock.send_msg('JOB:' + serialize(req))
            msg = sock.recv_msg()
            _job.uid = unserialize(msg)
            self._cluster._sched_jobs[_job.uid] = _job
            self._pending_jobs += 1
            self._complete.clear()
            if self.status_callback:
                self._cluster.worker_Q.put((self.status_callback,
                                            (DispyJob.Created, None, _job.job)))
            return _job.job
        except:
            logger.warning('Creating job for "%s", "%s" failed with "%s"',
                           str(args), str(kwargs), traceback.format_exc())
            _job.job._dispy_job_ = None
            del _job.job
            return None
        finally:
            sock.close()

    def cancel(self, job):
        _job = job._dispy_job_
        if _job is None or self._cluster._clusters.get(_job.compute_id, None) != self:
            logger.warning('Invalid job %s for cluster "%s"!', job.id, self._compute.name)
            return -1
        if job.status not in [DispyJob.Created, DispyJob.Running, DispyJob.ProvisionalResult]:
            logger.warning('Job %s is not valid for cancel (%s)', job.id, job.status)
            return -1

        job.status = DispyJob.Cancelled
        # assert self._pending_jobs >= 1
        sock = AsyncSocket(socket.socket(socket.AF_INET, socket.SOCK_STREAM), blocking=True,
                           keyfile=self._cluster.keyfile, certfile=self._cluster.certfile)
        sock.settimeout(MsgTimeout)
        try:
            sock.connect((self.scheduler_ip_addr, self.scheduler_port))
            sock.sendall(self._scheduler_auth)
            req = {'uid': _job.uid, 'compute_id': self._compute.id, 'auth': self._compute.auth}
            sock.send_msg('TERMINATE_JOB:' + serialize(req))
        except:
            logger.warning('Could not connect to scheduler to terminate job')
            return -1
        finally:
            sock.close()
        return 0

    def add_node(self, node):
        sock = AsyncSocket(socket.socket(socket.AF_INET, socket.SOCK_STREAM), blocking=True,
                           keyfile=self._cluster.keyfile, certfile=self._cluster.certfile)
        sock.settimeout(MsgTimeout)
        try:
            sock.connect((self.scheduler_ip_addr, self.scheduler_port))
            sock.sendall(self._scheduler_auth)
            req = {'compute_id': self._compute.id, 'auth': self._compute.auth, 'node_spec': node}
            sock.send_msg('ADD_NODESPEC:' + serialize(req))
            reply = sock.recv_msg()
            reply = unserialize(reply)
        except:
            logger.warning('Could not connect to scheduler to add node')
            reply = -1
        finally:
            sock.close()
        return reply

    def node_jobs(self, node, from_node=False):
        node = _node_ipaddr(node)
        if not node:
            return []
        sock = AsyncSocket(socket.socket(socket.AF_INET, socket.SOCK_STREAM), blocking=True,
                           keyfile=self._cluster.keyfile, certfile=self._cluster.certfile)
        sock.settimeout(MsgTimeout)
        try:
            sock.connect((self.scheduler_ip_addr, self.scheduler_port))
            sock.sendall(self._scheduler_auth)
            req = {'compute_id': self._compute.id, 'auth': self._compute.auth,
                   'node': node, 'from_node': bool(from_node)}
            sock.send_msg('NODE_JOBS:' + serialize(req))
            reply = sock.recv_msg()
            job_uids = unserialize(reply)
            _jobs = [self._cluster._sched_jobs.get(uid, None) for uid in job_uids]
        except:
            logger.warning('Could not connect to scheduler to get running jobs at node')
            _jobs = []
        finally:
            sock.close()
        jobs = [_job.job for _job in _jobs if _job is not None]
        return jobs

    def set_node_cpus(self, node, cpus):
        sock = AsyncSocket(socket.socket(socket.AF_INET, socket.SOCK_STREAM), blocking=True,
                           keyfile=self._cluster.keyfile, certfile=self._cluster.certfile)
        sock.settimeout(MsgTimeout)
        try:
            sock.connect((self.scheduler_ip_addr, self.scheduler_port))
            sock.sendall(self._scheduler_auth)
            req = {'compute_id': self._compute.id, 'auth': self._compute.auth, 'node': node}
            sock.send_msg('SET_NODE_CPUS:' + serialize(req))
            reply = sock.recv_msg()
            reply = unserialize(reply)
        except:
            logger.warning('Could not connect to scheduler to add node')
            return -1
        finally:
            sock.close()
        return reply

    def close(self):
        self._complete.wait()
        if hasattr(self, '_cluster'):
            cluster = self._cluster
            del self._cluster
            Coro(cluster.del_cluster, self).value()


def recover_jobs(recover_file):
    """
    If dispy client crashes or loses connection to nodes, the nodes
    will continue to execute scheduled jobs. This 'recover_jobs'
    function can be used to retrieve the results of those jobs
    (DispyJob objects).

    @recover_file is path to file in which dispy stored information
      about cluster (see 'recover_file' in JobCluster above). If
      incorrect 'recover_file' is used, this function issues a warning
      and will block.

    Returns list of DispyJob instances that will have .result,
    .stdout, .stderr etc.; however, the nodes don't keep track of .id,
    .args, .kwargs so they will be None.

    Once all the jobs that were scheduled at the time of crash are
    retrieved (if the jobs are still running, this function will block
    until all the jobs are finished and results obtained), nodes are
    closed (so they can serve new clients), 'recover_file' is removed
    and the jobs are returned.
    """

    shelf_nodes = {}
    computes = {}
    cluster = None

    shelf = shelve.open(recover_file, flag='r')
    for key, val in shelf.iteritems():
        if key.startswith('node_'):
            shelf_nodes[key[len('node_'):]] = val
        elif key.startswith('compute_'):
            computes[int(key[len('compute_'):])] = val
        elif key == '_cluster':
            cluster = val
        else:
            logger.warning('invalid key "%s" ignored' % key)
    shelf.close()
    if not cluster or not computes or not shelf_nodes:
        try:
            os.remove(recover_file)
        except:
            pass
        return []

    nodes = {}
    for ip_addr, info in shelf_nodes.iteritems():
        node = _Node(ip_addr, info['port'], 0, '', cluster['secret'],
                     keyfile=cluster['keyfile'], certfile=cluster['certfile'])
        node.auth = info['auth']
        nodes[node.ip_addr] = node

    def tcp_task(conn, addr, pending, coro=None):
        # generator
        conn.settimeout(MsgTimeout)
        msg = yield conn.recv_msg()
        if msg.startswith('JOB_REPLY:'):
            try:
                reply = unserialize(msg[len('JOB_REPLY:'):])
            except:
                logger.warning('invalid job reply from %s:%s ignored' % (addr[0], addr[1]))
                conn.close()
                raise StopIteration
            yield conn.send_msg('ACK')
            logger.debug('received reply for job %s' % reply.uid)
            job = DispyJob()
            job.result = reply.result
            job.stdout = reply.stdout
            job.stderr = reply.stderr
            job.exception = reply.exception
            job.start_time = reply.start_time
            job.end_time = reply.end_time
            job.status = DispyJob.Finished
            job.ip_addr = reply.ip_addr
            job.finish.set()
            pending['jobs'].append(job)
            pending['count'] -= 1
            if pending['count'] == 0 and pending['resend_done'] is True:
                pending['complete'].set()
        else:
            logger.debug('Invalid TCP message from %s ignored' % addr[0])
        conn.close()

    def tcp_server(ip_addr, pending, coro=None):
        coro.set_daemon()
        sock = AsyncSocket(socket.socket(socket.AF_INET, socket.SOCK_STREAM),
                           keyfile=cluster['keyfile'], certfile=cluster['certfile'])
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((ip_addr, cluster['port']))
        sock.listen(32)

        while True:
            try:
                conn, addr = yield sock.accept()
            except ssl.SSLError as err:
                logger.debug('SSL connection failed: %s', str(err))
                continue
            except GeneratorExit:
                break
            except:
                logger.debug(traceback.format_exc())
                continue
            else:
                Coro(tcp_task, conn, addr, pending)
        raise StopIteration

    def resend_requests(pending, coro=None):
        for compute_id, compute in computes.items():
            req = serialize({'compute_id': compute_id, 'auth': compute['auth']})
            for ip_addr in compute['nodes']:
                node = nodes.get(ip_addr, None)
                if not node:
                    continue
                reply = yield node.send('RESEND_JOB_RESULTS:' + req)
                try:
                    reply = unserialize(reply)
                    assert isinstance(reply, int)
                except:
                    logger.warning('Invalid resend reply from %s' % ip_addr)
                    continue
                if reply == 0:
                    yield node.send('CLOSE:' + req, reply=False)
                else:
                    logger.debug('pending jobs from %s for %s: %s' %
                                 (node.ip_addr, compute_id, reply))
                    pending['count'] += reply
        pending['resend_done'] = True

    pending = {'count': 0, 'resend_done': False, 'jobs': [], 'complete': threading.Event()}
    for ip_addr in cluster['ip_addrs']:
        if not ip_addr:
            ip_addr = ''
        Coro(tcp_server, ip_addr, pending)

    Coro(resend_requests, pending)

    pending['complete'].wait()

    for compute_id, compute in computes.iteritems():
        req = serialize({'compute_id': compute_id, 'auth': compute['auth']})
        for ip_addr in compute['nodes']:
            node = nodes.get(ip_addr, None)
            if not node:
                continue
            Coro(node.send, 'CLOSE:' + req, reply=False)

    try:
        os.remove(recover_file)
    except:
        logger.warning('Could not remove "%s"' % recover_file)
    return pending['jobs']


if __name__ == '__main__':
    import argparse

    logger.info('dispy version %s' % _dispy_version)

    parser = argparse.ArgumentParser()
    parser.add_argument('computation', help='program to distribute and parallelize')
    parser.add_argument('-c', action='store_false', dest='cleanup', default=True,
                        help='if True, nodes will remove any files transferred when '
                        'this computation is over')
    parser.add_argument('-d', '--debug', action='store_true', dest='loglevel', default=False,
                        help='if given, debug messages are printed')
    parser.add_argument('-a', action='append', dest='args', default=[],
                        help='argument(s) to program; repeat for multiple instances')
    parser.add_argument('-f', action='append', dest='depends', default=[],
                        help='dependencies (files) needed by program')
    parser.add_argument('-n', '--nodes', action='append', dest='nodes', default=[],
                        help='list of nodes (names or IP address) acceptable for this computation')
    parser.add_argument('--ip_addr', dest='ip_addr', default=None,
                        help='IP address of this client')
    parser.add_argument('--secret', dest='secret', default='',
                        help='authentication secret for handshake with nodes')
    parser.add_argument('--certfile', dest='certfile', default=None,
                        help='file containing SSL certificate')
    parser.add_argument('--keyfile', dest='keyfile', default=None,
                        help='file containing SSL key')
    parser.add_argument('--scheduler_node', dest='scheduler_node', default=None,
                        help='name or IP address where dispyscheduler is running to which '
                        'jobs are submitted')

    config = vars(parser.parse_args(sys.argv[1:]))
    # print(config)

    if config['loglevel']:
        logger.setLevel(logging.DEBUG)
        asyncoro.logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
    del config['loglevel']

    args = config.pop('args')

    if config['scheduler_node']:
        cluster = SharedJobCluster(**config)
    else:
        del config['scheduler_node']
        cluster = JobCluster(**config)

    jobs = []
    for n, arg in enumerate(args, start=1):
        job = cluster.submit(*(arg.split()))
        job.id = n
        jobs.append((job, arg))

    for job, args in jobs:
        job()
        sargs = ''.join(arg for arg in args)
        if job.exception:
            print('Job %s with arguments "%s" failed with "%s"' %
                  (job.id, sargs, job.exception))
            continue
        if job.result:
            print('Job %s with arguments "%s" exited with: "%s"' %
                  (job.id, sargs, str(job.result)))
        if job.stdout:
            print('Job %s with arguments "%s" produced output: "%s"' %
                  (job.id, sargs, job.stdout))
        if job.stderr:
            print('Job %s with argumens "%s" produced error messages: "%s"' %
                  (job.id, sargs, job.stderr))

    cluster.print_status()
    exit(0)
