#!/usr/bin/python

"""
dispy: Distribute computations among CPUs/cores on a single machine or machines
in cluster(s), grid, cloud etc. for parallel execution.  See
http://dispy.sourceforge.net or https://pgiri.github.com/dispy for details.
"""

import os
import sys
import time
import socket
import inspect
import stat
import threading
import re
import ssl
import hashlib
import traceback
import shelve
import datetime
import atexit
import functools
import Queue as queue
import numbers
import collections
import struct
import errno
import platform
import itertools
try:
    import netifaces
except ImportError:
    netifaces = None

import pycos
from pycos import Task, Pycos, AsyncSocket, Singleton, serialize, deserialize

__author__ = "Giridhar Pemmasani (pgiri@yahoo.com)"
__email__ = "pgiri@yahoo.com"
__copyright__ = "Copyright 2011, Giridhar Pemmasani"
__contributors__ = []
__maintainer__ = "Giridhar Pemmasani (pgiri@yahoo.com)"
__license__ = "Apache 2.0"
__url__ = "http://dispy.sourceforge.net"
__status__ = "Production"
__version__ = "4.10.3"

__all__ = ['logger', 'DispyJob', 'DispyNode', 'NodeAllocate', 'JobCluster', 'SharedJobCluster']

_dispy_version = __version__
MsgTimeout = 10
IPV6_MULTICAST_GROUP = 'ff05::b409:3171:9705:6159:5134:70'
IPV4_MULTICAST_GROUP = '239.255.51.34'


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

    __slots__ = ('id', 'result', 'stdout', 'stderr', 'exception',
                 'submit_time', 'start_time', 'end_time', 'status',
                 'ip_addr', 'finish', '_args', '_kwargs', '_dispy_job_')

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

    def __init__(self, args, kwargs):
        # id can be assigned by user as appropriate (e.g., to distinguish jobs)
        self.id = None
        # rest are read-only
        self.result = None
        self.stdout = None
        self.stderr = None
        self.exception = None
        self.submit_time = time.time()
        self.start_time = None
        self.end_time = None
        self.status = DispyJob.Created
        self.ip_addr = None
        self.finish = threading.Event()

        # rest are for dispy implementation only - these are opaque to clients
        self._args = args
        self._kwargs = kwargs
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


class DispyNodeAvailInfo(object):
    """A node's status is represented as available CPU as percent, memory in
    bytes and disk as bytes. This information is passed to NodeAllocte.allocate
    method and in cluster status callback with status DispyNode.AvailInfo.
    """
    def __init__(self, cpu, memory, disk, swap):
        self.cpu = cpu
        self.memory = memory
        self.disk = disk
        self.swap = swap


class DispyNode(object):
    """If 'cluster_status' is used when creating cluster, that function
    is called with an instance of this class as first argument.
    See 'cluster_status' in JobCluster below.
    """

    Initialized = DispyJob.Created - 1
    Closed = DispyJob.Finished + 5
    AvailInfo = Closed + 1

    def __init__(self, ip_addr, name, cpus):
        self.ip_addr = ip_addr
        self.name = name
        self.cpus = cpus
        self.avail_cpus = cpus
        self.busy = 0
        self.jobs_done = 0
        self.cpu_time = 0.0
        self.update_time = 0
        self.avail_info = None


class NodeAllocate(object):
    """Objects of this class describe if / how many CPUs in a node are
    allocated to clusters.

    Each element of 'nodes' passed to JobCluster or SharedJobCluster
    is an object of this class; if the element passed is a string
    (host name or IP address), a tuple (see documentation for
    details), it is converted to NodeAllocate object with
    '_parse_node_allocs' function.

    This class can be specialized (inherited) to override, for
    example, 'allocate' method.
    """
    def __init__(self, host, port=None, cpus=0):
        self.ip_addr = _node_ipaddr(host)
        if not self.ip_addr:
            logger.warning('host "%s" is invalid', host)
            self.ip_rex = ''
        else:
            self.ip_rex = self.ip_addr.replace('.', '\\.').replace('*', '.*')
        if port:
            try:
                port = int(port)
                assert port > 0
            except Exception:
                logger.warning('port must be > 0 for node "%s"', host)
                port = None
        self.port = port
        if cpus:
            try:
                cpus = int(cpus)
            except Exception:
                logger.warning('Invalid cpus for "%s" ignored', host)
                cpus = 0
        self.cpus = cpus

    def allocate(self, cluster, ip_addr, name, cpus, avail_info=None, platform=''):
        """When a node is found, dispy calls this method with the
        cluster for which the node is being allocated, IP address,
        name and CPUs available on that node. This method should
        return a number indicating number of CPUs to use. If return
        value is 0, the node is not used for that cluster.
        """
        if re.match(self.ip_rex, ip_addr):
            if self.cpus > 0:
                cpus = min(cpus, self.cpus)
            elif (cpus + self.cpus) > 0:
                cpus = cpus + self.cpus
            return cpus
        return 0


# a cluster's "status" function (not "cluster_status" callback)
# returns this structure; "nodes" is list of DispyNode objects and
# "jobs_pending" is number of jobs that are not done yet
ClusterStatus = collections.namedtuple('ClusterStatus', ['nodes', 'jobs_pending'])


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


def _same_file(tgt, xf):
    """Internal use only.
    """
    # TODO: compare checksum?
    try:
        stat_buf = os.stat(tgt)
        if (stat_buf.st_size == xf.stat_buf.st_size and
            abs(stat_buf.st_mtime - xf.stat_buf.st_mtime) <= 1):
            return True
    except Exception:
        return False


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
        info = socket.getaddrinfo(node, None)[0]
    except Exception:
        return None

    ip_addr = info[-1][0]
    if info[0] == socket.AF_INET6:
        # canonicalize so different platforms resolve to same string
        ip_addr = ip_addr.split('%')[0]
        ip_addr = re.sub(r'^0+', '', ip_addr)
        ip_addr = re.sub(r':0+', ':', ip_addr)
        ip_addr = re.sub(r'::+', '::', ip_addr)
    return ip_addr


def _parse_node_allocs(nodes):
    """Internal use only.
    """
    node_allocs = []
    for node in nodes:
        if isinstance(node, NodeAllocate):
            node_allocs.append(node)
        elif isinstance(node, str):
            node_allocs.append(NodeAllocate(node))
        elif isinstance(node, dict):
            node_allocs.append(NodeAllocate(node.get('host', '*'), node.get('port', None),
                                            node.get('cpus', 0)))
        elif isinstance(node, tuple):
            node_allocs.append(NodeAllocate(*node))
        elif isinstance(node, list):
            node_allocs.append(NodeAllocate(*tuple(node)))
        elif isinstance(node, DispyNode):
            node_allocs.append(NodeAllocate(node.ip_addr))
        else:
            logger.warning('Ignoring node specification %s', type(node))
    return [node_alloc for node_alloc in node_allocs if node_alloc.ip_addr]


def host_addrinfo(host=None, socket_family=None, ipv4_multicast=False):
    """If 'host' is given (as either host name or IP address), resolve it and
    fill AddrInfo structure. If 'host' is not given, netifaces module is used to
    find appropriate IP address. If 'socket_family' is given, IP address with that
    'socket_family' is used. It should be either 'socket.AF_INET' (for IPv4) or
    'socket.AF_INET6' (for IPv6).
    """

    class AddrInfo(object):
        def __init__(self, family, ip, ifn, broadcast, netmask):
            self.family = family
            self.ip = ip
            self.ifn = ifn
            if family == socket.AF_INET and ipv4_multicast:
                self.broadcast = IPV4_MULTICAST_GROUP
            else:
                self.broadcast = broadcast
            self.netmask = netmask
            self.ext_ip_addr = None
            if os.name == 'nt':
                self.bind_addr = ip
            elif platform.system() in ('Darwin', 'DragonFlyBSD', 'FreeBSD', 'OpenBSD', 'NetBSD'):
                if family == socket.AF_INET and (not ipv4_multicast):
                    self.bind_addr = ''
                else:
                    self.bind_addr = self.broadcast
            else:
                self.bind_addr = self.broadcast

    def canonical_ipv6(ip_addr):
        # canonicalize so different platforms resolve to same string
        ip_addr = ip_addr.split('%')[0]
        ip_addr = re.sub(r'^0+', '', ip_addr)
        ip_addr = re.sub(r':0+', ':', ip_addr)
        ip_addr = re.sub(r'::+', '::', ip_addr)
        return ip_addr

    if socket_family:
        if socket_family not in (socket.AF_INET, socket.AF_INET6):
            return None
    hosts = []
    if host:
        best = None
        for addr in socket.getaddrinfo(host, None):
            if socket_family and addr[0] != socket_family:
                continue
            if not best or addr[0] == socket.AF_INET:
                best = addr
        if best:
            socket_family = best[0]
            if best[0] == socket.AF_INET6:
                addr = canonical_ipv6(best[-1][0])
            else:
                addr = best[-1][0]
            hosts.append(addr)
        else:
            return None

    if socket_family:
        socket_families = [socket_family]
    else:
        socket_families = [socket.AF_INET, socket.AF_INET6]

    addrinfos = []
    if netifaces:
        for iface in netifaces.interfaces():
            ifn = 0
            iface_infos = []
            for sock_family in socket_families:
                for link in netifaces.ifaddresses(iface).get(sock_family, []):
                    netmask = link.get('netmask', None)
                    if sock_family == socket.AF_INET:
                        addr = str(link['addr'])
                        broadcast = link.get('broadcast', '<broadcast>')
                        # Windows seems to have broadcast same as addr
                        if broadcast.startswith(addr):
                            broadcast = '<broadcast>'
                        try:
                            addrs = socket.getaddrinfo(addr, None, sock_family, socket.SOCK_STREAM)
                        except Exception:
                            addrs = []
                        for addr in addrs:
                            if hosts and addr[-1][0] not in hosts:
                                continue
                            addrinfo = AddrInfo(sock_family, addr[-1][0], addr[-1][-1],
                                                broadcast, netmask)
                            iface_infos.append(addrinfo)
                    else:  # sock_family == socket.AF_INET6
                        addr = str(link['addr'])
                        broadcast = link.get('broadcast', IPV6_MULTICAST_GROUP)
                        if broadcast.startswith(addr):
                            broadcast = IPV6_MULTICAST_GROUP
                        if_sfx = ['']
                        if not ifn and ('%' not in addr.split(':')[-1]):
                            if_sfx.append('%' + iface)
                        for sfx in if_sfx:
                            if ifn and sfx:
                                break
                            try:
                                addrs = socket.getaddrinfo(addr + sfx, None, sock_family,
                                                           socket.SOCK_STREAM)
                            except Exception:
                                continue
                            for addr in addrs:
                                if addr[-1][-1]:
                                    if ifn and addr[-1][-1] != ifn:
                                        logger.warning('inconsistent scope IDs for %s: %s != %s',
                                                       iface, ifn, addr[-1][-1])
                                    ifn = addr[-1][-1]
                                if sfx:
                                    continue
                                addr = canonical_ipv6(addr[-1][0])
                                if hosts and addr not in hosts:
                                    continue
                                addrinfo = AddrInfo(sock_family, addr, ifn, broadcast, netmask)
                                iface_infos.append(addrinfo)
                        if ifn:
                            for addrinfo in iface_infos:
                                if not addrinfo.ifn:
                                    addrinfo.ifn = ifn
            addrinfos.extend(iface_infos)

    else:
        if not host:
            host = socket.gethostname()
        netmask = None
        for sock_family in socket_families:
            try:
                addrs = socket.getaddrinfo(host, None, sock_family, socket.SOCK_STREAM)
            except Exception:
                continue
            for addr in addrs:
                ifn = addr[-1][-1]
                if sock_family == socket.AF_INET:
                    broadcast = '<broadcast>'
                    addr = addr[-1][0]
                else:  # sock_family == socket.AF_INET6
                    addr = canonical_ipv6(addr[-1][0])
                    broadcast = IPV6_MULTICAST_GROUP
                    logger.warning('IPv6 may not work without "netifaces" package!')
                addrinfo = AddrInfo(sock_family, addr, ifn, broadcast, netmask)
                if hosts:
                    if addrinfo.ip in hosts:
                        return addrinfo
                    else:
                        continue
                addrinfos.append(addrinfo)

    best = None
    for sock_family in socket_families:
        for addrinfo in addrinfos:
            if addrinfo.ip in hosts:
                return addrinfo
            if addrinfo.family != sock_family:
                continue
            if addrinfo.family == socket.AF_INET:
                if not best or (len(best.ip) < len(addrinfo.ip)) or best.ip.startswith('127'):
                    best = addrinfo
            else:
                if addrinfo.ip.startswith('fd'):
                    # TODO: How to detect / avoid temporary addresses (privacy extensions)?
                    if addrinfo.ifn:
                        return addrinfo
                if not best or (len(best.ip) < len(addrinfo.ip)) or best.ip.startswith('fe80'):
                    best = addrinfo
        if best and best.family == socket.AF_INET and (not best.ip.startswith('127')):
            return best
    return best


# This tuple stores information about partial functions; for
# now'setup' and 'cleanup' functions can be partial functions.
# TODO: useful to have 'compute' as partial function as well?
_Function = collections.namedtuple('_Function', ['name', 'args', 'kwargs'])
logger = pycos.Logger('dispy')


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
        self.code = ''
        self.dest_path = None
        self.xfer_files = set()
        self.reentrant = False
        self.exclusive = True
        self.setup = None
        self.cleanup = None
        self.scheduler_ip_addr = None
        self.scheduler_port = None
        self.node_ip_addr = None
        self.auth = None
        self.job_result_port = None
        self.pulse_interval = None

    def __getstate__(self):
        state = dict(self.__dict__)
        return state


class _XferFile(object):
    """Internal use only.
    """
    def __init__(self, name, dest_path, compute_id=None):
        self.name = name
        self.dest_path = dest_path
        self.compute_id = compute_id
        self.stat_buf = os.stat(name)
        self.sep = os.sep


class _Node(object):
    """Internal use only.
    """
    __slots__ = ['ip_addr', 'port', 'name', 'cpus', 'avail_cpus', 'busy', 'cpu_time', 'clusters',
                 'auth', 'secret', 'keyfile', 'certfile', 'last_pulse', 'scheduler_ip_addr',
                 '_jobs', 'pending_jobs', 'avail_info', 'platform', 'sock_family']

    def __init__(self, ip_addr, port, cpus, sign, secret, platform='',
                 keyfile=None, certfile=None):
        self.ip_addr = ip_addr
        if re.match('\d+\.', ip_addr):
            self.sock_family = socket.AF_INET
        else:
            self.sock_family = socket.AF_INET6
        self.port = port
        self.name = None
        self.cpus = cpus
        self.avail_cpus = cpus
        self.busy = 0.0
        self.cpu_time = 0.0
        self.clusters = set()
        self.auth = auth_code(secret, sign)
        self.secret = secret
        self.keyfile = keyfile
        self.certfile = certfile
        self.last_pulse = None
        self.scheduler_ip_addr = None
        self.pending_jobs = []
        self.avail_info = None
        self.platform = platform

    def setup(self, compute, exclusive=True, task=None):
        # generator
        compute.scheduler_ip_addr = self.scheduler_ip_addr
        compute.node_ip_addr = self.ip_addr
        compute.exclusive = exclusive
        reply = yield self.send('COMPUTE:' + serialize(compute), task=task)
        try:
            cpus = deserialize(reply)
            assert isinstance(cpus, int) and cpus > 0
        except Exception:
            logger.warning('Transfer of computation "%s" to %s failed', compute.name, self.ip_addr)
            raise StopIteration(-1)
        if not self.cpus:
            self.cpus = cpus
        for xf in compute.xfer_files:
            resp = yield self.xfer_file(xf, task=task)
            if resp != 0:
                logger.error('Could not transfer file "%s"', xf.name)
                yield self.close(compute, task=task)
                raise StopIteration(resp)

        resp = yield self.send('SETUP:' + serialize(compute.id), timeout=0, task=task)
        if resp != 0:
            logger.warning('Setup of computation "%s" on %s failed: %s',
                           compute.name, self.ip_addr, resp)
            yield self.close(compute, task=task)
            raise StopIteration(resp)
        self.last_pulse = time.time()
        raise StopIteration(0)

    def send(self, msg, reply=True, timeout=MsgTimeout, task=None):
        # generator
        sock = socket.socket(self.sock_family, socket.SOCK_STREAM)
        sock = AsyncSocket(sock, keyfile=self.keyfile, certfile=self.certfile)
        sock.settimeout(timeout)
        try:
            yield sock.connect((self.ip_addr, self.port))
            yield sock.sendall(self.auth)
            yield sock.send_msg(msg)
            if reply:
                resp = yield sock.recv_msg()
            else:
                resp = 0
        except Exception:
            logger.error('Could not connect to %s:%s, %s',
                         self.ip_addr, self.port, traceback.format_exc())
            # TODO: mark this node down, reschedule on different node?
            resp = traceback.format_exc()
        finally:
            sock.close()

        if resp == 'ACK':
            resp = 0
        raise StopIteration(resp)

    def xfer_file(self, xf, task=None):
        # generator
        sock = socket.socket(self.sock_family, socket.SOCK_STREAM)
        sock = AsyncSocket(sock, keyfile=self.keyfile, certfile=self.certfile)
        sock.settimeout(MsgTimeout)
        try:
            yield sock.connect((self.ip_addr, self.port))
            yield sock.sendall(self.auth)
            yield sock.send_msg('FILEXFER:' + serialize(xf))
            recvd = yield sock.recv_msg()
            recvd = deserialize(recvd)
            with open(xf.name, 'rb') as fd:
                sent = 0
                while sent == recvd:
                    data = fd.read(1024000)
                    if not data:
                        break
                    yield sock.sendall(data)
                    sent += len(data)
                    recvd = yield sock.recv_msg()
                    recvd = deserialize(recvd)
            if recvd == xf.stat_buf.st_size:
                resp = 0
            else:
                resp = -1
        except Exception:
            logger.error('Could not transfer %s to %s', xf.name, self.ip_addr)
            # TODO: mark this node down, reschedule on different node?
            resp = -1
        finally:
            sock.close()
        raise StopIteration(resp)

    def close(self, compute, terminate_pending=False, task=None):
        # generator
        logger.debug('Closing node %s for %s / %s', self.ip_addr, compute.name, compute.id)
        req = {'compute_id': compute.id, 'auth': compute.auth, 'node_ip_addr': self.ip_addr,
               'terminate_pending': terminate_pending}
        try:
            yield self.send('CLOSE:' + serialize(req), reply=True, task=task)
        except Exception:
            logger.debug('Deleting computation %s/%s from %s failed',
                         compute.id, compute.name, self.ip_addr)


class _DispyJob_(object):
    """Internal use only.
    """

    __slots__ = ('job', 'uid', 'compute_id', 'hash', 'node', 'pinned',
                 'xfer_files', '_args', '_kwargs', 'code')

    def __init__(self, compute_id, args, kwargs):
        job_deps = kwargs.pop('dispy_job_depends', [])
        self.job = DispyJob(args, kwargs)
        self.job._dispy_job_ = self
        self._args = self.job._args
        self._kwargs = self.job._kwargs
        self.uid = None
        self.compute_id = compute_id
        self.hash = os.urandom(10).encode('hex')
        self.node = None
        self.pinned = None
        self.xfer_files = []
        self.code = ''
        depend_ids = set()
        cwd = os.getcwd()
        for dep in job_deps:
            if isinstance(dep, basestring) or inspect.ismodule(dep):
                if inspect.ismodule(dep):
                    name = dep.__file__
                    if name.endswith('.pyc'):
                        name = name[:-1]
                    if not name.endswith('.py'):
                        logger.warning('Invalid module "%s" - must be python source.', dep)
                        continue
                    if name.startswith(cwd):
                        dst = os.path.dirname(name[len(cwd):].lstrip(os.sep))
                    elif dep.__package__:
                        dst = dep.__package__.replace('.', os.sep)
                    else:
                        dst = os.path.dirname(dep.__name__.replace('.', os.sep))
                else:
                    name = os.path.abspath(dep)
                    if name.startswith(cwd):
                        dst = os.path.dirname(name[len(cwd):].lstrip(os.sep))
                    else:
                        dst = '.'
                if name in depend_ids:
                    continue
                self.xfer_files.append(_XferFile(name, dst, compute_id))
                depend_ids.add(name)
            elif (inspect.isfunction(dep) or inspect.isclass(dep) or
                  (hasattr(dep, '__class__') and hasattr(dep, '__module__'))):
                if inspect.isfunction(dep) or inspect.isclass(dep):
                    pass
                elif hasattr(dep, '__class__') and inspect.isclass(dep.__class__):
                    dep = dep.__class__
                if id(dep) in depend_ids:
                    continue
                try:
                    lines = inspect.getsourcelines(dep)[0]
                except Exception:
                    logger.warning('Invalid job depends eleement "%s"; ignoring it.', dep)
                    continue
                lines[0] = lines[0].lstrip()
                self.code += '\n' + ''.join(lines)
                depend_ids.add(id(dep))
            else:
                logger.warning('Invalid job depends element "%s"; ignoring it.', dep)

    def __getstate__(self):
        state = {'uid': self.uid, 'hash': self.hash, 'compute_id': self.compute_id,
                 '_args': self._args if isinstance(self._args, str) else serialize(self._args),
                 '_kwargs': self._kwargs if isinstance(self._kwargs, str)
                                         else serialize(self._kwargs),
                 'xfer_files': self.xfer_files, 'code': self.code}
        return state

    def __setstate__(self, state):
        for k, v in state.iteritems():
            setattr(self, k, v)

    def __lt__(self, other):
        return self.uid < other.uid

    def __eq__(self, other):
        return isinstance(other, _DispyJob_) and self.uid == other.uid

    def run(self, task=None):
        # generator
        logger.debug('Running job %s on %s', self.uid, self.node.ip_addr)
        self.job.start_time = time.time()
        for xf in self.xfer_files:
            resp = yield self.node.xfer_file(xf, task=task)
            if resp:
                logger.warning('Transfer of file "%s" to %s failed', xf.name, self.node.ip_addr)
                raise Exception(-1)
        resp = yield self.node.send('JOB:' + serialize(self), task=task)
        # TODO: deal with NAKs (reschedule?)
        if resp != 0:
            logger.warning('Failed to run %s on %s: %s', self.job.id, self.node.ip_addr, resp)
            raise Exception(str(resp))
        raise StopIteration(resp)

    def finish(self, status):
        job = self.job
        job.status = status
        if status != DispyJob.ProvisionalResult:
            job._args = ()
            job._kwargs = {}
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
    __metaclass__ = Singleton

    def __init__(self, ip_addr=None, ext_ip_addr=None, port=None, node_port=None,
                 ipv4_udp_multicast=False, shared=False, secret='', keyfile=None, certfile=None,
                 recover_file=None):
        if not hasattr(self, 'pycos'):
            self.pycos = Pycos()
            logger.info('dispy client version: %s', __version__)
            self.ipv4_udp_multicast = bool(ipv4_udp_multicast)
            self.addrinfos = {}
            if isinstance(ip_addr, list):
                ip_addrs = ip_addr
            else:
                ip_addrs = [ip_addr]
            if isinstance(ext_ip_addr, list):
                ext_ip_addrs = ext_ip_addr
            else:
                ext_ip_addrs = [ext_ip_addr]

            for i in range(len(ip_addrs)):
                ip_addr = ip_addrs[i]
                if i < len(ext_ip_addrs):
                    ext_ip_addr = ext_ip_addrs[i]
                else:
                    ext_ip_addr = None
                addrinfo = host_addrinfo(host=ip_addr, ipv4_multicast=self.ipv4_udp_multicast)
                if not addrinfo:
                    logger.warning('Ignoring invalid ip_addr %s', ip_addr)
                    continue
                if addrinfo.family == socket.AF_INET6 and not hasattr(socket, 'inet_pton'):
                    if os.name == 'nt':
                        raise Exception('"win_inet_pton" module is required for IPv6')
                if ext_ip_addr:
                    ext_ip_addr = _node_ipaddr(ext_ip_addr)
                    if not ext_ip_addr:
                        logger.warning('Ignoring invalid ext_ip_addr %s', ext_ip_addrs[i])
                if not ext_ip_addr:
                    ext_ip_addr = addrinfo.ip
                addrinfo.ext_ip_addr = ext_ip_addr
                self.addrinfos[addrinfo.ext_ip_addr] = addrinfo
            if not self.addrinfos:
                raise Exception('No valid IP address found')

            if port:
                port = int(port)
            else:
                if shared:
                    port = 0
                else:
                    port = 51347
            self.port = port
            if node_port:
                node_port = int(node_port)
            else:
                node_port = 51348
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
            self._sched_jobs = {}
            self._sched_event = pycos.Event()
            self._abandoned_jobs = {}
            self._job_id = itertools.count(start=1)
            self.terminate = False
            self.sign = hashlib.sha1(os.urandom(20))
            for ext_ip_addr in self.addrinfos:
                self.sign.update(ext_ip_addr.encode())
            self.sign = self.sign.hexdigest()

            self.auth = auth_code(self.secret, self.sign)

            if isinstance(recover_file, str):
                self.recover_file = recover_file
            else:
                now = datetime.datetime.now()
                self.recover_file = '_dispy_%.4i%.2i%.2i%.2i%.2i%.2i' % \
                                    (now.year, now.month, now.day,
                                     now.hour, now.minute, now.second)
            atexit.register(self.shutdown)
            self.timer_task = Task(self.timer_proc)

            try:
                self.shelf = shelve.open(self.recover_file, flag='c', writeback=True)
                self.shelf['_cluster'] = {'ip_addrs': ip_addrs, 'ext_ip_addrs': ext_ip_addrs,
                                          'port': self.port, 'sign': self.sign,
                                          'secret': self.secret, 'auth': self.auth,
                                          'keyfile': self.keyfile, 'certfile': self.certfile}
                self.shelf.sync()
            except Exception:
                raise Exception('Could not create fault recover file "%s"' %
                                self.recover_file)
            logger.info('Storing fault recovery information in "%s"', self.recover_file)

            self.select_job_node = self.load_balance_schedule
            self._scheduler = Task(self._schedule_jobs)
            self.start_time = time.time()
            self.compute_id = int(1000 * self.start_time)

            self.worker_Q = queue.Queue()
            self.worker_thread = threading.Thread(target=self.worker)
            self.worker_thread.daemon = True
            self.worker_thread.start()

            if self.shared:
                port_bound_event = None
            else:
                port_bound_event = pycos.Event()
            self.tcp_tasks = []
            self.udp_tasks = []
            udp_addrinfos = {}
            for addrinfo in self.addrinfos.values():
                self.tcp_tasks.append(Task(self.tcp_server, addrinfo, port_bound_event))
                if self.shared:
                    continue
                udp_addrinfos[addrinfo.bind_addr] = addrinfo

            for bind_addr, addrinfo in udp_addrinfos.items():
                self.udp_tasks.append(Task(self.udp_server, bind_addr, addrinfo, port_bound_event))

    def udp_server(self, bind_addr, addrinfo, port_bound_event, task=None):
        # generator
        task.set_daemon()
        udp_sock = AsyncSocket(socket.socket(addrinfo.family, socket.SOCK_DGRAM))
        # udp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        while 1:
            try:
                udp_sock.bind((bind_addr, self.port))
            except socket.error as exc:
                if exc.errno == errno.EADDRINUSE:
                    logger.warning('Port %s seems to be used by another program ...', self.port)
                else:
                    logger.warning('Error binding to port %s: %s ...', self.port, exc.errno)
                yield task.sleep(5)
            except Exception:
                logger.warning('Could not bind to port %s: %s', self.port, traceback.format_exc())
                yield task.sleep(5)
            else:
                break

        if addrinfo.family == socket.AF_INET:
            if self.ipv4_udp_multicast:
                mreq = socket.inet_aton(addrinfo.broadcast) + socket.inet_aton(addrinfo.ip)
                udp_sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
        else:  # addrinfo.family == socket.AF_INET6
            mreq = socket.inet_pton(addrinfo.family, addrinfo.broadcast)
            mreq += struct.pack('@I', addrinfo.ifn)
            udp_sock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_JOIN_GROUP, mreq)
            try:
                udp_sock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 1)
            except Exception:
                pass
        port_bound_event.set()
        del port_bound_event
        while 1:
            try:
                msg, addr = yield udp_sock.recvfrom(1000)
            except GeneratorExit:
                break
            if msg.startswith('PING:'):
                try:
                    info = deserialize(msg[len('PING:'):])
                    if info['version'] != _dispy_version:
                        logger.warning('Ignoring %s due to version mismatch', addr[0])
                        continue
                    assert info['port'] > 0
                    assert info['ip_addr']
                    # socket.inet_aton(status['ip_addr'])
                except Exception:
                    # logger.debug(traceback.format_exc())
                    logger.debug('Ignoring node %s', addr[0])
                    continue
                auth = auth_code(self.secret, info['sign'])
                node = self._nodes.get(info['ip_addr'], None)
                if node and node.auth == auth:
                    continue
                sock = AsyncSocket(socket.socket(addrinfo.family, socket.SOCK_STREAM),
                                   keyfile=self.keyfile, certfile=self.certfile)
                sock.settimeout(MsgTimeout)
                msg = {'version': _dispy_version, 'port': self.port, 'sign': self.sign,
                       'node_ip_addr': info['ip_addr']}
                msg['ip_addrs'] = [ai.ext_ip_addr for ai in self.addrinfos.values()]
                try:
                    yield sock.connect((info['ip_addr'], info['port']))
                    yield sock.sendall(auth)
                    yield sock.send_msg('PING:' + serialize(msg))
                except GeneratorExit:
                    break
                except Exception:
                    logger.debug(traceback.format_exc())
                finally:
                    sock.close()
            else:
                pass
        udp_sock.close()

    def tcp_server(self, addrinfo, port_bound_event, task=None):
        # generator
        task.set_daemon()
        if port_bound_event:
            yield port_bound_event.wait()
        del port_bound_event
        sock = AsyncSocket(socket.socket(addrinfo.family, socket.SOCK_STREAM),
                           keyfile=self.keyfile, certfile=self.certfile)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind((addrinfo.ip, self.port))
        except Exception:
            logger.warning('Could not bind TCP server to %s:%s', addrinfo.ip, self.port)
            raise StopIteration
        if not self.port:
            self.port = sock.getsockname()[1]
        logger.debug('dispy client at %s:%s', addrinfo.ip, self.port)
        sock.listen(128)

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
        sock.close()

    def tcp_req(self, conn, addr, task=None):
        # generator
        conn.settimeout(MsgTimeout)
        msg = yield conn.recv_msg()
        if msg.startswith('JOB_REPLY:'):
            try:
                info = deserialize(msg[len('JOB_REPLY:'):])
            except Exception:
                logger.warning('Invalid job reply from %s:%s ignored', addr[0], addr[1])
            else:
                yield self.job_reply_process(info, conn, addr)
            conn.close()

        elif msg.startswith('PULSE:'):
            msg = msg[len('PULSE:'):]
            try:
                info = deserialize(msg)
                node = self._nodes[info['ip_addr']]
                assert 0 <= info['cpus'] <= node.cpus
                node.last_pulse = time.time()
                yield conn.send_msg('PULSE')
                if info['avail_info']:
                    node.avail_info = info['avail_info']
                    for cluster in node.clusters:
                        if cluster.status_callback:
                            dispy_node = cluster._dispy_nodes.get(node.ip_addr, None)
                            if not dispy_node:
                                continue
                            dispy_node.avail_info = info['avail_info']
                            dispy_node.update_time = node.last_pulse
                            self.worker_Q.put((cluster.status_callback,
                                               (DispyNode.AvailInfo, dispy_node, None)))
            except Exception:
                logger.warning('Ignoring pulse message from %s', addr[0])
                # logger.debug(traceback.format_exc())
            conn.close()

        elif msg.startswith('JOB_STATUS:'):
            conn.close()
            # message from dispyscheduler
            try:
                info = deserialize(msg[len('JOB_STATUS:'):])
                _job = self._sched_jobs[info['uid']]
                assert _job.hash == info['hash']
            except Exception:
                logger.warning('Invalid job status from %s:%s ignored', addr[0], addr[1])
            else:
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
                        logger.warning('Invalid job status for shared cluster: %s', job.status)
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
        elif msg.startswith('PONG:'):
            conn.close()
            try:
                info = deserialize(msg[len('PONG:'):])
                if info['version'] != _dispy_version:
                    logger.warning('Ignoring node %s due to version mismatch: %s != %s',
                                   info['ip_addr'], info['version'], _dispy_version)
                    raise StopIteration
                assert info['auth'] == self.auth
            except (AssertionError):
                logger.warning('Ignoring node %s ("secret" mismatch)', info['ip_addr'])
            except (Exception) as err:
                logger.warning('Ignoring node %s: %s', addr[0], err)
            else:
                self.add_node(info)

        elif msg.startswith('PING:'):
            sock_family = conn.family
            conn.close()
            try:
                info = deserialize(msg[len('PING:'):])
                if info['version'] != _dispy_version:
                    logger.warning('Ignoring %s due to version mismatch', addr[0])
                    raise StopIteration
                assert info['port'] > 0
                assert info['ip_addr']
                # socket.inet_aton(status['ip_addr'])
            except Exception:
                # logger.debug(traceback.format_exc())
                logger.debug('Ignoring node %s', addr[0])
                raise StopIteration
            auth = auth_code(self.secret, info['sign'])
            node = self._nodes.get(info['ip_addr'], None)
            if node:
                if node.auth == auth:
                    raise StopIteration
            sock = AsyncSocket(socket.socket(sock_family, socket.SOCK_STREAM),
                               keyfile=self.keyfile, certfile=self.certfile)
            sock.settimeout(MsgTimeout)
            msg = {'version': _dispy_version, 'port': self.port, 'sign': self.sign,
                   'node_ip_addr': info['ip_addr']}
            msg['ip_addrs'] = [addrinfo.ext_ip_addr for addrinfo in self.addrinfos.values()]
            try:
                yield sock.connect((info['ip_addr'], info['port']))
                yield sock.sendall(auth)
                yield sock.send_msg('PING:' + serialize(msg))
            except Exception:
                logger.debug(traceback.format_exc())
            finally:
                sock.close()

        elif msg.startswith('FILEXFER:'):
            try:
                xf = deserialize(msg[len('FILEXFER:'):])
                msg = yield conn.recv_msg()
                job_reply = deserialize(msg)
            except Exception:
                logger.debug(traceback.format_exc())
            else:
                yield self.file_xfer_process(job_reply, xf, conn, addr)
            conn.close()

        elif msg.startswith('NODE_CPUS:'):
            conn.close()
            try:
                info = deserialize(msg[len('NODE_CPUS:'):])
                node = self._nodes.get(info['ip_addr'], None)
                if not node:
                    raise StopIteration
                auth = auth_code(self.secret, info['sign'])
                if auth != node.auth:
                    logger.warning('Invalid signature from %s', node.ip_addr)
                    raise StopIteration
                cpus = info['cpus']
            except Exception:
                raise StopIteration
            if cpus < 0:
                logger.warning('Node requested using %s CPUs, disabling it',
                               node.ip_addr, cpus)
                cpus = 0
            logger.debug('Setting cpus for %s to %s', node.ip_addr, cpus)
            # TODO: set node.cpus to min(cpus, node.cpus)?
            node.cpus = cpus
            if cpus > node.avail_cpus:
                node.avail_cpus = cpus
                node_computations = []
                for cluster in self._clusters.itervalues():
                    if cluster in node.clusters:
                        continue
                    compute = cluster._compute
                    for node_alloc in cluster._node_allocs:
                        cpus = node_alloc.allocate(cluster, node.ip_addr, node.name,
                                                   node.avail_cpus, avail_info=node.avail_info,
                                                   platform=node.platform)
                        if cpus <= 0:
                            continue
                        node.cpus = min(node.avail_cpus, cpus)
                        node_computations.append(compute)
                        break
                if node_computations:
                    Task(self.setup_node, node, node_computations)
                yield self._sched_event.set()
            else:
                node.avail_cpus = cpus
            for cluster in node.clusters:
                dispy_node = cluster._dispy_nodes.get(node.ip_addr, None)
                if dispy_node:
                    dispy_node.cpus = cpus

        elif msg.startswith('TERMINATED:'):
            conn.close()
            try:
                info = deserialize(msg[len('TERMINATED:'):])
            except Exception:
                # logger.debug(traceback.format_exc())
                pass
            else:
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
                    clusters = list(node.clusters)
                    node.clusters = set()
                    for cluster in clusters:
                        dispy_node = cluster._dispy_nodes.pop(node.ip_addr, None)
                        if not dispy_node:
                            continue
                        dispy_node.avail_cpus = dispy_node.cpus = dispy_node.busy = 0
                        if cluster.status_callback:
                            self.worker_Q.put((cluster.status_callback,
                                               (DispyNode.Closed, dispy_node, None)))
                    self.reschedule_jobs(dead_jobs)

        elif msg.startswith('NODE_STATUS:'):
            conn.close()
            # this message is from dispyscheduler for SharedJobCluster
            try:
                info = deserialize(msg[len('NODE_STATUS:'):])
                cluster = self._clusters[info['compute_id']]
                assert info['auth'] == cluster._compute.auth
            except Exception:
                logger.debug('Invalid node status from %s:%s ignored', addr[0], addr[1])
                # logger.debug(traceback.format_exc())
            else:
                if info['status'] == DispyNode.AvailInfo:
                    dispy_node = cluster._dispy_nodes.get(info['ip_addr'], None)
                    if dispy_node:
                        dispy_node.avail_info = info['avail_info']
                        if cluster.status_callback:
                            self.worker_Q.put((cluster.status_callback,
                                               (DispyNode.AvailInfo, dispy_node, None)))
                elif info['status'] == DispyNode.Initialized:
                    dispy_node = info['dispy_node']
                    dispy_node.update_time = time.time()
                    node = self._nodes.get(dispy_node.ip_addr, None)
                    if node:
                        node.name = dispy_node.name
                        node.cpus = dispy_node.cpus
                    else:
                        node = _Node(dispy_node.ip_addr, 0, dispy_node.cpus, '', '', platform='')
                        node.name = dispy_node.name
                        self._nodes[node.ip_addr] = node
                    cluster._dispy_nodes[dispy_node.ip_addr] = dispy_node
                    if cluster.status_callback:
                        self.worker_Q.put((cluster.status_callback,
                                           (DispyNode.Initialized, dispy_node, None)))
                elif info['status'] == DispyNode.Closed:
                    dispy_node = cluster._dispy_nodes.get(info['ip_addr'], None)
                    if dispy_node:
                        dispy_node.avail_cpus = dispy_node.cpus = 0
                        if cluster.status_callback:
                            self.worker_Q.put((cluster.status_callback,
                                               (DispyNode.Closed, dispy_node, None)))
                else:
                    logger.warning('Invalid node status %s from %s:%s ignored',
                                   info['status'], addr[0], addr[1])

        elif msg.startswith('SCHEDULED:'):
            try:
                info = deserialize(msg[len('SCHEDULED:'):])
                assert self.shared
                cluster = self._clusters.get(info['compute_id'], None)
                assert info['pulse_interval'] is None or info['pulse_interval'] >= 1
                self.pulse_interval = info['pulse_interval']
                self.timer_task.resume(True)
                yield conn.send_msg('ACK')
                cluster._scheduled_event.set()
            except Exception:
                yield conn.send_msg('NAK')
            conn.close()

        elif msg.startswith('RELAY_INFO:'):
            try:
                info = deserialize(msg[len('RELAY_INFO:'):])
                assert info['version'] == _dispy_version
                msg = {'sign': self.sign, 'ip_addrs': [info['scheduler_ip_addr']],
                       'port': self.port}
                if 'auth' in info and info['auth'] != self.auth:
                    msg = None
            except Exception:
                msg = None
            yield conn.send_msg(serialize(msg))
            conn.close()

        else:
            logger.warning('Invalid message from %s:%s ignored', addr[0], addr[1])
            # logger.debug(traceback.format_exc())
            conn.close()

    def timer_proc(self, task=None):
        task.set_daemon()
        reset = True
        last_pulse_time = last_ping_time = last_poll_time = time.time()
        timeout = None
        while 1:
            if reset:
                timeout = num_min(self.pulse_interval, self.ping_interval, self.poll_interval)

            try:
                reset = yield task.suspend(timeout)
            except GeneratorExit:
                break
            if reset:
                continue

            now = time.time()
            if self.pulse_interval and (now - last_pulse_time) >= self.pulse_interval:
                last_pulse_time = now
                if self.shared:
                    clusters = self._clusters.values()
                    for cluster in clusters:
                        msg = {'client_ip_addr': cluster._compute.scheduler_ip_addr,
                               'client_port': cluster._compute.job_result_port}
                        sock = socket.socket(cluster.addrinfo.family, socket.SOCK_STREAM)
                        sock = AsyncSocket(sock, keyfile=self.keyfile, certfile=self.certfile)
                        sock.settimeout(MsgTimeout)
                        try:
                            yield sock.connect((cluster.scheduler_ip_addr, cluster.scheduler_port))
                            yield sock.sendall(cluster._scheduler_auth)
                            yield sock.send_msg('PULSE:' + serialize(msg))
                        except Exception:
                            pass
                        sock.close()
                else:
                    dead_nodes = {}
                    for node in self._nodes.itervalues():
                        if node.busy and node.last_pulse is not None and \
                           (node.last_pulse + (5 * self.pulse_interval)) <= now:
                            logger.warning('Node %s is not responding; removing it (%s, %s, %s)',
                                           node.ip_addr, node.busy, node.last_pulse, now)
                            dead_nodes[node.ip_addr] = node
                    if dead_nodes:
                        for node in dead_nodes.itervalues():
                            clusters = list(node.clusters)
                            node.clusters = set()
                            for cluster in clusters:
                                dispy_node = cluster._dispy_nodes.pop(node.ip_addr, None)
                                if not dispy_node:
                                    continue
                                dispy_node.avail_cpus = dispy_node.cpus = dispy_node.busy = 0
                                if cluster.status_callback:
                                    self.worker_Q.put((cluster.status_callback,
                                                       (DispyNode.Closed, dispy_node, None)))
                            del self._nodes[node.ip_addr]
                        dead_jobs = [_job for _job in self._sched_jobs.itervalues()
                                     if _job.node is not None and _job.node.ip_addr in dead_nodes]
                        self.reschedule_jobs(dead_jobs)

            if self.ping_interval and (now - last_ping_time) >= self.ping_interval:
                last_ping_time = now
                for cluster in self._clusters.itervalues():
                    Task(self.discover_nodes, cluster, cluster._node_allocs)

            if self.poll_interval and (now - last_poll_time) >= self.poll_interval:
                last_poll_time = now
                for cluster in self._clusters.itervalues():
                    Task(self.poll_job_results, cluster)

    def file_xfer_process(self, job_reply, xf, sock, addr):
        _job = self._sched_jobs.get(job_reply.uid, None)
        if _job is None or _job.hash != job_reply.hash:
            logger.warning('Ignoring invalid file transfer from job %s at %s',
                           job_reply.uid, addr[0])
            yield sock.send_msg(serialize(-1))
            raise StopIteration
        node = self._nodes.get(job_reply.ip_addr, None)
        if node:
            node.last_pulse = time.time()
        tgt = os.path.join(self.dest_path, xf.dest_path.replace(xf.sep, os.sep),
                           xf.name.split(xf.sep)[-1])
        if not os.path.isdir(os.path.dirname(tgt)):
            os.makedirs(os.path.dirname(tgt))
        with open(tgt, 'wb') as fd:
            recvd = 0
            while recvd < xf.stat_buf.st_size:
                yield sock.send_msg(serialize(recvd))
                data = yield sock.recvall(min(xf.stat_buf.st_size-recvd, 1024000))
                if not data:
                    break
                fd.write(data)
                recvd += len(data)
            yield sock.send_msg(serialize(recvd))
        if recvd != xf.stat_buf.st_size:
            logger.warning('Transfer of file "%s" failed', tgt)
            # TODO: remove file?
        os.utime(tgt, (xf.stat_buf.st_atime, xf.stat_buf.st_mtime))
        os.chmod(tgt, stat.S_IMODE(xf.stat_buf.st_mode))

    def send_ping_node(self, ip_addr, port=None, task=None):
        ping_msg = {'version': _dispy_version, 'sign': self.sign, 'port': self.port,
                    'node_ip_addr': ip_addr}
        ping_msg['ip_addrs'] = [addrinfo.ext_ip_addr for addrinfo in self.addrinfos.values()]
        if not port:
            port = self.node_port
        if re.match('\d+\.', ip_addr):
            sock_family = socket.AF_INET
        else:
            sock_family = socket.AF_INET6
        tcp_sock = AsyncSocket(socket.socket(sock_family, socket.SOCK_STREAM),
                               keyfile=self.keyfile, certfile=self.certfile)
        tcp_sock.settimeout(MsgTimeout)
        try:
            yield tcp_sock.connect((ip_addr, int(port)))
            yield tcp_sock.sendall('x' * len(self.auth))
            yield tcp_sock.send_msg('PING:' + serialize(ping_msg))
        except Exception:
            pass
        tcp_sock.close()

    def broadcast_ping(self, addrinfos=[], port=None, task=None):
        # generator
        if not port:
            port = self.node_port
        ping_msg = {'version': _dispy_version, 'sign': self.sign, 'port': self.port}
        ping_msg['ip_addrs'] = [addrinfo.ext_ip_addr for addrinfo in self.addrinfos.values()]
        if not addrinfos:
            addrinfos = self.addrinfos.values()
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
                yield bc_sock.sendto('PING:' + serialize(ping_msg), (addrinfo.broadcast, port))
            except Exception:
                pass
            bc_sock.close()

    def discover_nodes(self, cluster, node_allocs, task=None):
        for node_alloc in node_allocs:
            # TODO: we assume subnets are indicated by '*', instead of
            # subnet mask; this is a limitation, but specifying with
            # subnet mask a bit cumbersome.
            if node_alloc.ip_rex.find('*') >= 0:
                Task(self.broadcast_ping, port=node_alloc.port)
            else:
                ip_addr = node_alloc.ip_addr
                if ip_addr in self._nodes:
                    continue
                port = node_alloc.port
                Task(self.send_ping_node, ip_addr, port)
        yield 0

    def poll_job_results(self, cluster, task=None):
        # generator
        for ip_addr in cluster._dispy_nodes:
            node = self._nodes.get(ip_addr, None)
            if not node or not node.port:
                continue
            sock = AsyncSocket(socket.socket(node.sock_family, socket.SOCK_STREAM),
                               keyfile=self.keyfile, certfile=self.certfile)
            sock.settimeout(MsgTimeout)
            try:
                req = {'compute_id': cluster._compute.id, 'auth': cluster._compute.auth}
                reply = yield node.send('PENDING_JOBS:' + serialize(req))
                reply = deserialize(reply)
            except Exception:
                logger.debug(traceback.format_exc())
                continue
            finally:
                sock.close()

            for uid in reply['done']:
                _job = self._sched_jobs.get(uid, None)
                if _job is None:
                    continue
                conn = AsyncSocket(socket.socket(node.sock_family, socket.SOCK_STREAM),
                                   keyfile=self.keyfile, certfile=self.certfile)
                conn.settimeout(MsgTimeout)
                try:
                    yield conn.connect((node.ip_addr, node.port))
                    req = {'compute_id': cluster._compute.id, 'auth': cluster._compute.auth,
                           'uid': uid, 'hash': _job.hash}
                    yield conn.sendall(node.auth)
                    yield conn.send_msg('RETRIEVE_JOB:' + serialize(req))
                    reply = yield conn.recv_msg()
                    reply = deserialize(reply)
                except Exception:
                    logger.debug(traceback.format_exc())
                    continue
                else:
                    if isinstance(reply, _JobReply):
                        yield self.job_reply_process(reply, conn, (node.ip_addr, node.port))
                    else:
                        logger.debug('Invalid reply for %s', uid)
                finally:
                    conn.close()

    def add_cluster(self, cluster, task=None):
        compute = cluster._compute
        if self.shared:
            self._clusters[compute.id] = cluster
            for xf in compute.xfer_files:
                xf.compute_id = compute.id

            node = _Node(cluster.scheduler_ip_addr, cluster.scheduler_port, 0, '', '',
                         platform='', keyfile=self.keyfile, certfile=self.certfile)
            node.auth = cluster._scheduler_auth
            self._nodes[cluster.scheduler_ip_addr] = node
            dispy_node = DispyNode(cluster.scheduler_ip_addr, None, 0)
            dispy_node.avail_info = node.avail_info
            cluster._dispy_nodes[dispy_node.ip_addr] = dispy_node
            info = self.shelf['_cluster']
            info['port'] = self.port
            self.shelf['_cluster'] = info
            info = {'name': compute.name, 'auth': compute.auth,
                    'nodes': [cluster.scheduler_ip_addr]}
            self.shelf['compute_%s' % compute.id] = info
            info = {'port': cluster.scheduler_port, 'auth': cluster._scheduler_auth,
                    'scheduler': True}
            self.shelf['node_%s' % (cluster.scheduler_ip_addr)] = info
            self.shelf.sync()
            if cluster.poll_interval:
                self.poll_interval = num_min(self.poll_interval, cluster.poll_interval)
            if self.poll_interval:
                self.timer_task.resume(True)
            raise StopIteration

        # if a node is added with 'allocate_node', compute is already
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
                self.timer_task.resume(True)

        Task(self.discover_nodes, cluster, cluster._node_allocs)
        compute_nodes = []
        for ip_addr, node in self._nodes.iteritems():
            if cluster in node.clusters:
                continue
            for node_alloc in cluster._node_allocs:
                cpus = node_alloc.allocate(cluster, node.ip_addr, node.name, node.avail_cpus,
                                           avail_info=node.avail_info, platform=node.platform)
                if cpus <= 0:
                    continue
                node.cpus = min(node.avail_cpus, cpus)
                compute_nodes.append(node)
        for node in compute_nodes:
            Task(self.setup_node, node, [compute])
        yield None

    def del_cluster(self, cluster, task=None):
        # generator
        if self._clusters.pop(cluster._compute.id, None) != cluster:
            logger.warning('Cluster %s already closed?', cluster._compute.name)
            raise StopIteration

        if self.shared:
            sock = socket.socket(cluster.addrinfo.family, socket.SOCK_STREAM)
            sock = AsyncSocket(sock, keyfile=self.keyfile, certfile=self.certfile)
            sock.settimeout(MsgTimeout)
            yield sock.connect((cluster.scheduler_ip_addr, cluster.scheduler_port))
            yield sock.sendall(cluster._scheduler_auth)
            req = {'compute_id': cluster._compute.id, 'auth': cluster._compute.auth,
                   'terminate_pending': cluster._complete.is_set()}
            yield sock.send_msg('CLOSE:' + serialize(req))
            sock.close()
        else:
            cid = cluster._compute.id
            cluster._jobs = []
            cluster._pending_jobs = 0
            # remove cluster from all nodes before closing (which uses
            # yield); otherwise, scheduler may access removed cluster
            # through node.clusters
            close_nodes = []
            for dispy_node in cluster._dispy_nodes.itervalues():
                node = self._nodes.get(dispy_node.ip_addr, None)
                if not node:
                    continue
                if not cluster._complete.is_set():
                    drop_jobs = [i for i, _job in enumerate(node.pending_jobs)
                                 if _job.compute_id == cid]
                    for i in reversed(drop_jobs):
                        node.pending_jobs.remove(i)
                node.clusters.discard(cluster)
                close_nodes.append((Task(node.close, cluster._compute,
                                         terminate_pending=cluster._complete.is_set()),
                                    dispy_node))
            cluster._dispy_nodes.clear()
            for close_task, dispy_node in close_nodes:
                yield close_task.finish()
                dispy_node.update_time = time.time()
                if cluster.status_callback:
                    self.worker_Q.put((cluster.status_callback,
                                       (DispyNode.Closed, dispy_node, None)))
        self.shelf.pop('compute_%s' % (cluster._compute.id), None)
        # TODO: prune nodes in shelf
        self.shelf.sync()

    def setup_node(self, node, computations, task=None):
        # generator
        task.set_daemon()
        for compute in computations:
            # NB: to avoid computation being sent multiple times, we
            # add to cluster's _dispy_nodes before sending computation
            # to node
            cluster = self._clusters.get(compute.id, None)
            if not cluster or node.ip_addr in cluster._dispy_nodes:
                continue
            dispy_node = DispyNode(node.ip_addr, node.name, node.cpus)
            dispy_node.avail_cpus = node.avail_cpus
            dispy_node.avail_info = node.avail_info
            cluster._dispy_nodes[node.ip_addr] = dispy_node
            self.shelf['node_%s' % (node.ip_addr)] = {'port': node.port, 'auth': node.auth}
            shelf_compute = self.shelf['compute_%s' % compute.id]
            shelf_compute['nodes'].append(node.ip_addr)
            self.shelf['compute_%s' % compute.id] = shelf_compute
            self.shelf.sync()
            r = yield node.setup(compute, exclusive=True, task=task)
            if r or compute.id not in self._clusters:
                cluster._dispy_nodes.pop(node.ip_addr, None)
                logger.warning('Failed to setup %s for compute "%s": %s',
                               node.ip_addr, compute.name, r)
                # TODO: delete node from shelf's cluster._dispy_nodes
                del self.shelf['node_%s' % (node.ip_addr)]
                self.shelf.sync()
                yield node.close(compute, task=task)
            else:
                dispy_node.update_time = time.time()
                node.clusters.add(cluster)
                self._sched_event.set()
                if cluster.status_callback:
                    self.worker_Q.put((cluster.status_callback,
                                       (DispyNode.Initialized, dispy_node, None)))

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
                         self.secret, platform=info['platform'],
                         keyfile=self.keyfile, certfile=self.certfile)
            node.name = info['name']
            node.avail_info = info['avail_info']
            self._nodes[node.ip_addr] = node
        else:
            node.last_pulse = time.time()
            auth = auth_code(self.secret, info['sign'])
            if info['cpus'] > 0:
                node.avail_cpus = info['cpus']
                node.cpus = min(node.cpus, node.avail_cpus)
                for cluster in node.clusters:
                    dispy_node = cluster._dispy_nodes.get(node.ip_addr, None)
                    if dispy_node:
                        dispy_node.avail_cpus = node.avail_cpus
                        dispy_node.cpus = node.cpus
            else:
                logger.warning('Invalid "cpus" %s from %s ignored', info['cpus'], info['ip_addr'])
            if node.port == info['port'] and node.auth == auth:
                return
            logger.debug('Node %s rediscovered', info['ip_addr'])
            node.port = info['port']
            if node.auth is not None:
                dead_jobs = [_job for _job in self._sched_jobs.itervalues()
                             if _job.node is not None and _job.node.ip_addr == node.ip_addr]
                self.reschedule_jobs(dead_jobs)
                node.busy = 0
                node.auth = auth
                clusters = list(node.clusters)
                node.clusters = set()
                for cluster in clusters:
                    dispy_node = cluster._dispy_nodes.pop(node.ip_addr, None)
                    if dispy_node and cluster.status_callback:
                        self.worker_Q.put((cluster.status_callback,
                                           (DispyNode.Closed, dispy_node, None)))
            node.auth = auth
        node_computations = []
        node.name = info['name']
        node.scheduler_ip_addr = info['scheduler_ip_addr']
        for cluster in self._clusters.itervalues():
            if cluster in node.clusters:
                continue
            compute = cluster._compute
            for node_alloc in cluster._node_allocs:
                cpus = node_alloc.allocate(cluster, node.ip_addr, node.name, node.avail_cpus,
                                           avail_info=node.avail_info, platform=node.platform)
                if cpus <= 0:
                    continue
                node.cpus = min(node.avail_cpus, cpus)
                node_computations.append(compute)
                break
        if node_computations:
            Task(self.setup_node, node, node_computations)

    def worker(self):
        # used for user callbacks only
        while 1:
            item = self.worker_Q.get(block=True)
            if item is None:
                self.worker_Q.task_done()
                break
            func, args = item
            try:
                func(*args)
            except Exception:
                logger.debug('Running %s failed: %s', func.func_name, traceback.format_exc())
            self.worker_Q.task_done()

    def finish_job(self, cluster, _job, status):
        # assert status in (DispyJob.Finished, DispyJob.Terminated, DispyJob.Abandoned)
        job = _job.job
        _job.finish(status)
        if cluster.callback:
            self.worker_Q.put((cluster.callback, (job,)))
        if status != DispyJob.ProvisionalResult:
            # assert cluster._pending_jobs > 0
            cluster._pending_jobs -= 1
            if cluster._pending_jobs == 0:
                cluster.end_time = time.time()
                cluster._complete.set()

    def job_reply_process(self, reply, sock, addr):
        _job = self._sched_jobs.pop(reply.uid, None)
        if _job:
            if reply.hash != _job.hash:
                self._sched_jobs[reply.uid] = _job
                logger.warning('Ignoring invalid reply for job %s from %s', reply.uid, addr[0])
                yield sock.send_msg('NAK'.encode())
                raise StopIteration
        else:
            _job = self._abandoned_jobs.pop(reply.uid, None)
            if _job:
                if reply.hash != _job.hash:
                    self._abandoned_jobs[reply.uid] = _job
                    logger.warning('Ignoring invalid reply for job %s from %s', reply.uid, addr[0])
                    yield sock.send_msg('NAK'.encode())
                    raise StopIteration
            else:
                logger.warning('Ignoring invalid reply for job %s from %s', reply.uid, addr[0])
                yield sock.send_msg('NAK'.encode())
                raise StopIteration

        job = _job.job
        job.ip_addr = reply.ip_addr
        node = self._nodes.get(reply.ip_addr, None)
        cluster = self._clusters.get(_job.compute_id, None)
        if not cluster:
            # job cancelled while/after closing computation
            if node and node.busy > 0:
                node.busy -= 1
                node.cpu_time += reply.end_time - reply.start_time
                node.last_pulse = time.time()
                self._sched_event.set()
            yield sock.send_msg('ACK')
            raise StopIteration
        if node:
            node.last_pulse = time.time()
        else:
            if self.shared:
                node = _Node(reply.ip_addr, 0, getattr(reply, 'cpus', 0), '', self.secret,
                             platform='', keyfile=None, certfile=None)
                self._nodes[reply.ip_addr] = node
                dispy_node = DispyNode(node.ip_addr, node.name, node.cpus)
                dispy_node.update_time = time.time()
                cluster._dispy_nodes[reply.ip_addr] = dispy_node
                if cluster.status_callback:
                    self.worker_Q.put((cluster.status_callback,
                                       (DispyNode.Initialized, dispy_node, None)))
            elif job.status == DispyJob.Abandoned:
                pass
            else:
                logger.warning('Invalid job reply? %s: %s', job.id, job.status)

        job.result = deserialize(reply.result)
        job.start_time = reply.start_time
        job.status = reply.status
        logger.debug('Received reply for job %s / %s from %s', job.id, _job.uid, job.ip_addr)
        if reply.status == DispyJob.ProvisionalResult:
            self._sched_jobs[_job.uid] = _job
            if cluster.callback:
                self.worker_Q.put((cluster.callback, (job,)))
        else:
            dispy_node = cluster._dispy_nodes.get(job.ip_addr, None)
            if node and dispy_node:
                if reply.status == DispyJob.Finished or reply.status == DispyJob.Terminated:
                    node.busy -= 1
                    node.cpu_time += reply.end_time - reply.start_time
                    dispy_node.busy -= 1
                    dispy_node.cpu_time += reply.end_time - reply.start_time
                    dispy_node.jobs_done += 1
                    dispy_node.update_time = time.time()
                elif reply.status == DispyJob.Cancelled:
                    assert self.shared is True
                else:
                    logger.warning('Invalid reply status: %s for job %s', reply.status, job.id)
            elif job.status == DispyJob.Abandoned:
                pass
            else:
                logger.warning('Invalid job reply (status)? %s: %s', job.id, job.status)

            job.stdout = reply.stdout
            job.stderr = reply.stderr
            job.exception = reply.exception
            job.end_time = reply.end_time
            self.finish_job(cluster, _job, reply.status)
            if cluster.status_callback:
                self.worker_Q.put((cluster.status_callback, (reply.status, dispy_node, job)))
            self._sched_event.set()
        yield sock.send_msg('ACK')

    def reschedule_jobs(self, dead_jobs):
        if not dead_jobs:
            return
        for _job in dead_jobs:
            cluster = self._clusters[_job.compute_id]
            del self._sched_jobs[_job.uid]
            dispy_node = cluster._dispy_nodes.get(_job.node.ip_addr, None)
            if dispy_node:
                dispy_node.cpus = 0
                dispy_node.busy = 0
                dispy_node.update_time = time.time()
            if cluster._compute.reentrant and not _job.pinned:
                logger.debug('Rescheduling job %s from %s', _job.uid, _job.node.ip_addr)
                dispy_job = _job.job
                dispy_job.status = DispyJob.Created
                dispy_job.ip_addr = None
                _job.node = None
                # TODO: call 'status_callback'?
                # _job.hash = os.urandom(10).encode('hex')
                cluster._jobs.append(_job)
            else:
                dispy_job = _job.job
                logger.debug('Job %s scheduled on %s abandoned', dispy_job.id, _job.node.ip_addr)
                dispy_job.status = DispyJob.Abandoned
                self._abandoned_jobs[_job.uid] = _job
                cluster._pending_jobs -= 1
                if cluster._pending_jobs == 0:
                    cluster.end_time = time.time()
                    cluster._complete.set()

            if cluster.status_callback:
                self.worker_Q.put((cluster.status_callback,
                                   (DispyJob.Abandoned, dispy_node, dispy_job)))
        self._sched_event.set()

    def run_job(self, _job, cluster, task=None):
        # generator
        node = _job.node
        dispy_node = cluster._dispy_nodes[node.ip_addr]
        try:
            yield _job.run(task=task)
        except EnvironmentError:
            logger.warning('Failed to run job %s on %s for computation %s; removing this node',
                           _job.uid, node.ip_addr, cluster._compute.name)
            logger.debug(traceback.format_exc())
            # TODO: remove the node from all clusters and globally?
            # this job might have been deleted already due to timeout
            node.clusters.discard(cluster)
            if node.pending_jobs:
                for njob in node.pending_jobs:
                    if njob.compute_id == cluster._compute.id:
                        dispy_job = njob.job
                        self.finish_job(cluster, njob, DispyJob.Cancelled)
                        if cluster.status_callback and dispy_node:
                            dispy_node.update_time = time.time()
                            self.worker_Q.put((cluster.status_callback,
                                               (DispyJob.Cancelled, dispy_node, dispy_job)))
                node.pending_jobs = [njob for njob in node.pending_jobs
                                     if njob.compute_id != cluster._compute.id]
            if self._sched_jobs.pop(_job.uid, None) == _job:
                if not _job.pinned:
                    cluster._jobs.insert(0, _job)
                node.busy -= 1
            self._sched_event.set()
        except Exception:
            logger.warning('Failed to run job %s on %s for computation %s',
                           _job.uid, node.ip_addr, cluster._compute.name)
            logger.debug(traceback.format_exc())
            # TODO: delay executing again for some time?
            # this job might have been deleted already due to timeout
            if self._sched_jobs.pop(_job.uid, None) == _job:
                dispy_job = _job.job
                self.finish_job(cluster, _job, DispyJob.Cancelled)
                if cluster.status_callback and dispy_node:
                    dispy_node.update_time = time.time()
                    self.worker_Q.put((cluster.status_callback,
                                       (DispyJob.Cancelled, dispy_node, dispy_job)))
                node.busy -= 1
            self._sched_event.set()
        else:
            # job may have already finished (in which case _job.job would be None)
            if _job.job:
                _job.job.ip_addr = node.ip_addr
                logger.debug('Running job %s / %s on %s (busy: %d / %d)',
                             _job.job.id, _job.uid, node.ip_addr, node.busy, node.cpus)
                _job.job.status = DispyJob.Running
                _job.job.start_time = time.time()
                dispy_node.busy += 1
                dispy_node.update_time = time.time()
                if cluster.status_callback:
                    self.worker_Q.put((cluster.status_callback,
                                       (DispyJob.Running, dispy_node, _job.job)))
        if (not cluster._compute.reentrant) and (not cluster.status_callback) and _job.job:
            _job.job._args = ()
            _job.job._kwargs = {}

    def wait(self, cluster, timeout):
        ret = cluster._complete.wait(timeout=timeout)
        if ret or not self._abandoned_jobs:
            return ret
        cid = cluster._compute.id
        for _job in self._abandoned_jobs.itervalues():
            if _job.compute_id == cid:
                _job.finish(DispyJob.Abandoned)
        self._abandoned_jobs = {uid: _job for uid, _job in self._abandoned_jobs.iteritems()
                                if _job.compute_id != cid}
        return 0

    def load_balance_schedule(self):
        host = None
        load = 1.0
        for node in self._nodes.itervalues():
            if node.busy >= node.cpus:
                continue
            if node.pending_jobs:
                host = node
                break
            if not any(cluster._jobs for cluster in node.clusters):
                continue
            if (node.busy / node.cpus) < load:
                load = node.busy / node.cpus
                host = node
        return host

    def _schedule_jobs(self, task=None):
        # generator
        while not self.terminate:
            # n = sum(len(cluster._jobs) for cluster in self._clusters.itervalues())
            node = self.select_job_node()
            if not node:
                self._sched_event.clear()
                yield self._sched_event.wait()
                continue
            if node.pending_jobs:
                _job = node.pending_jobs.pop(0)
                cluster = self._clusters[_job.compute_id]
            else:
                # TODO: strategy to pick a cluster?
                for cluster in node.clusters:
                    # assert node.ip_addr in cluster._dispy_nodes
                    if cluster._jobs:
                        _job = cluster._jobs.pop(0)
                        break
                else:
                    self._sched_event.clear()
                    yield self._sched_event.wait()
                    continue
            _job.node = node
            # assert node.busy < node.cpus
            self._sched_jobs[_job.uid] = _job
            node.busy += 1
            Task(self.run_job, _job, cluster)

        logger.debug('Scheduler quitting: %s', len(self._sched_jobs))
        self._sched_jobs = {}
        for udp_task in self.udp_tasks:
            udp_task.terminate()
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
                dispy_job = _job.job
                self.finish_job(cluster, _job, status)
                if cluster.status_callback:
                    dispy_node = cluster._dispy_nodes.get(_job.node.ip_addr, None)
                    if dispy_node:
                        dispy_node.update_time = time.time()
                        self.worker_Q.put((cluster.status_callback,
                                           (status, dispy_node, dispy_job)))
            for dispy_node in cluster._dispy_nodes.itervalues():
                node = self._nodes.get(dispy_node.ip_addr, None)
                if not node:
                    continue
                for _job in node.pending_jobs:
                    # TODO: delete only jobs for this cluster?
                    if _job.job.status == DispyJob.Running:
                        status = DispyJob.Terminated
                    else:
                        status = DispyJob.Cancelled
                    dispy_job = _job.job
                    self.finish_job(cluster, _job, status)
                    if cluster.status_callback:
                        dispy_node.update_time = time.time()
                        self.worker_Q.put((cluster.status_callback,
                                           (status, dispy_node, dispy_job)))
                node.pending_jobs = []
            cluster._jobs = []
            cluster._pending_jobs = 0
            yield self.del_cluster(cluster, task=task)
        self._clusters = {}
        self._nodes = {}
        logger.debug('Scheduler quit')

    def submit_job(self, _job, ip_addr=None, task=None):
        # generator
        _job.uid = id(_job)
        _job.job.id = next(self._job_id)
        cluster = self._clusters[_job.compute_id]
        if ip_addr:
            node = self._nodes.get(ip_addr, None)
            if not node or cluster not in node.clusters:
                raise StopIteration(-1)
            node.pending_jobs.append(_job)
            _job.pinned = node
        else:
            cluster._jobs.append(_job)
        cluster._pending_jobs += 1
        cluster._complete.clear()
        if cluster.status_callback:
            self.worker_Q.put((cluster.status_callback, (DispyJob.Created, None, _job.job)))
        self._sched_event.set()
        yield 0

    def cancel_job(self, job, task=None):
        # generator
        _job = job._dispy_job_
        if _job is None:
            logger.warning('Job %s is invalid for cancellation!', job.id)
            raise StopIteration(-1)
        cluster = self._clusters.get(_job.compute_id, None)
        if not cluster:
            logger.warning('Invalid job %s for cluster "%s"!', job.id, cluster._compute.name)
            raise StopIteration(-1)
        # assert cluster._pending_jobs >= 1
        if _job.job.status == DispyJob.Created:
            if _job.pinned:
                _job.pinned.pending_jobs.remove(_job)
            else:
                cluster._jobs.remove(_job)
            dispy_job = _job.job
            self.finish_job(cluster, _job, DispyJob.Cancelled)
            if cluster.status_callback:
                self.worker_Q.put((cluster.status_callback, (DispyJob.Cancelled, None, dispy_job)))
            logger.debug('Cancelled (removed) job %s', job.id)
            raise StopIteration(0)
        elif not (_job.job.status == DispyJob.Running or
                  _job.job.status == DispyJob.ProvisionalResult or _job.node is None):
            logger.warning('Job %s is not valid for cancel (%s)', job.id, _job.job.status)
            raise StopIteration(-1)
        _job.job.status = DispyJob.Cancelled
        # don't send this status - when job is terminated status/callback get called
        logger.debug('Job %s / %s is being terminated', _job.job.id, _job.uid)
        resp = yield _job.node.send('TERMINATE_JOB:' + serialize(_job), reply=False, task=task)
        if resp != 0:
            logger.debug('Terminating job %s / %s failed: %s', _job.job.id, _job.uid, resp)
            resp = -1
        raise StopIteration(resp)

    def allocate_node(self, cluster, node_allocs, task=None):
        # generator
        for i in range(len(node_allocs)-1, -1, -1):
            node = self._nodes.get(node_allocs[i].ip_addr, None)
            if node:
                dispy_node = cluster._dispy_nodes.get(node.ip_addr, None)
                if dispy_node:
                    node.clusters.add(cluster)
                    self._sched_event.set()
                    del node_allocs[i]
                    continue
        if not node_allocs:
            raise StopIteration(0)
        cluster._node_allocs.extend(node_allocs)
        cluster._node_allocs = sorted(cluster._node_allocs,
                                      key=lambda node_alloc: node_alloc.ip_rex, reverse=True)
        present = set()
        cluster._node_allocs = [na for na in cluster._node_allocs
                                if na.ip_rex not in present and not present.add(na.ip_rex)]
        del present
        yield self.add_cluster(cluster, task=task)
        yield self._sched_event.set()
        raise StopIteration(0)

    def deallocate_node(self, cluster, node, task=None):
        # generator
        if isinstance(node, DispyNode):
            node = cluster._dispy_nodes.get(node.ip_addr, None)
        elif isinstance(node, str):
            node = cluster._dispy_nodes.get(_node_ipaddr(node), None)
        else:
            node = None
        if node:
            node = self._nodes.get(node.ip_addr, None)
        if not node:
            raise StopIteration(-1)
        node.clusters.discard(cluster)
        yield 0

    def close_node(self, cluster, node, terminate_pending, task=None):
        # generator
        if isinstance(node, DispyNode):
            node = cluster._dispy_nodes.get(node.ip_addr, None)
        elif isinstance(node, str):
            node = cluster._dispy_nodes.get(_node_ipaddr(node), None)
        else:
            node = None
        if node:
            node = self._nodes.get(node.ip_addr, None)
        if not node:
            raise StopIteration(-1)
        node.clusters.discard(cluster)
        jobs = [_job for _job in node.pending_jobs if _job.compute_id == cluster._compute.id]
        if cluster.status_callback:
            dispy_node = cluster._dispy_nodes.get(node.ip_addr, None)
            for _job in jobs:
                self.worker_Q.put((cluster.status_callback,
                                   (DispyJob.Cancelled, dispy_node, _job.job)))
        if jobs:
            node.pending_jobs = [_job for _job in node.pending_jobs
                                 if _job.compute_id != cluster._compute.id]
        yield node.close(cluster._compute, terminate_pending=terminate_pending)

    def set_node_cpus(self, cluster, node, cpus, task=None):
        # generator
        if isinstance(node, DispyNode):
            node = cluster._dispy_nodes.get(node.ip_addr, None)
        elif isinstance(node, str):
            node = cluster._dispy_nodes.get(_node_ipaddr(node), None)
        else:
            node = None
        if node:
            node = self._nodes.get(node.ip_addr, None)
        if not node:
            raise StopIteration(-1)
        try:
            cpus = int(cpus)
        except ValueError:
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
        yield self._sched_event.set()
        raise StopIteration(cpus)

    def send_file(self, cluster, node, xf, task=None):
        if isinstance(node, DispyNode):
            node = cluster._dispy_nodes.get(node.ip_addr, None)
        elif isinstance(node, str):
            node = cluster._dispy_nodes.get(_node_ipaddr(node), None)
        else:
            node = None
        if node:
            node = self._nodes.get(node.ip_addr, None)
        if not node:
            raise StopIteration(-1)
        yield node.xfer_file(xf)

    def node_jobs(self, cluster, node, from_node, task=None):
        # generator
        if isinstance(node, DispyNode):
            node = cluster._dispy_nodes.get(node.ip_addr, None)
        elif isinstance(node, str):
            node = cluster._dispy_nodes.get(_node_ipaddr(node), None)
        else:
            node = None
        if node:
            node = self._nodes.get(node.ip_addr, None)
        if not node:
            raise StopIteration(-1)
        if cluster not in node.clusters:
            raise StopIteration([])
        if from_node:
            sock = socket.socket(node.sock_family, socket.SOCK_STREAM)
            sock = AsyncSocket(sock, keyfile=self.keyfile, certfile=self.certfile)
            sock.settimeout(MsgTimeout)
            try:
                yield sock.connect((node.ip_addr, node.port))
                yield sock.sendall(node.auth)
                req = {'compute_id': cluster._compute.id, 'auth': cluster._compute.auth}
                yield sock.send_msg('JOBS:' + serialize(req))
                info = yield sock.recv_msg()
                _jobs = [self._sched_jobs.get(uid, None) for uid in deserialize(info)]
                jobs = [_job.job for _job in _jobs if _job]
            except Exception:
                logger.debug(traceback.format_exc())
                jobs = []
            sock.close()
        else:
            jobs = [_job.job for _job in self._sched_jobs.itervalues()
                    if _job.node == node and _job.compute_id == cluster._compute.id]

        raise StopIteration(jobs)

    def shutdown(self):
        # non-generator
        if not self.shared:
            if self.terminate:
                return
            if any(cluster._pending_jobs for cluster in self._clusters.itervalues()):
                return
            logger.debug('Shutting down scheduler ...')
            self.terminate = True

            def _terminate_scheduler(self, task=None):
                yield self._sched_event.set()

            Task(_terminate_scheduler, self).value()
            self.worker_Q.put(None)
            self._scheduler.value()
            self.worker_Q.join()
        if self.shelf:
            # TODO: need to check all clusters are deleted?
            self.shelf.close()
            self.shelf = None
            for ext in ('', '.db', '.bak', '.dat', '.dir'):
                if os.path.isfile(self.recover_file + ext):
                    try:
                        os.remove(self.recover_file + ext)
                    except Exception:
                        pass
        if self.pycos:
            self.pycos.finish()
            self.pycos = None
        Singleton.empty(self.__class__)


class JobCluster(object):
    """Create an instance of cluster for a specific computation.
    """

    def __init__(self, computation, nodes=None, depends=[], callback=None, cluster_status=None,
                 ip_addr=None, port=None, node_port=None, ext_ip_addr=None,
                 ipv4_udp_multicast=False, dest_path=None, loglevel=logger.INFO,
                 setup=None, cleanup=True, ping_interval=None, pulse_interval=None,
                 poll_interval=None, reentrant=False, secret='', keyfile=None, certfile=None,
                 recover_file=None):
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

        @loglevel indicates message logging level.

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
        pycos.logger.setLevel(loglevel)
        if reentrant is not True and reentrant is not False:
            logger.warning('Invalid value for reentrant (%s) is ignored; '
                           'it must be either True or False', reentrant)
            reentrant = False
        if ping_interval is not None:
            try:
                ping_interval = float(ping_interval)
                assert 1.0 <= ping_interval <= 1000
            except Exception:
                raise Exception('Invalid ping_interval; must be between 1 and 1000')
        self.ping_interval = ping_interval
        if pulse_interval is not None:
            try:
                pulse_interval = float(pulse_interval)
                assert 1.0 <= pulse_interval <= 1000
            except Exception:
                raise Exception('Invalid pulse_interval; must be between 1 and 1000')
        self.pulse_interval = pulse_interval

        if poll_interval is not None:
            try:
                poll_interval = float(poll_interval)
                assert 5.0 <= poll_interval <= 1000
            except Exception:
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
            except Exception:
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
            except Exception:
                raise Exception('Invalid cluster_status function; '
                                'it must take excatly 3 arguments')
        self.status_callback = cluster_status

        if hasattr(self, 'scheduler_ip_addr'):
            shared = True
            self._node_allocs = []
        else:
            shared = False
            if not nodes:
                nodes = ['*']
            elif not isinstance(nodes, list):
                if isinstance(nodes, str):
                    nodes = [nodes]
                else:
                    raise Exception('"nodes" must be list of IP addresses or host names')
            self._node_allocs = _parse_node_allocs(nodes)
            if not self._node_allocs:
                raise Exception('"nodes" argument is invalid')
            self._node_allocs = sorted(self._node_allocs,
                                       key=lambda node_alloc: node_alloc.ip_rex, reverse=True)
        self._dispy_nodes = {}

        if inspect.isfunction(computation) or inspect.ismethod(computation):
            func = computation
            compute = _Compute(_Compute.func_type, func.func_name)
            lines = inspect.getsourcelines(func)[0]
            lines[0] = lines[0].lstrip()
            compute.code = ''.join(lines)
            if inspect.ismethod(computation):
                depends.append(computation.__self__.__class__)
        elif isinstance(computation, str):
            compute = _Compute(_Compute.prog_type, computation)
            depends.append(computation)
        else:
            raise Exception('Invalid computation type: %s' % type(computation))

        if setup:
            if inspect.isfunction(setup):
                depends.append(setup)
                compute.setup = _Function(setup.func_name, (), {})
            elif isinstance(setup, functools.partial):
                depends.append(setup.func)
                if setup.args:
                    args = setup.args
                else:
                    args = ()
                if setup.keywords:
                    kwargs = setup.keywords
                else:
                    kwargs = {}
                compute.setup = _Function(setup.func.func_name, args, kwargs)
            else:
                raise Exception('"setup" must be Python (partial) function')

        if inspect.isfunction(cleanup):
            depends.append(cleanup)
            compute.cleanup = _Function(cleanup.func_name, (), {})
        elif isinstance(cleanup, functools.partial):
            depends.append(cleanup.func)
            if cleanup.args:
                args = cleanup.args
            else:
                args = ()
            if cleanup.keywords:
                kwargs = cleanup.keywords
            else:
                kwargs = {}
            compute.cleanup = _Function(cleanup.func.func_name, args, kwargs)
        elif isinstance(cleanup, bool):
            compute.cleanup = cleanup
        else:
            raise Exception('"cleanup" must be Python (partial) function')

        self._cluster = _Cluster(ip_addr=ip_addr, port=port, node_port=node_port,
                                 ext_ip_addr=ext_ip_addr, ipv4_udp_multicast=ipv4_udp_multicast,
                                 shared=shared, secret=secret, keyfile=keyfile, certfile=certfile,
                                 recover_file=recover_file)
        atexit.register(self.shutdown)

        depend_ids = {}
        cwd = self._cluster.dest_path
        for dep in depends:
            if isinstance(dep, basestring) or inspect.ismodule(dep):
                if inspect.ismodule(dep):
                    name = dep.__file__
                    if name.endswith('.pyc'):
                        name = name[:-1]
                    if not name.endswith('.py'):
                        logger.warning('Invalid module "%s" - must be python source.', dep)
                        continue
                    if name.startswith(cwd):
                        dst = os.path.dirname(name[len(cwd):].lstrip(os.sep))
                    elif dep.__package__:
                        dst = dep.__package__.replace('.', os.sep)
                    else:
                        dst = os.path.dirname(dep.__name__.replace('.', os.sep))
                else:
                    if os.path.isfile(dep):
                        name = os.path.abspath(dep)
                    elif compute.type == _Compute.prog_type:
                        for p in os.environ['PATH'].split(os.pathsep):
                            f = os.path.join(p, dep)
                            if os.path.isfile(f):
                                logger.debug('Assuming "%s" is program "%s"', dep, f)
                                name = f
                                break
                    else:
                        raise Exception('Path "%s" is not valid' % dep)
                    if name.startswith(cwd):
                        dst = os.path.dirname(name[len(cwd):].lstrip(os.sep))
                    else:
                        dst = '.'
                if name in depend_ids:
                    continue
                try:
                    with open(name, 'rb') as fd:
                        pass
                    xf = _XferFile(name, dst, compute.id)
                    compute.xfer_files.add(xf)
                    depend_ids[name] = dep
                except Exception:
                    raise Exception('File "%s" is not valid' % name)
            elif (inspect.isfunction(dep) or inspect.isclass(dep) or
                  (hasattr(dep, '__class__') and hasattr(dep, '__module__'))):
                if inspect.isfunction(dep) or inspect.isclass(dep):
                    pass
                elif hasattr(dep, '__class__') and inspect.isclass(dep.__class__):
                    dep = dep.__class__
                if id(dep) in depend_ids:
                    continue
                try:
                    lines = inspect.getsourcelines(dep)[0]
                except Exception:
                    logger.warning('Depenendency "%s" is not valid', dep)
                    raise
                lines[0] = lines[0].lstrip()
                compute.code += '\n' + ''.join(lines)
                depend_ids[id(dep)] = id(dep)
            elif isinstance(dep, functools.partial):
                try:
                    lines = inspect.getsourcelines(dep.func)[0]
                except Exception:
                    logger.warning('Depenendency "%s" is not valid', dep)
                    raise
                lines[0] = lines[0].lstrip()
                compute.code += '\n' + ''.join(lines)
                depend_ids[id(dep)] = id(dep)
            else:
                raise Exception('Invalid dependency: %s' % dep)
        if compute.code:
            # make sure code can be compiled
            code = compile(compute.code, '<string>', 'exec')
            del code
        if dest_path:
            if not isinstance(dest_path, basestring):
                raise Exception('Invalid dest_path: it must be a string')
            dest_path = dest_path.strip()
            # we should check for absolute path in dispynode.py as well
            if dest_path.startswith(os.sep):
                logger.warning('dest_path must not be absolute path')
            dest_path = dest_path.lstrip(os.sep)
            compute.dest_path = dest_path

        compute.scheduler_port = self._cluster.port
        compute.auth = hashlib.sha1(os.urandom(20)).hexdigest()
        compute.job_result_port = self._cluster.port
        compute.reentrant = reentrant
        compute.pulse_interval = pulse_interval

        self._compute = compute
        self._pending_jobs = 0
        self._jobs = []
        self._complete = threading.Event()
        self._complete.set()
        self.cpu_time = 0
        self.start_time = time.time()
        self.end_time = None
        if not shared:
            Task(self._cluster.add_cluster, self).value()

    def submit(self, *args, **kwargs):
        """Submit a job for execution with the given arguments.

        Arguments should be serializable and should correspond to
        arguments for computation used when cluster is created.
        """
        if self._compute.type == _Compute.prog_type:
            args = [str(arg) for arg in args]
        try:
            _job = _DispyJob_(self._compute.id, args, kwargs)
        except Exception:
            logger.warning('Creating job for "%s", "%s" failed with "%s"',
                           str(args), str(kwargs), traceback.format_exc())
            return None
        if Task(self._cluster.submit_job, _job).value() == 0:
            return _job.job
        else:
            return None

    def submit_node(self, node, *args, **kwargs):
        """Submit a job for execution at 'node' with the given
        arguments. 'node' can be an instance of DispyNode (e.g., as
        received in cluster/job status callback) or IP address or host
        name.

        Arguments should be serializable and should correspond to
        arguments for computation used when cluster is created.
        """
        if isinstance(node, DispyNode):
            node = self._dispy_nodes.get(node.ip_addr, None)
        elif isinstance(node, str):
            node = self._dispy_nodes.get(_node_ipaddr(node), None)
        else:
            node = None
        if not node:
            logger.warning('Invalid node')
            return None

        if self._compute.type == _Compute.prog_type:
            args = [str(arg) for arg in args]
        try:
            _job = _DispyJob_(self._compute.id, args, kwargs)
        except Exception:
            logger.warning('Creating job for "%s", "%s" failed with "%s"',
                           str(args), str(kwargs), traceback.format_exc())
            return None
        if Task(self._cluster.submit_job, _job, node.ip_addr).value() == 0:
            return _job.job
        else:
            return None

    def cancel(self, job):
        """Cancel given job. If the job is not yet running on any
        node, it is simply removed from scheduler's queue. If the job
        is running on a node, it is terminated/killed.

        Returns 0 if the job has been cancelled (i.e., removed from
        the queue or terminated).
        """
        return Task(self._cluster.cancel_job, job).value()

    def discover_nodes(self, nodes):
        """Discover given list of nodes. Each node may be host name or IP address, or an
        instance of NodeAllocate. If a node is '*', UDP broadcast is used to
        detect all nodes in local network.
        """
        if not isinstance(nodes, list):
            nodes = [nodes]
        nodes = _parse_node_allocs(nodes)
        return Task(self._cluster.discover_nodes, self, nodes).value()

    def allocate_node(self, node):
        """Allocate given node for this cluster. 'node' may be (list of) host
        name or IP address, or an instance of NodeAllocate.
        """
        if not isinstance(node, list):
            node = [node]
        node_allocs = _parse_node_allocs(node)
        if not node_allocs:
            return -1
        return Task(self._cluster.allocate_node, self, node_allocs).value()

    def deallocate_node(self, node):
        """Deallocate given node for this cluster. 'node' may be host name or IP
        address, or an instance of NodeAllocate.
        """
        return Task(self._cluster.deallocate_node, self, node).value()

    def close_node(self, node, terminate_pending=False):
        """Close given node for this cluster. 'node' may be host name or IP
        address, or an instance of NodeAllocate.
        """
        return Task(self._cluster.close_node, self, node, terminate_pending).value()

    def node_jobs(self, node, from_node=False):
        """Returns list of jobs currently running on given node, given
        as host name or IP address.
        """
        return Task(self._cluster.node_jobs, self, node, from_node).value()

    def set_node_cpus(self, node, cpus):
        """Sets (alters) CPUs managed by dispy on a node, given as
        host name or IP address, to given number of CPUs. If the
        number of CPUs given is negative then that many CPUs are not
        used (from the available CPUs).
        """
        return Task(self._cluster.set_node_cpus, self, node, cpus).value()

    def send_file(self, path, node):
        """Send file with given 'path' to 'node'.  'node' can be an
        instance of DispyNode (e.g., as received in cluster status
        callback) or IP address or host name.
        """
        cwd = self._cluster.dest_path
        path = os.path.abspath(path)
        if path.startswith(cwd):
            dst = os.path.dirname(path[len(cwd):].lstrip(os.sep))
        else:
            dst = '.'
        xf = _XferFile(path, dst, self._compute.id)
        return Task(self._cluster.send_file, self, node, xf).value()

    @property
    def name(self):
        """Returns name of computation. If the computation is Python
        function, then this would be name of the function. If the
        computation is a program, then this would be name of the
        program (without path).
        """
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
        def _status(self, task=None):
            yield ClusterStatus(self._dispy_nodes.values(), self._pending_jobs)
        return Task(_status, self).value()

    def print_status(self, wall_time=None):
        """
        Prints status of cluster (see 'status').
        """
        print('')
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
            if dispy_node.jobs_done > 0:
                secs_per_job = dispy_node.cpu_time / dispy_node.jobs_done
            else:
                secs_per_job = 0
            print(' %-30.30s | %5s | %7s | %10.3f | %13.3f' %
                  (name, dispy_node.cpus, dispy_node.jobs_done,
                   secs_per_job, dispy_node.cpu_time))
        print('')
        if info.jobs_pending:
            print('Jobs pending: %s' % info.jobs_pending)
        msg = 'Total job time: %.3f sec' % cpu_time
        if not wall_time:
            wall_time = time.time() - self.start_time
        msg += ', wall time: %.3f sec, speedup: %.3f' % (wall_time, cpu_time / wall_time)
        print(msg)
        print('')

    # for backward compatibility
    stats = print_status

    def wait(self, timeout=None):
        """Wait for scheduled jobs to complete.
        """
        return self._cluster.wait(self, timeout)

    def __call__(self):
        """Wait for scheduled jobs to complete.
        """
        self.wait()

    def close(self, timeout=None, terminate=False):
        """Close the cluster (jobs can no longer be submitted to it). If there
        are any jobs pending, this method waits until they all finish, unless
        'terminate' is True in which case pending jobs are cancelled (removed or
        terminated by nodes executing them). Additional clusters may be created
        after this call returns.
        """
        if getattr(self, '_compute', None):
            ret = self._complete.wait(timeout=timeout)
            if not terminate and not ret:
                return False
            self._complete.set()
            Task(self._cluster.del_cluster, self).value()
            self._compute = None
            return True

    def shutdown(self):
        """Close the cluster and shutdown the scheduler (so additional
        clusters can't be created).
        """
        self.close()
        if self._cluster:
            cluster, self._cluster = self._cluster, None
            cluster.shutdown()


class SharedJobCluster(JobCluster):
    """SharedJobCluster should be used (instead of JobCluster) if two
    or more processes can simultaneously use dispy. In this case,
    'dispyscheduler' must be running on a node and 'scheduler_node'
    parameter should be set to that node's IP address or host name.

    @scheduler_node is name or IP address where dispyscheduler is
      running to which jobs are submitted.

    @scheduler_port is port where dispyscheduler is running at
    @scheduler_node.

    @port is port where this client will get job results from
    dispyscheduler.

    @pulse_interval for SharedJobCluster is not used; instead,
    dispyscheduler must be called with appropriate pulse_interval.
    The behaviour is same as for JobCluster.
    """
    def __init__(self, computation, nodes=None, depends=[], callback=None, cluster_status=None,
                 ip_addr=None, port=51347, scheduler_node=None, scheduler_port=None,
                 ext_ip_addr=None, loglevel=logger.INFO, setup=None, cleanup=True, dest_path=None,
                 poll_interval=None, reentrant=False, exclusive=False,
                 secret='', keyfile=None, certfile=None, recover_file=None):

        self.scheduler_ip_addr = _node_ipaddr(scheduler_node)
        self.addrinfo = host_addrinfo(host=ip_addr)
        self.addrinfo.family = socket.getaddrinfo(self.scheduler_ip_addr, None)[0][0]

        if not nodes:
            nodes = ['*']
        elif not isinstance(nodes, list):
            if isinstance(nodes, str):
                nodes = [nodes]
            else:
                raise Exception('"nodes" must be list of IP addresses or host names')
        node_allocs = _parse_node_allocs(nodes)
        if not node_allocs:
            raise Exception('"nodes" argument is invalid')
        node_allocs = [(na.ip_addr, na.port, na.cpus) for na in node_allocs]
        if ext_ip_addr:
            ext_ip_addr = host_addrinfo(host=ext_ip_addr).ip

        JobCluster.__init__(self, computation, depends=depends,
                            callback=callback, cluster_status=cluster_status,
                            ip_addr=ip_addr, port=port, ext_ip_addr=ext_ip_addr,
                            loglevel=loglevel, setup=setup, cleanup=cleanup, dest_path=dest_path,
                            poll_interval=poll_interval, reentrant=reentrant,
                            secret=secret, keyfile=keyfile, certfile=certfile,
                            recover_file=recover_file)

        def _terminate_scheduler(self, task=None):
            yield self._cluster._sched_event.set()

        # wait for scheduler to terminate
        self._cluster.terminate = True
        Task(_terminate_scheduler, self).value()
        self._cluster._scheduler.value()
        self._cluster.job_uid = None

        if not scheduler_port:
            scheduler_port = 51349

        # wait until tcp server has started
        while not self._cluster.port:
            time.sleep(0.1)

        sock = socket.socket(self.addrinfo.family, socket.SOCK_STREAM)
        sock = AsyncSocket(sock, blocking=True, keyfile=keyfile, certfile=certfile)
        sock.connect((self.scheduler_ip_addr, scheduler_port))
        sock.sendall(self._cluster.auth)
        req = {'version': _dispy_version, 'ip_addr': ext_ip_addr,
               'scheduler_ip_addr': self.scheduler_ip_addr}
        sock.send_msg('CLIENT:' + serialize(req))
        reply = sock.recv_msg()
        sock.close()
        reply = deserialize(reply)
        if reply['version'] != _dispy_version:
            raise Exception('dispyscheduler version "%s" is different from dispy version "%s"' %
                            reply['version'], _dispy_version)
        ext_ip_addr = reply['ip_addr']

        self.scheduler_port = reply['port']
        self._scheduler_auth = auth_code(secret, reply['sign'])
        self._compute.scheduler_ip_addr = ext_ip_addr
        self._compute.scheduler_port = self._cluster.port
        self._compute.job_result_port = self._cluster.port

        sock = AsyncSocket(socket.socket(self.addrinfo.family, socket.SOCK_STREAM), blocking=True,
                           keyfile=keyfile, certfile=certfile)
        sock.settimeout(MsgTimeout)
        try:
            sock.connect((self.scheduler_ip_addr, self.scheduler_port))
            sock.sendall(self._scheduler_auth)
            req = {'compute': self._compute, 'node_allocs': node_allocs,
                   'exclusive': bool(exclusive)}
            sock.send_msg('COMPUTE:' + serialize(req))
            reply = sock.recv_msg()
            reply = deserialize(reply)
            if isinstance(reply, dict):
                self._compute.id = reply['compute_id']
                self._compute.auth = reply['auth']
            else:
                raise Exception('Scheduler refused computation: %s' % reply)
        except Exception:
            raise
        finally:
            sock.close()

        for xf in self._compute.xfer_files:
            xf.compute_id = self._compute.id
            logger.debug('Sending file "%s"', xf.name)
            sock = socket.socket(self.addrinfo.family, socket.SOCK_STREAM)
            sock = AsyncSocket(sock, blocking=True, keyfile=keyfile, certfile=certfile)
            sock.settimeout(MsgTimeout)
            try:
                sock.connect((self.scheduler_ip_addr, self.scheduler_port))
                sock.sendall(self._scheduler_auth)
                sock.send_msg('FILEXFER:' + serialize(xf))
                recvd = sock.recv_msg()
                recvd = deserialize(recvd)
                sent = 0
                with open(xf.name, 'rb') as fd:
                    while sent == recvd:
                        data = fd.read(1024000)
                        if not data:
                            break
                        sock.sendall(data)
                        sent += len(data)
                        recvd = sock.recv_msg()
                        recvd = deserialize(recvd)
                assert recvd == xf.stat_buf.st_size
            except Exception:
                logger.error('Could not transfer %s to %s', xf.name, self.scheduler_ip_addr)
                # TODO: delete computation?
            sock.close()

        Task(self._cluster.add_cluster, self).value()
        self._scheduled_event = threading.Event()
        sock = AsyncSocket(socket.socket(self.addrinfo.family, socket.SOCK_STREAM), blocking=True,
                           keyfile=keyfile, certfile=certfile)
        sock.settimeout(MsgTimeout)
        sock.connect((self.scheduler_ip_addr, self.scheduler_port))
        sock.sendall(self._scheduler_auth)
        req = {'compute_id': self._compute.id, 'auth': self._compute.auth}
        sock.send_msg('SCHEDULE:' + serialize(req))
        resp = sock.recv_msg()
        sock.close()
        if resp == 'ACK':
            self._scheduled_event.wait()
            logger.debug('Computation %s created with %s', self._compute.name, self._compute.id)
        else:
            self._cluster._clusters.pop(self._compute.id, None)
            raise Exception('Computation "%s" could not be sent to scheduler' % self._compute.name)

    def submit(self, *args, **kwargs):
        """Submit a job for execution with the given arguments.

        Arguments should be serializable and should correspond to
        arguments for computation used when cluster is created.
        """
        return self.submit_node(None, *args, **kwargs)

    def submit_node(self, node, *args, **kwargs):
        """Submit a job for execution at 'node' with the given
        arguments. 'node' can be an instance of DispyNode (e.g., as
        received in cluster/job status callback) or IP address or host
        name.

        Arguments should be serializable and should correspond to
        arguments for computation used when cluster is created.
        """

        if node:
            if isinstance(node, DispyNode):
                node = node.ip_addr
            elif isinstance(node, str):
                pass
            else:
                node = None
            if not node:
                return None

        if self._compute.type == _Compute.prog_type:
            args = [str(arg) for arg in args]
        try:
            _job = _DispyJob_(self._compute.id, args, kwargs)
        except Exception:
            logger.warning('Creating job for "%s", "%s" failed with "%s"',
                           str(args), str(kwargs), traceback.format_exc())
            return None

        job = None
        try:
            for xf in _job.xfer_files:
                sock = AsyncSocket(socket.socket(self.addrinfo.family, socket.SOCK_STREAM),
                                   blocking=True,
                                   keyfile=self._cluster.keyfile, certfile=self._cluster.certfile)
                sock.settimeout(MsgTimeout)
                sock.connect((self.scheduler_ip_addr, self.scheduler_port))
                sock.sendall(self._scheduler_auth)
                sock.send_msg('FILEXFER:' + serialize(xf))
                recvd = sock.recv_msg()
                recvd = deserialize(recvd)
                sent = 0
                with open(xf.name, 'rb') as fd:
                    while sent == recvd:
                        data = fd.read(1024000)
                        if not data:
                            break
                        sock.sendall(data)
                        sent += len(data)
                        recvd = sock.recv_msg()
                        recvd = deserialize(recvd)
                assert recvd == xf.stat_buf.st_size
                sock.close()

            sock = AsyncSocket(socket.socket(self.addrinfo.family, socket.SOCK_STREAM),
                               blocking=True,
                               keyfile=self._cluster.keyfile, certfile=self._cluster.certfile)
            sock.settimeout(MsgTimeout)
            sock.connect((self.scheduler_ip_addr, self.scheduler_port))
            sock.sendall(self._scheduler_auth)
            req = {'node': node, 'job': _job, 'auth': self._compute.auth}
            sock.send_msg('JOB:' + serialize(req))
            msg = sock.recv_msg()
            _job.uid = deserialize(msg)
            if _job.uid:
                self._cluster._sched_jobs[_job.uid] = _job
                self._pending_jobs += 1
                self._complete.clear()
                sock.send_msg('ACK')
                if self.status_callback:
                    self._cluster.worker_Q.put((self.status_callback,
                                                (DispyJob.Created, None, _job.job)))
                job = _job.job
                job.id = next(self._cluster._job_id)
            else:
                sock.send_msg('NAK'.encode())
                _job.job._dispy_job_ = None
                del _job.job
        except Exception:
            logger.warning('Creating job for "%s", "%s" failed with "%s"',
                           str(args), str(kwargs), traceback.format_exc())
            _job.job._dispy_job_ = None
            del _job.job
        finally:
            sock.close()
        return job

    def cancel(self, job):
        """Similar to 'cancel' of JobCluster.
        """
        _job = job._dispy_job_
        if _job is None or self._cluster._clusters.get(_job.compute_id, None) != self:
            logger.warning('Invalid job %s for cluster "%s"!', job.id, self._compute.name)
            return -1
        if job.status not in [DispyJob.Created, DispyJob.Running, DispyJob.ProvisionalResult]:
            logger.warning('Job %s is not valid for cancel (%s)', job.id, job.status)
            return -1

        job.status = DispyJob.Cancelled
        # assert self._pending_jobs >= 1
        sock = AsyncSocket(socket.socket(self.addrinfo.family, socket.SOCK_STREAM), blocking=True,
                           keyfile=self._cluster.keyfile, certfile=self._cluster.certfile)
        sock.settimeout(MsgTimeout)
        try:
            sock.connect((self.scheduler_ip_addr, self.scheduler_port))
            sock.sendall(self._scheduler_auth)
            req = {'uid': _job.uid, 'compute_id': self._compute.id, 'auth': self._compute.auth}
            sock.send_msg('TERMINATE_JOB:' + serialize(req))
        except Exception:
            logger.warning('Could not connect to scheduler to terminate job')
            return -1
        finally:
            sock.close()
        return 0

    def allocate_node(self, node):
        """Similar to 'allocate_node' of JobCluster.
        """
        if not isinstance(node, list):
            node = [node]
        node_allocs = _parse_node_allocs(node)
        if not node_allocs:
            raise StopIteration(-1)
        if len(node_allocs) != 1:
            return -1

        sock = AsyncSocket(socket.socket(self.addrinfo.family, socket.SOCK_STREAM), blocking=True,
                           keyfile=self._cluster.keyfile, certfile=self._cluster.certfile)
        sock.settimeout(MsgTimeout)
        try:
            sock.connect((self.scheduler_ip_addr, self.scheduler_port))
            sock.sendall(self._scheduler_auth)
            req = {'compute_id': self._compute.id, 'auth': self._compute.auth,
                   'node_alloc': node_allocs}
            sock.send_msg('ALLOCATE_NODE:' + serialize(req))
            reply = sock.recv_msg()
            reply = deserialize(reply)
        except Exception:
            logger.warning('Could not connect to scheduler to add node')
            reply = -1
        finally:
            sock.close()
        return reply

    def deallocate_node(self, node):
        """Similar to 'allocate_node' of JobCluster.
        """
        if isinstance(node, DispyNode):
            node = node.ip_addr
        elif isinstance(node, str):
            pass
        if not node:
            return -1
        sock = AsyncSocket(socket.socket(self.addrinfo.family, socket.SOCK_STREAM), blocking=True,
                           keyfile=self._cluster.keyfile, certfile=self._cluster.certfile)
        sock.settimeout(MsgTimeout)
        try:
            sock.connect((self.scheduler_ip_addr, self.scheduler_port))
            sock.sendall(self._scheduler_auth)
            req = {'compute_id': self._compute.id, 'auth': self._compute.auth, 'node': node}
            sock.send_msg('DEALLOCATE_NODE:' + serialize(req))
            reply = sock.recv_msg()
            reply = deserialize(reply)
        except Exception:
            logger.warning('Could not connect to scheduler to add node')
            reply = -1
        finally:
            sock.close()
        return reply

    def close_node(self, node, terminate_pending=False):
        """Similar to 'cloe_node' of JobCluster.
        """
        if isinstance(node, DispyNode):
            node = node.ip_addr
        elif isinstance(node, str):
            pass
        if not node:
            return -1
        sock = AsyncSocket(socket.socket(self.addrinfo.family, socket.SOCK_STREAM), blocking=True,
                           keyfile=self._cluster.keyfile, certfile=self._cluster.certfile)
        sock.settimeout(MsgTimeout)
        try:
            sock.connect((self.scheduler_ip_addr, self.scheduler_port))
            sock.sendall(self._scheduler_auth)
            req = {'compute_id': self._compute.id, 'auth': self._compute.auth, 'node': node,
                   'terminate_pending': terminate_pending}
            sock.send_msg('CLOSE_NODE:' + serialize(req))
            reply = sock.recv_msg()
            reply = deserialize(reply)
        except Exception:
            logger.warning('Could not connect to scheduler to add node')
            reply = -1
        finally:
            sock.close()
        return reply

    def node_jobs(self, node, from_node=False):
        """Similar to 'node_jobs' of JobCluster.
        """
        if isinstance(node, DispyNode):
            node = node.ip_addr
        elif isinstance(node, str):
            pass
        if not node:
            return []
        sock = AsyncSocket(socket.socket(self.addrinfo.family, socket.SOCK_STREAM), blocking=True,
                           keyfile=self._cluster.keyfile, certfile=self._cluster.certfile)
        sock.settimeout(MsgTimeout)
        try:
            sock.connect((self.scheduler_ip_addr, self.scheduler_port))
            sock.sendall(self._scheduler_auth)
            req = {'compute_id': self._compute.id, 'auth': self._compute.auth,
                   'node': node, 'get_uids': True, 'from_node': bool(from_node)}
            sock.send_msg('NODE_JOBS:' + serialize(req))
            reply = sock.recv_msg()
            job_uids = deserialize(reply)
            _jobs = [self._cluster._sched_jobs.get(uid, None) for uid in job_uids]
        except Exception:
            logger.warning('Could not connect to scheduler to get running jobs at node')
            _jobs = []
        finally:
            sock.close()
        jobs = [_job.job for _job in _jobs if _job]
        return jobs

    def set_node_cpus(self, node, cpus):
        """Similar to 'set_node_cpus' of JobCluster.
        """
        # setting a node's cpus may affect other clients, so (for now) disable it.
        return -1

        if isinstance(node, DispyNode):
            node = node.ip_addr
        elif isinstance(node, str):
            pass
        if not node:
            return -1
        sock = AsyncSocket(socket.socket(self.addrinfo.family, socket.SOCK_STREAM), blocking=True,
                           keyfile=self._cluster.keyfile, certfile=self._cluster.certfile)
        sock.settimeout(MsgTimeout)
        try:
            sock.connect((self.scheduler_ip_addr, self.scheduler_port))
            sock.sendall(self._scheduler_auth)
            req = {'compute_id': self._compute.id, 'auth': self._compute.auth, 'node': node,
                   'cpus': cpus}
            sock.send_msg('SET_NODE_CPUS:' + serialize(req))
            reply = sock.recv_msg()
            reply = deserialize(reply)
            if isinstance(reply, tuple) and reply[0]:
                node = self._dispy_nodes.get(reply[0], None)
                if node:
                    node.cpus = reply[1]
        except Exception:
            logger.warning('Could not connect to scheduler to add node')
            return -1
        finally:
            sock.close()
        return reply

    def send_file(self, path, node):
        """Send file with given 'path' to 'node'.  'node' can be an
        instance of DispyNode (e.g., as received in cluster status
        callback) or IP address or host name.
        """
        if isinstance(node, DispyNode):
            node = node.ip_addr
        elif isinstance(node, str):
            pass
        if not node:
            return -1

        cwd = self._cluster.dest_path
        path = os.path.abspath(path)
        if path.startswith(cwd):
            dst = os.path.dirname(path[len(cwd):].lstrip(os.sep))
        else:
            dst = '.'
        xf = _XferFile(path, dst, self._compute.id)
        sock = AsyncSocket(socket.socket(self.addrinfo.family, socket.SOCK_STREAM), blocking=True,
                           keyfile=self._cluster.keyfile, certfile=self._cluster.certfile)
        sock.settimeout(MsgTimeout)
        try:
            sock.connect((self.scheduler_ip_addr, self.scheduler_port))
            sock.sendall(self._scheduler_auth)
            sock.send_msg('SENDFILE:' + serialize({'node': node, 'xf': xf}))
            recvd = sock.recv_msg()
            recvd = deserialize(recvd)
            sent = 0
            with open(xf.name, 'rb') as fd:
                while sent == recvd:
                    data = fd.read(1024000)
                    if not data:
                        break
                    sock.sendall(data)
                    sent += len(data)
                    recvd = sock.recv_msg()
                    recvd = deserialize(recvd)
            assert recvd == xf.stat_buf.st_size
        except Exception:
            return -1
        else:
            return 0


def recover_jobs(recover_file=None, timeout=None, terminate_pending=False):
    """
    If dispy client crashes or loses connection to nodes, the nodes
    will continue to execute scheduled jobs. This 'recover_jobs'
    function can be used to retrieve the results of those jobs
    (DispyJob objects).

    @recover_file is path to file in which dispy stored information
      about cluster (see 'recover_file' in JobCluster above). If
      incorrect 'recover_file' is used, this function issues a warning
      and will block.

    @timeout is time limit in seconds for recovery. This function will
      return all jobs that finish before 'timeout'. Any jobs still
      running or couldn't be recovered before timeout will be ignored.

    @terminate_pending indicates if any jobs currently running should
      be terminated (so that, for example, node can be used for
      computations again right away instead of having to wait until
      all jobs finish).

    Returns list of DispyJob instances that will have .result,
    .stdout, .stderr etc.; however, the nodes don't keep track of .id,
    .args, .kwargs so they will be None.

    Once all the jobs that were scheduled at the time of crash are
    retrieved (if the jobs are still running, this function will block
    until all the jobs are finished and results obtained), nodes are
    closed (so they can serve new clients), 'recover_file' is removed
    and the jobs are returned.
    """

    if not recover_file:
        import glob
        recover_file = sorted(glob.glob('_dispy_*'))
        if recover_file:
            recover_file = recover_file[-1]
        else:
            print('Could not find recover file of the form "_dispy_*"')
            return []

    shelf_nodes = {}
    computes = {}
    cluster = None
    pycos_scheduler = pycos.Pycos.instance()

    try:
        shelf = shelve.open(recover_file, flag='r')
    except Exception:
        print('Could not open recover file "%s"' % recover_file)
        return []

    for key, val in shelf.iteritems():
        if key.startswith('node_'):
            shelf_nodes[key[len('node_'):]] = val
        elif key.startswith('compute_'):
            computes[int(key[len('compute_'):])] = val
        elif key == '_cluster':
            cluster = val
        else:
            logger.warning('Invalid key "%s" ignored', key)
    shelf.close()
    if not cluster or not computes or not shelf_nodes:
        for ext in ('', '.db', '.bak', '.dat', '.dir'):
            if os.path.isfile(recover_file + ext):
                try:
                    os.remove(recover_file + ext)
                except Exception:
                    pass
        return []

    nodes = {}
    for ip_addr, info in shelf_nodes.iteritems():
        node = _Node(ip_addr, info['port'], 0, '', cluster['secret'], platform='',
                     keyfile=cluster['keyfile'], certfile=cluster['certfile'])
        node.auth = info['auth']
        if info.get('scheduler'):
            node.scheduler_ip_addr = ip_addr
        nodes[node.ip_addr] = node

    def tcp_req(conn, addr, pending, task=None):
        # generator
        conn.settimeout(MsgTimeout)
        msg = yield conn.recv_msg()
        if msg.startswith('JOB_REPLY:'):
            try:
                reply = deserialize(msg[len('JOB_REPLY:'):])
            except Exception:
                logger.warning('Invalid job reply from %s:%s ignored', addr[0], addr[1])
                conn.close()
                raise StopIteration
            yield conn.send_msg('ACK')
            logger.debug('Received reply for job %s', reply.uid)
            job = DispyJob((), {})
            job.result = deserialize(reply.result)
            job.stdout = reply.stdout
            job.stderr = reply.stderr
            job.exception = reply.exception
            job.start_time = reply.start_time
            job.end_time = reply.end_time
            job.status = reply.status
            job.ip_addr = reply.ip_addr
            job.finish.set()
            pending['jobs'].append(job)
            pending['count'] -= 1
            if pending['count'] == 0 and pending['resend_req_done'] is True:
                pending['complete'].set()
        else:
            logger.debug('Invalid TCP message from %s ignored', addr[0])
        conn.close()

    def tcp_server(ip_addr, pending, task=None):
        task.set_daemon()
        addrinfo = host_addrinfo(host=ip_addr)
        sock = AsyncSocket(socket.socket(addrinfo.family, socket.SOCK_STREAM),
                           keyfile=cluster['keyfile'], certfile=cluster['certfile'])
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((ip_addr, cluster['port']))
        sock.listen(32)

        while 1:
            if pending['timeout'] is not None:
                timeout = pending['timeout'] - (time.time() - pending['start_time'])
                if timeout <= 0:
                    pending['complete'].set()
                    timeout = 2
                sock.settimeout(timeout)

            try:
                conn, addr = yield sock.accept()
            except ssl.SSLError as err:
                logger.debug('SSL connection failed: %s', str(err))
                continue
            except GeneratorExit:
                sock.close()
                break
            except socket.timeout:
                continue
            except Exception:
                continue
            else:
                Task(tcp_req, conn, addr, pending)
        raise StopIteration

    def resend_requests(pending, task=None):
        for compute_id, compute in computes.items():
            if pending['timeout'] is not None and \
               ((time.time() - pending['start_time']) > pending['timeout']):
                break
            req = {'compute_id': compute_id, 'auth': compute['auth']}
            for ip_addr in compute['nodes']:
                node = nodes.get(ip_addr, None)
                if not node:
                    continue
                reply = yield node.send('RESEND_JOB_RESULTS:' + serialize(req))
                try:
                    reply = deserialize(reply)
                    assert isinstance(reply, int)
                except Exception:
                    logger.warning('Invalid resend reply from %s', ip_addr)
                    continue
                logger.debug('Pending jobs from %s for %s: %s',
                             node.ip_addr, compute['name'], reply)
                if reply == 0:
                    req['node_ip_addr'] = ip_addr
                    yield node.send('CLOSE:' + serialize(req), reply=True, task=task)
                else:
                    pending['count'] += reply
        pending['resend_req_done'] = True
        if pending['count'] == 0:
            pending['complete'].set()

    pending = {'count': 0, 'resend_req_done': False, 'jobs': [], 'complete': threading.Event(),
               'timeout': timeout, 'start_time': time.time()}
    for ip_addr in cluster['ip_addrs']:
        if not ip_addr:
            ip_addr = ''
        Task(tcp_server, ip_addr, pending)

    Task(resend_requests, pending)

    pending['complete'].wait()

    for compute_id, compute in computes.iteritems():
        req = {'compute_id': compute_id, 'auth': compute['auth'],
               'terminate_pending': terminate_pending}
        for ip_addr in compute['nodes']:
            node = nodes.get(ip_addr, None)
            if not node:
                continue
            if node.scheduler_ip_addr:
                continue
            req['node_ip_addr'] = ip_addr
            Task(node.send, 'CLOSE:' + serialize(req), reply=True)

    if terminate_pending:
        # wait a bit to get cancelled job results
        for x in range(10):
            if pending['count'] == 0 and pending['resend_req_done'] is True:
                break
            time.sleep(0.2)

    pycos_scheduler.finish()

    if pending['count'] == 0 and pending['resend_req_done'] is True:
        for ext in ('', '.db', '.bak', '.dat', '.dir'):
            if os.path.isfile(recover_file + ext):
                try:
                    os.remove(recover_file + ext)
                except Exception:
                    pass
    return pending['jobs']


if __name__ == '__main__':
    import argparse

    logger.info('dispy version %s', _dispy_version)

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
    parser.add_argument('--certfile', dest='certfile', default='',
                        help='file containing SSL certificate')
    parser.add_argument('--keyfile', dest='keyfile', default='',
                        help='file containing SSL key')
    parser.add_argument('--scheduler_node', dest='scheduler_node', default=None,
                        help='name or IP address where dispyscheduler is running to which '
                        'jobs are submitted')

    config = vars(parser.parse_args(sys.argv[1:]))

    if config['loglevel']:
        logger.setLevel(logger.DEBUG)
        pycos.logger.setLevel(logger.DEBUG)
    else:
        logger.setLevel(logger.INFO)
    del config['loglevel']

    if config['certfile']:
        config['certfile'] = os.path.abspath(config['certfile'])
    else:
        config['certfile'] = None
    if config['keyfile']:
        config['keyfile'] = os.path.abspath(config['keyfile'])
    else:
        config['keyfile'] = None

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
