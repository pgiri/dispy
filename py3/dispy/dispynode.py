#!/usr/bin/env python3

"""
dispynode: Execute computations on behalf of dispy clients;
see accompanying 'dispy' for more details.
"""

import os
import sys
import time
import stat
import socket
import multiprocessing
import threading
import subprocess
import traceback
import logging
import marshal
import tempfile
import shutil
import glob
import functools
import inspect
import pickle
import io
import signal
try:
    import psutil
except ImportError:
    psutil = None
try:
    import netifaces
except:
    netifaces = None

from dispy import _JobReply, DispyJob, DispyNodeAvailInfo, _Function, _Compute, _XferFile, \
     _node_ipaddr, _dispy_version, auth_code, num_min, _same_file, MsgTimeout

import asyncoro
from asyncoro import Coro, AsynCoro, AsyncSocket, serialize, unserialize

__author__ = "Giridhar Pemmasani (pgiri@yahoo.com)"
__email__ = "pgiri@yahoo.com"
__copyright__ = "Copyright 2011, Giridhar Pemmasani"
__contributors__ = []
__maintainer__ = "Giridhar Pemmasani (pgiri@yahoo.com)"
__license__ = "MIT"
__url__ = "http://dispy.sourceforge.net"
__status__ = "Production"
__version__ = _dispy_version
__all__ = []

MaxFileSize = 0


def dispy_provisional_result(result, timeout=MsgTimeout):
    """Sends provisional result of computation back to the client.

    In some cases, such as optimizations, computations may send
    current (best) result to the client and continue computation (for
    next iteration) so that the client may decide to terminate
    computations based on the results or alter computations if
    necessary. The computations can use this function in such cases
    with the current result of computation as argument.

    'timeout' is seconds for socket connection/messages; i.e., if
    there is no I/O on socket (to client), this call fails. Default
    value for it is MsgTimeout (5) seconds.

    Returns 0 if result was delivered to client.
    """

    dispy_job_reply = __dispy_job_info.job_reply
    dispy_job_reply.status = DispyJob.ProvisionalResult
    dispy_job_reply.result = result
    dispy_job_reply.end_time = time.time()
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock = AsyncSocket(sock, blocking=True, keyfile=__dispy_job_keyfile,
                       certfile=__dispy_job_certfile)
    sock.settimeout(timeout)
    try:
        sock.connect(__dispy_job_info.reply_addr)
        sock.send_msg(b'JOB_REPLY:' + serialize(dispy_job_reply))
        ack = sock.recv_msg()
        assert ack == b'ACK'
    except:
        print("Couldn't send provisional results %s:\n%s" % (str(result), traceback.format_exc()))
        return -1
    else:
        return 0
    finally:
        sock.close()


def dispy_send_file(path, timeout=MsgTimeout):
    """Computations may use this function to send files back to the client.

    If the computations have small amount of data to be sent back to
    the client, then the return value can be used for that
    purpose. However, if (temporary) result is stored in file(s), then
    those file(s) can be sent back to the client.

    File at given 'path' is sent to the client, which saves the file
    with the same path under its working directory. If multiple jobs
    on different nodes send files, care must be taken to use different
    paths so files sent by one job don't overwrite files sent by other
    jobs.

    If file size exceeds 'MaxFileSize' bytes, this function returns -1,
    without sending it.

    'timeout' is seconds for socket connection/messages; i.e., if
    there is no I/O on socket (to client), this call fails. Default
    value for it is MsgTimeout (5) seconds.

    Return value of 0 indicates successfull transfer.
    """

    path = os.path.expanduser(path)
    xf = _XferFile(path, os.stat(path))
    if MaxFileSize and xf.stat_buf.st_size > MaxFileSize:
        return -1
    xf.name = os.path.splitdrive(path)[1]
    if xf.name.startswith(os.sep):
        xf.name = xf.name[len(os.sep):]
    dispy_job_reply = __dispy_job_info.job_reply
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock = AsyncSocket(sock, blocking=True,
                       keyfile=__dispy_job_keyfile, certfile=__dispy_job_certfile)
    sock.settimeout(timeout)
    try:
        sock.connect(__dispy_job_info.reply_addr)
        sock.send_msg('FILEXFER:'.encode() + serialize(xf))
        sock.send_msg(serialize(dispy_job_reply))
        recvd = sock.recv_msg()
        recvd = unserialize(recvd)
        with open(path, 'rb') as fd:
            sent = 0
            while sent == recvd:
                data = fd.read(1024000)
                if not data:
                    break
                sock.sendall(data)
                sent += len(data)
                recvd = sock.recv_msg()
                recvd = unserialize(recvd)
        assert recvd == xf.stat_buf.st_size
    except:
        print('Could not transfer file "%s": %s' % (path, traceback.format_exc()))
        return -1
    else:
        return 0
    finally:
        sock.close()


class _DispyJobInfo(object):
    """Internal use only.
    """
    def __init__(self, job_reply, reply_addr, compute, xfer_files):
        self.job_reply = job_reply
        self.reply_addr = reply_addr
        self.compute_id = compute.id
        self.compute_dest_path = compute.dest_path
        self.xfer_files = xfer_files
        self.compute_auth = compute.auth
        self.proc = None


def _dispy_job_func(__dispy_job_info, __dispy_job_certfile, __dispy_job_keyfile,
                    __dispy_job_name, __dispy_job_args, __dispy_job_kwargs,
                    __dispy_job_code, __dispy_job_globals, __dispy_path, __dispy_reply_Q):
    """Internal use only.
    """

    os.chdir(__dispy_path)
    sys.stdout = io.StringIO()
    sys.stderr = io.StringIO()
    __dispy_job_reply = __dispy_job_info.job_reply
    globals().update(__dispy_job_globals)
    try:
        exec(marshal.loads(__dispy_job_code[0]), globals())
        if __dispy_job_code[1]:
            exec(__dispy_job_code[1], globals())
        if __name__ == '__mp_main__':  # Windows multiprocessing process
            sys.modules['__mp_main__'].__dict__.update(globals())
        __dispy_job_args = unserialize(__dispy_job_args)
        __dispy_job_kwargs = unserialize(__dispy_job_kwargs)
        globals().update(locals())
        exec('__dispy_job_reply.result = %s(*__dispy_job_args, **__dispy_job_kwargs)' %
             __dispy_job_name, globals())
        __dispy_job_reply.status = DispyJob.Finished
    except:
        __dispy_job_reply.exception = traceback.format_exc()
        __dispy_job_reply.status = DispyJob.Terminated
    __dispy_job_reply.stdout = sys.stdout.getvalue()
    __dispy_job_reply.stderr = sys.stderr.getvalue()
    __dispy_job_reply.end_time = time.time()
    __dispy_job_info.proc = None
    __dispy_reply_Q.put(__dispy_job_reply)


class _DispyNode(object):
    """Internal use only.
    """
    def __init__(self, cpus, ip_addr=None, ext_ip_addr=None, node_port=None,
                 name='', scheduler_node=None, scheduler_port=None,
                 dest_path_prefix='', clean=False, secret='', keyfile=None, certfile=None,
                 zombie_interval=60, service_start=None, service_stop=None, service_end=None,
                 serve=-1, daemon=False):
        assert 0 < cpus <= multiprocessing.cpu_count()
        self.num_cpus = cpus
        if name:
            self.name = name
        else:
            self.name = socket.gethostname()
        if ip_addr:
            ip_addr = _node_ipaddr(ip_addr)
            if not ip_addr:
                raise Exception('invalid ip_addr')
        else:
            if netifaces:
                for iface in netifaces.interfaces():
                    for link in netifaces.ifaddresses(iface).get(netifaces.AF_INET, []):
                        if link.get('broadcast', None) and link.get('netmask', None):
                            ip_addr = socket.gethostbyname(link.get('addr', ''))
                            break
                    else:
                        continue
                    break
            if not ip_addr:
                ip_addr = socket.gethostbyname(socket.gethostname())
        if ip_addr.startswith('127.'):
            _dispy_logger.warning('node IP address %s seems to be loopback address; '
                                  'this will prevent communication with clients on '
                                  'other machines. ', ip_addr)
        if ext_ip_addr:
            ext_ip_addr = _node_ipaddr(ext_ip_addr)
            if not ext_ip_addr:
                raise Exception('invalid ext_ip_addr')
        else:
            ext_ip_addr = ip_addr

        if not self.name:
            try:
                self.name = socket.gethostbyaddr(ext_ip_addr)[0]
            except:
                self.name = ''

        if node_port is None:
            node_port = 51348

        self.ext_ip_addr = ext_ip_addr
        self.pulse_interval = None
        self.keyfile = keyfile
        self.certfile = certfile
        if self.keyfile:
            self.keyfile = os.path.abspath(self.keyfile)
        if self.certfile:
            self.certfile = os.path.abspath(self.certfile)

        self.asyncoro = AsynCoro()

        self.tcp_sock = AsyncSocket(socket.socket(socket.AF_INET, socket.SOCK_STREAM),
                                    keyfile=keyfile, certfile=certfile)
        self.tcp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.tcp_sock.bind((ip_addr, node_port))
        self.address = self.tcp_sock.getsockname()
        self.port = self.address[1]
        self.tcp_sock.listen(30)

        if not dest_path_prefix:
            dest_path_prefix = os.path.join(tempfile.gettempdir(), 'dispy', 'node')
        self.dest_path_prefix = os.path.abspath(dest_path_prefix.strip()).rstrip(os.sep)
        if clean:
            shutil.rmtree(self.dest_path_prefix, ignore_errors=True)
        if not os.path.isdir(self.dest_path_prefix):
            os.makedirs(self.dest_path_prefix)
            os.chmod(self.dest_path_prefix, stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)

        self.avail_cpus = self.num_cpus
        self.computations = {}
        self.job_infos = {}
        self.terminate = False
        self.sign = ''.join(hex(x)[2:] for x in os.urandom(10))
        self.secret = secret
        self.auth = auth_code(self.secret, self.sign)
        self.zombie_interval = 60 * zombie_interval
        if not scheduler_port:
            scheduler_port = 51347

        self.scheduler = {'ip_addr': None, 'port': scheduler_port, 'auth': set()}
        self.cpu_time = 0
        self.num_jobs = 0
        self.num_computations = 0

        with open(os.path.join(self.dest_path_prefix, 'config'), 'wb') as fd:
            config = {
                'ext_ip_addr': self.ext_ip_addr, 'port': self.port, 'avail_cpus': self.avail_cpus,
                'sign': self.sign, 'secret': self.secret, 'auth': self.auth
                }
            pickle.dump(config, fd)

        # prepend current directory in sys.path so computations can
        # load modules from current working directory
        sys.path.insert(0, '.')

        # start a process so all modules needed by dispynode are loaded
        proc = multiprocessing.Process(target=functools.partial(int), args=(42,))
        proc.start()
        proc.join()

        self.thread_lock = threading.Lock()
        self.udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.udp_sock.bind(('', self.port))
        _dispy_logger.info('serving %s cpus at %s:%s',
                           self.num_cpus, self.ext_ip_addr, self.port)
        _dispy_logger.debug('tcp server at %s:%s', self.address[0], self.address[1])
        self.udp_sock = AsyncSocket(self.udp_sock)

        self.reply_Q = multiprocessing.Queue()
        self.reply_Q_thread = threading.Thread(target=self.__reply_Q)
        self.reply_Q_thread.start()

        self.serve = serve
        self.timer_coro = Coro(self.timer_task)
        self.service_start = self.service_stop = self.service_end = None
        if isinstance(service_start, time.struct_time) and \
           (isinstance(service_stop, time.struct_time) or
           isinstance(service_end, time.struct_time)):
            self.service_start = (service_start.tm_hour, service_start.tm_min)
            if isinstance(service_stop, time.struct_time):
                self.service_stop = (service_stop.tm_hour, service_stop.tm_min)
            if isinstance(service_end, time.struct_time):
                self.service_end = (service_end.tm_hour, service_end.tm_min)
            Coro(self.service_schedule)

        self.__init_code = ''.join(inspect.getsource(dispy_provisional_result))
        self.__init_code += ''.join(inspect.getsource(dispy_send_file))
        self.__init_modules = dict(sys.modules)
        if os.name == 'nt':
            self.__init_globals = dict(globals())
        self.tcp_coro = Coro(self.tcp_server)
        self.udp_coro = Coro(self.udp_server, _node_ipaddr(scheduler_node), scheduler_port)
        if not daemon:
            Coro(self.read_stdin)

    def broadcast_ping_msg(self, coro=None):
        if (self.scheduler['ip_addr'] or self.job_infos or not self.avail_cpus or
           not self.service_available()):
            raise StopIteration
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        sock = AsyncSocket(sock)
        sock.settimeout(MsgTimeout)
        ping_msg = {'ip_addr': self.ext_ip_addr, 'port': self.port, 'sign': self.sign,
                    'version': _dispy_version, 'scheduler_ip_addr': None}
        try:
            yield sock.sendto('PING:'.encode() + serialize(ping_msg),
                              ('<broadcast>', self.scheduler['port']))
        except:
            _dispy_logger.debug(traceback.format_exc())
            pass
        sock.close()

    def send_pong_msg(self, info, addr, coro=None):
        if (self.scheduler['ip_addr'] or self.job_infos or not self.num_cpus or
           not self.service_available()):
            _dispy_logger.debug('Busy (%s/%s); ignoring ping message from %s',
                                self.avail_cpus, self.num_cpus, addr[0])
            raise StopIteration
        try:
            scheduler_ip_addrs = info['ip_addrs']
            if not info.get('relay', None):
                scheduler_ip_addrs.append(addr[0])
            scheduler_port = info['port']
        except:
            _dispy_logger.debug(traceback.format_exc())
            raise StopIteration

        if info.get('sign', None):
            pong_msg = {'ip_addr': self.ext_ip_addr, 'port': self.port, 'sign': self.sign,
                        'version': _dispy_version, 'name': self.name, 'cpus': self.avail_cpus,
                        'auth': auth_code(self.secret, info['sign'])}
            if psutil:
                pong_msg['avail_info'] = DispyNodeAvailInfo(
                    100.0 - psutil.cpu_percent(), psutil.virtual_memory().available,
                    psutil.disk_usage(self.dest_path_prefix).free,
                    100.0 - psutil.swap_memory().percent)
            else:
                pong_msg['avail_info'] = None

            for scheduler_ip_addr in scheduler_ip_addrs:
                addr = (scheduler_ip_addr, scheduler_port)
                pong_msg['scheduler_ip_addr'] = scheduler_ip_addr
                sock = AsyncSocket(socket.socket(socket.AF_INET, socket.SOCK_STREAM),
                                   keyfile=self.keyfile, certfile=self.certfile)
                sock.settimeout(MsgTimeout)
                try:
                    yield sock.connect(addr)
                    yield sock.send_msg('PONG:'.encode() + serialize(pong_msg))
                except:
                    _dispy_logger.debug('Could not connect to %s:%s', addr[0], addr[1])
                finally:
                    sock.close()
        else:
            sock = AsyncSocket(socket.socket(socket.AF_INET, socket.SOCK_DGRAM))
            sock.settimeout(MsgTimeout)
            ping_msg = {'ip_addr': self.ext_ip_addr, 'port': self.port, 'sign': self.sign,
                        'version': _dispy_version}
            for scheduler_ip_addr in scheduler_ip_addrs:
                addr = (scheduler_ip_addr, scheduler_port)
                ping_msg['scheduler_ip_addr'] = scheduler_ip_addr
                try:
                    yield sock.sendto('PING:'.encode() + serialize(ping_msg), addr)
                except:
                    _dispy_logger.debug(traceback.format_exc())
                    pass
            sock.close()

    def udp_server(self, scheduler_ip, scheduler_port, coro=None):
        coro.set_daemon()
        yield self.broadcast_ping_msg(coro=coro)
        ping_msg = {'ip_addr': self.ext_ip_addr, 'port': self.port, 'sign': self.sign,
                    'version': _dispy_version}

        def send_ping_msg(self, info, coro=None):
            sock = AsyncSocket(socket.socket(socket.AF_INET, socket.SOCK_DGRAM))
            sock.settimeout(MsgTimeout)
            addr = (info['ip_addr'], info['port'])
            info.update(ping_msg)
            info['scheduler_ip_addr'] = addr[0]
            try:
                yield sock.sendto('PING:'.encode() + serialize(info), addr)
            except:
                pass
            finally:
                sock.close()
            sock = AsyncSocket(socket.socket(socket.AF_INET, socket.SOCK_STREAM),
                               keyfile=self.keyfile, certfile=self.certfile)
            sock.settimeout(MsgTimeout)
            try:
                yield sock.connect(addr)
                yield sock.send_msg('PING:'.encode() + serialize(info))
            except:
                pass
            finally:
                sock.close()

        if scheduler_ip:
            Coro(send_ping_msg, self, {'ip_addr': scheduler_ip, 'port': scheduler_port})

        while True:
            msg, addr = yield self.udp_sock.recvfrom(1000)
            # TODO: process each message as separate Coro, so
            # exceptions are contained?
            if msg.startswith(b'PING:'):
                try:
                    info = unserialize(msg[len(b'PING:'):])
                    if info['version'] != _dispy_version:
                        _dispy_logger.warning('Ignoring %s due to version mismatch', addr[0])
                        continue
                except:
                    _dispy_logger.debug('Ignoring ping message from %s (%s)', addr[0], addr[1])
                    continue
                Coro(self.send_pong_msg, info, addr)
            elif msg.startswith(b'PULSE:'):
                try:
                    info = unserialize(msg[len(b'PULSE:'):])
                except:
                    _dispy_logger.warning('Ignoring PULSE from %s', addr[0])
                else:
                    if info['ip_addr'] == self.scheduler['ip_addr']:
                        now = time.time()
                        for compute in self.computations.values():
                            compute.last_pulse = now
            else:
                _dispy_logger.warning('Ignoring ping message from %s', addr[0])

    def tcp_server(self):
        while 1:
            try:
                conn, addr = yield self.tcp_sock.accept()
            except GeneratorExit:
                break
            except:
                _dispy_logger.debug(traceback.format_exc())
                continue
            Coro(self.tcp_serve_task, conn, addr)

    def tcp_serve_task(self, conn, addr, coro=None):
        def job_request_task(msg):
            try:
                _job = unserialize(msg)
            except:
                _dispy_logger.debug('Ignoring job request from %s', addr[0])
                # _dispy_logger.debug(traceback.format_exc())
                raise StopIteration

            compute = self.computations.get(_job.compute_id, None)
            if compute is not None:
                if compute.scheduler_ip_addr != self.scheduler['ip_addr'] or \
                   compute.scheduler_port != self.scheduler['port'] or \
                   compute.auth not in self.scheduler['auth']:
                    _dispy_logger.debug('Invalid scheduler IP address: scheduler %s:%s != %s:%s',
                                        compute.scheduler_ip_addr, compute.scheduler_port,
                                        self.scheduler['ip_addr'], self.scheduler['port'])
                    compute = None
            if self.avail_cpus == 0:
                try:
                    yield conn.send_msg('NAK (all cpus busy)'.encode())
                except:
                    pass
                raise StopIteration
            elif compute is None:
                _dispy_logger.warning('Invalid computation %s', _job.compute_id)
                try:
                    yield conn.send_msg(('NAK (invalid computation %s)' %
                                         _job.compute_id).encode())
                except:
                    pass
                raise StopIteration

            for xf in _job.xfer_files:
                if MaxFileSize and xf.stat_buf.st_size > MaxFileSize:
                    try:
                        yield conn.send_msg('NAK'.encode())
                    except:
                        pass
                    raise StopIteration

            reply_addr = (compute.scheduler_ip_addr, compute.job_result_port)
            _dispy_logger.debug('New job id %s from %s/%s',
                                _job.uid, addr[0], compute.scheduler_ip_addr)

            if compute.type == _Compute.func_type:
                reply = _JobReply(_job, self.ext_ip_addr)
                reply.start_time = time.time()
                job_info = _DispyJobInfo(reply, reply_addr, compute, _job.xfer_files)

                args = (job_info, self.certfile, self.keyfile, compute.name,
                        _job.args, _job.kwargs, (compute.code, _job.code),
                        compute.globals, compute.dest_path, self.reply_Q)
                try:
                    yield conn.send_msg(b'ACK')
                except:
                    _dispy_logger.warning('Failed to send response for new job to %s', str(addr))
                    raise StopIteration
                proc = multiprocessing.Process(target=_dispy_job_func, args=args)
                self.avail_cpus -= 1
                compute.pending_jobs += 1
                self.thread_lock.acquire()
                self.job_infos[_job.uid] = job_info
                self.thread_lock.release()
                try:
                    proc.start()
                except:
                    job_info.job_reply.status = DispyJob.Terminated
                    job_info.job_reply.exception = traceback.format_exc()
                    job_info.job_reply.end_time = time.time()
                    job_info.proc = None
                    self.reply_Q.put(job_info.job_reply)
                else:
                    job_info.proc = proc
                    job_info.job_reply.status = DispyJob.Running
                raise StopIteration
            elif compute.type == _Compute.prog_type:
                try:
                    yield conn.send_msg(b'ACK')
                except:
                    _dispy_logger.warning('Failed to send response for new job to %s', str(addr))
                    raise StopIteration
                reply = _JobReply(_job, self.ext_ip_addr)
                reply.start_time = time.time()
                job_info = _DispyJobInfo(reply, reply_addr, compute, _job.xfer_files)
                job_info.job_reply.status = DispyJob.Running
                self.thread_lock.acquire()
                self.job_infos[_job.uid] = job_info
                self.thread_lock.release()
                self.avail_cpus -= 1
                compute.pending_jobs += 1
                prog_thread = threading.Thread(target=self.__job_program, args=(_job, job_info))
                prog_thread.start()
                raise StopIteration
            else:
                try:
                    yield conn.send_msg(('NAK (invalid computation type "%s")' %
                                         compute.type).encode())
                except:
                    _dispy_logger.warning('Failed to send response for new job to %s', str(addr))

        def add_computation_task(msg):
            try:
                compute = unserialize(msg)
            except:
                try:
                    yield conn.send_msg(('Invalid computation request ignored').encode())
                except:
                    pass
                raise StopIteration
            if not ((self.scheduler['ip_addr'] is None and not self.scheduler['auth']) or
                    (self.scheduler['ip_addr'] == compute.scheduler_ip_addr and
                     self.scheduler['port'] == compute.scheduler_port and
                     self.service_available())):
                _dispy_logger.debug('Ignoring computation request from %s: %s, %s, %s',
                                    compute.scheduler_ip_addr, self.scheduler['ip_addr'],
                                    self.avail_cpus, self.num_cpus)
                try:
                    yield conn.send_msg(('Node busy').encode())
                except:
                    pass
                raise StopIteration

            if MaxFileSize:
                for xf in compute.xfer_files:
                    if xf.stat_buf.st_size > MaxFileSize:
                        try:
                            yield conn.send_msg(('File "%s" is too big; limit is %s' %
                                                 (xf.name, MaxFileSize)).encode())
                        except:
                            pass
                        raise StopIteration
            compute.xfer_files = set()
            dest = os.path.join(self.dest_path_prefix, compute.scheduler_ip_addr)
            if not os.path.isdir(dest):
                try:
                    os.mkdir(dest)
                except:
                    yield conn.send_msg(('Could not create destination path').encode())
                    raise StopIteration
            if compute.dest_path and isinstance(compute.dest_path, str):
                # TODO: get os.sep from client and convert (in case of mixed environments)?
                if not compute.dest_path.startswith(os.sep):
                    compute.dest_path = os.path.join(dest, compute.dest_path)
                if not os.path.isdir(compute.dest_path):
                    try:
                        os.makedirs(compute.dest_path)
                    except:
                        try:
                            yield conn.send_msg(('Could not create destination path').encode())
                        except:
                            pass
                        raise StopIteration
            else:
                compute.dest_path = tempfile.mkdtemp(prefix=compute.name + '_', dir=dest)
            os.chmod(compute.dest_path, stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)

            if compute.id in self.computations:
                _dispy_logger.warning('Computation "%s" (%s) is being replaced',
                                      compute.name, compute.id)
            setattr(compute, 'last_pulse', time.time())
            setattr(compute, 'pending_jobs', 0)
            setattr(compute, 'pending_results', 0)
            setattr(compute, 'zombie', False)
            setattr(compute, 'globals', {})
            setattr(compute, 'ante_modules', set(sys.modules.keys()))
            setattr(compute, 'file_uses', {})

            if compute.code:
                try:
                    code = compute.code
                    code += self.__init_code
                    code = compile(code, '<string>', 'exec')
                except:
                    if os.path.isdir(compute.dest_path):
                        os.rmdir(compute.dest_path)
                    try:
                        yield conn.send_msg(('%s: Computation "%s" could not be compiled' %
                                             (self.ext_ip_addr, compute.name)).encode())
                    except:
                        pass
                    raise StopIteration
                compute.code = marshal.dumps(code)

            if compute.type == _Compute.prog_type:
                compute.name = os.path.join(compute.dest_path, os.path.basename(compute.name))

            if not ((self.scheduler['ip_addr'] is None) or
                    (self.scheduler['ip_addr'] == compute.scheduler_ip_addr and
                     self.scheduler['port'] == compute.scheduler_port)):
                if os.path.isdir(compute.dest_path):
                    try:
                        os.rmdir(compute.dest_path)
                        yield conn.send_msg(serialize(-1))
                    except:
                        pass
                raise StopIteration

            self.computations[compute.id] = compute
            self.scheduler['ip_addr'] = compute.scheduler_ip_addr
            self.scheduler['port'] = compute.scheduler_port
            self.scheduler['auth'].add(compute.auth)
            compute_save = os.path.join(self.dest_path_prefix, '%s_%s' % (compute.id, compute.auth))
            with open(compute_save, 'wb') as fd:
                pickle.dump(compute, fd)

            # add variables needed for 'dispy_provisional_result' and
            # 'dispy_send_file' to compute.globals; but in Windows
            # compute.globals can't be passed via multiprocessing.Process
            if os.name == 'nt':
                compute.globals = {}
            else:
                for var in ('AsyncSocket', 'DispyJob', 'serialize', 'unserialize', '_XferFile',
                            'MaxFileSize', 'MsgTimeout'):
                    compute.globals[var] = globals()[var]
                compute.globals.update(self.__init_modules)
            compute.globals['_DispyNode'] = None

            try:
                yield conn.send_msg(serialize(self.avail_cpus))
            except:
                del self.computations[compute.id]
                compute.globals = {}
                self.scheduler['ip_addr'] = None
                self.scheduler['auth'].discard(compute.auth)
                os.remove(compute_save)
                if os.path.isdir(compute.dest_path):
                    try:
                        os.rmdir(compute.dest_path)
                    except:
                        pass
            else:
                self.pulse_interval = num_min(self.pulse_interval, compute.pulse_interval)
                if not self.pulse_interval:
                    self.pulse_interval = 10 * 60
                if self.zombie_interval:
                    self.pulse_interval = num_min(self.pulse_interval, self.zombie_interval / 5.0)
                self.timer_coro.resume(True)

        def xfer_file_task(msg):
            try:
                xf = unserialize(msg)
            except:
                _dispy_logger.debug('Ignoring file trasnfer request from %s', addr[0])
                raise StopIteration

            compute = self.computations.get(xf.compute_id, None)
            if not compute or (MaxFileSize and xf.stat_buf.st_size > MaxFileSize):
                _dispy_logger.error('Invalid file transfer for "%s"', xf.name)
                yield conn.send_msg(serialize(-1))
                raise StopIteration
            if xf.destination is None:
                # Remove the source file's directory hiearchy.
                tgt = os.path.join(compute.dest_path, os.path.basename(xf.name))
            else:
                # Retain the source file's needed directory hiearchy.
                tgt = os.path.join(compute.dest_path, xf.destination)
            if os.path.isfile(tgt) and _same_file(tgt, xf):
                if tgt in compute.file_uses:
                    compute.file_uses[tgt] += 1
                else:
                    compute.file_uses[tgt] = 2
                yield conn.send_msg(serialize(xf.stat_buf.st_size))
            else:
                try:
                    if xf.destination is not None:
                        # Create missing directories
                        os.makedirs(os.path.dirname(tgt), exist_ok=True)
                    with open(tgt, 'wb') as fd:
                        recvd = 0
                        _dispy_logger.debug('Copying file %s to %s (%s)',
                                            xf.name, tgt, xf.stat_buf.st_size)
                        while recvd < xf.stat_buf.st_size:
                            yield conn.send_msg(serialize(recvd))
                            data = yield conn.recvall(min(xf.stat_buf.st_size-recvd, 1024000))
                            if not data:
                                break
                            fd.write(data)
                            recvd += len(data)
                        yield conn.send_msg(serialize(recvd))
                    _dispy_logger.debug('Copied file %s, %s / %s',
                                        tgt, recvd, xf.stat_buf.st_size)
                    assert recvd == xf.stat_buf.st_size
                    os.utime(tgt, (xf.stat_buf.st_atime, xf.stat_buf.st_mtime))
                    os.chmod(tgt, stat.S_IMODE(xf.stat_buf.st_mode))
                except:
                    _dispy_logger.warning('Copying file "%s" failed with "%s"',
                                          xf.name, traceback.format_exc())
                    os.remove(tgt)
                else:
                    if tgt in compute.file_uses:
                        compute.file_uses[tgt] += 1
                    else:
                        compute.file_uses[tgt] = 1
            raise StopIteration  # xfer_file_task

        def setup_computation(msg):
            try:
                compute_id = unserialize(msg)
                compute = self.computations[compute_id]
                assert isinstance(compute.setup, _Function)
                os.chdir(compute.dest_path)
                localvars = {'_dispy_setup_args': compute.setup.args,
                             '_dispy_setup_kwargs': compute.setup.kwargs}
                if os.name == 'nt':
                    globalvars = globals()
                else:
                    globalvars = compute.globals
                exec(marshal.loads(compute.code), globalvars, localvars)
                exec('assert %s(*_dispy_setup_args, **_dispy_setup_kwargs) == 0' %
                     compute.setup.name, globalvars, localvars)
                if os.name == 'nt':
                    compute.globals.update({var: globals()[var] for var in globals()
                                            if var not in self.__init_globals})
            except:
                _dispy_logger.debug('Setup failed')
                resp = traceback.format_exc().encode()
            else:
                resp = b'ACK'
            if resp != b'ACK':
                if not compute.cleanup:
                    compute.cleanup = True
                compute.zombie = True
                self.cleanup_computation(compute)
            yield conn.send_msg(resp)

        def terminate_job_task(compute, job_info):
            if not job_info.proc:
                raise StopIteration
            _dispy_logger.debug('Terminating job %s of "%s"',
                                job_info.job_reply.uid, compute.name)
            job_info.proc.terminate()
            if isinstance(job_info.proc, multiprocessing.Process):
                for x in range(20):
                    if job_info.proc.is_alive():
                        yield coro.sleep(0.1)
                    else:
                        _dispy_logger.debug('Process "%s" for job %s terminated',
                                            compute.name, job_info.job_reply.uid)
                        break
                else:
                    _dispy_logger.warning('Could not kill process %s', compute.name)
                    raise StopIteration
            else:
                assert isinstance(job_info.proc, subprocess.Popen)
                for x in range(20):
                    rc = job_info.proc.poll()
                    _dispy_logger.debug('Program "%s" for job %s terminated with %s',
                                        compute.name, job_info.job_reply.uid, rc)
                    if rc is not None:
                        break
                    if x == 10:
                        _dispy_logger.debug('Killing job %s', job_info.job_reply.uid)
                        job_info.proc.kill()
                    yield coro.sleep(0.1)
                else:
                    _dispy_logger.warning('Could not kill process %s', compute.name)
                    raise StopIteration
            job_info.job_reply.end_time = time.time()
            job_info.proc = None
            self.thread_lock.acquire()
            if self.job_infos.get(job_info.job_reply.uid, None) == job_info:
                job_info.job_reply.status = DispyJob.Terminated
                self.reply_Q.put(job_info.job_reply)
            self.thread_lock.release()

        def retrieve_job_task(msg):
            # generator

            def send_reply(reply):
                try:
                    yield conn.send_msg(serialize(reply))
                except:
                    raise StopIteration(-1)
                raise StopIteration(0)

            try:
                req = unserialize(msg)
                uid = req['uid']
                compute_id = req['compute_id']
                auth = req['auth']
                job_hash = req['hash']
            except:
                yield send_reply(None)
                raise StopIteration

            pkl_path = os.path.join(self.dest_path_prefix, '%s_%s' % (compute_id, auth))
            compute = self.computations.get(compute_id, None)
            if not compute:
                with open(pkl_path, 'rb') as fd:
                    compute = pickle.load(fd)
            if not compute or compute.auth != auth:
                yield send_reply(None)
                raise StopIteration

            info_file = os.path.join(compute.dest_path, '_dispy_job_reply_%s' % uid)
            if not os.path.isfile(info_file):
                yield send_reply(None)
                raise StopIteration
            try:
                with open(info_file, 'rb') as fd:
                    job_reply = pickle.load(fd)
                assert job_reply.hash == job_hash
            except:
                yield send_reply(None)
                raise StopIteration

            try:
                yield conn.send_msg(serialize(job_reply))
                ack = yield conn.recv_msg()
                assert ack == b'ACK'
                compute.pending_results -= 1
                with open(pkl_path, 'wb') as fd:
                    pickle.dump(compute, fd)
            except:
                pass
            else:
                try:
                    os.remove(info_file)
                except:
                    pass
                if compute.pending_results == 0:
                    self.cleanup_computation(compute)

        # tcp_serve_task starts
        try:
            req = yield conn.recvall(len(self.auth))
        except:
            _dispy_logger.warning('Ignoring request; invalid client authentication?')
            conn.close()
            raise StopIteration
        msg = yield conn.recv_msg()
        if req != self.auth:
            if msg.startswith(b'PING:'):
                pass
            else:
                _dispy_logger.warning('Ignoring request; invalid client authentication?')
                conn.close()
                raise StopIteration
        if not msg:
            conn.close()
            raise StopIteration
        if msg.startswith(b'JOB:'):
            msg = msg[len(b'JOB:'):]
            yield job_request_task(msg)
            conn.close()
        elif msg.startswith(b'COMPUTE:'):
            msg = msg[len(b'COMPUTE:'):]
            yield add_computation_task(msg)
            conn.close()
        elif msg.startswith(b'FILEXFER:'):
            msg = msg[len(b'FILEXFER:'):]
            yield xfer_file_task(msg)
            conn.close()
        elif msg.startswith(b'SETUP:'):
            msg = msg[len(b'SETUP:'):]
            yield setup_computation(msg)
            conn.close()
        elif msg.startswith(b'CLOSE:'):
            msg = msg[len(b'CLOSE:'):]
            try:
                info = unserialize(msg)
                compute_id = info['compute_id']
                auth = info['auth']
                terminate_pending = info.get('terminate_pending', False)
            except:
                _dispy_logger.debug('Deleting computation failed with %s', traceback.format_exc())
            else:
                compute = self.computations.get(compute_id, None)
                if compute is None or compute.auth != auth:
                    _dispy_logger.warning('Computation "%s" is not valid', compute_id)
                else:
                    compute.zombie = True
                    if terminate_pending:
                        self.thread_lock.acquire()
                        job_infos = [job_info for job_info in self.job_infos.values()
                                     if job_info.compute_id == compute_id]
                        self.thread_lock.release()
                        for job_info in job_infos:
                            yield terminate_job_task(compute, job_info)
                    self.cleanup_computation(compute)
            yield conn.send_msg(b'ACK')
            conn.close()
        elif msg.startswith(b'TERMINATE_JOB:'):
            msg = msg[len(b'TERMINATE_JOB:'):]
            try:
                _job = unserialize(msg)
                compute = self.computations[_job.compute_id]
                # assert addr[0] == compute.scheduler_ip_addr
                self.thread_lock.acquire()
                job_info = self.job_infos.get(_job.uid, None)
                self.thread_lock.release()
                assert job_info is not None
            except:
                _dispy_logger.debug('Invalid terminate job request from %s, %s',
                                    addr[0], compute.scheduler_ip_addr)
            else:
                yield terminate_job_task(compute, job_info)
            conn.close()
        elif msg.startswith(b'RESEND_JOB_RESULTS:'):
            msg = msg[len(b'RESEND_JOB_RESULTS:'):]
            try:
                info = unserialize(msg)
                compute_id = info['compute_id']
                auth = info['auth']
            except:
                reply = 0
            else:
                compute = self.computations.get(compute_id, None)
                if compute is None or compute.auth != auth:
                    try:
                        with open(os.path.join(self.dest_path_prefix,
                                               '%s_%s' % (compute_id, auth)), 'rb') as fd:
                            compute = pickle.load(fd)
                    except:
                        pass
                if compute is None:
                    reply = 0
                else:
                    reply = compute.pending_results + compute.pending_jobs
            yield conn.send_msg(serialize(reply))
            conn.close()
            if reply > 0:
                yield self.resend_job_results(compute, coro=coro)
        elif msg.startswith(b'PING:'):
            try:
                info = unserialize(msg[len(b'PING:'):])
                if (info['version'] == _dispy_version and
                   not self.scheduler['ip_addr'] and not self.job_infos):
                    Coro(self.send_pong_msg, info, addr)
            except:
                _dispy_logger.debug(traceback.format_exc())
            conn.close()
        elif msg.startswith(b'PENDING_JOBS:'):
            msg = msg[len(b'PENDING_JOBS:'):]
            reply = {'done': [], 'pending': 0}
            try:
                info = unserialize(msg)
                compute_id = info['compute_id']
                auth = info['auth']
            except:
                pass
            else:
                compute = self.computations.get(compute_id, None)
                if compute is None or compute.auth != auth:
                    with open(os.path.join(self.dest_path_prefix,
                                           '%s_%s' % (compute_id, auth)), 'rb') as fd:
                        compute = pickle.load(fd)
                if compute is not None:
                    done = []
                    if compute.pending_results:
                        for result_file in glob.glob(os.path.join(compute.dest_path,
                                                                  '_dispy_job_reply_*')):
                            result_file = os.path.basename(result_file)
                            try:
                                uid = int(result_file[len('_dispy_job_reply_'):])
                            except:
                                pass
                            else:
                                done.append(uid)
                                # limit so as not to take up too much time
                                if len(done) > 50:
                                    break
                    reply['done'] = done
                    reply['pending'] = compute.pending_jobs
            yield conn.send_msg(serialize(reply))
            conn.close()
        elif msg.startswith(b'RETRIEVE_JOB:'):
            msg = msg[len(b'RETRIEVE_JOB:'):]
            yield retrieve_job_task(msg)
            conn.close()
        else:
            _dispy_logger.warning('Invalid request "%s" from %s',
                                  msg[:min(10, len(msg))], addr[0])
            resp = ('NAK (invalid command: %s)' % (msg[:min(10, len(msg))])).encode()
            try:
                yield conn.send_msg(resp)
            except:
                _dispy_logger.warning('Failed to send reply to %s', str(addr))
            conn.close()

    def resend_job_results(self, compute, coro=None):
        # TODO: limit number queued so as not to take up too much space/time
        if not os.path.isdir(compute.dest_path):
            raise StopIteration
        result_files = [f for f in os.listdir(compute.dest_path)
                        if f.startswith('_dispy_job_reply_')]
        result_files = result_files[:min(len(result_files), 64)]
        for result_file in result_files:
            result_file = os.path.join(compute.dest_path, result_file)
            try:
                with open(result_file, 'rb') as fd:
                    job_result = pickle.load(fd)
            except:
                _dispy_logger.debug('Could not load "%s"', result_file)
                # _dispy_logger.debug(traceback.format_exc())
                continue
            job_info = _DispyJobInfo(job_result, (compute.scheduler_ip_addr,
                                                  compute.job_result_port), compute, [])
            status = yield self._send_job_reply(job_info, resending=True)
            if status:
                break

    def timer_task(self, coro=None):
        coro.set_daemon()
        last_pulse_time = last_zombie_time = time.time()
        while True:
            reset = yield coro.suspend(self.pulse_interval)
            if reset:
                continue

            now = time.time()
            if self.pulse_interval and (now - last_pulse_time) >= self.pulse_interval:
                if self.scheduler['ip_addr']:
                    last_pulse_time = now
                    sock = AsyncSocket(socket.socket(socket.AF_INET, socket.SOCK_DGRAM))
                    sock.settimeout(MsgTimeout)
                    info = {'ip_addr': self.ext_ip_addr, 'port': self.port,
                            'cpus': self.num_cpus - self.avail_cpus,
                            'scheduler_ip_addr': self.scheduler['ip_addr']}
                    if psutil:
                        info['avail_info'] = DispyNodeAvailInfo(
                            100.0 - psutil.cpu_percent(), psutil.virtual_memory().available,
                            psutil.disk_usage(self.dest_path_prefix).free,
                            100.0 - psutil.swap_memory().percent)
                    else:
                        info['avail_info'] = None

                    yield sock.sendto(b'PULSE:' + serialize(info),
                                      (self.scheduler['ip_addr'], self.scheduler['port']))
                    sock.close()

                resend = [compute for compute in self.computations.values()
                          if compute.pending_results and not compute.zombie]
                for compute in resend:
                    Coro(self.resend_job_results, compute)

            if self.zombie_interval and (now - last_zombie_time) >= self.zombie_interval:
                last_zombie_time = now
                for compute in self.computations.values():
                    if (now - compute.last_pulse) > self.zombie_interval:
                        _dispy_logger.warning('Computation "%s" is marked as zombie', compute.name)
                        compute.zombie = True
                zombies = [compute for compute in self.computations.values()
                           if compute.zombie and compute.pending_jobs == 0]
                for compute in zombies:
                    _dispy_logger.warning('Deleting zombie computation "%s"', compute.name)
                    self.cleanup_computation(compute)
                for compute in zombies:
                    sock = AsyncSocket(socket.socket(socket.AF_INET, socket.SOCK_STREAM),
                                       keyfile=self.keyfile, certfile=self.certfile)
                    sock.settimeout(MsgTimeout)
                    _dispy_logger.debug('Sending TERMINATE to %s', compute.scheduler_ip_addr)
                    info = {'ip_addr': self.ext_ip_addr, 'port': self.port, 'sign': self.sign}
                    try:
                        yield sock.connect((compute.scheduler_ip_addr, compute.scheduler_port))
                        yield sock.send_msg('TERMINATED:'.encode() + serialize(info))
                    except:
                        pass
                    finally:
                        sock.close()
                if (not self.scheduler['ip_addr'] and not self.job_infos and self.avail_cpus > 0):
                    self.pulse_interval = None
                    yield self.broadcast_ping_msg(coro=coro)

    def service_available(self):
        if self.serve == 0:
            return False
        if not self.service_start or not self.service_end:
            return True
        now = time.localtime()
        if self.service_stop:
            end = self.service_stop
        else:
            end = self.service_end
        if self.service_start < end:
            if self.service_start <= (now.tm_hour, now.tm_min) < end:
                return True
        else:
            if (now.tm_hour, now.tm_min) >= self.service_start or \
               (now.tm_hour, now.tm_min) < end:
                return True
        return False

    def service_schedule(self, coro=None):
        coro.set_daemon()
        while True:
            yield coro.sleep(60)
            if self.service_available():
                yield self.broadcast_ping_msg(coro=coro)
            else:
                if self.scheduler['ip_addr']:
                    now = time.localtime()
                    if self.service_end and (now.tm_hour, now.tm_min) > self.service_end:
                        _dispy_logger.debug('Shutting down service')
                        self.shutdown(quit=False)
                    else:
                        _dispy_logger.debug('Stopping service')
                        sock = AsyncSocket(socket.socket(socket.AF_INET, socket.SOCK_STREAM),
                                           keyfile=self.keyfile, certfile=self.certfile)
                        sock.settimeout(MsgTimeout)
                        try:
                            yield sock.connect((self.scheduler['ip_addr'], self.scheduler['port']))
                            info = {'ip_addr': self.ext_ip_addr, 'sign': self.sign, 'cpus': 0}
                            yield sock.send_msg('NODE_CPUS:'.encode() + serialize(info))
                        except:
                            pass
                        finally:
                            sock.close()

    def __job_program(self, _job, job_info):
        compute = self.computations[_job.compute_id]
        if compute.name.endswith('.py'):
            program = [sys.executable, compute.name]
        else:
            program = [compute.name]
        args = unserialize(_job.args)
        program.extend(args)
        reply = job_info.job_reply
        try:
            os.chdir(compute.dest_path)
            env = {}
            env.update(os.environ)
            env['PATH'] = compute.dest_path + os.pathsep + env['PATH']
            job_info.proc = subprocess.Popen(program, stdout=subprocess.PIPE,
                                             stderr=subprocess.PIPE, env=env)

            assert isinstance(job_info.proc, subprocess.Popen)
            reply.stdout, reply.stderr = job_info.proc.communicate()
            reply.result = job_info.proc.returncode
            reply.status = DispyJob.Finished
        except:
            reply.exception = traceback.format_exc()
            reply.status = DispyJob.Terminated
        reply.end_time = time.time()
        job_info.proc = None
        self.reply_Q.put(reply)

    def __reply_Q(self):
        while True:
            job_reply = self.reply_Q.get()
            if job_reply is None:
                break
            self.thread_lock.acquire()
            job_info = self.job_infos.get(job_reply.uid, None)
            if job_info is not None:
                job_info.job_reply = job_reply
            self.thread_lock.release()
            if job_info is not None:
                self.num_jobs += 1
                self.cpu_time += (job_reply.end_time - job_reply.start_time)
                if job_info.proc is not None:
                    if isinstance(job_info.proc, multiprocessing.Process):
                        job_info.proc.join(2)
                    else:
                        job_info.proc.wait()
                Coro(self._send_job_reply, job_info, resending=False)
                compute = self.computations.get(job_info.compute_id, None)
                if not compute:
                    continue
                for xf in job_info.xfer_files:
                    path = os.path.join(compute.dest_path, os.path.basename(xf.name))
                    try:
                        compute.file_uses[path] -= 1
                        if compute.file_uses[path] == 0:
                            compute.file_uses.pop(path)
                            os.remove(path)
                    except:
                        _dispy_logger.warning('invalid file "%s" ignored', path)
                        continue

    def _send_job_reply(self, job_info, resending=False, coro=None):
        """Internal use only.
        """
        job_reply = job_info.job_reply
        _dispy_logger.debug('Sending result for job %s (%s) to %s',
                            job_reply.uid, job_reply.status, str(job_info.reply_addr))
        compute = self.computations.get(job_info.compute_id, None)
        if not resending:
            self.thread_lock.acquire()
            assert self.job_infos.pop(job_reply.uid, None) is not None
            self.thread_lock.release()
            self.avail_cpus += 1
            assert self.avail_cpus <= self.num_cpus
            if compute:
                compute.pending_jobs -= 1

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock = AsyncSocket(sock, keyfile=self.keyfile, certfile=self.certfile)
        sock.settimeout(MsgTimeout)
        try:
            yield sock.connect(job_info.reply_addr)
            yield sock.send_msg(b'JOB_REPLY:' + serialize(job_reply))
            ack = yield sock.recv_msg()
            assert ack == b'ACK'
        except:
            status = -1
            if not resending:
                # store job result so it can be sent when client is
                # reachable or recovered by user
                f = os.path.join(job_info.compute_dest_path, '_dispy_job_reply_%s' % job_reply.uid)
                _dispy_logger.error('Could not send reply for job %s to %s; saving it in "%s"',
                                    job_reply.uid, str(job_info.reply_addr), f)
                try:
                    with open(f, 'wb') as fd:
                        pickle.dump(job_reply, fd)
                except:
                    _dispy_logger.debug('Could not save reply for job %s', job_reply.uid)
                else:
                    if compute is not None:
                        compute.pending_results += 1
        else:
            status = 0

            if compute:
                compute.last_pulse = time.time()
                if resending:
                    compute.pending_results -= 1
                elif compute.pending_results:
                    Coro(self.resend_job_results, compute)

            if resending:
                f = os.path.join(job_info.compute_dest_path,
                                 '_dispy_job_reply_%s' % job_reply.uid)
                if os.path.isfile(f):
                    try:
                        os.remove(f)
                    except:
                        _dispy_logger.warning('Could not remove "%s"', f)
                if compute is None:
                    with open(os.path.join(self.dest_path_prefix,
                                           '%s_%s' % (job_info.compute_id, job_info.compute_auth)),
                              'rb') as fd:
                        compute = pickle.load(fd)
                    if compute:
                        compute.pending_results -= 1

        finally:
            sock.close()

        if compute and compute.pending_jobs == 0 and compute.zombie:
            self.cleanup_computation(compute)
        raise StopIteration(status)

    def cleanup_computation(self, compute):
        if not compute.zombie or compute.pending_jobs > 0:
            return
        if compute.pending_jobs != 0:
            _dispy_logger.debug('pending jobs for computation "%s"/%s: %s',
                                compute.name, compute.id, compute.pending_jobs)

        if self.computations.pop(compute.id, None) is None:
            _dispy_logger.warning('Invalid computation "%s" to cleanup ignored', compute.id)
            return

        self.num_computations += 1
        file_uses, compute.file_uses = compute.file_uses, {}
        globalvars, compute.globals = compute.globals, {}
        pkl_path = os.path.join(self.dest_path_prefix, '%s_%s' % (compute.id, compute.auth))
        if compute.pending_results == 0:
            try:
                os.remove(pkl_path)
            except:
                _dispy_logger.warning('Could not remove "%s"', pkl_path)
        else:
            with open(pkl_path, 'wb') as fd:
                pickle.dump(compute, fd)

        self.scheduler['auth'].discard(compute.auth)

        if ((not self.computations) and (not self.scheduler['auth']) and
           compute.scheduler_ip_addr == self.scheduler['ip_addr'] and
           compute.scheduler_port == self.scheduler['port']):
            self.scheduler['ip_addr'] = None
            self.pulse_interval = None
            self.timer_coro.resume(None)
            if self.serve > 0:
                self.serve -= 1
            Coro(self.broadcast_ping_msg)

        if compute.cleanup is False:
            if self.serve == 0:
                self.shutdown(quit=True)
            return
        os.chdir(self.dest_path_prefix)
        if isinstance(compute.cleanup, _Function):
            try:
                localvars = {'_dispy_cleanup_args': compute.cleanup.args,
                             '_dispy_cleanup_kwargs': compute.cleanup.kwargs}
                if os.name == 'nt':
                    globalvars = globals()
                exec(marshal.loads(compute.code), globalvars, localvars)
                exec('%s(*_dispy_cleanup_args, **_dispy_cleanup_kwargs)' %
                     compute.cleanup.name, globalvars, localvars)
            except:
                _dispy_logger.debug('Cleanup "%s" failed', compute.cleanup.name)
                _dispy_logger.debug(traceback.format_exc())

        if os.name == 'nt':
            for var in list(globals().keys()):
                if var not in self.__init_globals:
                    _dispy_logger.debug('Variable "%s" left behind by "%s" at %s is being removed',
                                        var, compute.name, compute.scheduler_ip_addr)
                    globals().pop(var, None)

            for var, value in self.__init_globals.items():
                if var in ('_dispy_node', '_dispy_conn', '_dispy_addr', '_dispy_config'):
                    continue
                if value != globals().get(var, None):
                    _dispy_logger.warning('Variable "%s" changed by "%s" at %s is being reset',
                                          var, compute.name, compute.scheduler_ip_addr)
                    globals()[var] = value

        for module in list(sys.modules.keys()):
            if module not in compute.ante_modules:
                sys.modules.pop(module, None)
        sys.modules.update(self.__init_modules)

        for path in os.listdir(compute.dest_path):
            path = os.path.join(compute.dest_path, path)
            if file_uses.get(path, 1) == 1:
                try:
                    if os.path.isfile(path) or os.path.islink(path):
                        os.remove(path)
                    elif os.path.isdir(path):
                        shutil.rmtree(path, ignore_errors=True)
                    else:
                        os.remove(path)
                except:
                    _dispy_logger.warning('Could not remove "%s"', path)

        if os.path.isdir(compute.dest_path) and \
           compute.dest_path.startswith(self.dest_path_prefix) and \
           len(os.listdir(compute.dest_path)) == 0:
            try:
                os.rmdir(compute.dest_path)
            except:
                _dispy_logger.warning('Could not remove directory "%s"', compute.dest_path)
            else:
                _dispy_logger.debug('Removed "%s"', compute.dest_path)

        if self.serve == 0:
            self.shutdown(quit=True)

    def shutdown(self, quit=True):
        def _shutdown(self, quit, coro=None):
            self.thread_lock.acquire()
            job_infos, self.job_infos = self.job_infos, {}
            if quit and self.reply_Q:
                self.reply_Q.put(None)
            self.scheduler['ip_addr'] = None
            self.scheduler['auth'] = set()
            self.avail_cpus += len(job_infos)
            if self.avail_cpus != self.num_cpus:
                _dispy_logger.warning('invalid cpus: %s / %s', self.avail_cpus, self.num_cpus)
            self.thread_lock.release()
            for uid, job_info in job_infos.items():
                job_info.proc.terminate()
                _dispy_logger.debug('process for %s is killed', uid)
                if isinstance(job_info.proc, multiprocessing.Process):
                    job_info.proc.join(2)
                else:
                    job_info.proc.wait()
            for cid, compute in list(self.computations.items()):
                sock = AsyncSocket(socket.socket(socket.AF_INET, socket.SOCK_STREAM),
                                   keyfile=self.keyfile, certfile=self.certfile)
                sock.settimeout(MsgTimeout)
                _dispy_logger.debug('Sending TERMINATE to %s', compute.scheduler_ip_addr)
                info = {'ip_addr': self.ext_ip_addr, 'port': self.port, 'sign': self.sign}
                try:
                    yield sock.connect((compute.scheduler_ip_addr, compute.scheduler_port))
                    yield sock.send_msg('TERMINATED:'.encode() + serialize(info))
                except:
                    pass
                sock.close()
                compute.pending_jobs = 0
                compute.zombie = True
                self.cleanup_computation(compute)
            if quit:
                self.tcp_coro.terminate()
                self.sign = ''

        if self.sign:
            Coro(_shutdown, self, quit)

    def read_stdin(self, coro=None):
        coro.set_daemon()
        thread_pool = asyncoro.AsyncThreadPool(1)
        while True:
            sys.stdout.write('\nEnter "quit" or "exit" to terminate dispynode,\n'
                             '  "stop" to stop service, "start" to restart service,\n'
                             '  "cpus" to change CPUs used, anything else to get status: ')
            sys.stdout.flush()
            try:
                cmd = yield thread_pool.async_task(input)
            except:
                continue

            cmd = cmd.strip().lower()
            if cmd in ('quit', 'exit'):
                break
            elif cmd in ('stop', 'start', 'cpus'):
                if cmd == 'stop':
                    cpus = 0
                elif cmd == 'start':
                    cpus = self.num_cpus
                elif cmd == 'cpus':
                    cpus = multiprocessing.cpu_count()
                    sys.stdout.write('Enter number of CPUs to use in range -%s to %s: ' %
                                     (cpus - 1, cpus))
                    sys.stdout.flush()
                    try:
                        cpus = yield thread_pool.async_task(input)
                        cpus = int(cpus)
                        if cpus >= 0:
                            assert cpus <= multiprocessing.cpu_count()
                        else:
                            cpus += multiprocessing.cpu_count()
                            assert cpus >= 0
                    except:
                        print('  Invalid cpus ignored')
                        continue
                    self.num_cpus = cpus

                self.avail_cpus = cpus - len(self.job_infos)

                if self.scheduler['ip_addr']:
                    sock = AsyncSocket(socket.socket(socket.AF_INET, socket.SOCK_STREAM),
                                       keyfile=self.keyfile, certfile=self.certfile)
                    sock.settimeout(MsgTimeout)
                    try:
                        yield sock.connect((self.scheduler['ip_addr'], self.scheduler['port']))
                        info = {'ip_addr': self.ext_ip_addr, 'sign': self.sign, 'cpus': cpus}
                        yield sock.send_msg('NODE_CPUS:'.encode() + serialize(info))
                    except:
                        pass
                    finally:
                        sock.close()
                else:
                    if self.num_cpus > 0:
                        Coro(self.broadcast_ping_msg)
            else:
                print('\n  Serving %d CPUs%s%s%s' %
                      (self.avail_cpus + len(self.job_infos),
                       ' from %s' % self.serivce_start if self.service_start else '',
                       ' to %s' % self.service_end if self.service_end else '',
                       ' for %d clients' % self.serve if self.serve > 0 else ''))
                print('  Completed:\n    %d Computations, %d jobs, %.3f sec CPU time' %
                      (self.num_computations, self.num_jobs, self.cpu_time))
                print('  Running:')
                for i, compute in enumerate(self.computations.values(), start=1):
                    print('    Client %s: %s @ %s running %s jobs' %
                          (i, compute.name, compute.scheduler_ip_addr, compute.pending_jobs))
                print('')
        self.shutdown(quit=True)


if __name__ == '__main__':
    import argparse
    import re

    _dispy_logger = asyncoro.Logger('dispynode')
    _dispy_logger.info('dispynode version %s', _dispy_version)

    parser = argparse.ArgumentParser()
    parser.add_argument('--config', dest='config', default=None,
                        help='use configuration in given file')
    parser.add_argument('--save_config', dest='save_config', default=False, action='store_true',
                        help='save configuration in --config file and exit')
    parser.add_argument('-c', '--cpus', dest='cpus', type=int, default=0,
                        help='number of cpus used by dispy; if negative, '
                        'that many cpus are not used')
    parser.add_argument('-d', '--debug', action='store_true', dest='loglevel', default=False,
                        help='if given, debug messages are printed')
    parser.add_argument('-i', '--ip_addr', dest='ip_addr', default='',
                        help='IP address to use (may be needed in case of multiple interfaces)')
    parser.add_argument('--ext_ip_addr', dest='ext_ip_addr', default='',
                        help='External IP address to use (needed in case of NAT firewall/gateway)')
    parser.add_argument('-p', '--node_port', dest='node_port', type=int, default=51348,
                        help='port number to use')
    parser.add_argument('--name', dest='name', default='',
                        help='name asscoiated to this node; default is obtained with gethostname()')
    parser.add_argument('--dest_path_prefix', dest='dest_path_prefix', default='',
                        help='path prefix where files sent by dispy are stored')
    parser.add_argument('--scheduler_node', dest='scheduler_node', default='',
                        help='name or IP address of scheduler to announce when starting')
    parser.add_argument('--scheduler_port', dest='scheduler_port', type=int, default=51347,
                        help='port number used by scheduler')
    parser.add_argument('--max_file_size', dest='max_file_size', default=str(MaxFileSize),
                        help='maximum file size of any file transferred (use 0 for unlimited size)')
    parser.add_argument('--zombie_interval', dest='zombie_interval', type=float, default=60.0,
                        help='interval in minutes to presume unresponsive scheduler is zombie')
    parser.add_argument('--service_start', dest='service_start', default='',
                        help='time of day in HH:MM format when to start service')
    parser.add_argument('--service_stop', dest='service_stop', default='',
                        help='time of day in HH:MM format when to stop service '
                        '(continue to execute running jobs, but no new jobs scheduled)')
    parser.add_argument('--service_end', dest='service_end', default='',
                        help='time of day in HH:MM format when to end service '
                        '(terminate running jobs)')
    parser.add_argument('--serve', dest='serve', type=int, default=-1,
                        help='number of clients to serve before exiting')
    parser.add_argument('--msg_timeout', dest='msg_timeout', type=float, default=MsgTimeout,
                        help='timeout used for messages to/from client in seconds')
    parser.add_argument('-s', '--secret', dest='secret', default='',
                        help='authentication secret for handshake with dispy clients')
    parser.add_argument('--certfile', dest='certfile', default='',
                        help='file containing SSL certificate')
    parser.add_argument('--keyfile', dest='keyfile', default='',
                        help='file containing SSL key')
    parser.add_argument('--clean', action='store_true', dest='clean', default=False,
                        help='if given, files copied from or generated by clients will be removed')
    parser.add_argument('--daemon', action='store_true', dest='daemon', default=False,
                        help='if given, input is not read from terminal '
                        '(to set CPUs or get status)')
    _dispy_config = vars(parser.parse_args(sys.argv[1:]))

    if _dispy_config['config'] and os.path.isfile(_dispy_config['config']):
        import configparser
        cfgp = configparser.ConfigParser()
        cfgp.read(_dispy_config['config'])
        _dispy_config, cfgp = dict(cfgp.items('DEFAULT')), _dispy_config
        if _dispy_config:
            for key, value in cfgp.items():
                if cfgp[key] != parser.get_default(key) or key not in _dispy_config:
                    print('set %s to %s' % (key, cfgp[key]))
                    _dispy_config[key] = cfgp[key]
            del key, value
        del cfgp
        # TODO: dump serialized config and deserialize after load instead?
        _dispy_config['cpus'] = int(_dispy_config['cpus'])
        _dispy_config['node_port'] = int(_dispy_config['node_port'])
        _dispy_config['scheduler_port'] = int(_dispy_config['scheduler_port'])
        _dispy_config['zombie_interval'] = float(_dispy_config['zombie_interval'])
        _dispy_config['msg_timeout'] = float(_dispy_config['msg_timeout'])
        _dispy_config['serve'] = int(_dispy_config['serve'])
        _dispy_config['loglevel'] = True if _dispy_config['loglevel'] == 'True' else False
        _dispy_config['clean'] = True if _dispy_config['clean'] == 'True' else False
        _dispy_config['daemon'] = True if _dispy_config['daemon'] == 'True' else False
    del parser

    if _dispy_config.pop('save_config', None):
        import configparser
        cfgp = configparser.ConfigParser(_dispy_config)
        if _dispy_config['config']:
            cfgfd = open(_dispy_config['config'], 'w')
            _dispy_config.pop('config')
        else:
            cfgfd = sys.stdout
        cfgp.write(cfgfd)
        exit(0)

    _dispy_config.pop('config', None)

    if _dispy_config['loglevel']:
        _dispy_logger.setLevel(logging.DEBUG)
        asyncoro.logger.setLevel(logging.DEBUG)
    else:
        _dispy_logger.setLevel(logging.INFO)
    del _dispy_config['loglevel']

    cpus = multiprocessing.cpu_count()
    if _dispy_config['cpus']:
        if _dispy_config['cpus'] > 0:
            if _dispy_config['cpus'] > cpus:
                raise Exception('CPU count must be <= %s' % cpus)
        else:
            if _dispy_config['cpus'] <= -cpus:
                raise Exception('CPU count must be > -%s' % cpus)
            cpus += _dispy_config['cpus']
            _dispy_config['cpus'] = cpus
    else:
        _dispy_config['cpus'] = cpus
    del cpus

    if _dispy_config['zombie_interval']:
        if _dispy_config['zombie_interval'] < 1:
            raise Exception('zombie_interval must be at least 1')

    MsgTimeout = _dispy_config['msg_timeout']
    del _dispy_config['msg_timeout']

    m = re.match(r'(\d+)([kKmMgGtT]?)', _dispy_config['max_file_size'])
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
    del m
    del _dispy_config['max_file_size']

    if _dispy_config['service_start']:
        _dispy_config['service_start'] = time.strptime(_dispy_config['service_start'], '%H:%M')
    if _dispy_config['service_stop']:
        _dispy_config['service_stop'] = time.strptime(_dispy_config['service_stop'], '%H:%M')
    if _dispy_config['service_end']:
        _dispy_config['service_end'] = time.strptime(_dispy_config['service_end'], '%H:%M')

    try:
        if os.getpgrp() != os.tcgetpgrp(sys.stdin.fileno()):
            _dispy_config['daemon'] = True
    except:
        pass

    # Python 3 under Windows blocks multiprocessing.Process on reading input;
    # pressing "Enter" twice works (for one subprocess). Until this is
    # understood / fixed, disable reading input.
    if os.name == 'nt':
        _dispy_config['daemon'] = True

    if psutil:
        psutil.cpu_percent(0.1)
    else:
        print('\n  "psutil" module is not available;')
        print('    node status (CPU, memory, disk and swap space usage) '
              'will not be sent to clients\n')

    _dispy_node = None
    _dispy_node = _DispyNode(**_dispy_config)
    del _dispy_config

    def shutdown(_, __):
        _dispy_node.shutdown(quit=True)

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGHUP, shutdown)

    _dispy_node.asyncoro.finish()
