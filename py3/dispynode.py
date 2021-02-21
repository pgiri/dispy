#!/usr/bin/python3

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
import marshal
import tempfile
import shutil
import glob
import functools
import pickle
import io
import signal
import platform
import copy
import struct
import hashlib
import re
import errno
try:
    import psutil
except ImportError:
    psutil = None
try:
    import netifaces
except ImportError:
    netifaces = None

import pycos
import dispy
import dispy.config
from dispy.config import MsgTimeout, MaxFileSize
from dispy import _JobReply, DispyJob, DispyNodeAvailInfo, _Compute, _XferFile, \
     _dispy_version, auth_code, num_min, _same_file
from pycos import Task, Pycos, AsyncSocket, serialize, deserialize

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

# Messages logged with 'dispynode_logger' in computations are shown at
# node (whereas 'print' statements are sent back to client with
# job.stdout)
dispynode_logger = pycos.Logger('dispynode')
# PyPI / pip packaging adjusts assertion below for Python 3.7+
assert sys.version_info.major == 3 and sys.version_info.minor < 7, \
    ('\n\n  This file is not suitable for Python version %s.%s directly;\n'
     '  see installation instructions at %s\n' %
     (sys.version_info.major, sys.version_info.minor, __url__))


def dispy_provisional_result(result, relay=False, timeout=MsgTimeout):
    """Sends provisional result of computation back to the client.

    In some cases, such as optimizations, computations may send
    current (best) result to the client and continue computation (for
    next iteration) so that the client may decide to terminate
    computations based on the results or alter computations if
    necessary. The computations can use this function in such cases
    with the current result of computation as argument.

    If 'relay' is False (default), the result is sent to client; otherwise, it
    is sent to scheduler, which can be different in the case of SharedJobCluster.

    'timeout' is seconds for socket connection/messages; i.e., if
    there is no I/O on socket (to client), this call fails. Default
    value for it is MsgTimeout (5) seconds.

    Returns 0 if result was delivered to client.
    """

    __dispy_job_reply.status = DispyJob.ProvisionalResult
    __dispy_job_reply.result = serialize(result)
    __dispy_job_reply.end_time = time.time()
    sock = socket.socket(__dispy_sock_family, socket.SOCK_STREAM)
    sock = AsyncSocket(sock, blocking=True, keyfile=__dispy_keyfile, certfile=__dispy_certfile)
    sock.settimeout(timeout)
    try:
        if relay:
            sock.connect(__dispy_job_reply_addr)
        else:
            sock.connect(__dispy_client_reply_addr)
        sock.send_msg(b'JOB_REPLY:' + serialize(__dispy_job_reply))
        ack = sock.recv_msg()
        assert ack == b'ACK'
    except Exception:
        return -1
    else:
        return 0
    finally:
        sock.close()


def dispy_send_file(path, relay=False, timeout=MsgTimeout):
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

    If 'relay' is False (default), the file is sent to client; otherwise, it
    is sent to scheduler, which can be different in the case of SharedJobCluster.

    'timeout' is seconds for socket connection/messages; i.e., if
    there is no I/O on socket (to client), this call fails. Default
    value for it is MsgTimeout (5) seconds.

    Return value of 0 indicates successfull transfer.
    """

    try:
        xf = _XferFile(path)
    except Exception:
        return -1
    if MaxFileSize and xf.stat_buf.st_size > MaxFileSize:
        return -1
    sock = socket.socket(__dispy_sock_family, socket.SOCK_STREAM)
    sock = AsyncSocket(sock, blocking=True, keyfile=__dispy_keyfile, certfile=__dispy_certfile)
    sock.settimeout(timeout)
    try:
        if relay:
            sock.connect(__dispy_job_reply_addr)
        else:
            sock.connect(__dispy_client_reply_addr)
        sock.send_msg('FILEXFER:'.encode() + serialize(xf))
        sock.send_msg(serialize(__dispy_job_reply))
        recvd = sock.recv_msg()
        recvd = deserialize(recvd)
        with open(path, 'rb') as fd:
            sent = 0
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
    finally:
        sock.close()


class _DispyJobInfo(object):
    """Internal use only.
    """
    def __init__(self, job_reply, compute, xfer_files):
        self.job_reply = job_reply
        self.compute_id = compute.id
        self.compute_dest_path = compute.dest_path
        self.xfer_files = xfer_files
        self.compute_auth = compute.auth
        self.addrinfo = None
        self.pid = None
        self.proc = None


def _dispy_job_func(__dispy_job_name, __dispy_job_code, __dispy_job_globals,
                    __dispy_job_args, __dispy_job_kwargs):
    """Internal use only.
    """

    suid = __dispy_job_globals.pop('suid', None)
    if suid is not None:
        sgid = __dispy_job_globals.pop('sgid', None)
        if hasattr(os, 'setresuid'):
            os.setresgid(sgid, sgid, sgid)
            os.setresuid(suid, suid, suid)
        else:
            os.setregid(sgid, sgid)
            os.setreuid(suid, suid)
        del sgid
    del suid

    reply_Q = __dispy_job_globals.pop('reply_Q')
    globals().update(__dispy_job_globals)
    globals()['_dispy_job_func'] = None
    sys.stdout = io.StringIO()
    sys.stderr = io.StringIO()
    try:
        if __dispy_job_code[0]:
            exec(marshal.loads(__dispy_job_code[0]), globals())
        if __dispy_job_code[1]:
            exec(__dispy_job_code[1], globals())
        if __name__ == '__mp_main__':  # Windows multiprocessing process
            sys.modules['__mp_main__'].__dict__.update(globals())
        localvars = {'dispy_job_args': deserialize(__dispy_job_args),
                     'dispy_job_kwargs': deserialize(__dispy_job_kwargs)}
        exec('__dispy_job_reply.result = %s(*dispy_job_args, **dispy_job_kwargs)' %
             __dispy_job_name, globals(), localvars)
        __dispy_job_reply.status = DispyJob.Finished
    except Exception:
        __dispy_job_reply.exception = traceback.format_exc()
        __dispy_job_reply.status = DispyJob.Terminated
        __dispy_job_reply.result = None

    __dispy_job_reply.result = serialize(__dispy_job_reply.result)
    __dispy_job_reply.stdout = sys.stdout.getvalue()
    __dispy_job_reply.stderr = sys.stderr.getvalue()
    __dispy_job_reply.end_time = time.time()
    reply_Q.put(__dispy_job_reply)


def _dispy_terminate_proc(proc_pid, task=None):
    """
    Internal use only.
    """

    def terminate_proc(how):
        if psutil and isinstance(proc_pid, psutil.Process):
            proc = proc_pid
            try:
                pid = proc.pid
                if how == 0:
                    if proc.is_running():
                        if proc.status() == psutil.STATUS_ZOMBIE:
                            try:
                                proc.wait(timeout=0.1)
                                return 0
                            except psutil.TimeoutExpired:
                                return 1
                        return 1
                    else:
                        return 0

                elif how == 1:
                    proc.terminate()

                elif how == 2:
                    proc.kill()
                    try:
                        proc.wait(timeout=0.5)
                        return 0
                    except psutil.TimeoutExpired:
                        return 1
            except psutil.ZombieProcess:
                try:
                    proc.wait(timeout=0.1)
                    return 0
                except Exception:
                    return 1
            except psutil.NoSuchProcess:
                return 0
            except Exception:
                dispynode_logger.info('Could not terminate PID %s: %s',
                                      proc.pid, traceback.format_exc())
                return -1
            return 1

        elif isinstance(proc_pid, multiprocessing.Process):
            proc = proc_pid
            try:
                if how == 0:
                    if proc.is_alive():
                        return 1
                    else:
                        return 0
                elif how == 1:
                    proc.terminate()
                elif how == 2:
                    kill = getattr(proc, 'kill', proc.terminate)
                    kill()
            except Exception:
                dispynode_logger.debug(traceback.format_exc())
                pass
            return 1

        elif isinstance(proc_pid, subprocess.Popen):
            proc = proc_pid
            try:
                if how == 0:
                    if proc.poll() is None:
                        return 1
                    else:
                        return 0
                elif how == 1:
                    proc.terminate()
                elif how == 2:
                    proc.kill()
            except Exception:
                pass
            return 1

        else:
            pid = proc_pid
            if how == 0:
                wait_nohang = getattr(os, 'WNOHANG', None)
                if wait_nohang:
                    try:
                        if os.waitpid(pid, wait_nohang)[0] == pid:
                            return 0
                    except Exception:
                        # TODO: check errno for all platforms
                        return 1
                # TODO: kill with 0 to check if process is running where possible
            if os.name == 'nt':
                signals = [signal.CTRL_BREAK_EVENT, signal.CTRL_BREAK_EVENT, signal.SIGTERM]
            else:
                signals = [0, signal.SIGTERM, signal.SIGKILL]
            try:
                os.kill(pid, signals[how])
            except OSError as exc:
                if exc.errno == errno.ESRCH or exc.errno == errno.EINVAL:
                    return 0
                else:
                    dispynode_logger.debug('Terminatnig PID %s with %s failed: %s',
                                           pid, how, traceback.format_exc())
                    return -1
            except Exception:
                dispynode_logger.debug('Terminatnig PID %s with %s failed: %s',
                                       pid, how, traceback.format_exc())
                return -1
            return 1

    how = 1
    for i in range(20):
        status = terminate_proc(how)
        if how == 0 and status == 0:
            raise StopIteration(0)
        if i == 15:
            how = 2
        else:
            how = 0
        yield task.sleep(0.3)
    raise StopIteration(-1)


def _dispy_setup_process(compute, pipe, client_globals):
    """
    Internal use only.
    """
    import sys
    import threading
    sys.modules.pop('pycos', None)
    globals().pop('pycos', None)
    import pycos

    os.chdir(compute.dest_path)
    sys.path.insert(0, compute.dest_path)
    suid = client_globals.pop('suid', None)
    if suid is not None:
        sgid = client_globals.pop('sgid', None)
        if hasattr(os, 'setresuid'):
            os.setresgid(sgid, sgid, sgid)
            os.setresuid(suid, suid, suid)
        else:
            os.setregid(sgid, sgid)
            os.setreuid(suid, suid)
    _dispy_terminate_proc = globals()['_dispy_terminate_proc']
    globals().update(client_globals)
    globals().pop('reply_Q', None)
    setup_args = client_globals.pop('setup_args', ())
    dispynode_logger.setLevel(client_globals.pop('loglevel'))

    if compute.code:
        try:
            if compute.code:
                exec(marshal.loads(compute.code), globals())
            if os.name != 'nt':
                compute.code = None
        except Exception:
            dispynode_logger.debug(traceback.format_exc())
            pipe.send({'setup_status': -1})
            return

    init_vars = set(globals().keys())
    if compute.setup:
        localvars = {'_dispy_setup_status': 0, '_dispy_setup_args': setup_args}
        try:
            exec('_dispy_setup_status = %s(*_dispy_setup_args)' %
                 compute.setup, globals(), localvars)
            _dispy_setup_status = localvars['_dispy_setup_status']
        except Exception:
            dispynode_logger.debug(traceback.format_exc())
            pipe.send({'setup_status': -1})
            return -1
        localvars = None
        compute.setup = None
    else:
        _dispy_setup_status = 0

    pipe.send({'setup_status': _dispy_setup_status})
    setup_globals = {var: value for var, value in globals().items() if var not in init_vars}

    if _dispy_setup_status == 0:
        if setup_globals:
            pipe.send({'use_setup_proc': True})
            if os.name == 'nt':
                client_globals.update(setup_globals)
        else:
            pipe.send({'use_setup_proc': False})
            compute.code = None

    elif _dispy_setup_status == 1:
        pipe.send({'setup_globals': setup_globals})
        compute.code = None

    init_vars = setup_globals = None
    reply_Q = client_globals['reply_Q']
    setup_pid = os.getpid()
    wait_nohang = getattr(os, 'WNOHANG', None)

    def sighandler(signum, frame):
        dispynode_logger.debug('setup_process received signal %s', signum)

    signal.signal(signal.SIGINT, sighandler)
    if os.name == 'nt':
        signal.signal(signal.SIGBREAK, sighandler)

    def terminate_job(msg):
        proc_pid = msg['pid']
        job_reply = msg['job_reply']
        # TODO: Currently job processes are not maintained. Perhaps it
        # is better / safer approach, but requires main process to
        # inform this process when a job is done so that process can
        # be removed. This requires extra bandwith. Since cancelling
        # jobs is relatively rare, it may be better to use psutil or
        # os.kill to terminate processes.

        if psutil:
            try:
                proc_pid = psutil.Process(proc_pid)
                if proc_pid.ppid() != setup_pid:
                    return
            except psutil.NoSuchProcess:
                # job is already done
                return
            except Exception:
                pass

        if pycos.Task(_dispy_terminate_proc, proc_pid).value():
            dispynode_logger.warning('Job %s (PID %s) could not be terminated',
                                     job_reply.uid, msg['pid'])
            return

        dispynode_logger.debug('Job %s terminated', job_reply.uid)
        job_reply.result = serialize(None)
        job_reply.status = DispyJob.Terminated
        job_reply.end_time = time.time()
        reply_Q.put(job_reply)

    while 1:
        try:
            msg = pipe.recv()
        except EOFError:
            break

        if msg['req'] == 'job':
            job_reply = msg['job_reply']
            client_globals['__dispy_job_reply'] = job_reply
            args = (compute.name, (compute.code, msg['code']), client_globals,
                    msg['args'], msg['kwargs'])
            job_proc = multiprocessing.Process(target=_dispy_job_func, args=args)
            job_proc.start()
            reply_Q.put({'req': 'job_pid', 'uid': job_reply.uid, 'pid': job_proc.pid,
                         'ppid': setup_pid})
            msg = args = None

        elif msg['req'] == 'terminate_job':
            thread = threading.Thread(target=terminate_job, args=(msg,))
            thread.daemon = True
            thread.start()

        elif msg['req'] == 'wait_pid':
            pid = msg.get('pid', None)
            job_reply = msg.get('job_reply', None)
            if pid and wait_nohang:
                try:
                    assert os.waitpid(pid, wait_nohang)[0] == pid
                except (OSError, Exception):
                    # print(traceback.format_exc())
                    pass
                else:
                    if job_reply:
                        job_reply.status = DispyJob.Terminated
                        job_reply.result = serialize(None)
                        job_reply.end_time = time.time()
                        reply_Q.put(job_reply)

        elif msg['req'] == 'quit':
            break

    if isinstance(compute.cleanup, str):
        localvars = {'_dispy_cleanup_args': setup_args}
        try:
            exec('%s(*_dispy_cleanup_args)' % compute.cleanup, globals(), localvars)
        except Exception:
            dispynode_logger.debug('"cleanup" for computation "%s" failed: %s',
                                   compute.id, traceback.format_exc())

    pipe.send('quit')
    exit(0)


class _Client(object):
    """
    Internal use only.
    """

    def __init__(self, compute):
        self.compute = compute
        self.globals = {}
        self.file_uses = {}
        self.pending_jobs = 0
        self.pending_results = 0
        self.jobs_done = pycos.Event()
        self.last_pulse = time.time()
        self.zombie = False
        self.setup_proc = None
        self.setup_args = None
        self.cleanup_args = None
        self.parent_pipe = self.child_pipe = None
        self.use_setup_proc = False
        self.sock_family = None
        self.jobs_done.set()

    def __getstate__(self):
        state = dict(self.__dict__)
        state.pop('jobs_done')
        return state

    def __setstate__(self, state):
        for k, v in state.items():
            setattr(self, k, v)
        self.jobs_done = pycos.Event()


class _DispyNode(object):
    """
    Internal use only.
    """

    def __init__(self, cpus, hosts=[], ext_hosts=[], name='', scheduler_host=None,
                 ipv4_udp_multicast=False, dest_path_prefix='', clean=False,
                 secret='', keyfile=None, certfile=None, admin_secret='', zombie_interval=60,
                 ping_interval=None, force_cleanup=False, serve=-1,
                 service_start=None, service_stop=None, service_end=None, safe_setup=True,
                 daemon=False, client_shutdown=False):
        assert 0 < cpus <= multiprocessing.cpu_count()
        self.num_cpus = cpus
        if name:
            self.name = name
        else:
            self.name = socket.gethostname()
            if 'localhost' in self.name.lower():
                self.name = ''
        if not hosts:
            hosts = [None]
        self.ipv4_udp_multicast = bool(ipv4_udp_multicast)
        self.addrinfos = []
        for i in range(len(hosts)):
            addrinfo = dispy.host_addrinfo(host=hosts[i], ipv4_multicast=self.ipv4_udp_multicast)
            if not addrinfo or not addrinfo.ip:
                dispynode_logger.warning('Ignoring invalid host %s', hosts[i])
                continue
            if addrinfo.ip.startswith('127.') or addrinfo.ip.startswith('fe80'):
                dispynode_logger.warning('node IP address %s seems to be loopback address; '
                                         'this will prevent communication with clients on '
                                         'other machines. ', addrinfo.ip)
            elif not self.name:
                try:
                    name = socket.gethostbyaddr(addrinfo.ip)
                except Exception:
                    pass
                else:
                    for name in [name[0]] + name[1]:
                        if 'localhost' not in name.lower():
                            self.name = name
                            break
            if i < len(ext_hosts):
                ext_host = dispy._node_ipaddr(ext_hosts[i])
                if ext_host:
                    addrinfo.ext_ip = ext_host
                else:
                    dispynode_logger.warning('Ignoring invalid ext_host %s', ext_hosts[i])

            self.addrinfos.append(addrinfo)

        if not self.addrinfos:
            raise Exception('No valid host')
        if not self.name:
            self.name = self.addrinfos[0].ip

        self.port = eval(dispy.config.NodePort)
        self.keyfile = keyfile
        self.certfile = certfile
        if self.keyfile:
            self.keyfile = os.path.abspath(self.keyfile)
        if self.certfile:
            self.certfile = os.path.abspath(self.certfile)
        if not dest_path_prefix:
            dest_path_prefix = tempfile.gettempdir()
        dest_path_prefix = os.path.join(dest_path_prefix, 'dispy', 'node')
        self.dest_path_prefix = os.path.abspath(dest_path_prefix.strip()).rstrip(os.sep)
        dispynode_logger.info('Files will be saved under "%s"', self.dest_path_prefix)
        self._safe_setup = bool(safe_setup)
        self._force_cleanup = bool(force_cleanup)

        self.suid = None
        self.sgid = None
        if ((hasattr(os, 'setresuid') or hasattr(os, 'setreuid')) and os.getuid() != os.geteuid()):
            self.suid = os.geteuid()
            self.sgid = os.getegid()
            if self.suid == 0:
                print('\n    WARNING: Python interpreter %s likely has suid set to 0 '
                      '\n    (administrator privilege), which is dangerous.\n\n' %
                      sys.executable)
            if self.sgid == 0:
                print('\n    WARNING: Python interpreter %s likely has sgid set to 0 '
                      '\n    (administrator privilege), which is dangerous.\n\n' %
                      sys.executable)

            os.setegid(os.getgid())
            os.seteuid(os.getuid())
            dispynode_logger.info('Computations will run with uid %s and gid %s' %
                                  (self.suid, self.sgid))

        self.pycos = Pycos()

        config = os.path.join(self.dest_path_prefix, 'config.pkl')
        if os.path.isfile(config):
            with open(config, 'rb') as fd:
                config = pickle.load(fd)
            if not clean:
                print('\n    Another dispynode server seems to be running with PID %s;'
                      '\n    terminate that process and rerun with "clean" option\n' %
                      config.get('pid', None))
                exit(1)
        else:
            config = {}

        if clean and config:
            node_proc = None
            if psutil:
                try:
                    node_proc = psutil.Process(config['pid'])
                    assert any(arg.endswith('dispynode.py') for arg in node_proc.cmdline())
                    assert node_proc.ppid() in (config['ppid'], 1)
                    if config['create_time']:
                        assert abs(node_proc.create_time() - config['create_time']) < 1
                except Exception:
                    print('\n    Apparently previous dispynode (PID %s) has gone away;'
                          '\n    please check manually and kill process(es) if necessary\n' %
                          config['pid'])
                    config = {}
                    node_proc = None
            else:
                print('\n    WARNING: Using "clean" without "psutil" module may be dangerous!\n')

            for name in glob.glob(os.path.join(self.dest_path_prefix, '*.pkl')):
                if name.startswith('_dispy_job_reply_'):
                    # job results may be retrieved by clients with
                    # 'recover_jobs' function, so keep them
                    continue
                if not config or not name.startswith('job_') or not os.path.isfile(name):
                    try:
                        if os.path.isdir(name):
                            shutil.rmtree(name, ignore_errors=True)
                        else:
                            os.remove(name)
                    except Exception:
                        print('Could not remove "%s"' % name)
                    continue
                with open(name, 'rb') as fd:
                    info = pickle.load(fd)
                    pid = info['pid']
                try:
                    os.remove(name)
                except Exception:
                    # print(traceback.format_exc())
                    pass
                dispynode_logger.debug('Killing process with ID %s', pid)
                if psutil:
                    try:
                        proc = psutil.Process(pid)
                        assert proc.is_running()
                        assert proc.ppid() == info['ppid']
                        if os.name == 'nt':
                            assert any(arg.startswith('from multiprocessing.')
                                       for arg in proc.cmdline())
                        else:
                            assert any(arg.endswith('dispynode.py') for arg in proc.cmdline())
                    except Exception:
                        continue
                    proc_pid = proc
                else:
                    proc_pid = pid

                if pycos.Task(_dispy_terminate_proc, proc_pid).value():
                    dispynode_logger.warning('Could not terminate PID %s', pid)

            proc_pid = config.get('pid', None)
            if proc_pid is not None:
                if node_proc:
                    proc_pid = node_proc
                print('Killing dispynode process ID %s; it may take sometime to finish.' %
                      config['pid'])
                if pycos.Task(_dispy_terminate_proc, proc_pid).value():
                    dispynode_logger.warning('Could not terminate PID %s', config['pid'])

            shutil.rmtree(self.dest_path_prefix, ignore_errors=True)

        if not os.path.isdir(self.dest_path_prefix):
            os.makedirs(self.dest_path_prefix)
        os.chmod(self.dest_path_prefix, stat.S_IRUSR | stat.S_IWUSR |
                 stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
        os.chmod(os.path.join(self.dest_path_prefix, '..'), stat.S_IRUSR | stat.S_IWUSR |
                 stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
        os.chdir(self.dest_path_prefix)

        self.avail_cpus = self.num_cpus
        self.clients = {}
        self.job_infos = {}
        self.terminate = False
        self.sign = hashlib.sha1(os.urandom(20))
        for addrinfo in self.addrinfos:
            self.sign.update(addrinfo.ip.encode())
        self.sign = self.sign.hexdigest()
        self.secret = secret
        self.auth = auth_code(self.secret, self.sign)
        self.admin_secret = admin_secret
        if admin_secret:
            self.admin_auth = auth_code(admin_secret, self.sign)
        else:
            self.admin_auth = None
        self.admin_sign = None
        self.zombie_interval = 60 * zombie_interval
        if ping_interval:
            self.ping_interval = ping_interval
        else:
            self.ping_interval = None
        self.pulse_interval = self.ping_interval
        scheduler_port = eval(dispy.config.ClientPort)

        self.scheduler = {'ip_addr': dispy._node_ipaddr(scheduler_host) if scheduler_host else None,
                          'port': scheduler_port, 'auth': set(), 'addrinfo': None}
        self.cpu_time = 0
        self.jobs_done = 0
        self.clients_done = 0
        self.pid = os.getpid()

        if psutil:
            proc = psutil.Process(self.pid)
            create_time = proc.create_time()
            ppid = proc.ppid()
        else:
            create_time = None
            if hasattr(os, 'getppid'):
                ppid = os.getppid()
            else:
                ppid = 1
        config = os.path.join(self.dest_path_prefix, 'config.pkl')
        with open(config, 'wb') as fd:
            pickle.dump({
                'hosts': [addrinfo.ip for addrinfo in self.addrinfos],
                'ext_hosts': [addrinfo.ext_ip for addrinfo in self.addrinfos],
                'port': self.port, 'avail_cpus': self.avail_cpus,
                'sign': self.sign, 'secret': self.secret, 'auth': self.auth,
                'keyfile': self.keyfile, 'certfile': self.certfile, 'pid': self.pid,
                'ppid': ppid, 'create_time': create_time
            }, fd)
            os.chmod(config, stat.S_IRUSR | stat.S_IWUSR)

        # prepend current directory in sys.path so computations can
        # load modules from current working directory
        sys.path.insert(0, '.')

        # start a process so all modules needed by dispynode are loaded
        proc = multiprocessing.Process(target=functools.partial(int), args=(42,))
        proc.start()
        proc.join()

        self.thread_lock = threading.Lock()

        self.reply_Q = multiprocessing.Queue()
        self.reply_Q_thread = threading.Thread(target=self.__reply_Q)
        self.reply_Q_thread.daemon = True
        self.reply_Q_thread.start()

        self.serve = serve
        self.timer_task = Task(self.timer_proc)
        self.service_start = self.service_stop = self.service_end = None
        if isinstance(service_start, int):
            self.service_start = service_start
        if isinstance(service_stop, int):
            self.service_stop = service_stop
        if isinstance(service_end, int):
            self.service_end = service_end

        self.service_schedule_task = Task(self.service_schedule)
        self.client_shutdown = client_shutdown

        self.__init_modules = dict(sys.modules)
        self.__init_environ = dict(os.environ)
        self.__init_globals = dict(globals())
        self.__init_globals.pop('_dispy_config',  None)
        self.__init_globals.pop('_dispy_cmd', None)
        self.__init_globals['_dispy_node'] = self

        udp_addrinfos = {}
        for addrinfo in self.addrinfos:
            Task(self.tcp_server, addrinfo)
            udp_addrinfos[addrinfo.bind_addr] = addrinfo

        for bind_addr, addrinfo in udp_addrinfos.items():
            Task(self.udp_server, addrinfo)

        if not daemon:
            self.cmd_task = Task(self.cmd_proc)
        dispynode_logger.info('"%s" serving %s cpus', self.name, self.num_cpus)

    def broadcast_ping_msg(self, task=None):
        if (self.scheduler['auth'] or self.job_infos or not self.avail_cpus or
            not self.service_available()):
            raise StopIteration
        for addrinfo in self.addrinfos:
            sock = AsyncSocket(socket.socket(addrinfo.family, socket.SOCK_DGRAM))
            sock.settimeout(MsgTimeout)
            ttl_bin = struct.pack('@i', 1)
            if addrinfo.family == socket.AF_INET:
                if self.ipv4_udp_multicast:
                    sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, ttl_bin)
                else:
                    sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
            else:  # addrinfo.family == socket.AF_INET6
                sock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_MULTICAST_HOPS, ttl_bin)
                sock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_MULTICAST_IF, addrinfo.ifn)
            ping_msg = {'ip_addr': addrinfo.ext_ip, 'port': self.port, 'sign': self.sign,
                        'version': _dispy_version, 'scheduler_ip_addr': None}
            sock.bind((addrinfo.ip, 0))
            try:
                yield sock.sendto('PING:'.encode() + serialize(ping_msg),
                                  (addrinfo.broadcast, self.scheduler['port']))
            except Exception:
                pass
            sock.close()

            if self.scheduler['ip_addr']:
                sock = AsyncSocket(socket.socket(addrinfo.family, socket.SOCK_DGRAM))
                sock.settimeout(MsgTimeout)
                ping_msg = {'scheduler_ip_addr': self.scheduler['ip_addr'],
                            'ip_addr': addrinfo.ext_ip, 'port': self.port,
                            'sign': self.sign, 'version': _dispy_version}
                try:
                    yield sock.sendto('PING:'.encode() + serialize(ping_msg),
                                      (self.scheduler['ip_addr'], self.scheduler['port']))
                except Exception:
                    pass
                finally:
                    sock.close()
                sock = AsyncSocket(socket.socket(addrinfo.family, socket.SOCK_STREAM),
                                   keyfile=self.keyfile, certfile=self.certfile)
                sock.settimeout(MsgTimeout)
                try:
                    yield sock.connect((self.scheduler['ip_addr'], self.scheduler['port']))
                    yield sock.send_msg('PING:'.encode() + serialize(ping_msg))
                except Exception:
                    pass
                sock.close()

    def send_pong_msg(self, info, addr):
        if (self.scheduler['auth'] or self.job_infos or not self.num_cpus or
            not self.service_available()):
            dispynode_logger.debug('Busy (%s/%s); ignoring ping message from %s',
                                   self.avail_cpus, self.num_cpus, addr[0])
            return

        scheduler_ip_addrs = info['ip_addrs']
        if (not info.get('relay', None) and isinstance(addr, tuple) and isinstance(addr[0], str)):
            scheduler_ip_addrs.append(addr[0])
        scheduler_port = info['port']
        msg = {'port': self.port, 'sign': self.sign, 'version': _dispy_version}
        sign = info.get('sign', '')
        if sign:
            msg.update({'name': self.name, 'cpus': self.avail_cpus, 'platform': platform.platform(),
                        'auth': auth_code(self.secret, sign)})
            if psutil:
                msg['avail_info'] = DispyNodeAvailInfo(
                    100.0 - psutil.cpu_percent(), psutil.virtual_memory().available,
                    psutil.disk_usage(self.dest_path_prefix).free,
                    100.0 - psutil.swap_memory().percent)
            else:
                msg['avail_info'] = None

        # TODO: pick appropriate addrinfo based on netmask if available
        for addrinfo in self.addrinfos:
            msg['ip_addr'] = addrinfo.ext_ip
            for scheduler_ip_addr in scheduler_ip_addrs:
                if not isinstance(scheduler_ip_addr, str):
                    continue
                if re.match(r'\d+\.', scheduler_ip_addr):
                    sock_family = socket.AF_INET
                else:
                    sock_family = socket.AF_INET6
                if sock_family != addrinfo.family:
                    continue

                def send_pong(msg, addrinfo, sched_ip, task=None):
                    sock = AsyncSocket(socket.socket(addrinfo.family, socket.SOCK_STREAM),
                                       keyfile=self.keyfile, certfile=self.certfile)
                    sock.settimeout(MsgTimeout)
                    try:
                        yield sock.connect((sched_ip, scheduler_port))
                        msg['scheduler_ip_addr'] = sched_ip
                        if sign:
                            yield sock.send_msg('PONG:'.encode() + serialize(msg))
                        else:
                            yield sock.send_msg('PING:'.encode() + serialize(msg))
                    except Exception:
                        pass
                    finally:
                        sock.close()

                Task(send_pong, msg, addrinfo, scheduler_ip_addr)

    def udp_server(self, addrinfo, task=None):
        task.set_daemon()

        def send_node_info(msg, addr, task=None):
            info = {'ip_addr': addrinfo.ext_ip, 'port': self.port, 'sign': self.sign,
                    'version': _dispy_version}
            sock = AsyncSocket(socket.socket(addrinfo.family, socket.SOCK_STREAM),
                               keyfile=self.keyfile, certfile=self.certfile)
            sock.settimeout(MsgTimeout)
            try:
                yield sock.connect((msg['ip_addr'], msg['port']))
            except (EnvironmentError, OSError):
                yield sock.connect((addr, msg['port']))
            try:
                yield sock.send_msg(b'NODE_INFO:' + serialize(info))
            except Exception:
                dispynode_logger.debug(traceback.format_exc())
                pass
            sock.close()

        udp_sock = AsyncSocket(socket.socket(addrinfo.family, socket.SOCK_DGRAM))
        udp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if hasattr(socket, 'SO_REUSEPORT'):
            try:
                udp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
            except Exception:
                pass

        while not self.port:
            yield task.sleep(0.2)

        udp_sock.bind((addrinfo.bind_addr, self.port))
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

        while 1:
            msg, addr = yield udp_sock.recvfrom(1000)
            if msg.startswith(b'PING:'):
                try:
                    info = deserialize(msg[len(b'PING:'):])
                    if info['version'] != _dispy_version:
                        dispynode_logger.warning('Ignoring %s due to version mismatch', addr[0])
                        continue
                except Exception:
                    dispynode_logger.debug('Ignoring ping message from %s (%s)', addr[0], addr[1])
                    continue
                self.send_pong_msg(info, addr)

            elif msg.startswith(b'PULSE:'):
                try:
                    info = deserialize(msg[len(b'PULSE:'):])
                except Exception:
                    dispynode_logger.warning('Ignoring PULSE from %s', addr[0])
                else:
                    if info['ip_addr'] == self.scheduler['ip_addr']:
                        now = time.time()
                        for client in self.clients.values():
                            client.last_pulse = now

            elif msg.startswith(b'NODE_INFO:'):
                try:
                    msg = deserialize(msg[len(b'NODE_INFO:'):])
                except Exception:
                    dispynode_logger.warning('Ignoring invalid INFO request from %s', addr[0])
                else:
                    if (msg.get('version', None) == _dispy_version and
                        msg.get('sign', 1) != self.admin_sign):
                        Task(send_node_info, msg, addr[0])

            else:
                dispynode_logger.warning('Ignoring ping message from %s', addr[0])

    def status_info(self):
        info = {'busy': self.num_cpus - self.avail_cpus, 'serve': self.serve,
                'clients_done': self.clients_done, 'jobs_done': self.jobs_done,
                'cpu_time': self.cpu_time, 'scheduler_ip': self.scheduler['ip_addr']}
        if psutil:
            info['avail_info'] = DispyNodeAvailInfo(
                100.0 - psutil.cpu_percent(), psutil.virtual_memory().available,
                psutil.disk_usage(self.dest_path_prefix).free,
                100.0 - psutil.swap_memory().percent)
        else:
            info['avail_info'] = None
        return info

    def tcp_server(self, addrinfo, task=None):
        task.set_daemon()

        tcp_sock = AsyncSocket(socket.socket(addrinfo.family, socket.SOCK_STREAM),
                               keyfile=self.keyfile, certfile=self.certfile)
        if self.port:
            tcp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        tcp_sock.bind((addrinfo.ip, self.port))
        if not self.port:
            self.port = tcp_sock.getsockname()[1]
        dispynode_logger.debug('TCP server at %s:%s', addrinfo.ip, self.port)
        tcp_sock.listen(30)

        while 1:
            try:
                conn, addr = yield tcp_sock.accept()
            except GeneratorExit:
                break
            except Exception:
                dispynode_logger.debug(traceback.format_exc())
                continue
            Task(self.tcp_req, conn, addr)
        tcp_sock.close()

    def tcp_req(self, conn, addr, task=None):

        def job_request(msg):
            error = None
            try:
                _job = deserialize(msg)
                client = self.clients[_job.compute_id]
                compute = client.compute
                assert compute.scheduler_ip_addr == self.scheduler['ip_addr']
            except IndexError:
                error = ('Invalid computation (node %s may have closed computation due to zombie)' %
                         compute.node_ip_addr)
            except AssertionError:
                error = 'Invalid scheduler for computation!'
            except Exception:
                error = 'Invalid job (%s)' % traceback.format_exc()
            else:
                if self.avail_cpus == 0:
                    error = 'All CPUs busy!'
                else:
                    for xf in _job.xfer_files:
                        if MaxFileSize and xf.stat_buf.st_size > MaxFileSize:
                            error = 'File %s must be less than %s in size' % (xf.name, MaxFileSize)
                            break

            if error:
                try:
                    yield conn.send_msg(error.encode())
                except Exception:
                    pass
                raise StopIteration

            # if (compute.scheduler_ip_addr != self.scheduler['ip_addr'] or
            #     compute.scheduler_port != self.scheduler['port'] or
            #     compute.auth not in self.scheduler['auth']):
            #     dispynode_logger.debug('Invalid scheduler IP address: scheduler %s:%s != %s:%s',
            #                            compute.scheduler_ip_addr, compute.scheduler_port,
            #                            self.scheduler['ip_addr'], self.scheduler['port'])
            #     raise StopIteration

            dispynode_logger.debug('New job id %s from %s/%s',
                                   _job.uid, addr[0], compute.scheduler_ip_addr)

            addrinfo = self.scheduler['addrinfo']
            reply = _JobReply(_job, addrinfo.ext_ip)
            job_info = _DispyJobInfo(reply, compute, _job.xfer_files)
            for addrinfo in self.addrinfos:
                if compute.node_ip_addr in (addrinfo.ip, addrinfo.ext_ip):
                    break
            else:
                addrinfo = None
            job_info.addrinfo = addrinfo
            job_info.job_reply.start_time = time.time()
            job_info.job_reply.status = DispyJob.Running
            self.thread_lock.acquire()
            self.job_infos[_job.uid] = job_info
            self.thread_lock.release()

            if compute.type == _Compute.func_type:
                try:
                    yield conn.send_msg(b'ACK')
                except Exception:
                    dispynode_logger.warning('Failed to send response for new job to %s', str(addr))
                    job_info.job_reply.status = DispyJob.Terminated
                    raise StopIteration

                self.avail_cpus -= 1
                client.pending_jobs += 1
                client.jobs_done.clear()
                try:
                    if client.use_setup_proc:
                        args = {'req': 'job', 'job_reply': job_info.job_reply, 'code': _job.code,
                                'args': _job._args, 'kwargs': _job._kwargs}
                        client.parent_pipe.send(args)
                    else:
                        client.globals['__dispy_job_reply'] = job_info.job_reply
                        args = (compute.name, (compute.code, _job.code),
                                client.globals, _job._args, _job._kwargs)
                        os.chdir(compute.dest_path)
                        job_info.proc = multiprocessing.Process(target=_dispy_job_func, args=args)
                        job_info.proc.start()
                        os.chdir(self.dest_path_prefix)
                        job_pkl = os.path.join(self.dest_path_prefix, 'job_%s.pkl' % reply.uid)
                        with open(job_pkl, 'wb') as fd:
                            pickle.dump({'pid': job_info.proc.pid, 'ppid': self.pid}, fd)
                except Exception:
                    job_info.job_reply.result = serialize(None)
                    job_info.job_reply.status = DispyJob.Terminated
                    job_info.job_reply.exception = traceback.format_exc()
                    job_info.job_reply.end_time = job_info.job_reply.start_time
                    job_info.proc = None
                    self.reply_Q.put(job_info.job_reply)
                _job._args = _job._kwargs = args = None
                raise StopIteration
            else:
                # compute.type == _Compute.prog_type:
                try:
                    yield conn.send_msg(b'ACK')
                except Exception:
                    dispynode_logger.warning('Failed to send response for new job to %s', str(addr))
                    job_info.job_reply.status = DispyJob.Terminated
                    raise StopIteration
                prog_thread = threading.Thread(target=self.__job_program, args=(_job, job_info))
                self.avail_cpus -= 1
                client.pending_jobs += 1
                client.jobs_done.clear()
                prog_thread.start()
                raise StopIteration

        def add_computation(msg):
            reply = None
            try:
                compute = deserialize(msg)
                for addrinfo in self.addrinfos:
                    if compute.node_ip_addr in (addrinfo.ip, addrinfo.ext_ip):
                        break
                else:
                    reply = 'Invalid node address: %s' % compute.node_ip_addr
            except Exception:
                reply = ('Invalid computation request ignored').encode()
            else:
                if self.scheduler['auth']:
                    if (self.scheduler['ip_addr'] == compute.scheduler_ip_addr and
                        self.scheduler['port'] == compute.scheduler_port):
                        if compute.id in self.clients:
                            reply = serialize(0)
                    else:
                        dispynode_logger.debug('Ignoring computation request from %s: %s, %s, %s',
                                               compute.scheduler_ip_addr, self.scheduler['ip_addr'],
                                               self.avail_cpus, self.num_cpus)
                        reply = serialize('Node busy')
                elif not self.service_available():
                    reply = serialize(-1)
            if reply:
                try:
                    yield conn.send_msg(reply)
                except Exception:
                    pass
                raise StopIteration

            if MaxFileSize:
                for xf in compute.xfer_files:
                    if xf.stat_buf.st_size > MaxFileSize:
                        try:
                            yield conn.send_msg(('File "%s" is too big; limit is %s' %
                                                 (xf.name, MaxFileSize)).encode())
                        except Exception:
                            pass
                        raise StopIteration
            compute.xfer_files = set()
            if compute.code:
                try:
                    compute.code = compile(compute.code, '<string>', 'exec')
                except Exception:
                    yield conn.send_msg(('Invalid computation code').encode())
                    raise StopIteration
                compute.code = marshal.dumps(compute.code)
            dest = compute.scheduler_ip_addr
            if os.name == 'nt':
                dest = dest.replace(':', '_')
            dest = os.path.join(self.dest_path_prefix, dest)
            if not os.path.isdir(dest):
                try:
                    os.mkdir(dest)
                except Exception:
                    yield conn.send_msg(('Could not create destination path').encode())
                    raise StopIteration
            os.chmod(dest, stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
            if compute.dest_path and isinstance(compute.dest_path, str):
                # TODO: get os.sep from client and convert (in case of mixed environments)?
                if not compute.dest_path.startswith(os.sep):
                    compute.dest_path = os.path.join(dest, compute.dest_path)
                if not os.path.isdir(compute.dest_path):
                    try:
                        os.mkdir(compute.dest_path)
                        os.chmod(compute.dest_path, stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR |
                                 stat.S_IRGRP | stat.S_IWGRP | stat.S_IXGRP | stat.S_ISGID)
                        if self.sgid is not None:
                            os.chown(compute.dest_path, -1, self.sgid)
                    except Exception:
                        try:
                            yield conn.send_msg(('Could not create destination path').encode())
                        except Exception:
                            pass
                        raise StopIteration
            else:
                compute.dest_path = tempfile.mkdtemp(prefix=str(compute.id) + '_', dir=dest)
                os.chmod(compute.dest_path, stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR |
                         stat.S_IRGRP | stat.S_IWGRP | stat.S_IXGRP | stat.S_ISGID)
            if self.sgid is not None:
                os.chown(compute.dest_path, -1, self.sgid)

            if compute.id in self.clients:
                dispynode_logger.warning('Computation "%s" (%s) is being replaced',
                                         compute.name, compute.id)

            if compute.type == _Compute.prog_type:
                compute.name = os.path.join(compute.dest_path, os.path.basename(compute.name))

            if not compute.cleanup and self._force_cleanup:
                compute.cleanup = True

            if not ((not self.scheduler['auth']) or
                    (self.scheduler['ip_addr'] == compute.scheduler_ip_addr and
                     self.scheduler['port'] == compute.scheduler_port)):
                if os.path.isdir(compute.dest_path):
                    try:
                        os.rmdir(compute.dest_path)
                        yield conn.send_msg(serialize(-1))
                    except Exception:
                        pass
                raise StopIteration

            client = _Client(compute)
            client.sock_family = conn.family
            pkl_path = os.path.join(self.dest_path_prefix, '%s_%s.pkl' % (compute.id, compute.auth))
            with open(pkl_path, 'wb') as fd:
                pickle.dump(client, fd)
            self.clients[compute.id] = client
            prev_scheduler = (self.scheduler['ip_addr'], self.scheduler['port'])
            self.scheduler['ip_addr'] = compute.scheduler_ip_addr
            self.scheduler['port'] = compute.scheduler_port
            self.scheduler['auth'].add(compute.auth)
            self.scheduler['addrinfo'] = addrinfo

            try:
                yield conn.send_msg(serialize(self.avail_cpus))
            except Exception:
                del self.clients[compute.id]
                self.scheduler['auth'].discard(compute.auth)
                self.scheduler['ip_addr'], self.scheduler['port'] = prev_scheduler
                self.scheduler['addrinfo'] = None
                os.remove(pkl_path)
                if os.path.isdir(compute.dest_path):
                    try:
                        os.rmdir(compute.dest_path)
                    except Exception:
                        pass
            else:
                self.pulse_interval = num_min(self.pulse_interval, compute.pulse_interval)
                if not self.pulse_interval:
                    self.pulse_interval = 10 * 60
                if self.zombie_interval:
                    self.pulse_interval = num_min(self.pulse_interval, self.zombie_interval / 5.0)
                self.timer_task.resume(True)
                dispynode_logger.debug('New computation "%s" from %s',
                                       compute.auth, compute.scheduler_ip_addr)

        def xfer_file_req(msg):
            try:
                xf = deserialize(msg)
            except Exception:
                dispynode_logger.debug('Ignoring file trasnfer request from %s', addr[0])
                raise StopIteration

            client = self.clients.get(xf.compute_id, None)
            if not client or (MaxFileSize and xf.stat_buf.st_size > MaxFileSize):
                dispynode_logger.error('Invalid file transfer for "%s"', xf.name)
                yield conn.send_msg(serialize(-1))
                raise StopIteration
            compute = client.compute
            tgt = os.path.join(compute.dest_path, xf.dest_path.replace(xf.sep, os.sep),
                               xf.name.split(xf.sep)[-1])
            if os.path.isfile(tgt) and _same_file(tgt, xf):
                if tgt in client.file_uses:
                    client.file_uses[tgt] += 1
                else:
                    client.file_uses[tgt] = 2
                yield conn.send_msg(serialize(xf.stat_buf.st_size))
            else:
                recvd = 0
                try:
                    if not os.path.isdir(os.path.dirname(tgt)):
                        os.makedirs(os.path.dirname(tgt))
                    with open(tgt, 'wb') as fd:
                        dispynode_logger.debug('Copying file %s to %s (%s)',
                                               xf.name, tgt, xf.stat_buf.st_size)
                        while recvd < xf.stat_buf.st_size:
                            yield conn.send_msg(serialize(recvd))
                            data = yield conn.recvall(min(xf.stat_buf.st_size-recvd, 1024000))
                            if not data:
                                break
                            fd.write(data)
                            recvd += len(data)
                        yield conn.send_msg(serialize(recvd))
                    assert recvd == xf.stat_buf.st_size
                    os.utime(tgt, (xf.stat_buf.st_atime, xf.stat_buf.st_mtime))
                    os.chmod(tgt, stat.S_IMODE(xf.stat_buf.st_mode))
                    # flags = stat.S_IMODE(xf.stat_buf.st_mode) & stat.S_IRWXU
                    # os.chmod(tgt, stat.S_IMODE(xf.stat_buf.st_mode) |
                    #          (stat.S_IRGRP if (flags & stat.S_IRUSR) else 0) |
                    #          (stat.S_IWGRP if (flags & stat.S_IWUSR) else 0) |
                    #          (stat.S_IXGRP if (flags & stat.S_IXUSR) else 0))
                    if self.sgid is not None:
                        os.chown(tgt, -1, self.sgid)
                except Exception:
                    dispynode_logger.warning('Copying file "%s" failed (%s / %s) with "%s"',
                                             xf.name, recvd, xf.stat_buf.st_size,
                                             traceback.format_exc())
                    os.remove(tgt)
                else:
                    if tgt in client.file_uses:
                        client.file_uses[tgt] += 1
                    else:
                        client.file_uses[tgt] = 1
            raise StopIteration  # xfer_file_req

        def setup_computation(msg, task=None):
            if not task:
                task = pycos.Pycos.cur_task()

            client = None
            try:
                msg = deserialize(msg)
                client = self.clients[msg['compute_id']]
                client.setup_args = msg['setup_args']
                assert isinstance(client.setup_args, tuple)
                assert client.cleanup_args is None
                client.cleanup_args = client.setup_args
                compute = client.compute
            except Exception:
                dispynode_logger.debug(traceback.format_exc())
                raise StopIteration(client, 'invalid computation')

            client.globals['dispy_node_name'] = self.name
            client.globals['dispy_node_ip_addr'] = compute.node_ip_addr
            client.globals['dispy_job_path'] = compute.dest_path
            client.globals['_DispyNode'] = None
            client.globals['_dispy_node'] = None
            client.globals['_Client'] = None
            client.globals['_dispy_config'] = None
            client.globals['_dispy_setup_process'] = None
            client.globals['_dispy_terminate_proc'] = None
            client.globals['__dispy_sock_family'] = client.sock_family
            client.globals['__dispy_certfile'] = self.certfile
            client.globals['__dispy_keyfile'] = self.keyfile
            client.globals['__dispy_job_reply_addr'] = (compute.scheduler_ip_addr,
                                                        compute.job_result_port)
            if compute.client_reply_addr:
                client.globals['__dispy_client_reply_addr'] = compute.client_reply_addr
            else:
                client.globals['__dispy_client_reply_addr'] = (compute.scheduler_ip_addr,
                                                               compute.job_result_port)
            client.globals['reply_Q'] = self.reply_Q
            client.globals['suid'] = self.suid
            client.globals['sgid'] = self.sgid

            setup_status = 0
            if (self._safe_setup and (compute.setup or isinstance(compute.cleanup, str) or
                                      self.suid is not None)):
                # TODO: use this only for function computations?
                client.globals['loglevel'] = dispynode_logger.level
                client.globals['setup_args'] = client.setup_args
                client.parent_pipe, client.child_pipe = multiprocessing.Pipe(duplex=True)
                args = (client.compute, client.child_pipe, client.globals)
                client.setup_proc = multiprocessing.Process(target=_dispy_setup_process, args=args)
                client.setup_proc.start()
                client.setup_args = None
                for i in range(40):
                    if client.parent_pipe.poll():
                        msg = client.parent_pipe.recv()
                        break
                    else:
                        yield task.sleep(0.25)
                else:
                    raise StopIteration(client, '"setup" has not finished in time')

                setup_status = msg['setup_status']
                if setup_status == 0 or self.suid is not None:
                    for i in range(10):
                        if client.parent_pipe.poll():
                            msg = client.parent_pipe.recv()
                            break
                        else:
                            yield task.sleep(0.25)
                    else:
                        raise StopIteration(client, 'setup process failed')
                    client.use_setup_proc = msg.get('use_setup_proc', True)

                elif setup_status == 1:
                    client.use_setup_proc = False
                    for i in range(10):
                        if client.parent_pipe.poll():
                            try:
                                msg = client.parent_pipe.recv()
                            except Exception:
                                raise StopIteration(client, 'importing setup globals failed')
                            else:
                                break
                        else:
                            yield task.sleep(0.25)
                    else:
                        raise StopIteration(client, 'setup process failed')
                    client.globals.update(msg.get('setup_globals', {}))

                elif setup_status == 2:
                    client.use_setup_proc = True

                else:
                    raise StopIteration(client, '"setup" failed with %s' % setup_status)

            if (not client.use_setup_proc):
                globalvars = dict(self.__init_globals)
                globalvars.update(client.globals)
                if compute.code:
                    exec(marshal.loads(compute.code), globalvars)
                if compute.setup and isinstance(client.setup_args, tuple):
                    init_vars = set(globalvars.keys())
                    localvars = {'_dispy_setup_status': 0, '_dispy_setup_args': client.setup_args}
                    os.chdir(compute.dest_path)
                    sys.path.insert(0, compute.dest_path)
                    exec('_dispy_setup_status = %s(*_dispy_setup_args)' %
                         compute.setup, globalvars, localvars)
                    try:
                        sys.path.remove(compute.dest_path)
                    except Exception:
                        pass
                    if localvars['_dispy_setup_status'] in (0, 1):
                        for var, value in globalvars.items():
                            if var not in init_vars:
                                client.globals[var] = value
                    else:
                        raise StopIteration(client, '"setup" failed with %s' %
                                            localvars['_dispy_setup_status'])
                    client.setup_args = None
                    os.chdir(self.dest_path_prefix)

                    for module in list(sys.modules.keys()):
                        if module not in self.__init_modules:
                            sys.modules.pop(module, None)
                    sys.modules.update(self.__init_modules)

            if client.setup_proc and isinstance(compute.cleanup, str):
                client.cleanup_args = None
            raise StopIteration(None, 'ACK')

        def terminate_job(client, job_info, task=None):
            compute = client.compute
            if job_info.proc:
                proc = job_info.proc
                pid = proc.pid
            else:
                proc = None
                pid = job_info.pid
            if not pid or job_info.job_reply.status != DispyJob.Running:
                raise StopIteration
            dispynode_logger.debug('Terminating job %s of "%s" (%s)',
                                   job_info.job_reply.uid, compute.name, pid)
            job_info.job_reply.status = DispyJob.Terminated
            if client.use_setup_proc:
                client.parent_pipe.send({'req': 'terminate_job', 'pid': job_info.pid,
                                         'job_reply': job_info.job_reply})
                raise StopIteration

            if not proc and psutil:
                try:
                    proc = psutil.Process(pid)
                    assert proc.is_running()
                    assert proc.ppid() == self.pid
                except (psutil.NoSuchProcess, psutil.ZombieProcess, Exception):
                    dispynode_logger.debug(traceback.format_exc())
                    raise StopIteration

            if proc:
                proc_pid = proc
            else:
                proc_pid = pid

            suid = client.globals.get('suid', None)
            if suid is None:
                status = yield _dispy_terminate_proc(proc_pid, task=task)
            else:
                # TODO: terminating process must be parent of process being
                # killed, so creating suid process won't work!
                status = -1

            if status:
                dispynode_logger.debug('Terminating job %s (PID %s) failed',
                                       job_info.job_reply.uid, pid)
                raise StopIteration

            job_reply = copy.copy(job_info.job_reply)
            job_reply.result = serialize(None)
            job_reply.end_time = time.time()
            self.reply_Q.put(job_reply)

        def retrieve_job(msg):
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

            client = self.clients.get(compute_id, None)
            if client:
                compute = client.compute
            else:
                pkl_path = os.path.join(self.dest_path_prefix, '%s_%s.pkl' % (compute_id, auth))
                try:
                    with open(pkl_path, 'rb') as fd:
                        client = pickle.load(fd)
                        compute = client.compute
                except Exception:
                    compute = None
            if not compute or compute.auth != auth:
                yield send_reply(None)
                raise StopIteration

            info_file = os.path.join(compute.dest_path, '_dispy_job_reply_%s.pkl' % uid)
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
                assert ack == b'ACK'
                client.pending_results -= 1
                pkl_path = os.path.join(self.dest_path_prefix, '%s_%s.pkl' % (compute_id, auth))
                with open(pkl_path, 'wb') as fd:
                    if 'reply_Q' in client.globals:
                        info = copy.copy(client)
                        info.globals = dict(client.globals)
                        info.globals.pop('reply_Q', None)
                    else:
                        info = client
                    pickle.dump(info, fd)
            except Exception:
                pass
            else:
                client.file_uses.pop(info_file, None)
                try:
                    os.remove(info_file)
                except Exception:
                    pass
                if client.pending_results == 0:
                    Task(self.cleanup_computation, client, close=True)

        def close_node(client, close, task=None):
            task.set_daemon()
            while 1:
                yield client.jobs_done.wait()
                if (yield self.cleanup_computation(client, close=close, task=task)) == 0:
                    break

        # tcp_req starts
        try:
            req = yield conn.recvall(len(self.auth))
        except Exception:
            dispynode_logger.warning('Ignoring request from %s:%s', addr[0], addr[1])
            conn.close()
            raise StopIteration
        msg = yield conn.recv_msg()
        if req != self.auth:
            if msg.startswith(b'PING:'):
                pass

            elif msg == b'NODE_STATUS:':
                if req == self.admin_auth:
                    info = self.status_info()
                else:
                    info = -1
                yield conn.send_msg(serialize(info))
                conn.close()
                raise StopIteration

            elif msg.startswith(b'NODE_INFO:'):
                if req == self.admin_auth:
                    info = self.status_info()
                    info.update({'name': self.name, 'service_start': self.service_start,
                                 'service_stop': self.service_stop, 'service_end': self.service_end,
                                 'cpus': self.avail_cpus, 'max_cpus': multiprocessing.cpu_count()})
                    info = serialize(info)
                    msg = deserialize(msg[len(b'NODE_INFO:'):])
                    self.admin_sign = msg.get('sign', None)
                else:
                    info = self.sign.encode()
                yield conn.send_msg(info)
                conn.close()
                raise StopIteration

            elif msg.startswith(b'SET_CPUS:'):
                if req == self.admin_auth:
                    msg = deserialize(msg[len(b'SET_CPUS:'):])
                    resp = self.set_cpus(msg.get('cpus', None))
                    if resp == 0:
                        resp = {'cpus': self.num_cpus}
                else:
                    resp = -1
                yield conn.send_msg(serialize(resp))
                conn.close()
                raise StopIteration

            elif msg.startswith(b'SERVICE_TIME:'):
                if req == self.admin_auth:
                    msg = deserialize(msg[len(b'SERVICE_TIME:'):])
                    resp = self.service_control(msg.get('control', None), msg.get('time', None))
                else:
                    resp = -1
                yield conn.send_msg(serialize(resp))
                conn.close()
                raise StopIteration

            elif msg.startswith(b'SERVE_CLIENTS:'):
                if req == self.admin_auth:
                    msg = deserialize(msg[len(b'SERVE_CLIENTS:'):])
                    serve = msg.get('serve', None)
                    if isinstance(serve, int):
                        self.serve = serve
                        resp = {'serve': serve}
                    else:
                        resp = -1
                else:
                    resp = -1
                yield conn.send_msg(serialize(resp))
                conn.close()
                raise StopIteration

            else:
                dispynode_logger.warning('Ignoring invalid request from %s:%s', addr[0], addr[1])
                conn.close()
                raise StopIteration
        if not msg:
            conn.close()
            raise StopIteration
        if msg.startswith(b'JOB:'):
            msg = msg[len(b'JOB:'):]
            yield job_request(msg)
            conn.close()
        elif msg.startswith(b'COMPUTE:'):
            msg = msg[len(b'COMPUTE:'):]
            yield add_computation(msg)
            conn.close()
        elif msg.startswith(b'FILEXFER:'):
            msg = msg[len(b'FILEXFER:'):]
            yield xfer_file_req(msg)
            conn.close()
        elif msg.startswith(b'SETUP:'):
            msg = msg[len(b'SETUP:'):]
            client, resp = yield setup_computation(msg, task=task)
            if client:
                client.zombie = True
                Task(self.cleanup_computation, client, close=True)
            yield conn.send_msg(resp.encode())
            conn.close()
        elif msg.startswith(b'CLOSE:'):
            msg = msg[len(b'CLOSE:'):]
            try:
                info = deserialize(msg)
                compute_id = info['compute_id']
                auth = info['auth']
                terminate_pending = info.get('terminate_pending', False)
                close = info.get('close', True)
            except Exception:
                dispynode_logger.debug('Deleting computation failed with %s',
                                       traceback.format_exc())
            else:
                client = self.clients.get(compute_id, None)
                if client:
                    compute = client.compute
                else:
                    compute = None
                if (compute is None or compute.auth != auth or
                    compute.node_ip_addr != info.get('node_ip_addr', None)):
                    dispynode_logger.warning('Computation "%s" is not valid', compute_id)
                else:
                    client.zombie = True
                    if terminate_pending:
                        self.thread_lock.acquire()
                        job_infos = [job_info for job_info in self.job_infos.values()
                                     if job_info.compute_id == compute_id]
                        self.thread_lock.release()
                        for job_info in job_infos:
                            Task(terminate_job, client, job_info)
                    Task(close_node, client, close)
            yield conn.send_msg(b'ACK')
            conn.close()
        elif msg.startswith(b'TERMINATE_JOB:'):
            msg = msg[len(b'TERMINATE_JOB:'):]
            try:
                _job = deserialize(msg)
                client = self.clients[_job.compute_id]
                # assert addr[0] == compute.scheduler_ip_addr
                self.thread_lock.acquire()
                job_info = self.job_infos.get(_job.uid, None)
                self.thread_lock.release()
                assert job_info is not None
            except Exception:
                dispynode_logger.debug('Invalid terminate job request from %s, %s',
                                       addr[0], client.compute.scheduler_ip_addr)
            else:
                Task(terminate_job, client, job_info)
            conn.close()
        elif msg.startswith(b'RESEND_JOB_RESULTS:'):
            msg = msg[len(b'RESEND_JOB_RESULTS:'):]
            try:
                info = deserialize(msg)
                compute_id = info['compute_id']
                auth = info['auth']
            except Exception:
                reply = 0
            else:
                client = self.clients.get(compute_id, None)
                if client:
                    compute = client.compute
                else:
                    compute = None
                if compute is None or compute.auth != auth:
                    try:
                        with open(os.path.join(self.dest_path_prefix,
                                               '%s_%s.pkl' % (compute_id, auth)), 'rb') as fd:
                            client = pickle.load(fd)
                            compute = client.compute
                    except Exception:
                        pass
                if compute is None:
                    reply = 0
                else:
                    reply = client.pending_results + client.pending_jobs
            yield conn.send_msg(serialize(reply))
            conn.close()
            if reply > 0:
                yield self.resend_job_results(client, task=task)
        elif msg.startswith(b'PING:'):
            try:
                info = deserialize(msg[len(b'PING:'):])
                if info['version'] == _dispy_version:
                    self.send_pong_msg(info, addr)
            except Exception:
                dispynode_logger.debug(traceback.format_exc())
            conn.close()

        elif msg == b'NODE_STATUS:':
            if self.admin_auth:
                info = -1
            else:
                info = self.status_info()
            yield conn.send_msg(serialize(info))
            conn.close()
            raise StopIteration

        elif msg.startswith(b'NODE_INFO:'):
            if self.admin_auth:
                info = self.sign.encode()
            else:
                info = self.status_info()
                info.update({'name': self.name, 'service_start': self.service_start,
                             'service_stop': self.service_stop, 'service_end': self.service_end,
                             'cpus': self.avail_cpus, 'max_cpus': multiprocessing.cpu_count()})
                info = serialize(info)
            yield conn.send_msg(info)
            conn.close()
            raise StopIteration

        elif msg.startswith(b'SERVICE_TIME:') or msg.startswith(b'SERVE_CLIENTS'):
            yield conn.send_msg(b'')
            conn.close()

        elif msg.startswith(b'JOBS:'):
            msg = msg[len(b'JOBS:'):]
            try:
                info = deserialize(msg)
                compute_id = info['compute_id']
                auth = info['auth']
            except Exception:
                pass
            else:
                client = self.clients.get(compute_id, None)
                if client:
                    compute = client.compute
                else:
                    compute = None
                if compute and compute.auth == auth:
                    reply = [uid for uid, job_info in self.job_infos.items()
                             if job_info.compute_id == compute_id]
                else:
                    reply = []
                yield conn.send_msg(serialize(reply))
            conn.close()

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
                client = self.clients.get(compute_id, None)
                if client:
                    compute = client.compute
                else:
                    compute = None
                if compute is None or compute.auth != auth:
                    pkl_path = os.path.join(self.dest_path_prefix, '%s_%s.pkl' % (compute_id, auth))
                    try:
                        with open(pkl_path, 'rb') as fd:
                            client = pickle.load(fd)
                            compute = client.compute
                    except Exception:
                        pass
                if compute is not None:
                    done = []
                    if client.pending_results:
                        for result_file in glob.glob(os.path.join(compute.dest_path,
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
                    reply['pending'] = client.pending_jobs
            yield conn.send_msg(serialize(reply))
            conn.close()
        elif msg.startswith(b'RETRIEVE_JOB:'):
            msg = msg[len(b'RETRIEVE_JOB:'):]
            yield retrieve_job(msg)
            conn.close()
        else:
            dispynode_logger.warning('Invalid request "%s" from %s',
                                     msg[:min(10, len(msg))], addr[0])
            resp = ('NAK (invalid command: %s)' % (msg[:min(10, len(msg))])).encode()
            try:
                yield conn.send_msg(resp)
            except Exception:
                dispynode_logger.warning('Failed to send reply to %s', str(addr))
            conn.close()

    def resend_job_results(self, client, task=None):
        # TODO: limit number queued so as not to take up too much space/time
        compute = client.compute
        if not os.path.isdir(compute.dest_path):
            raise StopIteration
        reply_files = [f for f in os.listdir(compute.dest_path)
                       if f.startswith('_dispy_job_reply_') and f.endswith('.pkl')]
        if len(reply_files) > 64:
            reply_files = reply_files[:64]
        for reply_file in reply_files:
            reply_file = os.path.join(compute.dest_path, reply_file)
            try:
                with open(reply_file, 'rb') as fd:
                    job_reply = pickle.load(fd)
            except Exception:
                dispynode_logger.debug('Could not load "%s"', reply_file)
                # dispynode_logger.debug(traceback.format_exc())
                continue
            job_info = _DispyJobInfo(job_reply, compute, [])
            status = yield self._send_job_reply(job_info, resending=True)
            if status:
                break

    def timer_proc(self, task=None):
        task.set_daemon()
        last_pulse_time = last_zombie_time = time.time()

        def reply_dead_jobs(job_infos, task=None):
            yield task.sleep(2)
            self.thread_lock.acquire()
            job_infos = [job_info for job_info in job_infos
                         if self.job_infos.get(job_info.job_reply.uid, None) == job_info]
            self.thread_lock.release()
            for job_info in job_infos:
                if isinstance(job_info.proc, multiprocessing.Process):
                    try:
                        job_info.proc.join(0.1)
                    except Exception:
                        continue
                elif psutil and isinstance(job_info.proc, psutil.Process):
                    try:
                        status = job_info.proc.status()
                    except (psutil.NoSuchProcess, Exception):
                        status = None
                    except psutil.ZombieProcess:
                        status = psutil.STATUS_ZOMBIE

                    if status == psutil.STATUS_ZOMBIE:
                        client = self.clients.get(job_info.compute_id, None)
                        if client and client.use_setup_proc:
                            client.parent_pipe.send({'req': 'wait_pid', 'pid': job_info.pid,
                                                     'job_reply': job_info.job_reply})
                            continue
                else:
                    continue

                job_reply = job_info.job_reply
                job_reply.status = DispyJob.Terminated
                job_reply.result = serialize(None)
                job_reply.end_time = time.time()
                self.reply_Q.put(job_reply)

        while 1:
            reset = yield task.suspend(self.pulse_interval)
            if reset:
                continue

            now = time.time()
            if self.pulse_interval and (now - last_pulse_time) >= self.pulse_interval:
                if self.scheduler['auth']:
                    addrinfo = self.scheduler['addrinfo']
                    last_pulse_time = now
                    info = {'ip_addr': addrinfo.ext_ip, 'port': self.port,
                            'cpus': self.num_cpus - self.avail_cpus,
                            'scheduler_ip_addr': self.scheduler['ip_addr']}
                    if psutil:
                        info['avail_info'] = DispyNodeAvailInfo(
                            100.0 - psutil.cpu_percent(), psutil.virtual_memory().available,
                            psutil.disk_usage(self.dest_path_prefix).free,
                            100.0 - psutil.swap_memory().percent)
                    else:
                        info['avail_info'] = None

                    sock = AsyncSocket(socket.socket(addrinfo.family, socket.SOCK_STREAM),
                                       keyfile=self.keyfile, certfile=self.certfile)
                    sock.settimeout(MsgTimeout)
                    try:
                        yield sock.connect((self.scheduler['ip_addr'], self.scheduler['port']))
                        yield sock.send_msg(b'PULSE:' + serialize(info))
                        if (yield sock.recv_msg()) == b'PULSE':
                            for client in self.client.values():
                                client.last_pulse = now
                    except Exception:
                        pass
                    sock.close()

                resend = [client for client in self.clients.values()
                          if client.pending_results and not client.zombie]
                for client in resend:
                    Task(self.resend_job_results, client)

            if self.zombie_interval and (now - last_zombie_time) >= self.zombie_interval:
                last_zombie_time = now

                self.thread_lock.acquire()
                job_infos = list(self.job_infos.values())
                self.thread_lock.release()
                dead_jobs = []
                for job_info in job_infos:
                    proc = job_info.proc
                    if isinstance(proc, multiprocessing.Process):
                        if not proc.is_alive():
                            dead_jobs.append(job_info)
                    elif isinstance(proc, subprocess.Popen):
                        if proc.poll() is not None:
                            dead_jobs.append(job_info)
                    elif psutil and isinstance(proc, psutil.Process):
                        try:
                            status = proc.status()
                        except (psutil.NoSuchProcess, psutil.ZombieProcess):
                            status = None
                        except Exception:
                            status = -1
                        if status is None or status == psutil.STATUS_ZOMBIE:
                            dead_jobs.append(job_info)
                if dead_jobs:
                    Task(reply_dead_jobs, dead_jobs)

                for client in self.clients.values():
                    if (now - client.last_pulse) > self.zombie_interval:
                        dispynode_logger.warning('Computation "%s" is marked as zombie',
                                                 client.compute.id)
                        client.zombie = True
                zombies = [client for client in self.clients.values()
                           if client.zombie and client.pending_jobs == 0]
                for client in zombies:
                    dispynode_logger.warning('Deleting zombie computation "%s"',
                                             client.compute.id)
                    Task(self.cleanup_computation, client, close=True)
                for client in zombies:
                    compute = client.compute
                    for addrinfo in self.addrinfos:
                        if compute.node_ip_addr in (addrinfo.ip, addrinfo.ext_ip):
                            break
                    else:
                        dispynode_logger.warning('%s is not valid address!', compute.node_ip_addr)
                        continue
                    sock = AsyncSocket(socket.socket(addrinfo.family, socket.SOCK_STREAM),
                                       keyfile=self.keyfile, certfile=self.certfile)
                    sock.settimeout(MsgTimeout)
                    dispynode_logger.debug('Sending TERMINATE to %s', compute.scheduler_ip_addr)
                    info = {'ip_addr': addrinfo.ext_ip, 'port': self.port, 'sign': self.sign}
                    try:
                        yield sock.connect((compute.scheduler_ip_addr, compute.scheduler_port))
                        yield sock.send_msg('TERMINATED:'.encode() + serialize(info))
                    except Exception:
                        pass
                    finally:
                        sock.close()
                if (not self.scheduler['auth']):
                    self.pulse_interval = self.ping_interval
                    yield self.broadcast_ping_msg(task=task)
            if self.ping_interval and (not self.scheduler['auth']):
                yield self.broadcast_ping_msg(task=task)

    def set_cpus(self, cpus):
        max_cpus = multiprocessing.cpu_count()
        if cpus is None:
            cpus = max_cpus
        else:
            if not isinstance(cpus, int):
                return -1
            if cpus >= 0:
                if cpus > max_cpus:
                    return -1
            else:
                cpus += max_cpus
                if cpus < 0:
                    return -1

        if cpus == self.num_cpus:
            return 0
        self.num_cpus = cpus
        self.avail_cpus = cpus - len(self.job_infos)
        if self.scheduler['auth']:
            def announce_cpus(task=None):
                addrinfo = self.scheduler['addrinfo']
                sock = AsyncSocket(socket.socket(addrinfo.family, socket.SOCK_STREAM),
                                   keyfile=self.keyfile, certfile=self.certfile)
                sock.settimeout(MsgTimeout)
                try:
                    yield sock.connect((self.scheduler['ip_addr'], self.scheduler['port']))
                    info = {'ip_addr': addrinfo.ext_ip, 'sign': self.sign, 'cpus': cpus}
                    yield sock.send_msg('NODE_CPUS:'.encode() + serialize(info))
                except Exception:
                    pass
                finally:
                    sock.close()
            Task(announce_cpus)
        else:
            if self.num_cpus > 0:
                Task(self.broadcast_ping_msg)
        return 0

    def service_available(self):
        if self.serve == 0:
            return False
        now = int(time.time())
        if self.service_start:
            if self.service_start >= now:
                return False
        if self.service_stop:
            if self.service_stop <= now:
                return False
        elif self.service_end:
            if self.service_end <= now:
                return False
        return True

    def service_control(self, control, service_time):
        if service_time is None:
            service_time = time.time()
        elif service_time == '0':
            service_time = None
        else:
            bod = time.localtime()
            bod = (int(time.time()) - (bod.tm_hour * 3600) - (bod.tm_min * 60))
            try:
                service_time = time.strptime(service_time, '%H:%M')
                service_time = bod + (service_time.tm_hour * 3600) + (service_time.tm_min * 60)
            except Exception:
                return -1

        if control == 'start_time':
            if service_time:
                if ((self.service_stop and self.service_stop < service_time) or
                    (self.service_end and self.service_end < service_time)):
                    service_time -= 24 * 3600
            self.service_start = service_time
        elif control == 'stop_time':
            if service_time:
                if self.service_start and self.service_start > service_time:
                    service_time += 24 * 3600
                if self.service_end and self.service_end < service_time:
                    return -1
            self.service_stop = service_time
        elif control == 'end_time':
            if service_time:
                if ((self.service_start and self.service_start > service_time) or
                    (self.service_stop and self.service_stop > service_time)):
                    service_time += 24 * 3600
            self.service_end = service_time
        else:
            return -1
        self.service_schedule_task.resume(update=True)
        return {'service_start': self.service_start, 'service_stop': self.service_stop,
                'service_end': self.service_end}

    def service_schedule(self, task=None):
        task.set_daemon()
        yield task.sleep(2)
        while 1:
            if self.service_start:
                now = int(time.time())
                if self.service_start > (now + 60):
                    res = yield task.sleep(self.service_start - now)
                    if res:
                        continue
            if self.service_available():
                yield self.broadcast_ping_msg(task=task)

            if self.service_stop:
                now = int(time.time())
                if self.service_stop > (now + 60):
                    res = yield task.sleep(self.service_stop - now)
                    if res:
                        continue

                dispynode_logger.debug('Stopping service')
                addrinfo = self.scheduler['addrinfo']
                sock = AsyncSocket(socket.socket(addrinfo.family, socket.SOCK_STREAM),
                                   keyfile=self.keyfile, certfile=self.certfile)
                sock.settimeout(MsgTimeout)
                try:
                    yield sock.connect((self.scheduler['ip_addr'], self.scheduler['port']))
                    info = {'ip_addr': addrinfo.ext_ip, 'sign': self.sign, 'cpus': 0}
                    yield sock.send_msg('NODE_CPUS:'.encode() + serialize(info))
                except Exception:
                    pass
                finally:
                    sock.close()

            if self.service_end:
                now = int(time.time())
                if self.service_end > (now + 60):
                    res = yield task.sleep(self.service_end - now)
                    if res:
                        continue
                dispynode_logger.debug('Shutting down service')
                self.shutdown('close')

            if self.service_start or self.service_stop or self.service_end:
                # advance times for next day
                if self.service_start:
                    self.service_start += 24 * 3600
                if self.service_stop:
                    self.service_stop += 24 * 3600
                if self.service_end:
                    self.service_end += 24 * 3600
            else:
                yield task.sleep()

    def __job_program(self, _job, job_info):

        def suid_program(program, env, suid, sgid, pipe):
            if hasattr(os, 'setresuid'):
                os.setresgid(sgid, sgid, sgid)
                os.setresuid(suid, suid, suid)
            else:
                os.setregid(sgid, sgid)
                os.setreuid(suid, suid)

            if os.name == 'nt':
                flags = subprocess.CREATE_NEW_PROCESS_GROUP
            else:
                flags = 0
            sub_proc = subprocess.Popen(program, stdout=subprocess.PIPE,
                                        stderr=subprocess.PIPE, env=env, creationflags=flags)

            def sighandler(signum, frame):
                sub_proc.send_signal(signum)
                # os.kill(sub_proc.pid, signum)

            signal.signal(signal.SIGINT, sighandler)
            signal.signal(signal.SIGTERM, sighandler)
            if os.name == 'nt':
                signal.signal(signal.SIGBREAK, sighandler)

            out, err = sub_proc.communicate()
            pipe.send((out, err))
            pipe.close()
            exit(sub_proc.returncode)

        client = self.clients[_job.compute_id]
        compute = client.compute
        if compute.name.endswith('.py'):
            program = [sys.executable, compute.name]
        else:
            program = [compute.name]
        args = deserialize(_job._args)
        program.extend(args)
        reply = job_info.job_reply
        try:
            os.chdir(compute.dest_path)
            env = {}
            env.update(os.environ)
            env['PATH'] = compute.dest_path + os.pathsep + env['PATH']
            for k, v in client.globals.items():
                if isinstance(v, str):
                    env[k] = v
                elif isinstance(v, int):
                    env[k] = repr(v)
            job_pkl = os.path.join(self.dest_path_prefix, 'job_%s.pkl' % (reply.uid))
            suid = client.globals.get('suid', None)
            if suid is not None:
                pipe = multiprocessing.Pipe(duplex=False)
                args = (program, env, suid, client.globals['sgid'], pipe[1])
                job_info.proc = multiprocessing.Process(target=suid_program, args=args)
                job_info.proc.start()
                os.chdir(self.dest_path_prefix)
                with open(job_pkl, 'wb') as fd:
                    pickle.dump({'pid': job_info.proc.pid, 'ppid': self.pid}, fd)
                job_info.proc.join()
                reply.stdout, reply.stderr = pipe[0].recv()
                pipe[0].close()
                reply.result = serialize(job_info.proc.exitcode)
            else:
                job_info.proc = subprocess.Popen(program, stdout=subprocess.PIPE,
                                                 stderr=subprocess.PIPE, env=env)
                os.chdir(self.dest_path_prefix)
                with open(job_pkl, 'wb') as fd:
                    pickle.dump({'pid': job_info.proc.pid, 'ppid': self.pid}, fd)
                reply.stdout, reply.stderr = job_info.proc.communicate()
                reply.result = serialize(job_info.proc.returncode)

            if reply.status == DispyJob.Running:
                reply.status = DispyJob.Finished
            else:
                reply.status = DispyJob.Terminated
        except Exception:
            reply.result = serialize(None)
            reply.status = DispyJob.Terminated
            reply.exception = traceback.format_exc()
            os.chdir(self.dest_path_prefix)
        reply.end_time = time.time()
        self.reply_Q.put(reply)

    def __reply_Q(self):
        while 1:
            item = self.reply_Q.get()
            if isinstance(item, dict) and item.get('req', None) == 'job_pid':
                self.thread_lock.acquire()
                job_info = self.job_infos.get(item.get('uid', None), None)
                self.thread_lock.release()
                if not job_info:
                    continue
                job_info.pid = item.get('pid', None)
                job_pkl = os.path.join(self.dest_path_prefix, 'job_%s.pkl' % item['uid'])
                with open(job_pkl, 'wb') as fd:
                    pickle.dump({'pid': job_info.pid, 'ppid': item['ppid']}, fd)
                if psutil:
                    try:
                        proc = psutil.Process(job_info.pid)
                        assert proc.ppid() == item['ppid']
                    except (psutil.NoSuchProcess, Exception):
                        pass
                    except psutil.ZombieProcess:
                        client = self.clients.get(job_info.compute_id, None)
                        if client:
                            client.parent_pipe.send({'req': 'wait_pid', 'pid': job_info.pid,
                                                     'job_reply': job_info.job_reply})
                    else:
                        job_info.proc = proc
                continue

            job_reply = item
            self.thread_lock.acquire()
            job_info = self.job_infos.pop(job_reply.uid, None)
            self.thread_lock.release()
            if not job_info:
                continue
            job_pkl = os.path.join(self.dest_path_prefix, 'job_%s.pkl' % (job_reply.uid))
            try:
                os.remove(job_pkl)
            except Exception:
                pass
            job_info.job_reply = job_reply
            self.jobs_done += 1
            self.cpu_time += (job_reply.end_time - job_reply.start_time)
            Task(self._send_job_reply, job_info, resending=False)
            proc, job_info.proc = job_info.proc, None
            if proc:
                if isinstance(proc, multiprocessing.Process):
                    proc.join(2)
                elif isinstance(proc, subprocess.Popen):
                    proc.wait()
            client = self.clients.get(job_info.compute_id, None)
            if not client:
                continue
            compute = client.compute
            for xf in job_info.xfer_files:
                path = os.path.join(compute.dest_path, xf.dest_path.replace(xf.sep, os.sep),
                                    xf.name.split(xf.sep)[-1])
                try:
                    client.file_uses[path] -= 1
                    if client.file_uses[path] == 0:
                        client.file_uses.pop(path)
                        os.remove(path)
                except Exception:
                    dispynode_logger.warning('Invalid file "%s" ignored', path)
                    continue

    def _send_job_reply(self, job_info, resending=False, task=None):
        """Internal use only.
        """
        job_reply = job_info.job_reply
        dispynode_logger.debug('Sending result for job %s (%s)',
                               job_reply.uid, job_reply.status)
        client = self.clients.get(job_info.compute_id, None)
        if not client:
            pkl_path = os.path.join(self.dest_path_prefix,
                                    '%s_%s.pkl' % (job_info.compute_id, job_info.compute_auth))
            try:
                with open(pkl_path, 'rb') as fd:
                    client = pickle.load(fd)
            except Exception:
                client = None
            if not client:
                dispynode_logger.debug('Ignoring reply for job %s for computation "%s"',
                                       job_reply.uid, job_info.compute_id)
                raise StopIteration

        reply_addr = client.globals['__dispy_job_reply_addr']
        if not resending:
            self.avail_cpus += 1
            # assert self.avail_cpus <= self.num_cpus
            client.pending_jobs -= 1
            if client.pending_jobs == 0:
                client.jobs_done.set()

        sock = socket.socket(client.sock_family, socket.SOCK_STREAM)
        sock = AsyncSocket(sock, keyfile=self.keyfile, certfile=self.certfile)
        sock.settimeout(MsgTimeout)
        try:
            yield sock.connect(reply_addr)
            yield sock.send_msg(b'JOB_REPLY:' + serialize(job_reply))
            ack = yield sock.recv_msg()
            assert ack == b'ACK'
        except Exception:
            status = -1
            if not resending and job_reply.status != DispyJob.Terminated:
                # store job result so it can be sent when client is
                # reachable or recovered by user
                f = os.path.join(job_info.compute_dest_path,
                                 '_dispy_job_reply_%s.pkl' % job_reply.uid)
                dispynode_logger.error('Could not send reply for job %s to %s; saving it in "%s"',
                                       job_reply.uid, str(reply_addr), f)
                try:
                    with open(f, 'wb') as fd:
                        pickle.dump(job_reply, fd)
                except Exception:
                    dispynode_logger.debug('Could not save reply for job %s', job_reply.uid)
                else:
                    client.file_uses[f] = 2
                    client.pending_results += 1
        else:
            status = 0

            client.last_pulse = time.time()
            if resending:
                client.pending_results -= 1
            elif client.pending_results:
                Task(self.resend_job_results, client)

            if resending:
                pkl_path = os.path.join(job_info.compute_dest_path,
                                        '_dispy_job_reply_%s.pkl' % job_reply.uid)
                if os.path.isfile(pkl_path):
                    try:
                        os.remove(pkl_path)
                    except Exception:
                        # print(traceback.format_exc())
                        dispynode_logger.warning('Could not remove file "%s"', pkl_path)
                client.pending_results -= 1
                if client.pending_results == 0:
                    pkl_path = os.path.join(self.dest_path_prefix,
                                            '%s_%s.pkl' % (job_info.compute_id,
                                                           job_info.compute_auth))
                    if os.path.exists(pkl_path):
                        try:
                            os.remove(pkl_path)
                        except Exception:
                            # print(traceback.format_exc())
                            dispynode_logger.warning('Could not remove file "%s"', pkl_path)
                elif client.pending_results < 0:
                    dispynode_logger.warning('Invalid pending results for job %s of "%s": %s',
                                             job_reply.uid, job_info.compute_id,
                                             client.pending_results)
        finally:
            sock.close()

        if client.pending_jobs == 0 and client.zombie:
            Task(self.cleanup_computation, client, close=True)
        raise StopIteration(status)

    def cleanup_computation(self, client, close=True, task=None):
        if not client.zombie or client.pending_jobs:
            raise StopIteration(-1)

        compute = client.compute
        if ((close and self.clients.pop(compute.id, None) != client) or
            (not close and self.clients.get(compute.id, None) != client)):
            dispynode_logger.debug('Computation %s already closed?', compute.id)
            raise StopIteration(0)

        parent_pipe, client.parent_pipe = client.parent_pipe, None
        if parent_pipe:
            try:
                parent_pipe.send({'req': 'quit'})
            except Exception:
                dispynode_logger.debug(traceback.format_exc())

        if isinstance(client.setup_proc, multiprocessing.Process):
            for i in range(20):
                if parent_pipe and parent_pipe.poll():
                    try:
                        msg = parent_pipe.recv()
                        if msg == 'quit':
                            break
                        else:
                            dispynode_logger.warning('Invalid pipe msg "%s" ignored', msg)
                    except Exception:
                        pass
                yield task.sleep(0.25)
            else:
                client.setup_proc.terminate()

            for i in range(10):
                if not client.setup_proc.is_alive():
                    break
                client.setup_proc.join(0.1)
            else:
                try:
                    os.kill(client.setup_proc.pid, self.signals[2])
                except OSError:
                    pass
                except Exception:
                    dispynode_logger.debug('Terminating setup process for %s failed: %s',
                                           client.compute.id, traceback.format_exc())

            if parent_pipe:
                try:
                    parent_pipe.close()
                except Exception:
                    pass
                try:
                    client.child_pipe.close()
                except Exception:
                    pass
                client.child_pipe = None

        if isinstance(compute.cleanup, str) and isinstance(client.cleanup_args, tuple):
            os.chdir(compute.dest_path)
            try:
                globalvars = dict(self.__init_globals)
                globalvars.update(client.globals)
                localvars = {'_dispy_cleanup_args': client.cleanup_args}
                if self.client_shutdown:
                    globalvars['dispynode_shutdown'] = lambda: setattr(self, 'serve', 0)
                if compute.code:
                    exec(marshal.loads(compute.code), globalvars)
                exec('%s(*_dispy_cleanup_args)' % compute.cleanup, globalvars, localvars)
            except Exception:
                dispynode_logger.debug('Cleanup "%s" failed', compute.cleanup)
                dispynode_logger.debug(traceback.format_exc())

            os.chdir(self.dest_path_prefix)
            for module in list(sys.modules.keys()):
                if module not in self.__init_modules:
                    sys.modules.pop(module, None)
            sys.modules.update(self.__init_modules)
        client.globals.clear()
        client.cleanup_args = None

        if not close:
            client.zombie = False
            for addrinfo in self.addrinfos:
                if compute.node_ip_addr in (addrinfo.ip, addrinfo.ext_ip):
                    break
            else:
                dispynode_logger.warning('%s is not valid address!', compute.node_ip_addr)
                raise StopIteration(-1)
            try:
                sock = AsyncSocket(socket.socket(addrinfo.family, socket.SOCK_STREAM),
                                   keyfile=self.keyfile, certfile=self.certfile)
                sock.settimeout(MsgTimeout)
                info = {'node_addr': addrinfo.ext_ip, 'node_auth': self.auth,
                        'compute_id': compute.id, 'auth': compute.auth}
                try:
                    yield sock.connect((compute.scheduler_ip_addr, compute.scheduler_port))
                    yield sock.send_msg('NODE_CLOSED:'.encode() + serialize(info))
                except Exception:
                    pass
                finally:
                    sock.close()
            except Exception:
                dispynode_logger.debug(traceback.format_exc())
                pass
            raise StopIteration(0)

        self.clients_done += 1
        self.scheduler['auth'].discard(compute.auth)
        dispynode_logger.debug('Computation "%s" from %s done',
                               compute.auth, compute.scheduler_ip_addr)
        if self.serve > 0:
            self.serve -= 1
        if self.serve == 0:
            Task(self.send_terminate)
            if self.avail_cpus:
                self.shutdown('terminate')
        else:
            if ((not self.clients) and (not self.scheduler['auth']) and
                compute.scheduler_ip_addr == self.scheduler['ip_addr'] and
                compute.scheduler_port == self.scheduler['port']):
                os.environ.update(self.__init_environ)
                for var in list(os.environ.keys()):
                    if var not in self.__init_environ:
                        os.environ.pop(var, None)
                self.pulse_interval = self.ping_interval
                self.timer_task.resume(None)
                Task(self.broadcast_ping_msg)

        if compute.cleanup:
            for path, use_count in client.file_uses.items():
                if use_count == 1:
                    try:
                        os.remove(path)
                    except Exception:
                        dispynode_logger.warning('Could not remove "%s"', path)

            if (os.path.isdir(compute.dest_path) and
                compute.dest_path.startswith(self.dest_path_prefix)):
                remove = True
                for dirpath, dirnames, filenames in os.walk(compute.dest_path, topdown=False):
                    for filename in filenames:
                        path = os.path.join(dirpath, filename)
                        use_count = client.file_uses.get(path, 1)
                        if use_count == 1:
                            try:
                                os.remove(path)
                            except Exception:
                                dispynode_logger.warning('Could not remove "%s"', path)
                        else:
                            remove = False
                    if remove:
                        if os.name == 'nt':
                            # a job process may not finished yet, so wait a bit if necessary
                            for i in range(10):
                                try:
                                    shutil.rmtree(dirpath)
                                except Exception:
                                    yield task.sleep(0.1)
                                    continue
                                else:
                                    break
                            else:
                                dispynode_logger.warning('Could not remove "%s"', dirpath)
                        else:
                            try:
                                shutil.rmtree(dirpath)
                            except Exception:
                                dispynode_logger.warning('Could not remove "%s"', dirpath)

        if client.pending_results == 0:
            pkl_path = os.path.join(self.dest_path_prefix, '%s_%s.pkl' % (compute.id, compute.auth))
            try:
                os.remove(pkl_path)
            except Exception:
                # print(traceback.format_exc())
                dispynode_logger.warning('Could not remove file "%s"', pkl_path)
        raise StopIteration(0)

    def send_terminate(self, task=None):
        if self.scheduler['ip_addr'] and self.scheduler['addrinfo']:
            addrinfo = self.scheduler['addrinfo']
            sock = AsyncSocket(socket.socket(addrinfo.family, socket.SOCK_STREAM),
                               keyfile=self.keyfile, certfile=self.certfile)
            sock.settimeout(MsgTimeout)
            dispynode_logger.debug('Sending TERMINATE to %s', self.scheduler['ip_addr'])
            info = {'ip_addr': addrinfo.ext_ip, 'port': self.port, 'sign': self.sign}
            try:
                yield sock.connect((self.scheduler['ip_addr'], self.scheduler['port']))
                yield sock.send_msg('TERMINATED:'.encode() + serialize(info))
            except Exception:
                pass
            sock.close()

        udp_addrinfos = {}
        for addrinfo in self.addrinfos:
            udp_addrinfos[addrinfo.bind_addr] = addrinfo

        for bind_addr, addrinfo in udp_addrinfos.items():
            sock = AsyncSocket(socket.socket(addrinfo.family, socket.SOCK_DGRAM))
            sock.settimeout(1)
            ttl_bin = struct.pack('@i', 1)
            if addrinfo.family == socket.AF_INET:
                if self.ipv4_udp_multicast:
                    sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, ttl_bin)
                else:
                    sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
            else:  # addrinfo.family == socket.AF_INET6
                sock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_MULTICAST_HOPS, ttl_bin)
                sock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_MULTICAST_IF, addrinfo.ifn)
            info = {'ip_addr': addrinfo.ext_ip, 'port': self.port, 'sign': self.sign}
            try:
                yield sock.sendto('TERMINATED:'.encode() + serialize(info),
                                  (addrinfo.broadcast, self.scheduler['port']))
            except Exception:
                pass
            sock.close()

    def shutdown(self, how):
        def _shutdown(self, how, task=None):
            self.thread_lock.acquire()
            if how == 'exit':
                if self.scheduler['auth']:
                    self.serve = 0
                    print('dispynode will quit when current computation from %s closes.',
                          self.scheduler['ip_addr'])
                    self.thread_lock.release()
                    raise StopIteration
            job_infos, self.job_infos = self.job_infos, {}
            self.avail_cpus += len(job_infos)
            if self.avail_cpus != self.num_cpus:
                dispynode_logger.warning('Invalid cpus: %s / %s', self.avail_cpus, self.num_cpus)
            self.avail_cpus = 0
            self.serve = 0
            self.thread_lock.release()
            for job_info in job_infos.values():
                proc, job_info.proc = job_info.proc, None
                if proc:
                    dispynode_logger.debug('Killing process %s for job %s',
                                           proc.pid, job_info.job_reply.uid)
                    try:
                        proc.terminate()
                    except Exception:
                        continue
                    if isinstance(proc, multiprocessing.Process):
                        proc.join(2)
                    elif isinstance(proc, subprocess.Popen):
                        proc.wait()
            for cid, client in list(self.clients.items()):
                client.pending_jobs = 0
                client.jobs_done.set()
                client.zombie = True
                Task(self.cleanup_computation, client, close=True)

            yield self.send_terminate(task=task)
            self.sign = ''
            # delay a bit for client to close node, in case shutdown
            # is called by 'dispynode_shutdown'
            yield task.sleep(0.1)

            cfg_file = os.path.join(self.dest_path_prefix, 'config.pkl')
            try:
                os.remove(cfg_file)
            except Exception:
                # print(traceback.format_exc())
                pass
            if os.name == 'nt':
                os.kill(self.pid, signal.CTRL_C_EVENT)
                print('\n\n   Under Windows extra Enter (input) may be required to quit!\n')
            else:
                os.kill(self.pid, signal.SIGINT)

        if self.sign:
            Task(_shutdown, self, how)

    def cmd_proc(self, task=None):

        def release(task=None):
            if self.scheduler['auth']:
                addrinfo = self.scheduler['addrinfo']
                info = {'ip_addr': addrinfo.ext_ip, 'port': self.port,
                        'cpus': self.num_cpus - self.avail_cpus,
                        'scheduler_ip_addr': self.scheduler['ip_addr']}
                if psutil:
                    info['avail_info'] = DispyNodeAvailInfo(
                        100.0 - psutil.cpu_percent(), psutil.virtual_memory().available,
                        psutil.disk_usage(self.dest_path_prefix).free,
                        100.0 - psutil.swap_memory().percent)
                else:
                    info['avail_info'] = None

                sock = AsyncSocket(socket.socket(addrinfo.family, socket.SOCK_STREAM),
                                   keyfile=self.keyfile, certfile=self.certfile)
                sock.settimeout(MsgTimeout)
                try:
                    yield sock.connect((self.scheduler['ip_addr'], self.scheduler['port']))
                    yield sock.send_msg(b'PULSE:' + serialize(info))
                    assert (yield sock.recv_msg()) == b'PULSE'
                except Exception:
                    for client in list(self.clients.values()):
                        if client.pending_jobs == 0:
                            client.zombie = True
                            Task(self.cleanup_computation, client, close=True)
                else:
                    print('   Scheduler (client) at %s is active, so computations are not closed' %
                          self.scheduler['ip_addr'])
                finally:
                    sock.close()

        task.set_daemon()
        thread_pool = pycos.AsyncThreadPool(1)
        if self.service_start:
            service_from = ' from %s' % time.strftime('%H:%M', time.localtime(self.service_start))
            if self.service_end:
                service_to = ' to %s' % time.strftime('%H:%M', time.localtime(self.service_end))
            else:
                service_to = ' to %s' % time.strftime('%H:%M', time.localtime(self.service_stop))
        else:
            service_from = service_to = ''
        while 1:
            cmd = yield task.receive()
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
                    except Exception:
                        print('  Invalid cpus ignored')
                        continue
                    if self.set_cpus(cpus):
                        print('  Setting cpus failed')

            elif cmd == 'release':
                Task(release)

            else:
                print('\n  Serving %d CPUs%s%s%s' %
                      (self.avail_cpus + len(self.job_infos), service_from, service_to,
                       ' for %d clients' % self.serve if self.serve > 0 else ''))
                print('  Completed:\n    %d Clients, %d jobs, %.3f sec CPU time' %
                      (self.clients_done, self.jobs_done, self.cpu_time))
                print('  Running:')
                for i, client in enumerate(self.clients.values(), start=1):
                    compute = client.compute
                    print('    Client %s: %s @ %s running %s jobs' %
                          (i, compute.name, compute.scheduler_ip_addr, client.pending_jobs))
                print('')
        self.shutdown(cmd)


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--config', dest='config', default='',
                        help='use configuration in given file')
    parser.add_argument('--save_config', dest='save_config', default='',
                        help='save configuration in given file and exit')
    parser.add_argument('-c', '--cpus', dest='cpus', type=int, default=0,
                        help='number of cpus used by dispy; if negative, '
                        'that many cpus are not used')
    parser.add_argument('-d', '--debug', action='store_true', dest='loglevel', default=False,
                        help='if given, debug messages are printed')
    parser.add_argument('-i', '--host', dest='hosts', action='append', default=[],
                        help='host name or IP address to use '
                        '(may be needed in case of multiple interfaces)')
    parser.add_argument('--ext_host', dest='ext_hosts', action='append', default=[],
                        help='external host name or IP address to use '
                        '(needed in case of NAT firewall/gateway)')
    parser.add_argument('-p', '--dispy_port', dest='dispy_port', type=int,
                        default=dispy.config.DispyPort, help='dispy port number')
    parser.add_argument('--ipv4_udp_multicast', dest='ipv4_udp_multicast', action='store_true',
                        default=False, help='use multicast for IPv4 UDP instead of broadcast')
    parser.add_argument('--name', dest='name', default='',
                        help='name asscoiated to this node; default is obtained with gethostname()')
    parser.add_argument('--dest_path_prefix', dest='dest_path_prefix', default='',
                        help='path prefix where files sent by dispy are stored')
    parser.add_argument('--scheduler_host', dest='scheduler_host', default='',
                        help='host name or IP address of scheduler to announce when starting')
    parser.add_argument('--max_file_size', dest='max_file_size', default=str(MaxFileSize),
                        help='maximum file size of any file transferred (use 0 for unlimited size)')
    parser.add_argument('--zombie_interval', dest='zombie_interval', type=float, default=60.0,
                        help='interval in minutes to presume unresponsive scheduler is zombie')
    parser.add_argument('--ping_interval', dest='ping_interval', type=int, default=0,
                        help='number of seconds between ping messages to discover scheduler')
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
    parser.add_argument('--client_shutdown', dest='client_shutdown', action='store_true',
                        default=False, help='if given, client can shutdown node')
    parser.add_argument('--force_cleanup', dest='force_cleanup', action='store_true',
                        default=False, help='if given, cleanup is always enabled')
    parser.add_argument('--msg_timeout', dest='msg_timeout', type=float, default=MsgTimeout,
                        help='timeout used for messages to/from client in seconds')
    parser.add_argument('-s', '--secret', dest='secret', default='',
                        help='authentication secret for handshake with dispy clients')
    parser.add_argument('--admin_secret', dest='admin_secret', default='',
                        help='authentication secret for dispy admin')
    parser.add_argument('--certfile', dest='certfile', default='',
                        help='file containing SSL certificate')
    parser.add_argument('--keyfile', dest='keyfile', default='',
                        help='file containing SSL key')
    parser.add_argument('--clean', action='store_true', dest='clean', default=False,
                        help='if given, files copied from or generated by clients will be removed')
    parser.add_argument('--unsafe_setup', action='store_false', dest='safe_setup', default=True,
                        help='run "setup" in "main" process instead of child process')
    parser.add_argument('--daemon', action='store_true', dest='daemon', default=False,
                        help='if given, input is not read from terminal '
                        '(to set CPUs or get status)')
    _dispy_config = vars(parser.parse_args(sys.argv[1:]))

    if _dispy_config['config']:
        import configparser
        cfg = configparser.ConfigParser()
        cfg.read(_dispy_config['config'])
        cfg = dict(cfg.items('DEFAULT'))
        cfg['cpus'] = int(cfg['cpus'])
        cfg['dispy_port'] = int(cfg['dispy_port'])
        cfg['ipv4_udp_multicast'] = cfg['ipv4_udp_multicast'] == 'True'
        cfg['zombie_interval'] = float(cfg['zombie_interval'])
        cfg['ping_interval'] = int(cfg['ping_interval']) if cfg['ping_interval'] else None
        cfg['msg_timeout'] = float(cfg['msg_timeout'])
        cfg['serve'] = int(cfg['serve'])
        cfg['loglevel'] = cfg['loglevel'] == 'True'
        cfg['clean'] = cfg['clean'] == 'True'
        cfg['safe_setup'] = cfg['safe_setup'] == 'True'
        cfg['force_cleanup'] = cfg['force_cleanup'] == 'True'
        cfg['daemon'] = cfg['daemon'] == 'True'
        cfg['hosts'] = cfg['hosts'].strip()
        if cfg['hosts']:
            cfg['hosts'] = eval(cfg['hosts'])
        cfg['ext_hosts'] = cfg['ext_hosts'].strip()
        if cfg['ext_hosts']:
            cfg['ext_hosts'] = eval(cfg['ext_hosts'])
        for key, value in _dispy_config.items():
            if _dispy_config[key] != parser.get_default(key) or key not in cfg:
                cfg[key] = _dispy_config[key]
        _dispy_config = cfg
        del key, value
    _dispy_config.pop('config', None)

    cfg = _dispy_config.pop('save_config', None)
    if cfg:
        import configparser
        for key, val in list(_dispy_config.items()):
            if not isinstance(val, str):
                _dispy_config[key] = str(val)
        _dispy_config = configparser.ConfigParser(_dispy_config)
        cfg = open(cfg, 'w')
        _dispy_config.write(cfg)
        cfg.close()
        exit(0)
    del parser, cfg

    if _dispy_config['loglevel']:
        dispynode_logger.setLevel(dispynode_logger.DEBUG)
        pycos.logger.setLevel(pycos.logger.DEBUG)
    else:
        dispynode_logger.setLevel(dispynode_logger.INFO)
    del _dispy_config['loglevel']

    dispy.config.DispyPort = _dispy_config.pop('dispy_port')
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

    if _dispy_config['ping_interval']:
        if _dispy_config['ping_interval'] < 10:
            raise Exception('ping_interval must be at least 10')
    else:
        _dispy_config['ping_interval'] = None

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

    if _dispy_config['certfile']:
        _dispy_config['certfile'] = os.path.abspath(_dispy_config['certfile'])
    else:
        _dispy_config['certfile'] = None
    if _dispy_config['keyfile']:
        _dispy_config['keyfile'] = os.path.abspath(_dispy_config['keyfile'])
    else:
        _dispy_config['keyfile'] = None

    # begining of day
    bod = time.localtime()
    bod = (int(time.time()) - (bod.tm_hour * 3600) - (bod.tm_min * 60))
    if _dispy_config['service_start']:
        _dispy_config['service_start'] = time.strptime(_dispy_config.pop('service_start'), '%H:%M')
        _dispy_config['service_start'] = (bod + (_dispy_config['service_start'].tm_hour * 3600) +
                                          (_dispy_config['service_start'].tm_min * 60))
    if _dispy_config['service_stop']:
        _dispy_config['service_stop'] = time.strptime(_dispy_config.pop('service_stop'), '%H:%M')
        _dispy_config['service_stop'] = (bod + (_dispy_config['service_stop'].tm_hour * 3600) +
                                         (_dispy_config['service_stop'].tm_min * 60))
    if _dispy_config['service_end']:
        _dispy_config['service_end'] = time.strptime(_dispy_config.pop('service_end'), '%H:%M')
        _dispy_config['service_end'] = (bod + (_dispy_config['service_end'].tm_hour * 3600) +
                                        (_dispy_config['service_end'].tm_min * 60))
    del bod
    if (_dispy_config['service_start'] or _dispy_config['service_stop'] or
        _dispy_config['service_end']):
        if not _dispy_config['service_start']:
            _dispy_config['service_start'] = int(time.time())
        if not _dispy_config['service_stop'] and not _dispy_config['service_end']:
            raise Exception('"service_stop" or "service_end" must also be given')
        if _dispy_config['service_stop']:
            if _dispy_config['service_start'] >= _dispy_config['service_stop']:
                raise Exception('"service_start" must be before "service_stop"')
        if _dispy_config['service_end']:
            if _dispy_config['service_start'] >= _dispy_config['service_end']:
                raise Exception('"service_start" must be before "service_end"')
            if (_dispy_config['service_stop'] and
                _dispy_config['service_stop'] >= _dispy_config['service_end']):
                raise Exception('"service_stop" must be before "service_end"')

    try:
        if os.getpgrp() != os.tcgetpgrp(sys.stdin.fileno()):
            _dispy_config['daemon'] = True
    except Exception:
        pass

    if os.name == 'nt' and not _dispy_config['daemon']:
        # Python 3 under Windows blocks multiprocessing.Process on reading
        # input; pressing "Enter" twice works (for one subprocess). Until
        # this is understood / fixed, disable reading input.

        # As of Sep 28, 2018, it seems multiprocessing works with reading input,
        # but don't what fixed it, so for now, leave it as is.

        # Update on Sep 30, 2018: While multiprocessing now works with
        # reading input, entering text seems to raise SIGINT or
        # SIGBREAK causing dispynode to quit, so leave it as daemon.

        # As of Aug 1, 2020, all seem okay with Python 3.8 and pywin32 228.
        print('\n  In the past, reading input in non-daemon mode seemed to\n'
              '  interfere with multiprocessing. Latest Python / pywin32\n'
              '  seem to be working fine. In case of issues, use "--daemon" option.\n')

    if psutil:
        psutil.cpu_percent(0.1)
    else:
        print('\n    WARNING: "psutil" module is not available;\n'
              '    wihout it, using "clean" option may be dangerous and\n'
              '    node status (CPU, memory, disk and swap space usage)\n'
              '    will not be sent to clients!\n')

    def sighandler(signum, frame):
        dispynode_logger.debug('dispynode received signal %s', signum)
        if not _dispy_node:
            return
        if os.path.isfile(os.path.join(_dispy_node.dest_path_prefix, 'config.pkl')):
            if signum == signal.SIGINT:
                _dispy_node.shutdown('exit')
            else:
                _dispy_node.shutdown('terminate')
        else:
            raise KeyboardInterrupt

    signal.signal(signal.SIGINT, sighandler)
    if os.name == 'nt':
        signal.signal(signal.SIGBREAK, sighandler)
    if not _dispy_config['daemon']:
        if hasattr(signal, 'SIGQUIT'):
            signal.signal(signal.SIGQUIT, sighandler)
        if hasattr(signal, 'SIGHUP'):
            signal.signal(signal.SIGHUP, sighandler)

    # if hasattr(signal, 'SIGCHLD'):
    #     signal.signal(signal.SIGCHLD, signal.SIG_IGN)
    del sighandler

    # TODO: reset these signals in processes that execute computations?

    dispynode_logger.info('version: %s (Python %s), PID: %s',
                          _dispy_version, platform.python_version(), os.getpid())
    _dispy_node = _DispyNode(**_dispy_config)

    if _dispy_config['daemon']:
        del _dispy_config
        while 1:
            try:
                time.sleep(3600)
            except (Exception, KeyboardInterrupt):
                if os.path.isfile(os.path.join(_dispy_node.dest_path_prefix, 'config.pkl')):
                    _dispy_node.shutdown('exit')
                else:
                    break
    else:
        del _dispy_config
        while 1:
            try:
                # wait a bit for any output for command is done
                time.sleep(0.2)
                _dispy_cmd = input(
                    '\nEnter "quit" or "exit" to terminate dispynode,\n'
                    '  "stop" to stop service, "start" to restart service,\n'
                    '  "release" to check and close computation,\n'
                    '  "cpus" to change CPUs used, anything else to get status: ')
                _dispy_cmd = _dispy_cmd.strip().lower()
                _dispy_node.cmd_task.send(_dispy_cmd)
            except (Exception, KeyboardInterrupt):
                if os.path.isfile(os.path.join(_dispy_node.dest_path_prefix, 'config.pkl')):
                    _dispy_node.shutdown('exit')
                else:
                    break

    _dispy_node.pycos.finish()
