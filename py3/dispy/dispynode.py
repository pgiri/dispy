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
from dispy import _JobReply, DispyJob, DispyNodeAvailInfo, _Function, _Compute, _XferFile, \
     _dispy_version, auth_code, num_min, _same_file, MsgTimeout
from pycos import Task, Pycos, AsyncSocket, serialize, deserialize

__author__ = "Giridhar Pemmasani (pgiri@yahoo.com)"
__email__ = "pgiri@yahoo.com"
__copyright__ = "Copyright 2011, Giridhar Pemmasani"
__contributors__ = []
__maintainer__ = "Giridhar Pemmasani (pgiri@yahoo.com)"
__license__ = "Apache 2.0"
__url__ = "http://dispy.sourceforge.net"
__status__ = "Production"
__version__ = _dispy_version
__all__ = []

MaxFileSize = 0
# Messages logged with 'dispynode_logger' in computations are shown at
# node (whereas 'print' statements are sent back to client with
# job.stdout)
dispynode_logger = pycos.Logger('dispynode')


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

    __dispy_job_reply.status = DispyJob.ProvisionalResult
    __dispy_job_reply.result = serialize(result)
    __dispy_job_reply.end_time = time.time()
    sock = socket.socket(__dispy_sock_family, socket.SOCK_STREAM)
    sock = AsyncSocket(sock, blocking=True, keyfile=__dispy_keyfile, certfile=__dispy_certfile)
    sock.settimeout(timeout)
    try:
        sock.connect(__dispy_job_reply_addr)
        sock.send_msg(b'JOB_REPLY:' + serialize(__dispy_job_reply))
        ack = sock.recv_msg()
        assert ack == b'ACK'
    except Exception:
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

    if not os.path.isfile(path):
        return -1
    path = os.path.abspath(path)
    cwd = os.getcwd()
    if path.startswith(cwd):
        dst = os.path.dirname(path[len(cwd):].lstrip(os.sep))
    else:
        dst = '.'
    xf = _XferFile(path, dst)
    if MaxFileSize and xf.stat_buf.st_size > MaxFileSize:
        return -1
    sock = socket.socket(__dispy_sock_family, socket.SOCK_STREAM)
    sock = AsyncSocket(sock, blocking=True, keyfile=__dispy_keyfile, certfile=__dispy_certfile)
    sock.settimeout(timeout)
    try:
        sock.connect(__dispy_job_reply_addr)
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
        __dispy_job_reply.result = serialize(None)

    __dispy_job_reply.result = serialize(__dispy_job_reply.result)
    __dispy_job_reply.stdout = sys.stdout.getvalue()
    __dispy_job_reply.stderr = sys.stderr.getvalue()
    __dispy_job_reply.end_time = time.time()
    reply_Q.put(__dispy_job_reply)


def _dispy_setup_process(compute, pipe, client_globals):
    """
    Internal use only.
    """
    import threading

    os.chdir(compute.dest_path)
    suid = client_globals.pop('suid', None)
    if suid is not None:
        sgid = client_globals.pop('sgid', None)
        if hasattr(os, 'setresuid'):
            os.setresgid(sgid, sgid, sgid)
            os.setresuid(suid, suid, suid)
        else:
            os.setregid(sgid, sgid)
            os.setreuid(suid, suid)
    globals().update(client_globals)
    globals().pop('reply_Q', None)
    dispynode_logger.setLevel(client_globals.pop('loglevel'))

    if compute.code:
        try:
            compute.code = compile(compute.code, '<string>', 'exec')
            exec(compute.code, globals())
            if os.name == 'nt':
                compute.code = marshal.dumps(compute.code)
            else:
                compute.code = None
        except Exception:
            dispynode_logger.debug(traceback.format_exc())
            pipe.send({'setup_status': -1})
            return

    init_vars = set(globals().keys())
    if compute.setup:
        localvars = {'_dispy_setup_status': 0, '_dispy_setup_args': compute.setup.args,
                     '_dispy_setup_kwargs': compute.setup.kwargs}
        try:
            exec('_dispy_setup_status = %s(*_dispy_setup_args, **_dispy_setup_kwargs)' %
                 compute.setup.name, globals(), localvars)
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

    def sighandler(signum, frame):
        dispynode_logger.debug('setup_process received signal %s', signum)

    signal.signal(signal.SIGINT, sighandler)
    if os.name == 'nt':
        signal.signal(signal.SIGBREAK, sighandler)

    wait_nohang = getattr(os, 'WNOHANG', None)
    if os.name == 'nt':
        signals = [signal.CTRL_BREAK_EVENT, signal.CTRL_BREAK_EVENT, signal.SIGTERM]
    else:
        signals = [signal.SIGTERM, 0, signal.SIGKILL]

    def terminate_job(msg):
        pid = msg['pid']
        job_reply = msg['job_reply']
        # TODO: Currently job processes are not maintained. Perhaps it
        # is better / safer approach, but requires main process to
        # inform this process when a job is done so that process can
        # be removed. This requires extra bandwith. Since cancelling
        # jobs is relatively rare, it may be better to use psutil or
        # os.kill to terminate processes.
        try:
            if psutil:
                proc = psutil.Process(pid)
                assert proc.is_running()
                proc.terminate()
            else:
                proc = None
                os.kill(pid, signals[0])
        except (OSError, Exception):
            # Likely job is done?
            return
        signum = signals[1]
        for i in range(20):
            if proc:
                if not proc.is_running():
                    break
                proc.wait(0.1)
                continue
            elif wait_nohang:
                try:
                    if os.waitpid(pid, wait_nohang)[0] == pid:
                        break
                except OSError:
                    # TODO: check err.errno for all platforms
                    break
                time.sleep(0.1)
                continue
            elif signum and i < 10:
                time.sleep(0.1)
                continue

            try:
                os.kill(pid, signum)
            except (OSError, Exception):
                # TODO: check err.errno for all platforms
                break
        else:
            try:
                if proc:
                    proc.kill()
                    proc.wait(1)
                else:
                    os.kill(pid, signals[2])
            except (OSError, Exception):
                pass
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

    if isinstance(compute.cleanup, _Function):
        localvars = {'_dispy_cleanup_args': compute.cleanup.args,
                     '_dispy_cleanup_kwargs': compute.cleanup.kwargs}
        try:
            exec('%s(*_dispy_cleanup_args, **_dispy_cleanup_kwargs)' %
                 compute.cleanup.name, globals(), localvars)
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
        self.last_pulse = time.time()
        self.zombie = False
        self.setup_proc = None
        self.parent_pipe = self.child_pipe = None
        self.use_setup_proc = False
        self.sock_family = None


class _DispyNode(object):
    """
    Internal use only.
    """

    def __init__(self, cpus, ip_addrs=[], ext_ip_addrs=[], node_port=None,
                 name='', scheduler_node=None, scheduler_port=None, ipv4_udp_multicast=False,
                 dest_path_prefix='', clean=False, secret='', keyfile=None, certfile=None,
                 zombie_interval=60, ping_interval=None, force_cleanup=False, serve=-1,
                 service_start=None, service_stop=None, service_end=None, safe_setup=True,
                 daemon=False, client_shutdown=False):
        assert 0 < cpus <= multiprocessing.cpu_count()
        self.num_cpus = cpus
        if name:
            self.name = name
        else:
            self.name = socket.gethostname()
        if not ip_addrs:
            ip_addrs = [None]
        self.ipv4_udp_multicast = bool(ipv4_udp_multicast)
        addrinfos = {}
        for i in range(len(ip_addrs)):
            ip_addr = ip_addrs[i]
            if i < len(ext_ip_addrs):
                ext_ip_addr = ext_ip_addrs[i]
            else:
                ext_ip_addr = None
            addrinfo = dispy.host_addrinfo(host=ip_addr, ipv4_multicast=self.ipv4_udp_multicast)
            if not addrinfo or not addrinfo.ip:
                dispynode_logger.warning('Ignoring invalid ip_addr %s', ip_addr)
                continue
            if addrinfo.ip.startswith('127.') or addrinfo.ip.startswith('fe80'):
                dispynode_logger.warning('node IP address %s seems to be loopback address; '
                                         'this will prevent communication with clients on '
                                         'other machines. ', addrinfo.ip)
            if ext_ip_addr:
                ext_ip_addr = dispy._node_ipaddr(ext_ip_addr)
                if not ext_ip_addr:
                    dispynode_logger.warning('Ignoring invalid ext_ip_addr %s', ext_ip_addrs[i])
            if not ext_ip_addr:
                ext_ip_addr = addrinfo.ip
            addrinfo.ext_ip_addr = ext_ip_addr
            addrinfos[ext_ip_addr] = addrinfo
        if not addrinfos:
            raise Exception('No valid ip_addr')
        self.addrinfos = addrinfos

        if node_port is None:
            node_port = 51348
        self.port = node_port
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

        if os.name == 'nt':
            self.signals = [signal.CTRL_BREAK_EVENT, signal.CTRL_BREAK_EVENT, signal.SIGTERM]
        else:
            self.signals = [signal.SIGTERM, 0, signal.SIGKILL]

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

        if clean:
            if psutil:
                try:
                    proc = psutil.Process(config['pid'])
                    assert any(arg.endswith('dispynode.py') for arg in proc.cmdline())
                    if config:
                        assert proc.ppid() in (config['ppid'], 1)
                        if config['create_time']:
                            assert abs(proc.create_time() - config['create_time']) < 1
                except Exception:
                    print('\n    Apparently previous dispynode (PID %s) has gone away;'
                          '\n    please check manually and kill process(es) if necessary\n' %
                          config.get('pid', None))
                    config = {}
            else:
                print('\n    WARNING: Using "clean" without "psutil" module may be dangerous!\n')

            wait_nohang = getattr(os, 'WNOHANG', None)
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
                            proc.terminate()
                        else:
                            assert any(arg.endswith('dispynode.py') for arg in proc.cmdline())
                            proc.terminate()
                    except Exception:
                        continue
                else:
                    proc = None
                    os.kill(pid, self.signals[0])

                signum = self.signals[1]
                for i in range(10):
                    time.sleep(0.2)
                    if proc:
                        proc.wait(0.1)
                        if not proc.is_running():
                            break
                        continue
                    elif wait_nohang:
                        try:
                            if os.waitpid(pid, wait_nohang)[0] == pid:
                                break
                        except (OSError, Exception):
                            # TODO: check err.errno for all platforms
                            break
                    elif signum:
                        continue

                    try:
                        os.kill(pid, signum)
                    except (OSError, Exception):
                        # TODO: check err.errno for all platforms
                        break
                else:
                    try:
                        if proc:
                            proc.kill()
                            proc.wait(1)
                        else:
                            os.kill(pid, self.signals[2])
                    except Exception:
                        print('  Could not kill job process with ID %s' % pid)
                        continue

            try:
                pid = config['pid']
                print('Killing dispynode process ID %s; it may take sometime to finish.' % pid)
                if psutil:
                    proc = psutil.Process(pid)
                    assert proc.ppid() == config['ppid']
                    assert any(arg.endswith('dispynode.py') for arg in proc.cmdline())
                else:
                    proc = None

                if os.name == 'nt':
                    signum = signal.CTRL_C_EVENT
                else:
                    signum = signal.SIGINT
                os.kill(pid, signum)

                signum = self.signals[1]
                for i in range(50):
                    time.sleep(0.2)
                    if proc:
                        proc.wait(0.1)
                        if not proc.is_running():
                            break
                        continue
                    elif wait_nohang:
                        try:
                            if os.waitpid(pid, wait_nohang)[0] == pid:
                                break
                        except OSError:
                            # TODO: check err.errno for all platforms
                            break
                    elif signum and i < 40:
                        continue

                    try:
                        os.kill(pid, signum)
                    except OSError:
                        # TODO: check err.errno for all platforms
                        break
                else:
                    try:
                        if proc:
                            proc.kill()
                            proc.wait(1)
                        else:
                            os.kill(pid, self.signals[2])
                    except Exception:
                        print('  Could not kill job process with ID %s' % pid)
                        print(traceback.format_exc())
                        exit(-1)

            except Exception:
                # print(traceback.format_exc())
                pass
            shutil.rmtree(self.dest_path_prefix, ignore_errors=True)

        if not os.path.isdir(self.dest_path_prefix):
            os.makedirs(self.dest_path_prefix)
        os.chmod(self.dest_path_prefix, stat.S_IRUSR | stat.S_IWUSR |
                 stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
        os.chmod(os.path.join(self.dest_path_prefix, '..'), stat.S_IRUSR | stat.S_IWUSR |
                 stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
        os.chdir(self.dest_path_prefix)

        self.pycos = Pycos()

        self.avail_cpus = self.num_cpus
        self.clients = {}
        self.job_infos = {}
        self.terminate = False
        self.sign = hashlib.sha1(os.urandom(20))
        for ext_ip_addr in self.addrinfos:
            self.sign.update(ext_ip_addr.encode())
        self.sign = self.sign.hexdigest()
        self.secret = secret
        self.auth = auth_code(self.secret, self.sign)
        self.zombie_interval = 60 * zombie_interval
        if ping_interval:
            self.ping_interval = ping_interval
        else:
            self.ping_interval = None
        self.pulse_interval = self.ping_interval
        if not scheduler_port:
            scheduler_port = 51347

        self.scheduler = {'ip_addr': dispy._node_ipaddr(scheduler_node) if scheduler_node else None,
                          'port': scheduler_port, 'auth': set(), 'addrinfo': None}
        self.cpu_time = 0
        self.num_jobs = 0
        self.num_computations = 0
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
                    'ip_addrs': [addrinfo.ip for addrinfo in self.addrinfos.values()],
                    'ext_ip_addrs': [addrinfo.ext_ip_addr for addrinfo in self.addrinfos.values()],
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
        if isinstance(service_start, int) and (isinstance(service_stop, int) or
                                               isinstance(service_end, int)):
            self.service_start = service_start
            if isinstance(service_stop, int):
                self.service_stop = service_stop
            if isinstance(service_end, int):
                self.service_end = service_end
            Task(self.service_schedule)
        self.client_shutdown = client_shutdown

        self.__init_modules = dict(sys.modules)
        self.__init_environ = dict(os.environ)
        self.__init_globals = dict(globals())
        self.__init_globals.pop('_dispy_config',  None)
        self.__init_globals.pop('_dispy_cmd', None)
        self.__init_globals['_dispy_node'] = self

        udp_addrinfos = {}
        for addrinfo in self.addrinfos.values():
            Task(self.tcp_server, addrinfo)
            udp_addrinfos[addrinfo.bind_addr] = addrinfo

        for bind_addr, addrinfo in udp_addrinfos.items():
            Task(self.udp_server, addrinfo)

        if not daemon:
            self.cmd_task = Task(self.cmd_proc)
        dispynode_logger.info('"%s" serving %s cpus', self.name, self.num_cpus)

    def broadcast_ping_msg(self, addrinfos=[], task=None):
        if (self.scheduler['auth'] or self.job_infos or not self.avail_cpus or
            not self.service_available()):
            raise StopIteration
        if not addrinfos:
            addrinfos = self.addrinfos.values()
        for addrinfo in addrinfos:
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
            ping_msg = {'ip_addr': addrinfo.ext_ip_addr, 'port': self.port, 'sign': self.sign,
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
                            'ip_addr': addrinfo.ext_ip_addr, 'port': self.port,
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

    def send_pong_msg(self, info, addr, task=None):
        if (self.scheduler['auth'] or self.job_infos or not self.num_cpus or
            not self.service_available()):
            dispynode_logger.debug('Busy (%s/%s); ignoring ping message from %s',
                                   self.avail_cpus, self.num_cpus, addr[0])
            raise StopIteration

        scheduler_ip_addrs = info['ip_addrs']
        scheduler_port = info['port']
        if (not info.get('relay', None) and isinstance(addr, tuple) and isinstance(addr[0], str)):
            scheduler_ip_addrs.append(addr[0])
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
        addrinfos = list(self.addrinfos.values())

        for addrinfo in addrinfos:
            msg['ip_addr'] = addrinfo.ext_ip_addr
            for scheduler_ip_addr in scheduler_ip_addrs:
                if not isinstance(scheduler_ip_addr, str):
                    continue
                if re.match('\d+\.', scheduler_ip_addr):
                    sock_family = socket.AF_INET
                else:
                    sock_family = socket.AF_INET6
                if sock_family != addrinfo.family:
                    continue
                msg['scheduler_ip_addr'] = scheduler_ip_addr
                sock = AsyncSocket(socket.socket(addrinfo.family, socket.SOCK_STREAM),
                                   keyfile=self.keyfile, certfile=self.certfile)
                sock.settimeout(MsgTimeout)
                try:
                    yield sock.connect((scheduler_ip_addr, scheduler_port))
                    if sign:
                        yield sock.send_msg('PONG:'.encode() + serialize(msg))
                    else:
                        yield sock.send_msg('PING:'.encode() + serialize(msg))
                except Exception:
                    pass
                finally:
                    sock.close()

    def udp_server(self, addrinfo, task=None):
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

        if not addrinfo.bind_addr:
            Task(self.broadcast_ping_msg)
            addrinfo = None
        else:
            Task(self.broadcast_ping_msg, [addrinfo])

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
                Task(self.send_pong_msg, info, addr)
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
            else:
                dispynode_logger.warning('Ignoring ping message from %s', addr[0])

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
            try:
                _job = deserialize(msg)
                client = self.clients[_job.compute_id]
                compute = client.compute
                assert compute.scheduler_ip_addr == self.scheduler['ip_addr']
            except Exception:
                try:
                    yield conn.send_msg('NAK'.encode())
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
            if self.avail_cpus == 0:
                try:
                    yield conn.send_msg('NAK (all cpus busy)'.encode())
                except Exception:
                    pass
                raise StopIteration

            for xf in _job.xfer_files:
                if MaxFileSize and xf.stat_buf.st_size > MaxFileSize:
                    try:
                        yield conn.send_msg('NAK'.encode())
                    except Exception:
                        pass
                    raise StopIteration

            dispynode_logger.debug('New job id %s from %s/%s',
                                   _job.uid, addr[0], compute.scheduler_ip_addr)

            addrinfo = self.scheduler['addrinfo']
            reply = _JobReply(_job, addrinfo.ext_ip_addr)
            job_info = _DispyJobInfo(reply, compute, _job.xfer_files)
            job_info.addrinfo = self.addrinfos.get(compute.node_ip_addr, None)
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
                prog_thread.start()
                raise StopIteration

        def add_computation(msg):
            reply = None
            try:
                compute = deserialize(msg)
                addrinfo = self.addrinfos[compute.node_ip_addr]
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
                compute_id = deserialize(msg)
                client = self.clients[compute_id]
                compute = client.compute
            except Exception:
                raise StopIteration(client, 'invalid computation')

            client.globals['dispy_node_name'] = self.name
            client.globals['dispy_node_ip_addr'] = compute.node_ip_addr
            client.globals['dispy_job_path'] = compute.dest_path
            client.globals['_DispyNode'] = None
            client.globals['_dispy_node'] = None
            client.globals['_Client'] = None
            client.globals['_dispy_config'] = None
            client.globals['_dispy_setup_process'] = None
            client.globals['__dispy_sock_family'] = client.sock_family
            client.globals['__dispy_certfile'] = self.certfile
            client.globals['__dispy_keyfile'] = self.keyfile
            client.globals['__dispy_job_reply_addr'] = (compute.scheduler_ip_addr,
                                                        compute.job_result_port)
            client.globals['reply_Q'] = self.reply_Q
            client.globals['suid'] = self.suid
            client.globals['sgid'] = self.sgid

            setup_status = 0
            if self._safe_setup and (compute.setup or (not isinstance(compute.cleanup, bool))):
                # TODO: use this only for function computations?
                client.globals['loglevel'] = dispynode_logger.level
                client.parent_pipe, client.child_pipe = multiprocessing.Pipe(duplex=True)
                args = (client.compute, client.child_pipe, client.globals)
                client.setup_proc = multiprocessing.Process(target=_dispy_setup_process, args=args)
                client.setup_proc.start()
                compute.setup = None
                for i in range(40):
                    if client.parent_pipe.poll():
                        msg = client.parent_pipe.recv()
                        break
                    else:
                        yield task.sleep(0.25)
                else:
                    raise StopIteration(client, '"setup" has not finished in time')

                setup_status = msg['setup_status']
                if setup_status == 0:
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
                    compute.code = compile(compute.code, '<string>', 'exec')
                    exec(compute.code, globalvars)
                    compute.code = marshal.dumps(compute.code)
                if compute.setup:
                    init_vars = set(globalvars.keys())
                    localvars = {'_dispy_setup_status': 0,
                                 '_dispy_setup_args': compute.setup.args,
                                 '_dispy_setup_kwargs': compute.setup.kwargs}
                    os.chdir(compute.dest_path)
                    exec('_dispy_setup_status = %s(*_dispy_setup_args, **_dispy_setup_kwargs)' %
                         compute.setup.name, globalvars, localvars)
                    if localvars['_dispy_setup_status'] in (0, 1):
                        for var, value in globalvars.items():
                            if var not in init_vars:
                                client.globals[var] = value
                    else:
                        raise StopIteration(client, '"setup" failed with %s' %
                                            localvars['_dispy_setup_status'])
                    compute.setup = None
                    os.chdir(self.dest_path_prefix)

                    for module in list(sys.modules.keys()):
                        if module not in self.__init_modules:
                            sys.modules.pop(module, None)
                    sys.modules.update(self.__init_modules)

            if client.setup_proc and isinstance(compute.cleanup, _Function):
                compute.cleanup = True
            raise StopIteration(None, 'ACK')

        def terminate_job(client, job_info, task=None):

            def kill_pid(pid, signum):
                suid = client.globals.get('suid', None)
                if suid is None:
                    try:
                        os.kill(pid, signum)
                    except (OSError, Exception):
                        return -1
                    return 0
                else:
                    sgid = client.globals['sgid']

                    def suid_kill():
                        if hasattr(os, 'setresuid'):
                            os.setresgid(sgid, sgid, sgid)
                            os.setresuid(suid, suid, suid)
                        else:
                            os.setregid(sgid, sgid)
                            os.setreuid(suid, suid)
                        try:
                            os.kill(pid, signum)
                        except (OSError, Exception):
                            return -1
                        return 0

                    proc = multiprocessing.Process(target=suid_kill)
                    proc.start()
                    proc.join()
                    return proc.exitcode

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
            else:
                if isinstance(proc, multiprocessing.Process):
                    if proc.is_alive():
                        kill_pid(proc.pid, signal.SIGTERM)
                    else:
                        raise StopIteration
                elif isinstance(proc, subprocess.Popen):
                    if proc.poll() is None:
                        kill_pid(proc.pid, signal.SIGTERM)
                    else:
                        raise StopIteration
                else:
                    if kill_pid(pid, self.signals[0]):
                        # job terminated?
                        # TODO: send reply? job already finished?
                        raise StopIteration

            wait_nohang = getattr(os, 'WNOHANG', None)
            signum = self.signals[1]
            for i in range(20):
                if proc:
                    if isinstance(proc, multiprocessing.Process):
                        if not proc.is_alive():
                            break
                        proc.join(0.1)
                    elif isinstance(proc, subprocess.Popen):
                        if proc.poll() is not None:
                            break
                        yield task.sleep(0.1)
                    continue
                elif wait_nohang:
                    try:
                        if os.waitpid(pid, wait_nohang)[0] == pid:
                            break
                    except OSError:
                        # TODO: check err.errno for all platforms
                        break
                    yield task.sleep(0.1)
                    continue
                elif signum and i < 10:
                    yield task.sleep(0.1)
                    continue

                if kill_pid(pid, signum):
                    # TODO: check err.errno for all platforms
                    break
            else:
                if kill_pid(pid, self.signals[2]):
                    # TODO: is there a way to be sure if process is
                    # killed / job finished etc.?
                    dispynode_logger.debug('Killing job %s (PID %s) failed: %s',
                                           job_info.job_reply.uid, pid, traceback.format_exc())
                    job_info.job_reply.status = DispyJob.Terminated
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
                    Task(self.cleanup_computation, client)

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
                Task(self.cleanup_computation, client)
            yield conn.send_msg(resp.encode())
            conn.close()
        elif msg.startswith(b'CLOSE:'):
            msg = msg[len(b'CLOSE:'):]
            try:
                info = deserialize(msg)
                compute_id = info['compute_id']
                auth = info['auth']
                terminate_pending = info.get('terminate_pending', False)
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
                    Task(self.cleanup_computation, client)
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
                    Task(self.send_pong_msg, info, addr)
            except Exception:
                dispynode_logger.debug(traceback.format_exc())
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
            yield task.sleep(5)
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
                    info = {'ip_addr': addrinfo.ext_ip_addr, 'port': self.port,
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
                    elif psutil and isinstance(proc, psutil.Process):
                        try:
                            status = proc.status()
                        except (psutil.NoSuchProcess, psutil.ZombieProcess, Exception):
                            status = None
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
                    Task(self.cleanup_computation, client)
                for client in zombies:
                    compute = client.compute
                    addrinfo = self.addrinfos.get(compute.node_ip_addr, None)
                    if not addrinfo:
                        continue
                    sock = AsyncSocket(socket.socket(addrinfo.family, socket.SOCK_STREAM),
                                       keyfile=self.keyfile, certfile=self.certfile)
                    sock.settimeout(MsgTimeout)
                    dispynode_logger.debug('Sending TERMINATE to %s', compute.scheduler_ip_addr)
                    info = {'ip_addr': addrinfo.ext_ip_addr, 'port': self.port, 'sign': self.sign}
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

    def service_available(self):
        if self.serve == 0:
            return False
        if not self.service_start:
            return True
        now = int(time.time())
        if self.service_stop:
            if (self.service_start <= now < self.service_stop):
                return True
        elif (self.service_start <= now < self.service_end):
            return True
        return False

    def service_schedule(self, task=None):
        task.set_daemon()
        while 1:
            if self.service_stop:
                now = int(time.time())
                yield task.sleep(self.service_stop - now)
                dispynode_logger.debug('Stopping service')
                addrinfo = self.scheduler['addrinfo']
                sock = AsyncSocket(socket.socket(addrinfo.family, socket.SOCK_STREAM),
                                   keyfile=self.keyfile, certfile=self.certfile)
                sock.settimeout(MsgTimeout)
                try:
                    yield sock.connect((self.scheduler['ip_addr'], self.scheduler['port']))
                    info = {'ip_addr': addrinfo.ext_ip_addr, 'sign': self.sign, 'cpus': 0}
                    yield sock.send_msg('NODE_CPUS:'.encode() + serialize(info))
                except Exception:
                    pass
                finally:
                    sock.close()

            if self.service_end:
                now = int(time.time())
                yield task.sleep(self.service_end - now)
                dispynode_logger.debug('Shutting down service')
                self.shutdown('close')

            # advance times for next day
            self.service_start += 24 * 3600
            if self.service_stop:
                self.service_stop += 24 * 3600
            if self.service_end:
                self.service_end += 24 * 3600
            now = int(time.time())
            yield task.sleep(self.service_start - now)
            yield self.broadcast_ping_msg(task=task)

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
                        dispynode_logger.info('job proc for %s: %s' % (job_info.pid, proc.status()))
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
            self.num_jobs += 1
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
            Task(self.cleanup_computation, client)
        raise StopIteration(status)

    def cleanup_computation(self, client, task=None):
        if not client.zombie or client.pending_jobs:
            raise StopIteration

        compute = client.compute
        if self.clients.pop(compute.id, None) != client:
            dispynode_logger.debug('Computation %s already closed?', compute.id)
            raise StopIteration

        parent_pipe, client.parent_pipe = client.parent_pipe, None
        if parent_pipe:
            try:
                parent_pipe.send({'req': 'quit'})
            except Exception:
                dispynode_logger.debug(traceback.format_exc())

        self.num_computations += 1
        self.scheduler['auth'].discard(compute.auth)

        if isinstance(compute.cleanup, _Function):
            os.chdir(compute.dest_path)
            try:
                globalvars = dict(self.__init_globals)
                globalvars.update(client.globals)
                localvars = {'_dispy_cleanup_args': compute.cleanup.args,
                             '_dispy_cleanup_kwargs': compute.cleanup.kwargs}
                if self.client_shutdown:
                    globalvars['dispynode_shutdown'] = lambda: setattr(self, 'serve', 0)
                if isinstance(compute.code, bytes):
                    exec(marshal.loads(compute.code), globalvars)
                else:
                    exec(compute.code, globalvars)
                exec('%s(*_dispy_cleanup_args, **_dispy_cleanup_kwargs)' %
                     compute.cleanup.name, globalvars, localvars)
            except Exception:
                dispynode_logger.debug('Cleanup "%s" failed', compute.cleanup.name)
                dispynode_logger.debug(traceback.format_exc())

            os.chdir(self.dest_path_prefix)
            for module in list(sys.modules.keys()):
                if module not in self.__init_modules:
                    sys.modules.pop(module, None)
            sys.modules.update(self.__init_modules)
        client.globals.clear()

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

        dispynode_logger.debug('Computation "%s" from %s done',
                               compute.auth, compute.scheduler_ip_addr)
        if self.serve > 0:
            self.serve -= 1
        if self.serve == 0:
            addrinfo = self.addrinfos[compute.node_ip_addr]
            sock = AsyncSocket(socket.socket(addrinfo.family, socket.SOCK_STREAM), blocking=True,
                               keyfile=self.keyfile, certfile=self.certfile)
            sock.settimeout(MsgTimeout)
            dispynode_logger.debug('Sending TERMINATE to %s', compute.scheduler_ip_addr)
            info = {'ip_addr': compute.node_ip_addr, 'port': self.port, 'sign': self.sign}
            try:
                sock.connect((compute.scheduler_ip_addr, compute.scheduler_port))
                sock.send_msg('TERMINATED:'.encode() + serialize(info))
            except Exception:
                pass
            sock.close()
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
                client.zombie = True
                Task(self.cleanup_computation, client)
            if self.scheduler['ip_addr']:
                addrinfo = self.scheduler['addrinfo']
                sock = AsyncSocket(socket.socket(addrinfo.family, socket.SOCK_STREAM),
                                   keyfile=self.keyfile, certfile=self.certfile)
                sock.settimeout(MsgTimeout)
                dispynode_logger.debug('Sending TERMINATE to %s', self.scheduler['ip_addr'])
                info = {'ip_addr': addrinfo.ext_ip_addr, 'port': self.port, 'sign': self.sign}
                try:
                    yield sock.connect((self.scheduler['ip_addr'], self.scheduler['port']))
                    yield sock.send_msg('TERMINATED:'.encode() + serialize(info))
                except Exception:
                    pass
                sock.close()

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
            else:
                os.kill(self.pid, signal.SIGINT)

        if self.sign:
            Task(_shutdown, self, how)

    def cmd_proc(self, task=None):

        def release(task=None):
            if self.scheduler['auth']:
                addrinfo = self.scheduler['addrinfo']
                info = {'ip_addr': addrinfo.ext_ip_addr, 'port': self.port,
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
                            Task(self.cleanup_computation, client)
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
                    self.num_cpus = cpus

                self.avail_cpus = cpus - len(self.job_infos)

                if self.scheduler['auth']:
                    addrinfo = self.scheduler['addrinfo']
                    sock = AsyncSocket(socket.socket(addrinfo.family, socket.SOCK_STREAM),
                                       keyfile=self.keyfile, certfile=self.certfile)
                    sock.settimeout(MsgTimeout)
                    try:
                        yield sock.connect((self.scheduler['ip_addr'], self.scheduler['port']))
                        info = {'ip_addr': addrinfo.ext_ip_addr, 'sign': self.sign, 'cpus': cpus}
                        yield sock.send_msg('NODE_CPUS:'.encode() + serialize(info))
                    except Exception:
                        pass
                    finally:
                        sock.close()
                else:
                    if self.num_cpus > 0:
                        Task(self.broadcast_ping_msg)
            elif cmd == 'release':
                Task(release)
            else:
                print('\n  Serving %d CPUs%s%s%s' %
                      (self.avail_cpus + len(self.job_infos), service_from, service_to,
                       ' for %d clients' % self.serve if self.serve > 0 else ''))
                print('  Completed:\n    %d Computations, %d jobs, %.3f sec CPU time' %
                      (self.num_computations, self.num_jobs, self.cpu_time))
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
    parser.add_argument('-i', '--ip_addr', dest='ip_addrs', action='append', default=[],
                        help='IP address to use (may be needed in case of multiple interfaces)')
    parser.add_argument('--ext_ip_addr', dest='ext_ip_addrs', action='append', default=[],
                        help='External IP address to use (needed in case of NAT firewall/gateway)')
    parser.add_argument('-p', '--node_port', dest='node_port', type=int, default=51348,
                        help='port number to use')
    parser.add_argument('--ipv4_udp_multicast', dest='ipv4_udp_multicast', action='store_true',
                        default=False, help='use multicast for IPv4 UDP instead of broadcast')
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
        cfg['node_port'] = int(cfg['node_port'])
        cfg['scheduler_port'] = int(cfg['scheduler_port'])
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
        cfg['ip_addrs'] = cfg['ip_addrs'].strip()
        if cfg['ip_addrs']:
            cfg['ip_addrs'] = eval(cfg['ip_addrs'])
        cfg['ext_ip_addrs'] = cfg['ext_ip_addrs'].strip()
        if cfg['ext_ip_addrs']:
            cfg['ext_ip_addrs'] = eval(cfg['ext_ip_addrs'])
        for key, value in _dispy_config.items():
            if _dispy_config[key] != parser.get_default(key) or key not in cfg:
                cfg[key] = _dispy_config[key]
        _dispy_config = cfg
        del key, value
    _dispy_config.pop('config', None)

    cfg = _dispy_config.pop('save_config', None)
    if cfg:
        import configparser
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

    if os.name == 'nt':
        # Python 3 under Windows blocks multiprocessing.Process on reading
        # input; pressing "Enter" twice works (for one subprocess). Until
        # this is understood / fixed, disable reading input.

        # As of Sep 28, 2018, it seems multiprocessing works with reading input,
        # but don't what fixed it, so for now, leave it as is.

        # Update on Sep 30, 2018: While multiprocessing now works with
        # reading input, entering text seems to raise SIGINT or
        # SIGBREAK causing dispynode to quit, so leave it as daemon.
        print('\n    Reading standard input disabled, as multiprocessing\n'
              '    does not seem to work with reading input under Windows')
        _dispy_config['daemon'] = True

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

    dispynode_logger.info('dispynode version: %s, PID: %s', _dispy_version, os.getpid())
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
            # wait a bit for any output for command is done
            time.sleep(0.1)
            try:
                _dispy_cmd = input(
                    '\nEnter "quit" or "exit" to terminate dispynode,\n'
                    '  "stop" to stop service, "start" to restart service,\n'
                    '  "release" to check and close computation,\n'
                    '  "cpus" to change CPUs used, anything else to get status: ')
            except (Exception, KeyboardInterrupt):
                if os.path.isfile(os.path.join(_dispy_node.dest_path_prefix, 'config.pkl')):
                    _dispy_node.shutdown('exit')
                else:
                    break
            else:
                _dispy_cmd = _dispy_cmd.strip().lower()
                _dispy_node.cmd_task.send(_dispy_cmd)

    _dispy_node.pycos.finish()
