#!/usr/bin/python2
"""
This file is part of dispy; see https://dispy.org for details.
"""

import sys
import os
import socket
import threading
import json
import cgi
import time
import ssl
import re
import hashlib
import struct
import collections
import traceback

import pycos
from pycos import Task, AsyncSocket, serialize, deserialize
import dispy
import dispy.config
from dispy.config import MsgTimeout
from dispy import DispyJob, DispyNodeAvailInfo, logger, _dispy_version

if sys.version_info.major > 2:
    from http.server import BaseHTTPRequestHandler, HTTPServer
    from urllib.parse import urlparse
else:
    from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
    from urlparse import urlparse

__author__ = "Giridhar Pemmasani (pgiri@yahoo.com)"
__email__ = "pgiri@yahoo.com"
__copyright__ = "Copyright 2019, Giridhar Pemmasani"
__contributors__ = []
__maintainer__ = "Giridhar Pemmasani (pgiri@yahoo.com)"
__license__ = "Apache 2.0"
__url__ = "https://dispy.org"

# Compatability function to work with both Python 2.7 and Python 3
if sys.version_info.major >= 3:
    def dict_iter(arg, iterator):
        return getattr(arg, iterator)()
else:
    def dict_iter(arg, iterator):
        return getattr(arg, 'iter' + iterator)()


class DispyAdminServer(object):

    class _NodePriv(object):

        def __init__(self, port, sock_family, sign):
            self.port = port
            self.sock_family = sock_family
            self.sign = sign
            self.auth = None

    class _NodeInfo(object):

        ip_re = re.compile(r'^((\d+\.\d+\.\d+\.\d+)|([0-9a-f:]+))$')

        def __init__(self, ip_addr, port, sock_family, sign):
            self.ip_addr = ip_addr
            self.name = ''
            self.max_cpus = 0
            self.cpus = 0
            self.scheduler_ip = None
            self.clients_done = 0
            self.jobs_done = 0
            self.cpu_time = 0
            self.busy = 0
            self.avail_info = None
            self.service_start = 0
            self.service_stop = 0
            self.service_end = 0
            self.serve = None
            self.update_time = 0
            self._priv = DispyAdminServer._NodePriv(port, sock_family, sign)

    class _HTTPRequestHandler(BaseHTTPRequestHandler):
        def __init__(self, ctx, DocumentRoot, *args):
            self._ctx = ctx
            self.DocumentRoot = DocumentRoot
            BaseHTTPRequestHandler.__init__(self, *args)

        @staticmethod
        def json_encode_nodes(arg):
            nodes = [dict(node.__dict__) for node in dict_iter(arg, 'values')]
            for node in nodes:
                node.pop('_priv', None)
                if node['avail_info']:
                    node['avail_info'] = node['avail_info'].__dict__
            return nodes

        def log_message(self, fmt, *args):
            # logger.debug('HTTP client %s: %s' % (self.client_address[0], fmt % args))
            return

        def do_GET(self):
            path = urlparse(self.path).path.lstrip('/')
            if path == '' or path == 'index.html':
                path = 'admin.html'
            path = os.path.join(self.DocumentRoot, path)
            try:
                with open(path) as fd:
                    data = fd.read()
                if path.endswith('.html'):
                    if path.endswith('admin.html') or path.endswith('admin_node.html'):
                        data = data % {'POLL_INTERVAL': str(self._ctx.poll_interval),
                                       'NODE_PORT': str(self._ctx.node_port)}
                    content_type = 'text/html'
                elif path.endswith('.js'):
                    content_type = 'text/javascript'
                elif path.endswith('.css'):
                    content_type = 'text/css'
                elif path.endswith('.ico'):
                    content_type = 'image/x-icon'
                self.send_response(200)
                self.send_header('Content-Type', content_type)
                if content_type == 'text/css' or content_type == 'text/javascript':
                    self.send_header('Cache-Control', 'private, max-age=86400')
                self.end_headers()
                self.wfile.write(data.encode())
                return
            except Exception:
                logger.warning('HTTP client %s: Could not read/send "%s"',
                               self.client_address[0], path)
                logger.debug(traceback.format_exc())
            self.send_error(404)
            return

        def do_POST(self):
            try:
                form = cgi.FieldStorage(fp=self.rfile, headers=self.headers,
                                        environ={'REQUEST_METHOD': 'POST'})
                client_request = self.path[1:]
            except Exception:
                logger.debug('Ignoring invalid POST request from %s', self.client_address[0])
                self.send_error(400)
                return

            if client_request == 'update':
                uid = None
                for item in form.list:
                    if item.name == 'uid':
                        uid = item.value.strip()
                        break
                if uid != self._ctx.client_uid:
                    self.send_error(400, 'invalid uid')
                    return
                self._ctx.client_uid_time = time.time()
                self._ctx.lock.acquire()
                nodes = self.__class__.json_encode_nodes(self._ctx.updates)
                self._ctx.updates.clear()
                self._ctx.lock.release()
                self.send_response(200)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.end_headers()
                self.wfile.write(json.dumps(nodes).encode())
                return

            elif client_request == 'node_info':
                ip_addr = None
                uid = None
                for item in form.list:
                    if item.name == 'host':
                        # if it looks like IP address, skip resolving
                        if re.match(DispyAdminServer._NodeInfo.ip_re, item.value):
                            ip_addr = item.value
                        else:
                            ip_addr = dispy._node_ipaddr(item.value)
                    elif item.name == 'uid':
                        uid = item.value.strip()
                if uid != self._ctx.client_uid:
                    self.send_error(400, 'invalid uid')
                    return
                self._ctx.client_uid_time = time.time()
                node = self._ctx.nodes.get(ip_addr, None)
                self.send_response(200)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.end_headers()
                if node:
                    node = dict(node.__dict__)
                    node.pop('_priv', None)
                    if node['avail_info']:
                        node['avail_info'] = node['avail_info'].__dict__
                else:
                    node = {}
                self.wfile.write(json.dumps(node).encode())
                return

            elif client_request == 'status':
                uid = None
                for item in form.list:
                    if item.name == 'uid':
                        uid = item.value.strip()
                        break
                if uid != self._ctx.client_uid:
                    self.send_error(400, 'invalid uid')
                    return
                self._ctx.client_uid_time = time.time()
                self._ctx.lock.acquire()
                nodes = self.__class__.json_encode_nodes(self._ctx.nodes)
                self._ctx.lock.release()
                self.send_response(200)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.end_headers()
                self.wfile.write(json.dumps(nodes).encode())
                return

            elif client_request == 'get_uid':
                uid = None
                for item in form.list:
                    if item.name == 'uid':
                        uid = item.value.strip()
                    elif item.name == 'poll_interval':
                        try:
                            poll_interval = int(item.value)
                            assert poll_interval >= 5
                        except Exception:
                            self.send_error(400, 'invalid poll interval')
                            return
                # TODO: only allow from http server?
                uid = self._ctx.set_uid(self.client_address[0], poll_interval, uid)
                if not uid:
                    self.send_error(400, 'invalid uid')
                    return
                self.send_response(200)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.end_headers()
                self.wfile.write(json.dumps(uid).encode())
                return

            elif client_request == 'set_secret':
                secret = None
                uid = None
                for item in form.list:
                    if item.name == 'secret':
                        secret = item.value.strip()
                    elif item.name == 'uid':
                        uid = item.value.strip()
                if secret and uid == self._ctx.client_uid:
                    self._ctx.client_uid_time = time.time()
                    self._ctx.set_secret(secret)
                    self.send_response(200)
                    self.send_header('Content-Type', 'application/json; charset=utf-8')
                    self.end_headers()
                    self.wfile.write(json.dumps(0).encode())
                else:
                    self.send_error(400)
                return

            elif client_request == 'add_node':
                host = ''
                port = None
                uid = None
                for item in form.list:
                    if item.name == 'host':
                        host = item.value
                    elif item.name == 'port':
                        try:
                            port = int(item.value)
                        except Exception:
                            port = None
                    elif item.name == 'uid':
                        uid = item.value.strip()
                if uid != self._ctx.client_uid:
                    self.send_error(400, 'invalid uid')
                    return
                if host and port:
                    ip_addr = dispy._node_ipaddr(host)
                    if ip_addr:
                        info = {'ip_addr': ip_addr, 'port': port}
                        Task(self._ctx.add_node, info)
                        self.send_response(200)
                        self.send_header('Content-Type', 'application/json; charset=utf-8')
                        self.end_headers()
                        self.wfile.write(json.dumps(0).encode())
                    else:
                        self.send_error(400)
                return

            elif client_request == 'service_time':
                hosts = []
                svc_time = None
                control = None
                uid = None
                for item in form.list:
                    if item.name == 'hosts':
                        hosts = [str(host) for host in json.loads(item.value)]
                    elif item.name == 'control':
                        control = item.value
                    elif item.name == 'time':
                        svc_time = item.value.strip()
                    elif item.name == 'uid':
                        uid = item.value.strip()
                if uid != self._ctx.client_uid:
                    self.send_error(400, 'invalid uid')
                    return
                self._ctx.client_uid_time = time.time()
                for host in hosts:
                    Task(self._ctx.service_time, host, control, svc_time)
                self.send_response(200)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.end_headers()
                self.wfile.write(json.dumps(0).encode())
                return

            elif client_request == 'set_cpus':
                hosts = []
                cpus = None
                uid = None
                for item in form.list:
                    if item.name == 'hosts':
                        hosts = [str(host) for host in json.loads(item.value)]
                        if not hosts:
                            self.send_error(400, 'invalid nodes')
                            return
                    elif item.name == 'cpus':
                        cpus = item.value
                        if cpus is not None:
                            try:
                                cpus = int(item.value)
                            except Exception:
                                self.send_error(400, 'invalid CPUs')
                                return
                    elif item.name == 'uid':
                        uid = item.value.strip()
                if uid != self._ctx.client_uid:
                    self.send_error(400, 'invalid uid')
                    return
                for host in hosts:
                    Task(self._ctx.set_cpus, host, cpus)
                self._ctx.client_uid_time = time.time()
                self.send_response(200)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.end_headers()
                self.wfile.write(json.dumps(0).encode())
                return

            elif client_request == 'serve_clients':
                host = ''
                serve = None
                uid = None
                for item in form.list:
                    if item.name == 'host':
                        host = item.value
                    elif item.name == 'serve':
                        serve = item.value
                        try:
                            serve = int(serve)
                        except Exception:
                            pass
                    elif item.name == 'uid':
                        uid = item.value.strip()
                if (uid == self._ctx.client_uid and isinstance(serve, int) and
                    Task(self._ctx.serve_clients, host, serve).value() == 0):
                    self._ctx.client_uid_time = time.time()
                    self.send_response(200)
                    self.send_header('Content-Type', 'application/json; charset=utf-8')
                    self.end_headers()
                    self.wfile.write(json.dumps(0).encode())
                else:
                    self.send_error(400)
                    return
                return

            elif client_request == 'poll_interval':
                uid = None
                interval = None
                for item in form.list:
                    if item.name == 'interval':
                        try:
                            interval = int(item.value)
                        except Exception:
                            if interval is not None:
                                logger.warning('%s: invalid poll interval "%s" ignored',
                                               self._ctx.client_uid, item.value)
                                self.send_error(400)
                                return
                    elif item.name == 'uid':
                        uid = item.value.strip()
                if (uid == self._ctx.client_uid and self._ctx.set_poll_interval(interval) == 0):
                    self._ctx.client_uid_time = time.time()
                    self.send_response(200)
                    self.send_header('Content-Type', 'application/json; charset=utf-8')
                    self.end_headers()
                    self.wfile.write(json.dumps(0).encode())
                else:
                    self.send_error(400)
                return

            logger.debug('Bad POST request from %s: %s', self.client_address[0], client_request)
            self.send_error(400)
            return

    def __init__(self, DocumentRoot, secret='', http_host='localhost',
                 poll_interval=60, ping_interval=600, hosts=[], ipv4_udp_multicast=False,
                 certfile=None, keyfile=None):
        http_port = dispy.config.HTTPServerPort
        self.node_port = eval(dispy.config.NodePort)
        self.info_port = eval(dispy.config.ClientPort)
        self.lock = threading.Lock()
        self.client_uid = None
        self.client_uid_time = 0
        self.nodes = {}
        self.updates = {}
        if poll_interval < 1:
            logger.warning('invalid poll_interval value %s; it must be at least 1', poll_interval)
            poll_interval = 1
        self.poll_interval = poll_interval
        self.ping_interval = ping_interval
        self.secret = secret
        self.keyfile = keyfile
        self.certfile = certfile
        self.ipv4_udp_multicast = bool(ipv4_udp_multicast)
        self.addrinfos = []

        if not hosts:
            hosts = [None]
        for host in hosts:
            addrinfo = dispy.host_addrinfo(host=host, ipv4_multicast=self.ipv4_udp_multicast)
            if not addrinfo:
                logger.warning('Ignoring invalid host %s', host)
                continue
            self.addrinfos.append(addrinfo)
        if not self.addrinfos:
            raise Exception('No valid host name / IP address found')
        self.sign = hashlib.sha1(os.urandom(20))
        for addrinfo in self.addrinfos:
            self.sign.update(addrinfo.ip.encode())
        self.sign = self.sign.hexdigest()
        self.auth = dispy.auth_code(self.secret, self.sign)

        self.tcp_tasks = []
        self.udp_tasks = []
        udp_addrinfos = {}
        for addrinfo in self.addrinfos:
            self.tcp_tasks.append(Task(self.tcp_server, addrinfo))
            udp_addrinfos[addrinfo.bind_addr] = addrinfo

        for bind_addr, addrinfo in udp_addrinfos.items():
            self.udp_tasks.append(Task(self.udp_server, addrinfo))

        self._server = HTTPServer((http_host, http_port), lambda *args:
                                  self.__class__._HTTPRequestHandler(self, DocumentRoot, *args))
        if certfile:
            self._server.socket = ssl.wrap_socket(self._server.socket, keyfile=keyfile,
                                                  certfile=certfile, server_side=True)
        self.timer = Task(self.timer_proc)
        self._httpd_thread = threading.Thread(target=self._server.serve_forever)
        self._httpd_thread.daemon = True
        self._httpd_thread.start()
        self.client_host = self._server.socket.getsockname()[0]
        logger.info('Started HTTP%s server at %s:%s', 's' if certfile else '',
                    self.client_host, self._server.socket.getsockname()[1])

    def tcp_server(self, addrinfo, task=None):
        task.set_daemon()
        sock = AsyncSocket(socket.socket(addrinfo.family, socket.SOCK_STREAM),
                           keyfile=self.keyfile, certfile=self.certfile)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind((addrinfo.ip, self.info_port))
        except Exception:
            logger.warning('Could not bind TCP server to %s:%s', addrinfo.ip, self.info_port)
            raise StopIteration
        logger.info('dispyadmin TCP server at %s:%s', addrinfo.ip, self.info_port)
        sock.listen(16)

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

    def udp_server(self, addrinfo, task=None):
        task.set_daemon()
        udp_sock = AsyncSocket(socket.socket(addrinfo.family, socket.SOCK_DGRAM))
        udp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if hasattr(socket, 'SO_REUSEPORT'):
            try:
                udp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
            except Exception:
                pass

        udp_sock.bind((addrinfo.bind_addr, self.info_port))
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

            if msg.startswith('PING:'):
                try:
                    info = deserialize(msg[len('PING:'):])
                    if info['version'] != _dispy_version:
                        logger.warning('Ignoring %s due to version mismatch', addr[0])
                        continue
                    assert info['port'] > 0
                    assert info['ip_addr']
                except Exception:
                    logger.debug('Ignoring node %s', addr[0])
                    continue
                node = self.nodes.get(info['ip_addr'], None)
                if node:
                    if node._priv.sign == info['sign']:
                        Task(self.update_node_info, node)
                    else:
                        node._priv.sign = info['sign']
                        node._priv.auth = None
                        Task(self.get_node_info, node)
                else:
                    info['family'] = addrinfo.family
                    Task(self.add_node, info)

            elif msg.startswith('TERMINATED:'):
                try:
                    info = deserialize(msg[len('TERMINATED:'):])
                    assert info['ip_addr']
                except Exception:
                    logger.debug('Ignoring node %s', addr[0])
                    continue
                node = self.nodes.get(info['ip_addr'], None)
                if node and node._priv.sign == info['sign']:
                    with self.lock:
                        self.nodes.pop(info['ip_addr'], None)

    def tcp_req(self, conn, addr, task=None):
        conn.settimeout(MsgTimeout)
        msg = yield conn.recv_msg()
        if msg.startswith('NODE_INFO:'):
            try:
                info = deserialize(msg[len('NODE_INFO:'):])
                dispy.logger.info('info: %s', info)
                node = info.get('ip_addr', None)
                if info.get('version', None) != _dispy_version:
                    dispy.logger.warning('Ignoring node at %s due to version mismatch (%s != %s)',
                                         info.get('ip_addr', None),
                                         info.get('version', None), _dispy_version)
                    raise StopIteration
                assert info['sign']
                info['family'] = conn.family
            except Exception:
                # dispy.logger.debug(traceback.format_exc())
                raise StopIteration
            finally:
                conn.close()
            yield self.add_node(info)
            raise StopIteration

    def set_node_info(self, node, info):
        node.scheduler_ip = info['scheduler_ip']
        node.clients_done = info['clients_done']
        node.jobs_done = info['jobs_done']
        node.cpu_time = info['cpu_time']
        node.busy = info['busy']
        node.serve = info['serve']
        if 'service_start' in info:
            node.service_start = info['service_start']
            node.service_stop = info['service_stop']
            node.service_end = info['service_end']
            node.avail_info = info['avail_info']
        node.update_time = time.time()
        with self.lock:
            self.updates[node.ip_addr] = node

    def get_node_info(self, node, task=None):
        auth = node._priv.auth
        if not auth:
            auth = dispy.auth_code(self.secret, node._priv.sign)
        sock = AsyncSocket(socket.socket(node._priv.sock_family, socket.SOCK_STREAM),
                           keyfile=self.keyfile, certfile=self.certfile)
        sock.settimeout(MsgTimeout)
        try:
            yield sock.connect((node.ip_addr, node._priv.port))
            yield sock.sendall(auth)
            yield sock.send_msg('NODE_INFO:' + serialize({'sign': self.sign}))
            info = yield sock.recv_msg()
        except Exception:
            dispy.logger.debug('Could not get node information from %s:%s',
                               node.ip_addr, node._priv.port)
            # dispy.logger.debug(traceback.format_exc())
            raise StopIteration(-1)
        finally:
            sock.close()
        try:
            info = deserialize(info)
            node.name = info['name']
            node.cpus = info['cpus']
            node.max_cpus = info['max_cpus']
        except Exception:
            sign = info.decode()
            if node._priv.sign == sign:
                node.update_time = time.time()
                raise StopIteration(0)
            else:
                node._priv.sign = sign
                ret = yield self.get_node_info(node, task=task)
                raise StopIteration(ret)
        else:
            node._priv.auth = auth
            self.set_node_info(node, info)
            raise StopIteration(0)

    def add_node(self, info, task=None):
        sign = info.get('sign', '')
        family = info.get('family', None)
        if not family:
            for addr in socket.getaddrinfo(info['ip_addr'], info['port'], type=socket.SOCK_STREAM,
                                           proto=socket.IPPROTO_TCP):
                family = addr[0]
                break
        node = DispyAdminServer._NodeInfo(info['ip_addr'], info['port'], family, sign)
        ret = yield self.get_node_info(node, task=task)
        if ret == 0:
            with self.lock:
                self.nodes[node.ip_addr] = node
                self.updates[node.ip_addr] = node

    def set_secret(self, secret, task=None):
        with self.lock:
            self.secret = secret
            for node in self.nodes.values():
                if not node._priv.auth:
                    Task(self.get_node_info, node)
        self.timer.resume()

    def set_cpus(self, host, cpus, task=None):
        node = self.nodes.get(host, None)
        if not node or not node._priv.auth:
            raise StopIteration(-1)
        sock = AsyncSocket(socket.socket(node._priv.sock_family, socket.SOCK_STREAM),
                           keyfile=self.keyfile, certfile=self.certfile)
        sock.settimeout(MsgTimeout)
        try:
            yield sock.connect((node.ip_addr, node._priv.port))
            yield sock.sendall(node._priv.auth)
            yield sock.send_msg('SET_CPUS:'.encode() + serialize({'cpus': cpus}))
            resp = yield sock.recv_msg()
            info = deserialize(resp)
            node.cpus = info['cpus']
        except Exception:
            dispy.logger.debug('Setting cpus of %s to %s failed', host, cpus)
            raise StopIteration(-1)
        else:
            raise StopIteration(0)
        finally:
            sock.close()

    def service_time(self, host, control, time, task=None):
        node = self.nodes.get(dispy._node_ipaddr(host), None)
        if not node or not node._priv.auth:
            raise StopIteration(-1)
        sock = AsyncSocket(socket.socket(node._priv.sock_family, socket.SOCK_STREAM),
                           keyfile=self.keyfile, certfile=self.certfile)
        sock.settimeout(MsgTimeout)
        try:
            yield sock.connect((node.ip_addr, node._priv.port))
            yield sock.sendall(node._priv.auth)
            yield sock.send_msg('SERVICE_TIME:'.encode() +
                                serialize({'control': control, 'time': time}))
            resp = yield sock.recv_msg()
            info = deserialize(resp)
            node.service_start = info['service_start']
            node.service_stop = info['service_stop']
            node.service_end = info['service_end']
            resp = 0
        except Exception:
            resp = -1
        sock.close()
        if resp:
            dispy.logger.debug('Setting service %s time of %s to %s failed', control, host, time)
        raise StopIteration(resp)

    def serve_clients(self, host, serve, task=None):
        node = self.nodes.get(dispy._node_ipaddr(host), None)
        if not node or not node._priv.auth:
            raise StopIteration(-1)
        sock = AsyncSocket(socket.socket(node._priv.sock_family, socket.SOCK_STREAM),
                           keyfile=self.keyfile, certfile=self.certfile)
        sock.settimeout(MsgTimeout)
        try:
            yield sock.connect((node.ip_addr, node._priv.port))
            yield sock.sendall(node._priv.auth)
            yield sock.send_msg('SERVE_CLIENTS:'.encode() + serialize({'serve': serve}))
            resp = yield sock.recv_msg()
            info = deserialize(resp)
            node.serve = info['serve']
            resp = 0
        except Exception:
            dispy.logger.debug('Setting serve clients %s to %s failed', host, serve)
            resp = -1
        finally:
            sock.close()
        raise StopIteration(resp)

    def update_node_info(self, node, task=None):
        sock = AsyncSocket(socket.socket(node._priv.sock_family, socket.SOCK_STREAM),
                           keyfile=self.keyfile, certfile=self.certfile)
        sock.settimeout(MsgTimeout)
        try:
            yield sock.connect((node.ip_addr, node._priv.port))
            yield sock.sendall(node._priv.auth)
            yield sock.send_msg('NODE_STATUS:')
            info = yield sock.recv_msg()
            info = deserialize(info)
            if isinstance(info, dict):
                self.set_node_info(node, info)
        except Exception:
            # TODO: remove node if update is long ago?
            pass
        finally:
            sock.close()

    def set_poll_interval(self, interval):
        if not isinstance(interval, int):
            if interval is None:
                self.timer.resume()
                return 0
            else:
                return -1
        if not interval:
            self.timer.resume()
            return 0
        elif interval >= 5:
            self.poll_interval = interval
            self.timer.resume(update=interval)
            return 0
        else:
            return -1

    def set_uid(self, client_host, poll_interval, uid=None):
        now = time.time()
        try:
            poll_interval = int(poll_interval)
            assert poll_interval >= 5
        except Exception:
            return None

        if ((uid == self.client_uid) or (not self.client_uid) or
            (self.client_host == client_host) or
            ((now - self.client_uid_time) > min(5 * self.poll_interval, 600))):
            if not uid:
                uid = hashlib.sha1(os.urandom(20)).hexdigest()
            self.client_uid = uid
            self.client_uid_time = now
            self.client_host = client_host
            if self.poll_interval != poll_interval:
                self.poll_interval = poll_interval
            self.timer.resume(update=poll_interval)
            return uid
        else:
            logger.warning('Ignoring client at %s; currently controlled by client at %s',
                           client_host, self.client_host)
            return None

    def discover_nodes(self, task=None):
        for addrinfo in list(self.addrinfos):
            info_msg = {'ip_addr': addrinfo.ip, 'port': self.info_port,
                        'sign': self.sign, 'version': _dispy_version}
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
                bc_sock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_MULTICAST_IF,
                                   addrinfo.ifn)
            bc_sock.bind((addrinfo.ip, 0))
            try:
                yield bc_sock.sendto('NODE_INFO:' + serialize(info_msg),
                                     (addrinfo.broadcast, self.node_port))
            except Exception:
                pass
            bc_sock.close()

    def timer_proc(self, task=None):
        task.set_daemon()

        Task(self.discover_nodes)
        last_ping = time.time()
        interval = self.poll_interval
        while 1:
            now = time.time()
            if (now - last_ping) >= self.ping_interval:
                Task(self.discover_nodes)
                last_ping = now
                if (now - self.client_uid_time) > (5 * self.ping_interval):
                    interval *= 2
            with self.lock:
                nodes = list(self.nodes.values())
            # TODO: it may be better to have nodes send updates periodically
            for node in nodes:
                if node._priv.auth:
                    Task(self.update_node_info, node)
            update = yield task.sleep(interval)
            if update:
                interval = update

    def shutdown(self, wait=True):
        """This method should be called by user program to close the
        http server.
        """
        if wait:
            logger.info(
                'HTTP server waiting for %s seconds for client updates before quitting' %
                self.poll_interval)
            time.sleep(self.poll_interval)
        self._server.shutdown()
        self._server.server_close()


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--config', dest='config', default='',
                        help='use configuration in given file')
    parser.add_argument('--save_config', dest='save_config', default='',
                        help='save configuration in given file and exit')
    parser.add_argument('--secret', dest='secret', default='',
                    help='"secret" to access cluster (must be same as "admin_secret" to dispynode)')
    parser.add_argument('--http_host', dest='http_host', default='localhost',
                        help='name or IP address where http server starts')
    parser.add_argument('--http_port', dest='http_port', default=dispy.config.HTTPServerPort,
                        type=int, help='port number where http server starts')
    parser.add_argument('--info_port', dest='info_port', default=eval(dispy.config.ClientPort),
                        type=int,
                        help='port number where nodes send information (same as client port)')
    parser.add_argument('--node_port', dest='node_port', default=eval(dispy.config.NodePort),
                        type=int, help='port number where nodes run')
    parser.add_argument('--poll_interval', dest='poll_interval', default='60',
                        help='interval to collect latest status from nodes')
    parser.add_argument('--ping_interval', dest='ping_interval', default='600',
                        help='interval to discover (new) nodes')
    parser.add_argument('--certfile', dest='certfile', default='',
                        help='file containing SSL certificate')
    parser.add_argument('--keyfile', dest='keyfile', default='',
                        help='file containing SSL key')
    parser.add_argument('--ipv4_udp_multicast', dest='ipv4_udp_multicast', action='store_true',
                        default=False, help='use multicast for IPv4 UDP instead of broadcast')
    parser.add_argument('-i', '--host', dest='hosts', action='append', default=[],
                        help='host name or IP address to listen from nodes '
                        '(may be needed in case of multiple interfaces)')
    parser.add_argument('-d', '--debug', action='store_true', dest='debug', default=False,
                        help='if given, debug messages are printed')
    DocumentRoot = os.path.join(os.path.dirname(__file__), 'data')
    config = vars(parser.parse_args(sys.argv[1:]))
    if config.pop('debug', False):
        dispy.logger.setLevel(dispy.logger.DEBUG)
    if config['config']:
        import configparser
        cfg = configparser.ConfigParser()
        cfg.read(config['config'])
        cfg = dict(cfg.items('DEFAULT'))
        cfg['ipv4_udp_multicast'] = cfg['ipv4_udp_multicast'] == 'True'
        cfg['hosts'] = cfg['hosts'].strip()
        if cfg['hosts']:
            cfg['hosts'] = eval(cfg['hosts'])
        config = cfg
    config['poll_interval'] = int(config['poll_interval'])
    config['ping_interval'] = int(config['ping_interval'])
    config.pop('config', None)

    cfg = config.pop('save_config', None)
    if cfg:
        import configparser
        config = configparser.ConfigParser(config)
        cfg = open(cfg, 'w')
        config.write(cfg)
        cfg.close()
        exit(0)

    dispy.config.HTTPServerPort = int(config.pop('http_port'))
    dispy.config.ClientPort = str(int(config.pop('info_port')))
    dispy.config.NodePort = str(int(config.pop('node_port')))
    server = DispyAdminServer(DocumentRoot, **config)
    while 1:
        try:
            cmd = raw_input(
                '\nEnter "quit" or "exit" to quit,\n'
                '  "secret" to add admin secret,\n'
                '  "scan" to find nodes :')
        except KeyboardInterrupt:
            break
        cmd = cmd.strip().lower()
        if cmd == 'secret':
            secret = input('Enter admin secret :')
            secret = secret.strip()
            if secret:
                server.set_secret(secret)
        elif cmd == 'scan':
            Task(server.discover_nodes)
        elif cmd == 'quit' or cmd == 'exit':
            break
        else:
            print('Ignoring invalid command')

    server.shutdown()
