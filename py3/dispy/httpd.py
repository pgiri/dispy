"""
This file is part of dispy project.
See http://dispy.sourceforge.net for details.
"""

__author__ = "Giridhar Pemmasani (pgiri@yahoo.com)"
__email__ = "pgiri@yahoo.com"
__copyright__ = "Copyright 2015, Giridhar Pemmasani"
__contributors__ = []
__maintainer__ = "Giridhar Pemmasani (pgiri@yahoo.com)"
__license__ = "MIT"
__url__ = "http://dispy.sourceforge.net"

__all__ = ['DispyHTTPServer']

import sys
import os
import threading
import json
import cgi
import time
import socket
import ssl
import traceback

if sys.version_info.major > 2:
    from http.server import BaseHTTPRequestHandler, HTTPServer
    from urllib.parse import urlparse
else:
    from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
    from urlparse import urlparse

import dispy
from dispy import DispyJob

class DispyHTTPServer(object):
    class _HTTPRequestHandler(BaseHTTPRequestHandler):
        def __init__(self, ctx, DocumentRoot, *args):
            self._dispy_ctx = ctx
            self._dispy_ctx._http_handler = self
            self.DocumentRoot = DocumentRoot
            BaseHTTPRequestHandler.__init__(self, *args)

        def log_message(self, fmt, *args):
            # return # uncomment 'return' statement to disable messages from HTTP server
            dispy.logger.debug('HTTP client %s: %s' % (self.client_address[0], fmt % args))

        def do_GET(self):
            if self.path == '/cluster_updates':
                self._dispy_ctx._cluster_lock.acquire()
                nodes = self._dispy_ctx._cluster_updates.values()
                self._dispy_ctx._cluster_updates = {}
                self._dispy_ctx._cluster_lock.release()
                self.send_response(200)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.end_headers()
                updates = {'jobs':{'submitted':self._dispy_ctx._jobs_submitted,
                                   'done':self._dispy_ctx._jobs_done},
                           'nodes':[node.__dict__ for node in nodes]}
                self.wfile.write(json.dumps(updates).encode())
                return
            elif self.path == '/cluster_status':
                self._dispy_ctx._cluster_lock.acquire()
                nodes = self._dispy_ctx._cluster_status.values()
                self._dispy_ctx._cluster_lock.release()
                self.send_response(200)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.end_headers()
                updates = {'jobs':{'submitted':self._dispy_ctx._jobs_submitted,
                                   'done':self._dispy_ctx._jobs_done},
                           'nodes':[node.__dict__ for node in nodes]}
                self.wfile.write(json.dumps(updates).encode())
                return
            elif self.path == '/nodes':
                self._dispy_ctx._cluster_lock.acquire()
                nodes = self._dispy_ctx._cluster_status.values()
                self._dispy_ctx._cluster_lock.release()
                self.send_response(200)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.end_headers()
                nodes = [node.__dict__ for node in nodes]
                self.wfile.write(json.dumps(nodes).encode())
                return
            else:
                parsed_path = urlparse(self.path)
                path = parsed_path.path.lstrip('/')
                if path == '' or path == 'index.html':
                    path = 'monitor.html'
                path = os.path.join(self.DocumentRoot, path)
                try:
                    f = open(path)
                    data = f.read()
                    if path.endswith('.html'):
                        if path.endswith('monitor.html') or path.endswith('node.html'):
                            data = data % {'TIMEOUT':str(self._dispy_ctx._poll_sec)}
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
                    f.close()
                    return
                except:
                    dispy.logger.warning('HTTP client %s: Could not read/send "%s"',
                                         self.client_address[0], path)
                    dispy.logger.debug(traceback.format_exc())
                self.send_error(404)
                return
            dispy.logger.debug('Bad GET request from %s: %s' % (self.client_address[0], self.path))
            self.send_error(400)
            return

        def do_POST(self):
            form = cgi.FieldStorage(fp=self.rfile, headers=self.headers,
                                    environ={'REQUEST_METHOD':'POST'})
            if self.path == '/node_jobs':
                node = {}
                for item in form.list:
                    if item.name == 'name_ip':
                        try:
                            ip_addr = socket.gethostbyname(item.value)
                        except:
                            ip_addr = item.value
                        node = self._dispy_ctx._cluster_status.get(ip_addr, {})
                        break
                if node:
                    jobs = self._dispy_ctx._cluster.running_jobs(node.ip_addr)
                    # args and kwargs are sent as strings in Python  notation,
                    # so an object's __str__ or __repr__ is used if provided;
                    # TODO: check job is in _dispy_ctx's jobs?
                    jobs = [{'uid':id(job), 'job_id':str(job.id),
                             'args':', '.join(str(arg) for arg in job.args),
                             'kwargs':', '.join('%s=%s' % (key, val) \
                                                for key, val in job.kwargs.items()),
                             'sched_time_ms':int(1000 * job.start_time)} for job in jobs]
                    node = node.__dict__
                else:
                    jobs = []
                self.send_response(200)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.end_headers()
                self.wfile.write(json.dumps({'node':node, 'jobs':jobs}).encode())
                return
            elif self.path == '/node_info':
                node = {}
                for item in form.list:
                    if item.name == 'name_ip':
                        try:
                            ip_addr = socket.gethostbyname(item.value)
                        except:
                            ip_addr = item.value
                        node = self._dispy_ctx._cluster_status.get(ip_addr, {})
                        if node:
                            node = node.__dict__
                        break

                self.send_response(200)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.end_headers()
                self.wfile.write(json.dumps(node).encode())
                return
            elif self.path == '/cancel_jobs':
                uids = []
                for item in form.list:
                    if item.name == 'uid':
                        try:
                            uids.append(int(item.value))
                        except ValueError:
                            dispy.logger.debug('Cancel job uid "%s" is invalid' % item.value)

                jobs = [self._dispy_ctx._jobs.get(int(uid), None) for uid in uids]
                jobs = [job for job in jobs if job is not None]

                cancelled = [id(job) for job in jobs if self._dispy_ctx._cluster.cancel(job) == 0]
                self.send_response(200)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.end_headers()
                self.wfile.write(json.dumps(cancelled).encode())
                return
            elif self.path == '/add_node':
                node = {'name_ip':'', 'port':None, 'cpus':0}
                node_id = None
                for item in form.list:
                    if item.name == 'name_ip':
                        node['name_ip'] = item.value
                    elif item.name == 'port':
                        node['port'] = item.value
                    elif item.name == 'cpus':
                        try:
                            node['cpus'] = int(item.value)
                        except:
                            pass
                    elif item.name == 'id':
                        node_id = item.value
                if node['name_ip']:
                    if self._dispy_ctx._cluster.add_node(node) == 0:
                        self.send_response(200)
                        self.send_header('Content-Type', 'text/html')
                        self.end_headers()
                        node['id'] = node_id
                        self.wfile.write(json.dumps(node).encode())
                        return
            elif self.path == '/set_poll_sec':
                for item in form.list:
                    if item.name != 'timeout':
                        continue
                    try:
                        timeout = int(item.value)
                        if timeout < 1:
                            timeout = 0
                    except:
                        dispy.logger.warning('HTTP client %s: invalid timeout "%s" ignored', 
                                             self.client_address[0], item.value)
                        timeout = 0
                    self._dispy_ctx._poll_sec = timeout
                    self.send_response(200)
                    self.send_header('Content-Type', 'text/html')
                    self.end_headers()
                    return
            elif self.path == '/set_cpus':
                node_cpus = {}
                for item in form.list:
                    node_cpus[item.name] = self._dispy_ctx._cluster.set_node_cpus(item.name, item.value)

                self.send_response(200)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.end_headers()
                self.wfile.write(json.dumps(node_cpus).encode())
                return

            dispy.logger.debug('Bad POST request from %s: %s' % (self.client_address[0], self.path))
            self.send_error(400)
            return

    def __init__(self, cluster, host='', port=8181, poll_sec=10, DocumentRoot=None,
                 keyfile=None, certfile=None):
        self._cluster = cluster
        if cluster.status is None:
            cluster.status = self.cluster_status
        if not DocumentRoot:
            DocumentRoot = os.path.join(os.path.dirname(__file__), 'data')
        self._jobs = {}
        self._cluster_status = {}
        # TODO: maintain updates for each client separately, so
        # multiple clients can view the status?
        self._cluster_updates = {}
        self._cluster_lock = threading.Lock()
        self._jobs_submitted = 0
        self._jobs_done = 0
        if poll_sec < 1:
            dispy.logger.warning('invalid poll_sec value %s; it must be at least 1' % poll_sec)
            poll_sec = 1
        self._poll_sec = poll_sec
        self._http_handler = None
        self._server = HTTPServer((host, port), lambda *args: \
                                  self.__class__._HTTPRequestHandler(self, DocumentRoot, *args))
        if certfile:
            self._server.socket = ssl.wrap_socket(self._server.socket, keyfile=keyfile,
                                                  certfile=certfile, server_side=True)
        self._httpd_thread = threading.Thread(target=self._server.serve_forever)
        self._httpd_thread.daemon = True
        self._httpd_thread.start()
        dispy.logger.info('Started HTTP%s server at %s' % \
                           ('(S)' if certfile else '', str(self._server.socket.getsockname())))

    def cluster_status(self, status, node, job):
        if status == DispyJob.Created:
            self._jobs_submitted += 1
            self._jobs[id(job)] = job
            return
        if status == DispyJob.Finished or status == DispyJob.Terminated or \
               status == DispyJob.Cancelled or status == DispyJob.Abandoned:
            self._jobs_done += 1
            self._jobs.pop(id(job), None)

        if node is not None:
            # even if node closed, keep it; let UI decide how to indicate status
            node.update_epoch_ms = int(1000 * time.time())
            self._cluster_lock.acquire()
            self._cluster_status[node.ip_addr] = node
            self._cluster_updates[node.ip_addr] = node
            self._cluster_lock.release()

    def shutdown(self, wait=True):
        if wait:
            dispy.logger.debug(
                'HTTP server waiting for %s seconds for client updates before quitting',
                self._poll_sec)
            time.sleep(self._poll_sec)
        self._server.shutdown()
