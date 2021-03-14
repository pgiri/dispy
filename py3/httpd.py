"""
This file is part of dispy; see https://dispy.org for details.
"""

import sys
import os
import threading
import json
import cgi
import time
import ssl
import re
import functools
import copy
import traceback

import dispy
from dispy import DispyJob, DispyNodeAvailInfo, logger

if sys.version_info.major > 2:
    from http.server import BaseHTTPRequestHandler, HTTPServer
    from urllib.parse import urlparse
else:
    from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
    from urlparse import urlparse

__author__ = "Giridhar Pemmasani (pgiri@yahoo.com)"
__email__ = "pgiri@yahoo.com"
__copyright__ = "Copyright 2015, Giridhar Pemmasani"
__contributors__ = []
__maintainer__ = "Giridhar Pemmasani (pgiri@yahoo.com)"
__license__ = "Apache 2.0"
__url__ = "https://dispy.org"

__all__ = ['DispyHTTPServer']

# Compatability function to work with both Python 2.7 and Python 3
if sys.version_info.major >= 3:
    def dict_iter(arg, iterator):
        return getattr(arg, iterator)()
else:
    def dict_iter(arg, iterator):
        return getattr(arg, 'iter' + iterator)()


class DispyHTTPServer(object):

    class _ClusterInfo(object):

        ip_re = re.compile(r'^((\d+\.\d+\.\d+\.\d+)|([0-9a-f:]+))$')

        def __init__(self, cluster):
            self.cluster = cluster
            self.jobs_submitted = 0
            self.jobs_done = 0
            self.jobs_pending = 0
            self.jobs = {}
            self.status = {}
            # TODO: maintain updates for each client separately, so
            # multiple clients can view the status?
            self.updates = {}

    class _HTTPRequestHandler(BaseHTTPRequestHandler):
        def __init__(self, ctx, DocumentRoot, *args):
            self._ctx = ctx
            self.DocumentRoot = DocumentRoot
            BaseHTTPRequestHandler.__init__(self, *args)

        @staticmethod
        def json_encode_nodes(arg):
            nodes = [dict(node.__dict__) for node in dict_iter(arg, 'values')]
            for node in nodes:
                if isinstance(node['avail_info'], DispyNodeAvailInfo):
                    node['avail_info'] = node['avail_info'].__dict__
            return nodes

        def log_message(self, fmt, *args):
            # logger.debug('HTTP client %s: %s' % (self.client_address[0], fmt % args))
            return

        def do_GET(self):

            if self.path == '/cluster_updates':
                self._ctx._cluster_lock.acquire()
                clusters = [
                    {'name': name,
                     'jobs': {'submitted': cluster.jobs_submitted, 'done': cluster.jobs_done},
                     'nodes': self.__class__.json_encode_nodes(cluster.updates)
                     } for name, cluster in dict_iter(self._ctx._clusters, 'items')
                    ]
                for cluster in dict_iter(self._ctx._clusters, 'values'):
                    cluster.updates.clear()
                self._ctx._cluster_lock.release()
                self.send_response(200)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.end_headers()
                self.wfile.write(json.dumps(clusters).encode())
                return

            elif self.path == '/cluster_status':
                self._ctx._cluster_lock.acquire()
                clusters = [
                    {'name': name,
                     'jobs': {'submitted': cluster.jobs_submitted, 'done': cluster.jobs_done},
                     'nodes': self.__class__.json_encode_nodes(cluster.status)
                     } for name, cluster in dict_iter(self._ctx._clusters, 'items')
                    ]
                self._ctx._cluster_lock.release()
                self.send_response(200)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.end_headers()
                self.wfile.write(json.dumps(clusters).encode())
                return

            elif self.path == '/nodes':
                self._ctx._cluster_lock.acquire()
                nodes = [
                    {'name': name,
                     'nodes': self.__class__.json_encode_nodes(cluster.status)
                     } for name, cluster in dict_iter(self._ctx._clusters, 'items')
                    ]
                self._ctx._cluster_lock.release()
                self.send_response(200)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.end_headers()
                self.wfile.write(json.dumps(nodes).encode())
                return

            else:
                parsed_path = urlparse(self.path)
                path = parsed_path.path.lstrip('/')
                if path == '' or path == 'index.html':
                    path = 'monitor.html'
                path = os.path.join(self.DocumentRoot, path)
                try:
                    with open(path) as fd:
                        data = fd.read()
                    if path.endswith('.html'):
                        if path.endswith('monitor.html') or path.endswith('node.html'):
                            data = data % {'TIMEOUT': str(self._ctx._poll_sec),
                                           'SHOW_JOB_ARGS': 'true' if self._ctx._show_args
                                                            else 'false'}
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
            logger.debug('Bad GET request from %s: %s', self.client_address[0], self.path)
            self.send_error(400)
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

            if client_request == 'node_info':
                ip_addr = None
                for item in form.list:
                    if item.name == 'host':
                        # if it looks like IP address, skip resolving
                        if re.match(DispyHTTPServer._ClusterInfo.ip_re, item.value):
                            ip_addr = item.value
                        else:
                            ip_addr = dispy._node_ipaddr(item.value)
                        break
                self._ctx._cluster_lock.acquire()
                cluster_infos = [(name, cluster_info) for name, cluster_info in
                                 self._ctx._clusters.items()]
                self._ctx._cluster_lock.release()
                cluster_jobs = {}
                node = None
                show_args = self._ctx._show_args
                for name, cluster_info in cluster_infos:
                    cluster_node = cluster_info.status.get(ip_addr, None)
                    if not cluster_node:
                        cluster_jobs[name] = []
                        continue
                    if node:
                        node.jobs_done += cluster_node.jobs_done
                        node.cpu_time += cluster_node.cpu_time
                        node.update_time = max(node.update_time, cluster_node.update_time)
                        node.tx += cluster_node.tx
                        node.rx += cluster_node.rx
                    else:
                        node = copy.copy(cluster_node)
                    # jobs = cluster_info.cluster.node_jobs(ip_addr)
                    jobs = [job for job in dict_iter(cluster_info.jobs, 'values')
                            if job.ip_addr == ip_addr]
                    # args and kwargs are sent as strings in Python,
                    # so an object's __str__ or __repr__ is used if provided;
                    # TODO: check job is in _ctx's jobs?
                    jobs = [{'uid': job._uid, 'job_id': str(job.id),
                             'args': ', '.join(str(arg) for arg in job._args)
                                     if show_args else '',
                             'kwargs': ', '.join('%s=%s' % (key, val)
                                                 for key, val in job._kwargs.items())
                                     if show_args else '',
                             'start_time_ms': int(1000 * job.start_time), 'cluster': name}
                            for job in jobs]
                    cluster_jobs[name] = jobs
                self.send_response(200)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.end_headers()
                if node:
                    if node.avail_info:
                        node.avail_info = node.avail_info.__dict__
                    self.wfile.write(json.dumps({'node': node.__dict__,
                                                 'cluster_jobs': cluster_jobs}).encode())
                return

            elif client_request == 'cancel_jobs':
                uids = []
                for item in form.list:
                    if item.name == 'uid':
                        try:
                            uids.append(int(item.value))
                        except ValueError:
                            logger.debug('Cancel job uid "%s" is invalid', item.value)

                self._ctx._cluster_lock.acquire()
                cluster_jobs = [(cluster_info.cluster, cluster_info.jobs.get(uid, None))
                                for cluster_info in self._ctx._clusters.values()
                                for uid in uids]
                self._ctx._cluster_lock.release()
                cancelled = []
                for cluster, job in cluster_jobs:
                    if not job:
                        continue
                    if cluster.cancel(job) == 0:
                        cancelled.append(job._uid)
                self.send_response(200)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.end_headers()
                self.wfile.write(json.dumps(cancelled).encode())
                return

            elif client_request == 'add_node':
                node = {'host': '', 'port': None, 'cpus': 0, 'cluster': None}
                node_id = None
                cluster = None
                for item in form.list:
                    if item.name == 'host':
                        node['host'] = item.value
                    elif item.name == 'cluster':
                        node['cluster'] = item.value
                    elif item.name == 'port':
                        node['port'] = item.value
                    elif item.name == 'cpus':
                        try:
                            node['cpus'] = int(item.value)
                        except Exception:
                            pass
                    elif item.name == 'id':
                        node_id = item.value
                if node['host']:
                    self._ctx._cluster_lock.acquire()
                    clusters = [cluster_info.cluster for name, cluster_info in
                                self._ctx._clusters.items()
                                if name == node['cluster'] or not node['cluster']]
                    self._ctx._cluster_lock.release()
                    for cluster in clusters:
                        cluster.allocate_node(node)
                    self.send_response(200)
                    self.send_header('Content-Type', 'text/html')
                    self.end_headers()
                    node['id'] = node_id
                    self.wfile.write(json.dumps(node).encode())
                    return

            elif (client_request == 'close_node' or client_request == 'allocate_node' or
                  client_request == 'deallocate_node'):
                nodes = []
                cluster_infos = []
                resp = -1
                for item in form.list:
                    if item.name == 'cluster':
                        self._ctx._cluster_lock.acquire()
                        if item.value == '*':
                            cluster_infos = list(self._ctx._clusters.values())
                        else:
                            cluster_infos = [self._ctx._clusters.get(item.value, None)]
                            if not cluster_infos[0]:
                                cluster_infos = []
                        self._ctx._cluster_lock.release()
                    elif item.name == 'nodes':
                        nodes = json.loads(item.value)
                        nodes = [str(node) for node in nodes]

                if cluster_infos and nodes:
                    resp = 0
                    for cluster_info in cluster_infos:
                        fn = getattr(cluster_info.cluster, client_request)
                        if fn:
                            for node in nodes:
                                resp |= fn(node)
                        else:
                            resp = -1
                self.send_response(200)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.end_headers()
                self.wfile.write(json.dumps(resp).encode())
                return

            elif client_request == 'update':
                for item in form.list:
                    if item.name == 'timeout':
                        try:
                            timeout = int(item.value)
                            if timeout < 1:
                                timeout = 0
                            self._ctx._poll_sec = timeout
                        except Exception:
                            logger.warning('HTTP client %s: invalid timeout "%s" ignored',
                                           self.client_address[0], item.value)
                    elif item.name == 'show_job_args':
                        if item.value == 'true':
                            self._ctx._show_args = True
                        else:
                            self._ctx._show_args = False
                return

            elif client_request == 'set_cpus':
                node_cpus = {}
                for item in form.list:
                    self._ctx._cluster_lock.acquire()
                    for cluster_info in self._ctx._clusters.values():
                        node = cluster_info.status.get(item.name, None)
                        if node:
                            node_cpus[item.name] = cluster_info.cluster.set_node_cpus(
                                item.name, item.value)
                            if node_cpus[item.name] >= 0:
                                break
                    self._ctx._cluster_lock.release()

                self.send_response(200)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.end_headers()
                self.wfile.write(json.dumps(node_cpus).encode())
                return

            logger.debug('Bad POST request from %s: %s', self.client_address[0], client_request)
            self.send_error(400)
            return

    def __init__(self, cluster, host='', port=8181, poll_sec=10, DocumentRoot=None,
                 keyfile=None, certfile=None, show_job_args=True):
        self._cluster_lock = threading.Lock()
        self._clusters = {}
        if cluster:
            cluster_info = self.__class__._ClusterInfo(cluster)
            self._clusters[cluster.name] = cluster_info
            http_status = functools.partial(self.cluster_status, cluster_info)
            if cluster.cluster_status:
                client_status = cluster.cluster_status

                def chain_status(status, node, job):
                    http_status(status, node, job)
                    client_status(status, node, job)

                cluster.cluster_status = chain_status
            else:
                cluster.cluster_status = http_status
        if not DocumentRoot:
            DocumentRoot = os.path.join(os.path.dirname(__file__), 'data')
        if poll_sec < 1:
            logger.warning('invalid poll_sec value %s; it must be at least 1', poll_sec)
            poll_sec = 1
        self._poll_sec = poll_sec
        self._show_args = bool(show_job_args)
        self._server = HTTPServer((host, port), lambda *args:
                                  self.__class__._HTTPRequestHandler(self, DocumentRoot, *args))
        if certfile:
            self._server.socket = ssl.wrap_socket(self._server.socket, keyfile=keyfile,
                                                  certfile=certfile, server_side=True)
        self._httpd_thread = threading.Thread(target=self._server.serve_forever)
        self._httpd_thread.daemon = True
        self._httpd_thread.start()
        logger.info('Started HTTP%s server at %s',
                    's' if certfile else '', str(self._server.socket.getsockname()))

    def cluster_status(self, cluster_info, status, node, job):
        """This method is called by JobCluster/SharedJobCluster
        whenever there is a change in cluster as it is set to
        cluster's 'status' parameter (unless it is already set to
        another method, in which case, this method should be called
        through chaining).
        """
        if status == DispyJob.Created:
            return
        if status == DispyJob.Running:
            self._cluster_lock.acquire()
            cluster_info.jobs_submitted += 1
            cluster_info.jobs[job._uid] = job
            self._cluster_lock.release()
        elif (status == DispyJob.Finished or status == DispyJob.Terminated or
              status == DispyJob.Cancelled or status == DispyJob.Abandoned):
            self._cluster_lock.acquire()
            cluster_info.jobs_done += 1
            cluster_info.jobs.pop(job._uid, None)
            self._cluster_lock.release()

        if node:
            # even if node closed, keep it; let UI decide how to indicate status
            self._cluster_lock.acquire()
            cluster_info.status[node.ip_addr] = node
            cluster_info.updates[node.ip_addr] = node
            self._cluster_lock.release()

    def shutdown(self, wait=True):
        """This method should be called by user program to close the
        http server.
        """
        if wait:
            logger.info(
                'HTTP server waiting for %s seconds for client updates before quitting' %
                self._poll_sec)
            time.sleep(self._poll_sec)
        self._server.shutdown()
        self._server.server_close()

    def add_cluster(self, cluster):
        """If more than one cluster is used in a program, they can be
        added to http server for monitoring.
        """
        if cluster.name in self._clusters:
            logger.warning('Cluster "%s" is already registered', cluster.name)
            return
        cluster_info = self.__class__._ClusterInfo(cluster)
        self._clusters[cluster.name] = cluster_info
        if cluster.cluster_status is None:
            cluster.cluster_status = functools.partial(self.cluster_status, cluster_info)

    def del_cluster(self, cluster):
        """When a cluster is no longer needed to be monitored with
        http server, the cluster can be removed from http server with
        this method.
        """
        self._clusters.pop(cluster.name)
