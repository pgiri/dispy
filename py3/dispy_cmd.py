#!/usr/bin/python3

"""
dispy: Distribute computations among CPUs/cores on a single machine or machines
in cluster(s), grid, cloud etc. for parallel execution.  See https://dispy.org for details.
"""

import os
import sys

import pycos
from dispy import logger, JobCluster, SharedJobCluster, _dispy_version

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


if __name__ == '__main__':
    import argparse

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

    logger.info('dispy version %s', _dispy_version)

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
