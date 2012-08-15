#!/usr/bin/env python

"""
dispynetrelay: Relay ping messages from client(s) to nodes
in a network; see accompanying 'dispy' for more details.
"""

__author__ = "Giridhar Pemmasani (pgiri@yahoo.com)"
__email__ = "pgiri@yahoo.com"
__copyright__ = "Copyright 2011, Giridhar Pemmasani"
__contributors__ = []
__maintainer__ = "Giridhar Pemmasani (pgiri@yahoo.com)"
__license__ = "MIT"
__url__ = "http://dispy.sourceforge.net"
__status__ = "Production"

import os
import sys
import socket
import struct
import time
import select
import logging
import cPickle as pickle

from dispy import _node_ipaddr, _dispy_version
from asyncoro import serialize, unserialize

logger = logging.getLogger('dispynetrelay')
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(message)s'))
logger.addHandler(handler)
del handler

__version__ = _dispy_version
__all__ = []

class DispyNetRelay(object):
    """Internal use only.
    """
    def __init__(self):
        pass

    def relay_pings(self, ip_addr='', netmask=None, node_port=51348,
                    scheduler_node=None, scheduler_port=51347):
        netaddr = None
        if not netmask:
            try:
                ip_addr, bits = ip_addr.split('/')
                socket.inet_aton(ip_addr)
                netmask = (0xffffffff << (32 - int(bits))) & 0xffffffff
                netaddr = (struct.unpack('>L', socket.inet_aton(ip_addr))[0]) & netmask
            except:
                netmask = '255.255.255.255'
        if ip_addr:
            socket.inet_aton(ip_addr)
        else:
            ip_addr = socket.gethostbyname(socket.gethostname())
        if not netaddr and netmask:
            try:
                if isinstance(netmask, str):
                    netmask = struct.unpack('>L', socket.inet_aton(netmask))[0]
                else:
                    assert isinstance(netmask, int)
                assert netmask > 0
                netaddr = (struct.unpack('>L', socket.inet_aton(ip_addr))[0]) & netmask
            except:
                logger.warning('Invalid netmask')

        try:
            socket.inet_ntoa(struct.pack('>L', netaddr))
            socket.inet_ntoa(struct.pack('>L', netmask))
        except:
            netaddr = netmask = None

        scheduler_version = _dispy_version

        bc_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        bc_sock.bind(('', 0))
        bc_sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

        scheduler_ip_addr = _node_ipaddr(scheduler_node)
        if scheduler_ip_addr and scheduler_port:
            relay_request = serialize({'scheduler_ip_addr':scheduler_ip_addr,
                                       'scheduler_port':scheduler_port,
                                       'version':scheduler_version})
            bc_sock.sendto('PING:%s' % relay_request, ('<broadcast>', node_port))

        ping_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        ping_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        ping_sock.bind(('', node_port))
        pong_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        pong_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        pong_sock.bind(('', scheduler_port))
        logger.info('Listening on %s:%s', ip_addr, node_port)
        last_ping = 0
        while True:
            ready = select.select([ping_sock, pong_sock], [], [])[0]
            for sock in ready:
                if sock == ping_sock:
                    msg, addr = ping_sock.recvfrom(1024)
                    if not msg.startswith('PING:'):
                        logger.debug('Ignoring message "%s" from %s',
                                     msg[:max(len(msg), 5)], addr[0])
                        continue
                    if netaddr and (struct.unpack('>L', socket.inet_aton(addr[0]))[0] & netmask) == netaddr:
                        logger.debug('Ignoring own ping (from %s)', addr[0])
                        continue
                    if (time.time() - last_ping) < 10:
                        logger.warning('Ignoring ping (from %s) for 10 more seconds', addr[0])
                        time.sleep(10)
                    last_ping = time.time()
                    logger.debug('Ping message from %s (%s)', addr[0], addr[1])
                    try:
                        data = unserialize(msg[len('PING:'):])
                        scheduler_ip_addr = data['scheduler_ip_addr']
                        scheduler_port = data['scheduler_port']
                        scheduler_version = data['version']
                        assert isinstance(scheduler_ip_addr, str)
                        assert isinstance(scheduler_port, int)
                    except:
                        logger.debug('Ignoring ping message from %s (%s)', addr[0], addr[1])
                        continue
                    relay_request = serialize({'scheduler_ip_addr':scheduler_ip_addr,
                                               'scheduler_port':scheduler_port,
                                               'version':scheduler_version})
                    bc_sock.sendto('PING:%s' % relay_request, ('<broadcast>', node_port))
                else:
                    assert sock == pong_sock
                    msg, addr = pong_sock.recvfrom(1024)
                    if not msg.startswith('PONG:'):
                        logger.debug('Ignoring pong message "%s" from %s',
                                     msg[:max(len(msg), 5)], addr[0])
                        continue
                    # if netaddr and (struct.unpack('>L', socket.inet_aton(addr[0]))[0] & netmask) == netaddr:
                    #     logger.debug('Ignoring own pong (from %s)', addr[0])
                    #     continue
                    if not (scheduler_ip_addr and scheduler_port):
                        logger.debug('Ignoring pong message from %s', str(addr))
                        continue
                    logger.debug('Pong message from %s (%s)', addr[0], addr[1])
                    try:
                        pong = unserialize(msg[len('PONG:'):])
                        assert isinstance(pong['host'], str)
                        assert isinstance(pong['port'], int)
                        assert isinstance(pong['cpus'], int)
                        relay_request = serialize({'scheduler_ip_addr':scheduler_ip_addr,
                                                   'scheduler_port':scheduler_port,
                                                   'version':scheduler_version})
                        relay_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                        relay_sock.sendto('PING:%s' % relay_request,
                                          (pong['host'], node_port))
                        relay_sock.close()
                    except:
                        # raise
                        logger.debug('Ignoring pong message from %s (%s)', addr[0], addr[1])

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--debug', action='store_true', dest='loglevel', default=False,
                        help='if given, debug messages are printed')
    parser.add_argument('-i', '--ip_addr', dest='ip_addr', default='',
                        help='IP address to use (may be needed in case of multiple interfaces)')
    parser.add_argument('--scheduler_node', dest='scheduler_node', default='',
                        help='name or IP address of scheduler to announce when starting')
    parser.add_argument('--scheduler_port', dest='scheduler_port', type=int, default=51347,
                        help='port number used by scheduler')
    parser.add_argument('--node_port', dest='node_port', type=int, default=51348,
                        help='port number used by nodes')
    parser.add_argument('--netmask', dest='netmask', default=None,
                        help='netmask of local network')
    config = vars(parser.parse_args(sys.argv[1:]))
    # print config

    if config['loglevel']:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
    del config['loglevel']

    node = DispyNetRelay()
    node.relay_pings(**config)
