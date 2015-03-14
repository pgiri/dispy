***********
 dispynode
***********

.. include:: common.rst

'dispynode.py' (or dispynode) program should be running on each of
the nodes. It executes jobs for dispy clients. Usually,
no options are needed; '-d' option may be useful to see log of jobs
being executed.

Below are various options to invoke dispynode program:

* *-d* enables debug messages that show trace of execution.

* *-c n* or *--cpus=n* sets the number of processing units to
  *n*. Without this option, dispynode will use all the processing
  units available on that node. If *n* is positive, it must be at
  least 1 and at most number of processing units on that node;
  dispynode will then use at most *n* processors. If *n* is negative,
  then that many processing units are not used by dispynode.

* *-i addr* or *--ip_addr=addr* directs dispynode
  to use given *addr* for communication, instead of the IP
  address associated with the host name.

* *--ext_ip_addr=addr* directs dispynode to announce *addr* in network
  communication so that the node can be used if it is behind NAT
  firewall/gateway that is configured to use *addr*. See
  :ref:`node_NAT` below.

* *-p n* or *--node_port=n* directs dispynode to use given port *n*
  instead of default port 51348.

* *-s secret* or *--secret=secret* directs dispynode to use 'secret' for
  hashing handshake communication with dispy scheduler.

* *--dest_path_prefix=path* directs dispynode to use *path* as prefix
  for storing files sent by dispy scheduler. If a cluster uses
  *dest_path* option (when creating cluster with JobCluster or
  SharedJobCluster), then *dest_path* is appened to *path*
  prefix. With this, files from different clusters can be
  automatically stored in different directories, to avoid
  conflicts. Unless *cleanup=False* option is used when creating a
  cluster, dispynode will remove all files and directories created
  after the cluster is terminated.

* *--scheduler_node=addr*: If the node is in the same network as the
  dispy scheduler or when no jobs are scheduled at the time dispynode
  is started, this option is not necessary. However, if jobs are
  already scheduled and scheduler and node are on different networks,
  the given *addr* is used for handshake with the
  scheduler.

* *--scheduler_port=n* directs dispynode to use
  port *n* to communicate with scheduler. Default value is
  51347. When using this option, make sure dispy scheduler is also
  directed to use same port.

* *--keyfile=path* is path to file containing private key for SSL
  communication, same as 'keyfile' parameter to `ssl.wrap_socket of
  Python ssl module
  <https://docs.python.org/2/library/ssl.html#socket-creation>`_. This
  key may be stored in 'certfile' itself, in which case this must be
  *None*.

* *--certfile=path* is path to file containing SSL certificate, same
  as 'certfile' parameter to `ssl.wrap_socket of Python ssl module
  <https://docs.python.org/2/library/ssl.html#socket-creation>`_.

* *--max_file_size n* specifies maximum size of any file transferred
  from/to clients. If size of a file transferred by exceeds *n*,
  the file will be truncated.

* *--zombie_interval=n* indicates dispynode to assume a scheduler is a
  zombie if there is no communication from it for *n*
  minutes. dispynode doesn't terminate jobs submitted by a zombie
  scheduler; instead, when all the jobs scheduled are completed, the
  node frees itself from that scheduler so other schedulers may use
  the node.

.. _node_NAT:

NAT/Firewall Forwarding
=======================

As explained in :doc:`dispy` documentation, *ext_ip_addr* can be used
in case dispynode is behding a NAT firewall/gateway and the NAT
forwards UDP and TCP ports 51348 to the IP address where dispynode is
running. Thus, assuming NAT firewall/gateway is at (public) IP address
a.b.c.d, dispynode is to run at (private) IP address 192.168.5.33 and
NAT forwards UDP and TCP ports 51348 to 192.168.5.33, dispynode can be
invoked as::

    dispynode.py -i 192.168.5.33 --ext_ip_addr a.b.c.d

If multiple dispynodes are needed behind a.b.c.d, then each must be
started with different 'port' argument and those ports must be
forwarded to nodes appropriately. For example, to continue the
example, if 192.168.5.34 is another node that can run dispynode, then
it can be started on it as::

    dispynode.py -i 192.168.5.34 -p 51350 --ext_ip_addr a.b.c.d

and configure NAT to forward UDP and TCP ports 51350 to
192.168.5.34. Then dispy client can use the nodes with::

    cluster = JobCluster(compute, nodes=[('a.b.c.d', 51347), ('a.b.c.d', 51350)])

