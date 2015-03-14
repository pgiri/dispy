****************
 dispyscheduler
****************

.. include:: common.rst

As mentioned in :doc:`dispy` and :doc:`index`, if jobs are submitted
with :class:`JobCluster` instead of :class:`SharedJobCluster`, then
dispyscheduler is not needed; :class:`JobCluster` includes the
scheduler. If there is a need to use same node by more than one
cluster at the same time (e.g., multiple client programs need to run
simultaneously), then :class:`SharedJobCluster` must be used, along
with running 'dispyscheduler' on a node. Usually, no options are
needed when invoking this program.

Below are various options to invoking dispyscheduler:

* *-d* enables debug messages that show trace of execution.

* *-n node1 -n node2* or *--nodes node1 --nodes
  node2* etc. sends handshake messages when dispyscheduler
  starts. This is not needed if scheduler and nodes are on same
  network.

* *-i addr* or *--ip_addr=addr* directs dispyscheduler to use given
  *addr* for communication, instead of the IP address associated with
  the host name.

* *--ext_ip_addr=addr* directs dispyscheduler to announce *addr* in
  network communication so that the scheduler can be used if it is
  behind NAT firewall/gateway that is configured to use *addr*. See
  below.

* *-p n* or *--port=n* directs dispyscheduler to
  use given port *n* instead of default port 51347 for UDP
  and TCP communication for job results.

* *--scheduler_port=n* directs dispyscheduler to use given port *n*
  instead of default port 51349 for job scheduler.

* *--node_port=n* directs dispyscheduler to use given port *n* instead
  of default port 51348 where dispynodes must be running.

* *--node_secret secret* directs dispyscheduler to use 'secret' for
  hashing handshake communication with nodes.

* *--cluster_secret secret* directs dispyscheduler to use
  'secret' for hashing handshake communication with clusters.

* *--node_keyfile* is file containing private key for SSL
  communication with nodes, same as 'keyfile' parameter to
  `ssl.wrap_socket of Python ssl module
  <https://docs.python.org/2/library/ssl.html#socket-creation>`_. This
  key may be stored in 'node_certfile' itself, in which case this must
  be *None*.

* *--node_certfile* is file containing SSL certificate with nodes,
   same as 'certfile' parameter to `ssl.wrap_socket of Python ssl
   module
   <https://docs.python.org/2/library/ssl.html#socket-creation>`_.

* *--cluster_keyfile* is file containing private key for SSL
  communication with clusters, same as 'keyfile' parameter to
  `ssl.wrap_socket of Python ssl module
  <https://docs.python.org/2/library/ssl.html#socket-creation>`_. This
  key may be stored in *cluster_certfile* itself, in which case this
  must be *None*.

* *--cluster_certfile* is file containing SSL certificate with
  clusters, same as 'certfile' parameter to `ssl.wrap_socket of Python
  ssl module
  <https://docs.python.org/2/library/ssl.html#socket-creation>`_.

* *--pulse_interval n* directs nodes it controls to send pulse
  messages every *n* seconds; if a pulse message is not received
  within 5*n, then a node is presumed dead. In that case, if a cluster
  set 'reentrant=True', then jobs scheduled on that node will be
  migrated to other node(s) if possible; if *reentrant=False*, then
  jobs are automatically cancelled. *n* must be between 1 and 600.

* *--ping_interval* is number of seconds.  Normally dispyscheduler can
  locate nodes running dispynode by broadcasting UDP ping messages on
  local network and point-to-point UDP messages to nodes on remote
  networks. However, UDP messages may get lost.  Ping interval is
  number of seconds between repeated ping messages to find any nodes
  that have missed previous ping messages.


.. _scheduler_NAT:

NAT/Firewall Forwarding
=======================

As explained in :doc:`dispy` and :doc:`dispynode` documentation,
*ext_ip_addr* can be used to use services behind NAT
firewall/gateway. This option can be used with dispyscheduler,
too. This is especially useful if there are many nodes in a network
behind NAT firewall/gateway (otherwise, as explained in dispynode
documentation, each dispynode should be started with a different port
and all those ports forwarded appropriately). Assuming that
dispyscheduler is to run on a node with (private) IP address
192.168.20.55 and it is behind NAT firewall/gateway at (public) IP
address a.b.c.d, dispyscheduler can be invoked as::

    dispynode.py -i 192.168.20.55 --ext_ip_addr a.b.c.d

and setup NAT to forward UDP and TCP ports 51347 and TCP port 51349 to
192.168.20.55.  Then dispy clients can use nodes in this network with::

    cluster = SharedJobCluster(compute, nodes=['*'], scheduler_node='a.b.c.d')


