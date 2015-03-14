***************
 dispynetrelay
***************

.. include:: common.rst

dispynetrelay relays information about nodes on its network to dispy
scheduler(s).


If dispy scheduler and nodes are on same network, dispynetrelay is not
needed. If they are not, then there are two choices to use nodes on
a different network:

* When cluster is created with dispy, 'nodes' option must
  specify all the nodes (either IP addresses or host names)
  explicitly. This can be cumbersome if there are many nodes on
  different network(s).

* dispynetrelay is running on one node per network. In this
  case, dispynetrelay recognizes nodes on its network and relays
  that information to dispy scheduler. In this case, 'nodes' option
  to dispy client need to specify only the node(s) running
  dispynetrelay - all the nodes in that network can then be used by
  dispy. Note that 'nodes' option is also used to filter matching
  nodes, so '*' may be added to the 'nodes' option to use all
  nodes.

Below are various options to invoking dispynetrelay:

* *--ip_addr addr* directs dispynetrelay to use given *addr* for
  communication, instead of the IP address associated with the host
  name.

* *--scheduler_node addr* sets dispynetrelay to direct nodes on its
  network to handshake with scheduler running at *addr*. If this
  option is not used, the IP address where dispynetrelay is running
  should be added to the 'nodes' argument when creating cluster (with
  JobCluster or SharedJobCluster).

* *--scheduler_port n* directs dispynetrelay to communicate with
  scheduler with given port *n* instead of default port 51349.

* *--node_port n* directs dispynetrelay to communicate with nodes in
  its network with port *n* instead of default port
  51348.

* *-d* enables debug messages that show trace of
  execution. This may not be very useful to end users.

