*******
 dispy
*******

.. include:: common.rst

dispy is a module that provides API for the client to create
cluster(s) that define which nodes can execute computations. These
clusters can then be used to schedule jobs (execution of computations
used to create cluster with different parameters). The jobs can later
be examined for result of execution, output, error messages and
exception trace (if the execution failed).

While dispy and other components have various options that cover
rather comprehensive use cases, making it seem complex, most of the
options have default values that likely work for common cases. For
example, starting :doc:`dispynode` program on each of the nodes on a
local network and using JobCluster with computation, and possibly
*depends* parameters, as done with example in :doc:`index`, may be
sufficient.

There are two ways to create clusters with dispy: :class:`JobCluster`
and :class:`SharedJobCluster`. If only one instance of client program
may be running at anytime, :class:`JobCluster` is simple to use; it
already contains a scheduler that will schedule jobs to nodes running
:doc:`dispynode`. If, however, multiple programs using dispy may be
running simultaneously, :class:`JobCluster` cannot be used - each of
the schedulers in each instance of dispy will assume the nodes are
controlled exclusively by each, causing conflicts. Instead,
:class:`SharedJobCluster` must be used. In this case,
:doc:`dispyscheduler` program must also be running on some computer
and :class:`SharedJobCluster` must set *scheduler_node* parameter with
the node running dispyscheduler (default is the host that calls
:class:`SharedJobCluster`).

.. _JobCluster:

JobCluster
==========

.. class:: JobCluster(computation, nodes=['\*'], depends=[], callback=None, ip_addr=None, ext_ip_addr=None, port=51347, node_port=51348, fault_recover=False, dest_path=None, loglevel=logging.WARNING, setup=None, cleanup=True, pulse_interval=None, ping_interval=None, reentrant=False, secret='', keyfile=None, certfile=None)

   Creates and returns a cluster for given computation.

   *computation* should be either a Python function or a string. If it
   is a string, it must be path to executable program. This
   computation is sent to nodes in the given cluster. When a job is
   submitted (to invoke computation with arguments), dispynode
   executes the computation with those arguments in isolation - the
   computation should not depend on global state, such as modules
   imported in the main program or global variables etc., except as
   described below for *setup* parameter (see also :doc:`examples`).

   *nodes* must be a list, each element of which must be either a
   string or a pair (tuple of two elements). If element is a string,
   it must be either IP address or host name. If element is a pair,
   first element of pair must be IP address or name and second element
   must be port number where that node is serving (needed if it is
   different from default 51348). This list serves two purposes: dispy
   initially sends a request to all the nodes listed to find out
   information about them (e.g., number of processing units available
   for dispy), then sends given computation to only those nodes that
   match the listed nodes (dispy may know about nodes not listed in
   this computation, as it also broadcasts identification request).
   Wildcard '\*' can be used to match (part of) any IP address; e.g.,
   '192.168.3.\*' matches any node whose IP address starts with
   '192.168.3'.  If there are any nodes beyond local network, then all
   such nodes should be mentioned in *nodes*. If there are many such
   nodes (on outside local network), it may be cumbersome to list them
   all (it is not possible to send identification request to outside
   networks with wildcard in them); in that case, :doc:`dispynetrelay`
   may be started on one of the nodes on that network and the node
   running dispynetrelay should be added to nodes list (and a wildcard
   for that network, so that other nodes on that network match that
   wildcard); see :doc:`examples`.

   *depends* is list of dependencies needed for *computation*. Each
   element of this list can be either Python function or Python class
   or an instance of class (object) or a Python module or path to
   file. Only Python modules that are not present on nodes already
   need be listed; standard modules that are present on all nodes do
   not need to be listed here.

   *callback* is a function. When a job's results become available,
   dispy will call provided callback function with that job as the
   argument. If a job sends provisional results with
   'dispy_provisional_result' multiple times, then dispy will call
   provided callback each such time. The (provisional) results of
   computation can be retrieved with 'result' field of job, etc. While
   computations are run on nodes in isolated environments, callbacks
   are run in the context of user programs from which
   (Shared)JobCluster is called - for example, callbacks can access
   global variables in programs that created cluster(s).

   *ip_addr* is IP address to use for (client) communication. If
   it is not set, all configured network interfaces are used. If it is
   a string, it must be either a host name or IP address, in which case
   corresponding interface address is used. If it is a list, each must
   be a string of host name or IP address, in which case interface
   addresses for each of those is used.

   *ext_ip_addr* is a string or list of strings of host names or IP
   addresses to use as return address for node to client
   communication. This may be needed in case the client is behind a NAT
   firewall/gateway and (some) nodes are outside. Typically, in such a
   case, ext_ip_addr must be the address of NAT firewall/gateway and
   the NAT firewall/gateway must forward ports to ip_addr
   appropriately. See below for more on NAT/firewall information.

   *port* is port to use for (client) communication. Usually not
   necessary. If not given, dispy will request socket library to choose
   any available port.

   *node_port* is port to use for communicating with nodes
   (servers). If this is different from default, 'dispynode' programs
   must be run with the same port.

   *dest_path* is directory on the nodes where files are transferred
   to. Default is to create a separate directory for each
   computation. If a computation transfers files (dependencies) and
   same computation is run again with same files, the transfer can be
   avoided by specifying same dest_path, along with the option
   'cleanup=False'.

   *fault_recover* must be either True or a string. If it is a string,
   dispy uses it as path to file where it stores information about
   running jobs (jobs scheduled but not finished yet). In case user
   program terminates unexpectedly (for example, because of network
   failure, uncaught exception), the results of submitted jobs can be
   later retrieved through 'fault_recover_jobs' function (see
   below). If this option is True, dispy will store information about
   jobs in a file of the form '_dispy_fault_recover_YYYYMMDDHHMMSS' in
   the current directory. Note that dispy keeps information about only
   the jobs that have been submitted for execution but not finished
   yet. Once a job is finished (i.e., job result is received by dispy's
   scheduler), dispy deletes its information (for time and space
   efficiency). If it is necessary to keep the information about
   finished jobs, callbacks can be used to persist job results.

   *loglevel* is message priority for `logging
   <http://docs.python.org/2/library/logging.html>`_ module.

   *setup*, if not None, must be a Python function that takes no
   arguments. This function is transferred, along with dependencies and
   computation, to each node and called after all dependencies have
   been copied to *dest_path* and working directory has also
   been set to *dest_path* (which would also be the working
   directory when computation jobs are executed).

   The setup function is called exactly once before any jobs are
   executed; the function should return 0 to indicate successful
   initialization. This function can be used, for example, to prepare
   data as needed to execute jobs (efficiently). Under Linux, OS X (and
   other Unix variants, but not Windows), child processes (that execute
   computation jobs) inherit parent's address space (among other
   things), so the setup function can even copy data in to global
   variables; these variables can be used (as read-only data) in
   jobs. If this feature is used, care must be taken not to share same
   node with other computations, as one computation's global variables
   may interfere with another computation's, and *cleanup*
   function may be provided, for example, to delete the global
   variables initialized.

   Unlike the job functions which are executed in separate processes,
   the setup and cleanup functions are executed within the dispynode
   process itself so global variables created in setup function are
   seen in job functions (see :doc:`examples`). Consequently, when
   setup/cleanup functions are used, a node should not be shared by
   more than one computaiton; otherwise, one computation's jobs may
   have access to another computation's jobs. Moreover, cleanup
   function may not be able to undo all the steps done in setup
   function (e.g., unloading modules imported in setup may leak
   memory). So it may be necessary in some cases to restart dispynode
   when setup/cleanup functions are used.

   *cleanup*: If True (default value), any files (dependencies)
   transferred will be deleted after the computation is done. If it is
   False, the files are left on the nodes; this may speedup if same
   files are needed for another cluster later. However, this can be
   security risk and/or require manual cleanup. If same files are used
   for multiple clusters, then cleanup may be set to False and
   same *dest_path* used.

   If *cleanup* is neither True nor False, it must be a Python
   function that takes no arguments. This function is transferred to
   each of the nodes and executed on the nodes before deleting files as
   mentiond above. If *setup* function creates global variables,
   for example, then *cleanup* function may delete those
   variables.

   *pulse_interval* is number of seconds between 'pulse' messages that
   nodes send to indicate they are alive and computing submitted
   jobs. If this value is given as an integer or floating number
   between 1 and 600, then a node is presumed dead if 5*pulse_interval
   seconds elapse without a pulse message. See 'reentrant' below.

   *reentrant* must be either True or False. This value is used
   only if 'pulse_interval' is set for any of the clusters. If
   pulse_interval is given and reentrant is False (default), jobs
   scheduled for a dead node are automatically cancelled (for such jobs,
   execution result, output and error fields are set to None, exception
   field is set to 'Cancelled' and status is set to Cancelled); if
   reentrant is True, then jobs scheduled for a dead node are resubmitted
   to other available nodes.

   *ping_interval* is number of seconds.  Normally dispy can locate
   nodes running dispynode by broadcasting UDP ping messages on local
   network and point-to-point UDP messages to nodes on remote
   networks. However, UDP messages may get lost.  Ping interval is
   number of seconds between repeated ping messages to find any nodes
   that have missed previous ping messages.

   *secret* is a string that is (hashed and) used for handshaking of
   communication with nodes. This prevents unauthorized use of
   nodes. However, the hashed string (not the secret itself) is passed
   in clear text, so an unauthorized, determined person may be able to
   figure out how to circumvent. This feature can be used, for example,
   to easily create a private cluster with small number of machines in
   a large network.

   *keyfile* is path to file containing private key for SSL
   communication, same as 'keyfile' parameter to `ssl.wrap_socket of
   Python ssl module
   <https://docs.python.org/2/library/ssl.html#socket-creation>`_. This
   key may be stored in 'certfile' itself, in which case this should
   be *None*.

   *certfile* is path to file containing SSL certificate, same as
   'certfile' parameter to `ssl.wrap_socket of Python ssl module
   <https://docs.python.org/2/library/ssl.html#socket-creation>`_.

.. _SharedJobCluster:

SharedJobCluster
================

  SharedJobCluster has almost the same syntax, except as noted below.

.. class:: SharedJobCluster(computation, nodes=['\*'], depends=[], ip_addr=None, port=None, scheduler_node=None, scheduler_port=None, ext_ip_addr=None dest_path=None, loglevel=logging.WARNING, cleanup=True, reentrant=False, secret='', keyfile=None, certfile=None)

  where all arguments common to :class:`JobCluster` are same, and

  *port* is client's port (which is 51347 in the case of
  :class:`JobCluster`) and by default choosen randomly to an unused
  port. This is so that in case dispyscheduler is running on the same
  computer, the client port would be different from the port used by
  dispyscheduler. In case the client needs to get results from
  dispyscheduler in remote network and port forwarding is used, a
  specific port (e.g., 51347) can be given.

  *scheduler_node* is either IP address or host name where
  :doc:`dispyscheduler` is running; if it is not given, the node where
  :class:`SharedJobCluster` is invoked is used

  *pulse_interval* is not used in case of :class:`SharedJobCluster`;
  instead, :doc:`dispyscheduler` program must be started with
  '--pulse_interval' option appropriately.

  *secret* is a string that is (hashed and) used for handshaking of
  communication with dispyscheduler.

.. _Cluster:

Cluster
=======

A cluster created by either :class:`JobCluster` or
:class:`SharedJobCluster` has following methods:

.. method:: cluster.submit(\*args, \*\*kwargs)

    Creates a :ref:`DispyJob` object (see below for useful fields of
    this object), schedules it for execution on an eligible node
    (whenever one becomes available) and returns the job. Results from
    execution of computation with given arguments will be available in
    the job object after execution finishes.

    This method should be called with the arguments exactly as
    expected by the *computation* given to :class:`JobCluster` or
    :class:`SharedJobCluster`. If *computation* is a Python function,
    the arguments may also contain keyword arguments. All arguments
    must be serializable (picklable), as these are sent over the
    network to the nodes. If an argument is a class object that
    contains non-serializable members, then the classes may provide
    **__getstate__** method for this purpose (see '_Job' class in
    dispy.py for an example). If *computation* is a standalone
    program, then all arguments must be strings.

.. method:: cluster()
            cluster.wait()

    Waits for all submitted jobs to complete.

.. method:: cluster.close()

    Waits for all submitted jobs to complete and then closes the
    computation (such as removing any transferred files at the nodes,
    deleting *computation* itself from the nodes).

.. method:: cluster.cancel(job)

    Terminates given job (returned from previous
    :meth:`cluster.submit`) removes it from the scheduler, terminating
    (killing) it if it has started execution at a node. Note that if
    the job execution has any side effects (such as updating database,
    files etc.), cancelling a job may leave unpredictable side
    effects, depending on at what point job is terminated.

.. method:: cluster.stats()

    Prints statistics about nodes, time each node spent executing jobs
    etc.

.. _DispyJob:

DispyJob
========

The result of :meth:`cluster.submit` call of a cluster is an instance
of DispyJob (see dispy.py), which can be used to examine status of job
execution, retrieve job results etc. The job instance has *id* field
that can be used to set any value appropriate (rest of the fields are
either read-only, or not meant for user programs). For example, *id*
field can be set to a unique value to distinguish one job from
another.

Job's *status* field is read-only field; its value is one of
*Created*, *Running*, *Finished*, *Cancelled* or *Terminated*, all
class properties, indicating current status of job.  If job is created
for :class:`SharedJobCluster`, *status* is not updated to *Running*
when job is actually running.

When a submitted job is called with **job()**, it returns that job's
execution result, waiting until the job is finished if it has not
finished yet. After a job is finished, its following attributes can be
accessed:

* *result* will have computation's result - return value if
  computation is a function and exit status if it is a
  program. **job.result** is same as return value of **job()**

* *stdout* and *stderr* will have stdout and stderr strings of the
  execution of computation at server node

* *exception* will have exception trace if computation raises any
  exception; in this case **job.result** will be *None*

* *start_time* will be the time when job was scheduled for execution
  on a node

* *end_time* will be time when results became available

Job's result, stdout and stderr should not be large - these are
buffered and hence will consume memory (not stored on disk). Moreover,
like *args* and *kwargs*, result should be serializable (picklable
object). If result is (or has) an instance of Python class, that class
may have to provide **__getstate__** function to serialize the object.

After jobs are submitted, :meth:`cluster.wait` can be used to wait
until all submitted jobs for that cluster have finished. If necessary,
results of execution can be retrieved by either **job()** or
**job.result**, as described above.

Fault Recovery
==============

As mentioned above, if *fault_recover* option is used when creating a
cluster, dispy stores information about scheduled but unfinished jobs
in a file. If user program then terminates unexpectedly, the nodes
that execute those jobs can't send the results back to dispy. In such
cases, the results for the jobs can be retrieved from the nodes with
the function in dispy.

.. method:: fault_recover_jobs(fault_recover_file, ip_addr=None, secret='',
	        node_port=51348, certfile=None, keyfile=None)


    *fault_recover_file* is path to the file used or created
    when JobCluster is used.

    *ip_addr* is IP address to use for (client)
    communication. This may be needed in case the client has multiple
    interfaces and default interface is not the right choice (this
    would be same as the 'ip_addr' option used for JobCluster).

    *secret* is a string that is (hashed and) used for
    handshaking of communication with nodes (should same as the one used
    when creating JobCluster).

    *node_port* is port to use for communicating with nodes
    (servers). If this should be different from default, 'dispynode'
    programs must be run with the same port. This option should be same
    as the one used when creating JobCluster.

    *certfile* is path to file containing SSL certificate (see Python
    'ssl' module) (same as the one used when creating JobCluster).

    *keyfile* is path to file containing private key for SSL
    communication (see Python 'ssl' module). This key may be stored in
    *certfile* itself, in which case *keyfile* should be *None*. This
    option should be same as the one used when creating
    :class:`JobCluster`.

This function reads the information about jobs in the
fault_recover_file, retrieves :ref:`DispyJob` instance (that contains
results, stdout, stderr, status etc.) for each job that was scheduled
for execution but unfinished at the time of crash, and returns them as
a list. If a job has finished executing at the time
'fault_recover_jobs' function is called, the information about that is
deleted from both the node and fault_recover_file, so the results for
finished jobs can't be retrieved more than once. However, if a job is
still executing, the status field of :ref:`DispyJob` would be
**DispyJob.Running** and the results for this job can be retrieved
again (until that job finishes) by calling 'fault_recover_jobs'. Note
that *fault_recover_jobs* is available as separate function - it
doesn't need :class:`JobCluster` or :class:`SharedJobCluster`
instance. In fact, *fault_recover_jobs* function must not be used when
a cluster that uses same recover file is currently running.

Note that dispy sends only the given computation and its dependencies
to the nodes; the program itself is not transferred. So if computation
is a python function, it must import all the modules used by it, even
if the program imported those modules before cluster is created.

.. _ProvisionalResult:

Provisional/Intermediate Results
================================

.. method:: dispy_provisional_result(result)

    Computations (if they are Python functions) can use this function
    to send provisional or intermediate results to the client. For
    example, in optimization computations, there may be many (sub)
    optimal results that the computations can send to the client. The
    client may ignore, cancel computations or create additional
    computations based on these results. When computation calls
    **dispy_provisional_result(result)**, the Python object *result*
    (which must be serializable) is sent back to the client and
    computation continues to execute. The client should use *callback*
    option to process the information, as shown in the example::

	import random, dispy

	def compute(n, threshold): # executed on nodes
	    import random, time, socket
	    name = socket.gethostname()
	    for i in xrange(0, n):
		r = random.uniform(0, 1)
		if r <= threshold:
		    # possible result
		    dispy_provisional_result((name, r))
		time.sleep(0.1)
	    # final result
	    return None

	def job_callback(job): # executed at the client
	    if job.status == dispy.DispyJob.ProvisionalResult:
		if job.result[1] < 0.005:
		    # acceptable result; terminate jobs
		    print('%s computed: %s' % (job.result[0], job.result[1]))
		    # 'jobs' and 'cluster' are created in '__main__' below
		    for j in jobs:
			if j.status in [dispy.DispyJob.Created, dispy.DispyJob.Running,
					dispy.DispyJob.ProvisionalResult]:
			    cluster.cancel(j)

	if __name__ == '__main__':
	    cluster = dispy.JobCluster(compute, callback=job_callback)
	    jobs = []
	    for n in range(4):
		job = cluster.submit(random.randint(50,100), 0.2)
		if job is None:
		    print('creating job %s failed!' % n)
		    continue
		job.id = n
		jobs.append(job)
	    cluster.wait()
	    cluster.stats()
	    cluster.close()

  In the above example, computations send provisional result if
  computed number is &lt;= threshold (0.2). If the number computed is
  less than 0.005, job_callback deems it acceptable and terminates
  computations.

.. _SendFile:

Transferring Files
==================

.. method:: dispy_send_file(path)

    Computations (if they are Python functions) can use this function
    to send files (on server nodes) to the client. This is useful if
    the nodes and client don't use same file system and computations
    generate files.

    As explained in :ref:`DispyJob`, computation results (return value
    in the case of Python functions and exit status in the case of
    programs), along with any output, errors and exception trace are
    transferred to the client after a job is finished executing. As
    these are stored in memory (and sent back as serializable
    objects), they should not be too big in size. If computations
    generate large amount of data, the data can be saved in file(s)
    (on the nodes) and then transfer to the client with
    **dispy_send_file(path)**. The files are saved on the client with
    the same path under the directory where the client is running.

    For example, a computation that saves data in a file named
    "file.dat" and transferrs to the client is::

	def compute(n):
	    # ... generate "file.dat"
	    dispy_send_file("file.dat")
	    # ... computation may continue, possibly send more files
	    return result

    **dispy_send_file** returns 0 if the file is transferred
    successfully. The maximum size of the file transferred is as per
    *max_file_size* option to :doc:`dispynode`; its default value is
    10MB.

    All the files sent by the computations are saved under the
    directory where client program is running. If more than one file
    is sent (generated on multiple nodes, say), care must be taken to
    use different paths/names to prevent overwriting files.

    Note that *depends* parameter to :ref:`JobCluster` and
    :ref:`SharedJobCluster` can be used to transfer files (and other
    types of dependencies) from client to nodes.

.. _NAT:

NAT/Firewall Forwarding
=======================

By default dispy client uses UDP and TCP ports 51347, dispynode uses
UDP and TCP ports 51348, and dispyscheduler uses UDP and TCP pots
51347 and TCP port 51348. If client/node/scheduler are behind a NAT
firewall/gateway, then these ports must be forwarded appropriately and
*ext_ip_addr* option must be used. For example, if dispy client is
behdind NAT firewall/gateway, :class:`JobCluster` /
:class:`SharedJobCluster` must set *ext_ip_addr* to the NAT
firewall/gateway address and forward UDP and TCP ports 51347 to the IP
address where client is running. Similarly, if dispynode is behind NAT
firewall/gateway, *ext_ip_addr* option must be used.

.. _Cloud:

Cloud Computing with Amazon EC2
===============================

*ext_ip_addr* of :class:`JobCluster` and :doc:`dispynode` can be used
to work with Amazon EC2 cloud computing service. With EC2 service, a
node has a private IP address (called 'Private DNS Address') that uses
private network of the form 10.x.x.x and public address (called
'Public DNS Address') that is of the form
ec2-x-x-x-x.x.amazonaws.com. After launching instance(s), one can copy
dispy files to the node(s) and run dispynode as::

  dispynode.py --ext_ip_addr ec2-x-x-x-x.y.amazonaws.com

(this address can't be used with '-i'/'--ip_addr' option, as the
network interface is configured with private IP address only). This
node can then be used by dispy client from outside EC2 network by
specifying ec2-x-x-x-x.x.amazonaws.com in the 'nodes' list (thus,
using EC2 servers to augment processing units). Roughly, dispy uses
'ext_ip_addr' similar to NAT - it announces 'ext_ip_addr' to other
services instead of the configured 'ip_addr' so that external services
send requests to 'ext_ip_addr' and if firewall/gateway forwards them
appropriately, dispy will process them.

