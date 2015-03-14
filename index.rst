.. dispy documentation master file, created by
   sphinx-quickstart on Fri Nov 14 21:32:45 2014.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

.. include:: common.rst

################################################################
 dispy: Python framework for distributed and parallel computing
################################################################

dispy is a `Python <http://www.python.org>`_ framework for parallel
execution of computations by distributing them across multiple
processors in a single machine (SMP), among many machines in a
cluster, grid or cloud.  dispy is well suited for data parallel (SIMD)
paradigm where a computation (Python function or standalone program)
is evaluated with different (large) datasets independently with no
communication among computation tasks (except for computation tasks
sending intermediate results to the client). If
communication/cooperation among tasks is needed, `asyncoro
<http://asyncoro.sourceforge.net>`_ framework could be used.

Salient features of dispy are:

* dispy is implemented with `asyncoro
  <http://asyncoro.sourceforge.net>`_, an independent framework for
  asynchronous, concurrent, distributed, network programming with
  coroutines (without threads). asyncoro uses non-blocking sockets
  with I/O notification mechanisms epoll, kqueue and poll, and Windows
  I/O Completion Ports (IOCP) for high performance and scalability, so
  dispy works efficiently with a single node or large cluster(s) of
  nodes. asyncoro itself has support for distributed/parallel
  computing, including transferring computations, files etc., and
  message passing (for communicating with client and other computation
  tasks), although it doesn't include job scheduling.

* Computations (Python functions or standalone programs) and their
  dependencies (files, Python functions, classes, modules) are
  distributed to nodes automatically. Computations, if they are Python
  functions, can also transfer files on the nodes to the client.

* Computation nodes can be anywhere on the network (local or
  remote). For security, either simple hash based authentication or
  SSL encryption can be used.

* After each execution is finished, the results of execution, output,
  errors and exception trace are made available for further
  processing.

* Nodes may become available dynamically: dispy will schedule jobs
  whenever a node is available and computations can use that node.

* If callback function is provided, dispy executes that function
  when a job is finished; this can be used for processing job
  results as they become available.

* Client-side and server-side fault recovery are supported:

  If user program (client) terminates unexpectedly (e.g., due to
  uncaught exception), the nodes continue to execute scheduled
  jobs. If client-side fault recover option is used when creating a
  cluster, the results of the scheduled (but unfinished at the time of
  crash) jobs for that cluster can be retrieved later.

  If a computation is marked reentrant (with *reentrant=True* option)
  when a cluster is created and a node (server) executing jobs for
  that computation fails, dispy automatically resubmits those jobs to
  other available nodes.

* dispy can be used in a single process to use all the nodes
  exclusively (with :class:`JobCluster`) or in multiple processes
  simultaneously sharing the nodes (with :class:`SharedJobCluster` and
  :doc:`dispyscheduler` program).

* Amazon cloud computing platform EC2 can be used as compute nodes,
  either exclusively or in addition to any local compute nodes;
  see :ref:`Cloud` for details.

dispy works with Python versions 2.7+ and 3.1+ and tested on Linux, OS
X and Windows; it may work on other platforms too. dispy works with
`PyPy <http://pypy.org>`_ as well.

Download/Installation
=====================

dispy is availble through `Python Package Index (PyPI)
<https://pypi.python.org/pypi/dispy>`_ so it can be easily installed
for Python 2.7+ with::

   pip install dispy

and/or for Python 3.1+ with::

   pip3 install dispy

dispy can also be downloaded from `Sourceforge Files
<http://sourceforge.net/projects/dispy/files>`_.  Files in 'py2'
directory in the downloaded package are to be used with Python 2.7+
and files in 'py3' directory are to be used with Python 3.1+.

Quick Guide
===========

Below is a quick guide on how to use dispy. More details are available
in :doc:`dispy`.

dispy consists of 4 components:

* :doc:`dispy` (client) provides two ways of creating "clusters":
  :meth:`JobCluster` when only one instance of dispy may run and
  :meth:`SharedJobCluster` when multiple instances may run (in separate
  processes). If :meth:`JobCluster` is used, the job scheduler is
  included in it will distribute jobs on the server nodes; if
  :meth:`SharedJobCluster` is used, a separate scheduler
  (:doc:`dispyscheduler`) must be running.

* :doc:`dispynode` executes jobs on behalf of dispy. dispynode must be
  running on each of the (server) nodes that form the cluster.

* :doc:`dispyscheduler` is needed only when :meth:`SharedJobCluster`
  is used; this provides a scheduler that can be shared by multiple
  dispy clients simultaneously.

* :doc:`dispynetrelay` is needed when nodes are located across
  different networks. If all nodes are on local network or if all
  remote nodes can be listed in 'nodes' parameter when creating
  cluster, there is no need for dispynetrelay - the scheduler can
  discover such nodes automatically. However, if there are many nodes
  on remote network(s), dispynetrelay can be used to relay information
  about the nodes on that network to scheduler, without having to list
  all nodes in 'nodes' parameter.

As a tutorial, consider the following program, in which function
*compute* is distributed to nodes on a local network for parallel
execution. First, run dispynode program ('dispynode.py') on each of
the nodes on the network. Now run the program below, which creates a
cluster with function *compute*; this cluster is then used to create
jobs to execute *compute* with a random number 10 times.::

    # 'compute' is distributed to each node running 'dispynode';
    # runs on each processor in each of the nodes
    def compute(n):
	import time, socket
	time.sleep(n)
	host = socket.gethostname()
	return (host, n)

    if __name__ == '__main__':
	import dispy, random
	cluster = dispy.JobCluster(compute)
	jobs = []
	for n in range(10):
	    # run 'compute' with a random number between 5 and 20
	    job = cluster.submit(random.randint(5,20))
	    job.id = n
	    jobs.append(job)
	# cluster.wait() # wait for all scheduled jobs to finish
	for job in jobs:
	    host, n = job() # waits for job to finish and returns results
	    print('%s executed job %s at %s with %s' % (host, job.id, job.start_time, n))
	    # other fields of 'job' that may be useful:
	    # print(job.stdout, job.stderr, job.exception, job.ip_addr, job.start_time, job.end_time)
	cluster.stats()

dispy schedules the jobs on the processors in the nodes running
dispynode. The nodes execute each job with the job's arguments in
isolation - computations shouldn't depend on global state, such as
modules imported outside of computations, global variables
etc. (except if 'setup' parameter is used, as explained in
:doc:`dispy` and :doc:`examples`). In this case, *compute* needs
modules *time* and *socket*, so it must import them. The program then
gets results of execution for each job with **job()**.

dispy can also be used as a command line tool; in this case the
computations should only be programs and dependencies should only be
files.::

    dispy.py -f /some/file1 -f file2 -a "arg11 arg12" -a "arg21 arg22" -a "arg3" /some/program

will distribute '/some/program' with dependencies '/some/file1' and
'file2' and then execute '/some/program' in parallel with arg11 and
arg12 (two arguments to the program), arg21 and arg22 (two arguments),
and arg3 (one argument).

Contents
========

.. toctree::
   :maxdepth: 2
   :numbered:

   dispy
   dispynode
   dispyscheduler
   dispynetrelay
   examples
   
Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

