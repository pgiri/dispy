dispy
######

    .. note:: Full documentation for dispy is now available at `dispy.org
              <https://dispy.org>`_.

`dispy <https://dispy.org>`_ is a comprehensive, yet easy
to use framework for creating and using compute clusters to execute computations
in parallel across multiple processors in a single machine (SMP), among many
machines in a cluster, grid or cloud.  dispy is well suited for data parallel
(SIMD) paradigm where a computation is evaluated with different (large) datasets
independently with no communication among computation tasks (except for
computation tasks sending intermediate results to the client).

dispy works with Python versions 2.7+ and 3.1+ on Linux, Mac OS X and Windows; it may
work on other platforms (e.g., FreeBSD and other BSD variants) too.

Features
--------

* dispy is implemented with `pycos <https://pycos.org>`_,
  an independent framework for asynchronous, concurrent, distributed, network
  programming with tasks (without threads). pycos uses non-blocking sockets with
  I/O notification mechanisms epoll, kqueue and poll, and Windows I/O Completion
  Ports (IOCP) for high performance and scalability, so dispy works efficiently
  with a single node or large cluster(s) of nodes. pycos itself has support for
  distributed/parallel computing, including transferring computations, files
  etc., and message passing (for communicating with client and other computation
  tasks).  While dispy can be used to schedule jobs of a computation to get the
  results, pycos can be used to create `distributed communicating processes
  <https://pycos.org/dispycos.html>`_, for broad range of use cases.

* Computations (Python functions or standalone programs) and their
  dependencies (files, Python functions, classes, modules) are
  distributed automatically.

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

  If a computation is marked reentrant when a cluster is created and a
  node (server) executing jobs for that computation fails, dispy
  automatically resubmits those jobs to other available nodes.

* dispy can be used in a single process to use all the nodes
  exclusively (with ``JobCluster`` - simpler to use) or in multiple
  processes simultaneously sharing the nodes (with
  ``SharedJobCluster`` and *dispyscheduler* program).

* Cluster can be `monitored and managed
  <https:/dispy.org/httpd.html>`_ with web browser.

Dependencies
------------

dispy requires pycos_ for concurrent, asynchronous network programming with tasks. pycos is
automatically installed if dispy is installed with pip. Under Windows efficient polling notifier
I/O Completion Ports (IOCP) is supported only if `pywin32 <https://github.com/mhammond/pywin32>`_
is installed; otherwise, inefficient *select* notifier is used.

Installation
------------
To install dispy, run::

   python -m pip install dispy

Release Notes
-------------

Short summary of changes for each release can be found at `News
<https://pycos.com/forum/viewforum.php?f=11>`_. Detailed logs / changes are at
github `commits <https://github.com/pgiri/dispy/commits/master>`_.

Authors
-------
* Giridhar Pemmasani

Links
-----
* Documentation is at `dispy.org`_.
* `Examples <https://dispy.org/examples.html>`_.
* `Github (Code Respository) <https://github.com/pgiri/dispy>`_.
