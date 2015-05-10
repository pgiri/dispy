dispy
######

`dispy <http://dispy.sourceforge.net>`_ is a rather comprehensive, yet
easy to use framework for creating and using compute clusters to
execute computations in parallel across multiple processors in a
single machine (SMP), among many machines in a cluster, grid or cloud.
dispy is well suited for data parallel (SIMD) paradigm where a
computation is evaluated with different (large) datasets independently
with no communication among computation tasks (except for computation
tasks sending intermediate results to the client). If
communication/cooperation among tasks is needed, `asyncoro
<http://pypi.python.org/pypi/asyncoro>`_ framework could be used.

dispy works with Python versions 2.7+ and 3.1+. It has been tested
with Linux, OS X and Windows; it may work on other platforms too.

Features
--------
* dispy is implemented with `asyncoro
  <http://pypi.python.org/pypi/asyncoro>`_, an independent framework
  for asynchronous, concurrent, distributed, network programming with
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
  <http://dispy.sourceforge.net/httpd.html`_ with web browser.

Dependencies
------------
dispy requires `asyncoro <http://pypi.python.org/pypi/asyncoro>`_ for
concurrent, asynchronous network programming with coroutines. asyncoro
can be installed for Python 2.7+ with::

   pip install asyncoro

or for Python 3.1+ with::

   pip3 install asyncoro

Under Windows efficient polling notifier I/O Completion Ports (IOCP)
is supported only if `pywin32
<http://sourceforge.net/projects/pywin32/files/pywin32/>`_ is
installed; otherwise, inefficient *select* notifier is used.

Installation
------------
To install dispy for Python 2.7+, run::

   pip install dispy

or to install dispy for Python 3.1+, run::

   pip3 install dispy

Authors
-------
* Giridhar Pemmasani

Links
-----
* `Project page <http://dispy.sourceforge.net>`_.
* `Examples <http://dispy.sourceforge.net/examples.html>`_.
* `Changes <https://sourceforge.net/p/dispy/news/>`_.
