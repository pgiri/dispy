**********
 Examples
**********

.. include:: common.rst

dispy can be used to distribute programs or Python program
fragments to nodes and execute them in parallel. It supports various
options to handle rather comprehensive cases (such as fault
tolerance, sharing nodes in multiple programs simulataneously, using
nodes in multiple networks etc.); however, in common setups, the usage
is simple, as done in the demonstrative examples below.

#.
  A simple script that distributes a program (say,
  '/path/to/program') to all the nodes in a local network running
  :doc:`dispynode`, executes them with a
  sequence of numbers is::

    import dispy
    cluster = dispy.JobCluster('/path/to/program')
    for i in range(50):
	cluster.submit(i)

  The program '/path/to/program' on the client computer is transferred
  to each of the nodes, so if the program is a binary program then all
  the nodes should have same architecture as the client.

  In this case we assume that the programs execute and save the
  computation results in a database, file system etc. If we are
  interested in exit status, output from each run etc., then we need
  to collect each of the jobs submitted from which interested attributes
  can be retrieved, as done in the example below.

#.
  A canonical cluster that distributes computation 'compute'
  (Python function) to nodes running :doc:`dispynode` on a local
  network, schedules jobs with the cluster, gets jobs' results and
  prints them is::

    # function 'compute' is distributed and executed with arguments
    # supplied with 'cluster.submit' below
    def compute(n):
	import time, socket
	time.sleep(n)
	host = socket.gethostname()
	return (host, n)

    if __name__ == '__main__':
	import dispy, random
	cluster = dispy.JobCluster(compute)
	jobs = []
	for n in range(20):
	    job = cluster.submit(random.randint(5,20))
	    job.id = n
	    jobs.append(job)
	# cluster.wait() # wait until all jobs finish
	for job in jobs:
	    host, n = job() # waits for job to finish and returns results
	    print('%s executed job %s at %s with %s' % (host, job.id, job.start_time, n))
	    # other fields of 'job' that may be useful:
	    # job.stdout, job.stderr, job.exception, job.ip_addr, job.end_time
	cluster.stats()

  If the computation has any dependencies, such as classes, objects
  or files, they can be specified with 'depends' argument and dispy will
  distribute them along with the computation.

#.
  Continuing futile but illustrative examples, the program below
  distributes computation to be executed with instances of a class.::

    class C:
	def __init__(self, i, n):
	    self.i = i
	    self.n = n

	def show(self):
	    print('%s: %.2f' % (self.i, self.n))

    def compute(obj):
	# obj is an instance of C
	import time
	time.sleep(obj.n)
	obj.show()
	return obj.n

    if __name__ == '__main__':
	import random, dispy
	cluster = dispy.JobCluster(compute, depends=[C])
	jobs = []
	for i in range(10):
	    c = C(i, random.uniform(1, 3)) # create object of C
	    job = cluster.submit(c) # it is sent to a node for executing 'compute'
	    job.id = c # store this object for later use
	    jobs.append(job)
	for job in jobs:
	    job() # wait for job to finish
	    print('%s: %.2f / %s' % (job.id.i, job.result, job.stdout))

  Note that class C is given in 'depends' so the code for it is
  transferred to the nodes automatically, so that the objects created in
  client program work transparently in 'compute' on remote nodes. The
  objects are serialized using Pickle and sent over the to the nodes, so
  the objects must be serializable. If they are not serializable (e.g.,
  they contain references to locks), then the class must provide
  **__getstate__** and **__setstate__** methods; see `Python object
  serialization <http://docs.python.org/2/library/pickle.html>`_ for
  details. In addition, the objects shouldn't contain file descriptors,
  references to other objects not being transferred etc., which are not
  valid on remote nodes.

#.
  *setup* and *cleanup* parameters can be used to set / unset global
  variables on a node. This feature works with Linux, OS X and other Unix
  variants (but not Windows) where child processes (that run
  computation jobs) share address space (for read-only purposes) with
  parent (dispynode program where *setup* function is executed). In
  the example below, data in file "file.dat" is read and stored in
  global variable in *setup* function. The jobs work on the data in
  the global variable. The *cleanup* function deletes the global
  variable.::

    def setup():
	# read data in file to global variable
	global data
	data = open('file.dat').read()
	return 0

    def cleanup():
	del globals()['data']

    def compute(n):
	import hashlib
	alg = hashlib.algorithms[n % len(hashlib.algorithms)]
	csum = getattr(hashlib, alg)()
	# 'data' global variable has file data
	csum.update(data)
	return (alg, csum.hexdigest())

    if __name__ == '__main__':
	import dispy
	cluster = dispy.JobCluster(compute, depends=['file.dat'], setup=setup, cleanup=cleanup)
	jobs = []
	for n in range(10):
	    job = cluster.submit(n)
	    job.id = n
	    jobs.append(job)

	for job in jobs:
	    job()
	    print('%s: %s : %s' % (job.id, job.result[0], job.result[1]))

#.
  *setup* and *cleanup* can also be used to create shared variables
  so jobs executing on a node can share state (in a limited way, using
  multiprocessing module's shared memory support). The program below
  creates an integer shared variable that is updated by jobs running on
  that node.

  Note that, as mentioned above, under Windows child processes don't
  inherit address space from parent process, so global variables
  created in *setup* function will not be available in job function.::

    def setup():
	import multiprocessing, multiprocessing.sharedctypes
	# import 'random' into global scope, create global shared variable
	global random, shvar
	import random
	lock = multiprocessing.Lock()
	shvar = multiprocessing.sharedctypes.Value('i', 1, lock=lock)
	return 0

    def cleanup():
	del globals()['shvar']
	# unload 'random' module (doesn't undo everything import does)
	del sys.modules['random']
	del globals()['random']

    def compute():
	r = random.randint(1, 10)
	global shvar
	shvar.value += r
	return shvar.value

    if __name__ == '__main__':
	import dispy
	cluster = dispy.JobCluster(compute, setup=setup, cleanup=cleanup)
	jobs = []
	for n in range(10):
	    job = cluster.submit()
	    job.id = n
	    jobs.append(job)

	for job in jobs:
	    job()
	    if job.status != dispy.DispyJob.Finished:
		print('job %s failed: %s' % (job.id, job.exception))
	    else:
		print('%s: %s' % (job.id, job.result))

#.
  *dispy_send_file* can be used to transfer file(s) to the
  client. Assume that computaion creates files with the parameter
  given (in this case *n*) so different runs create different files
  (otherwise, file(s) sent by one computation will overwrite files
  sent by other computations). Such files can be sent to the client with::

    def compute(n):
	import time
	time.sleep(n)
	# assume that computation saves data in file n.dat
	dispy_send_file(str(n) + '.dat') # send file to client
	# ... continue further computations
	return n

    if __name__ == '__main__':
	import dispy, random
	cluster = dispy.JobCluster(compute)
	jobs = []
	for n in range(20):
	    job = cluster.submit(random.randint(5,20))
	    job.id = n
	    jobs.append(job)
	for job in jobs:
	    job()
	    print('job %s results in file %s' % (job.id, str(job.id) + '.dat'))

  If the client needs to process the files as soon as they are
  transferred, :ref:`ProvisionalResult` feature along with callback
  can be used to notify the client.

This framework can be customized for various use cases; some examples
are:

* **cluster = dispy.JobCluster(compute, depends=[ClassA, moduleB,
  'file1'])** distributes 'compute' along with ClassA (Python object),
  moduleB (Python object) and 'file1', a file on client
  computer. Presumably ClassA, moduleB and file1 are needed by
  'compute'.

* **cluster = dispy.JobCluster(compute, nodes=['node20',
  '192.168.2.21', 'node24'])** sends computation to nodes 'node20',
  'node24' and node with IP address '192.168.2.21'.  These nodes could
  be in different networks, as explicit names / IP addresses are
  listed.

* If nodes are on remote network, then certain ports need to be
  forwarded as the nodes connect to the client to send status /
  results of jobs; see :ref:`NAT`. If port forwarding is not
  possible, then ssh tunneling can be used. To use this, ssh to each
  node with **ssh -R 51347:127.0.0.1:51347 node** (to possibly execute
  :doc:`dispynode` program on the node if not already running), then
  specify *ip_addr=127.0.0.1* to :class:`JobCluster`; dispy issues a
  warning about using localhost address, but in this case the warning
  is harmless.

* **cluster = dispy.JobCluster(compute, nodes=['192.168.2.\*'])**
  sends computation to all nodes whose IP address starts with
  '192.168.2'.  In this case, it is assumed that '192.168.2' is local
  network (since UDP broadcast is used to discover nodes in a network
  and broadcasting packets don't cross networks).

* **cluster = dispy.JobCluster(compute, nodes=['192.168.3.5',
  '192.168.3.22', '172.16.11.22', 'node39', '192.168.2.\*'])** sends
  computation to nodes with IP addresses '192.168.3.5',
  '192.168.3.22', '172.16.11.22' and node 'node39' (since explicit
  names / IP addresses are listed, they could be on different
  networks), all nodes whose IP address starts with '192.168.2' (local
  network).

* **cluster = dispy.JobCluster(compute, nodes=['192.168.3.5', '192.168.3.\*', '192.168.2.\*'])**
  In this case, dispy will send identification request to node with IP address '192.168.3.5'.
  If this node is running 'dispynetrelay', then all the nodes on that network are eligible for
  executing this computation, as wildcard '192.168.3.*' matches IP addresses of those nodes.
  In addition, computation is also sent to all nodes whose IP address starts with '192.168.2'
  (local network).

* **cluster = dispy.JobCluster(compute, nodes=['192.168.3.5', '192.168.8.20', '172.16.2.99', '\*'])**
  In this case, dispy will send identification request to nodes with IP address '192.168.3.5',
  '192.168.8.20' and '172.16.2.99'. If these nodes all are running dispynetrelay, then all
  the nodes on those networks are eligible for executing this computation, as wildcard '\*'
  matches IP addresses of those nodes. In addition, computation is also sent to all nodes on
  local network (since they also match wildcard '\*' and identification request is broadcast
  on local network).

* Assuming that 192.168.1.39 is the (private) IP address where dispy
  client is used, a.b.c.d is the (public) IP address of NAT
  firewall/gateway (that can be reached from outside) and dispynode is
  running at another public IP address e.f.g.h (so that a.b.c.d and
  e.f.g.h can communicate, but e.f.g.h can't communicate with
  192.168.1.39), **cluster = dispy.JobCluster(compute,
  ip_addr='192.168.1.39', ext_ip_addr='a.b.c.d', nodes=['e.f.g.h'])**
  would work if NAT firewall/gateway forwards UDP and TCP ports 51347
  to 192.168.1.39.

* **cluster = dispy.JobCluster(compute, secret='super')** distributes
  'compute' to nodes that also use secret 'super' (i.e., nodes started
  with **dispynode.py -s super**). Note that secret is used only for
  establishing communication initially, but not used to encrypt
  programs or code for python objects. This can be useful to prevent
  other users from (inadvertantly) using the nodes. If encryption is
  needed, use SSL; see below.

* **cluster = dispy.JobCluster(compute, certfile='mycert',
  keyfile='mykey')** distributes 'compute' and encrypts all
  communication using SSL certificate stored in 'mycert' file and key
  stored in 'mykey' file. In this case, dispynode must also use same
  certificate and key; i.e., each dispynode must be invoked with
  **dispynode --certfile="mycert" --keyfile="mykey"'**

  If both certificate and key are stored in same file, say,
  'mycertkey', they are expected to be in certfile:
  **cluster = dispy.JobCluster(compute, certfile='mycertkey')**

* **cluster1 = dispy.JobCluster(compute1, nodes=['192.168.3.2', '192.168.3.5'])** |br|
  **cluster2 = dispy.JobCluster(compute2, nodes=['192.168.3.10', '192.168.3.11'])** |br|
  distribute 'compute1' to nodes 192.168.3.2 and 192.168.3.5, and 'compute2' to
  nodes 192.168.3.10 and 192.168.3.11. With this setup, specific computations can be
  scheduled on certain node(s). As mentioned above, with JobCluster,
  the set of nodes for one cluster must be disjoint with set of
  nodes in any other cluster running at the same time. Otherwise,
  :class:`SharedJobCluster` must be used.

A simple version of `word count example from MapReduce
<http://en.wikipedia.org/wiki/MapReduce>`_::

    # a version of word frequency example from mapreduce tutorial

    def mapper(doc):
	# input reader and map function are combined
	import os
	words = []
	with open(os.path.join('/tmp', doc)) as fd:
	    for line in fd:
		words.extend((word.lower(), 1) for word in line.split() \
			     if len(word) > 3 and word.isalpha())
	return words

    def reducer(words):
	# we should generate sorted lists which are then merged,
	# but to keep things simple, we use dicts
	word_count = {}
	for word, count in words:
	    if word not in word_count:
		word_count[word] = 0
	    word_count[word] += count
	# print('reducer: %s to %s' % (len(words), len(word_count)))
	return word_count

    if __name__ == '__main__':
	import dispy, logging
	# map
	# nodes node1 and node2 have 'doc1', 'doc2' etc. on their
	# local storage under /tmp, so no need to transfer them
	map_cluster = dispy.JobCluster(mapper, nodes=['node1', 'node2'], pulse_interval=2,
				       reentrant=True)
	reduce_cluster = dispy.JobCluster(reducer, nodes=['*'], pulse_interval=2,
					  reentrant=True)
	map_jobs = []
	for f in ['doc1', 'doc2', 'doc3', 'doc4', 'doc5']:
	    job = map_cluster.submit(f)
	    map_jobs.append(job)
	reduce_jobs = []
	for map_job in map_jobs:
	    words = map_job()
	    if not words:
		print(map_job.exception)
		continue
	    # simple partition
	    n = 0
	    while n < len(words):
		m = min(len(words) - n, 1000)
		reduce_job = reduce_cluster.submit(words[n:n+m])
		reduce_jobs.append(reduce_job)
		n += m
	# reduce
	word_count = {}
	for reduce_job in reduce_jobs:
	    words = reduce_job()
	    if not words:
		print(reduce_job.exception)
		continue
	    for word, count in words.iteritems():
		if word not in word_count:
		    word_count[word] = 0
		word_count[word] += count
	# sort words by frequency and print
	for word in sorted(word_count, key=lambda x: word_count[x], reverse=True):
	    count = word_count[word]
	    print(word, count)
	reduce_cluster.stats()

