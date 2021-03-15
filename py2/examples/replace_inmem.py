# In this example, which is a variation of 'node_setup.py',
# dispy's "resetup_node" method is used to replace in-memory data to illustrate
# how to change working dataset. Although "resetup_node" feature can be used
# with any platform, this example doesn't work with Windows, as in-memory
# feature doesn't work with Windows.

# this function is executed by a node to initialize for computation; in this case, this function
# loads data in given file into global variable, which is available for computation jobs
# (as read-only data)
def setup(data_file, n):
    global data, algorithms, hashlib, file_name

    import hashlib, os, sys
    data = open(data_file, 'rb').read()  # read data in file to global variable
    os.remove(data_file)  # data_file can now be deleted
    file_name = data_file
    if sys.version_info.major > 2:
        algorithms = list(hashlib.algorithms_guaranteed)
    else:
        algorithms = hashlib.algorithms
    algorithms = [alg for alg in algorithms if (not alg.startswith('shake'))]
    return 0

# this function is executed by node when closing for computation (after all jobs are finished);
# in this case, the function removes global variables initialized with 'setup'
def cleanup(data_file, n):
    global data, algorithms, hashlib, file_name
    del data, algorithms, file_name

# this function is executed by each computation job; in this case, the function uses
# global variables (in memory) initialized in 'setup'.
def compute(i, n):
    # 'hashlib', 'data' and 'algorithms' global variables are initialized in 'setup'
    alg = algorithms[i % len(algorithms)]
    csum = getattr(hashlib, alg)()
    csum.update(data)
    time.sleep(n)
    return (dispy_node_ip_addr, file_name, alg, csum.hexdigest())

# this function is executed at client (this program) when a job's status has changed
def job_status(job):
    if job.status == dispy.DispyJob.Finished:
        print('\njob %s finished by %s, %s of %s is %s' % (job.id, job.result[0], job.result[2],
                                                           job.result[1], job.result[3]))
    else:
        print('\njob %s failed: %s' % (job.id, job.exception))

if __name__ == '__main__':
    import dispy, sys, os, glob, random
    data_files = glob.glob(os.path.join(os.path.abspath(os.path.dirname(sys.argv[0])), '*.py'))
    file_id = 0
    nodes = {}

    class NodeAllocate(dispy.NodeAllocate):
        def allocate(self, cluster, ip_addr, name, cpus, avail_info=None, platform='*'):
            global file_id
            if len(nodes) == 2:
                # use at most 2 nodes, to illustrate this example
                return 0
            if platform.startswith('Windows'):
                # In-memory is not supported with Windows
                print('Ignoring node %s as in-memory data is not supported with Windows' % ip_addr)
                return 0
            data_file = data_files[file_id % len(data_files)]
            file_id += 1
            nodes[ip_addr] = data_file  # keep track of which node processes which file
            print('Node %s (%s) processes "%s"' % (ip_addr, name, data_file))
            # files are saved at server under computation's directory so send just basename
            self.setup_args = (os.path.basename(data_file), file_id)
            self.depends = [data_file]
            return max(2, cpus)  # use at most 2 cpus (for illustration)

    cluster = dispy.JobCluster(compute, nodes=[NodeAllocate(host='*')],
                               setup=setup, cleanup=cleanup, job_status=job_status,
                               loglevel=dispy.logger.DEBUG)
    for i in range(1, 7):  # submit 7 jobs
        job = cluster.submit(i, random.uniform(2, 5))
        job.id = i
    cluster.wait()  # alternately, job_status above can decide when to call resetup_node
    # call 'resetup_node' on nodes to process more files
    for ip_addr in nodes.keys():
        cluster.resetup_node(ip_addr)
    for i in range(i+1, i+7):  # submit 7 more jobs
        job = cluster.submit(i, random.uniform(2, 5))
        job.id = i
    cluster.wait()

    cluster.print_status()
    cluster.close()
