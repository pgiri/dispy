# Example program that uses 'setup' and 'cleanup' functions to
# initialize/de-initialize global variables on each node before
# computations are executed. Computations use data in global variables
# instead of reading input for each job. The in-memory data is replaced
# at 5th job to illustrate how to change working dataset.

def setup(data_file, n):
    dispynode_logger.info('\n  Setup: %s, %s\n', data_file, n)
    # read data in file to global variable
    global data, algorithms, hashlib, file_name

    import hashlib
    data = open(data_file).read()  # read file in to memory; data_file can now be deleted
    file_name = data_file
    if sys.version_info.major > 2:
        data = data.encode() # convert to bytes
        algorithms = list(hashlib.algorithms_guaranteed)
    else:
        algorithms = hashlib.algorithms
    algorithms = [alg for alg in algorithms if (not alg.startswith('shake'))]

    return 0

def cleanup(data_file, n):
    dispynode_logger.info('\n  Cleanup: %s, %s\n', data_file, n)
    global data, algorithms, hashlib, file_name
    del data, algorithms, file_name

def compute(i, n):
    global hashlib
    # 'data' and 'algorithms' global variables are initialized in 'setup'
    alg = algorithms[i % len(algorithms)]
    csum = getattr(hashlib, alg)()
    csum.update(data)
    time.sleep(n)
    return (dispy_node_ip_addr, file_name, alg, csum.hexdigest())

def job_status(job):
    if job.status == dispy.DispyJob.Finished:
        print('\njob %s finished by %s, %s of %s is %s' % (job.id, job.result[0], job.result[2],
                                                           job.result[1], job.result[3]))
    else:
        print('\njob %s failed: %s' % (job.id, job.exception))

if __name__ == '__main__':
    import dispy, sys, os, glob, random
    data_files = glob.glob(os.path.join(os.path.dirname(sys.argv[0]), '*.py'))
    file_id = 0
    nodes = {}

    class NodeAllocate(dispy.NodeAllocate):
        def allocate(self, cluster, ip_addr, name, cpus, avail_info=None, platform='*'):
            global file_id
            if len(nodes) == 2:
                # use at most 2 nodes, to simplify this example
                return 0
            if platform.startswith('Windows'):
                # In-memory is not supported with Windows
                return 0
            data_file = data_files[file_id % len(data_files)]
            file_id += 1
            nodes[ip_addr] = data_file
            print('Node %s (%s) processes "%s"' % (ip_addr, name, data_file))
            self.setup_args = (data_file, file_id)
            self.depends = [data_file]
            return max(2, cpus)  # use at most 2 cpus for illustration

    cluster = dispy.JobCluster(compute, nodes=[NodeAllocate(host='*')],
                               setup=setup, cleanup=cleanup, callback=job_status,
                               loglevel=dispy.logger.DEBUG)
    for i in range(1, 7):
        job = cluster.submit(i, random.uniform(2, 5))
        job.id = i
    cluster.wait()
    for ip_addr in nodes:
        cluster.resetup_node(ip_addr)
    for i in range(i+1, i+7):
        job = cluster.submit(i, random.uniform(2, 5))
        job.id = i
    cluster.wait()

    cluster.print_status()
    cluster.close()
