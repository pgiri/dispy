# Example program that uses 'setup' and 'cleanup' functions to
# initialize/de-initialize global variables on each node before
# computations are executed. Computations use data in global variables
# instead of reading input for each job.

# Under Windows global variables must be serializable, so modules
# can't be global variables: See
# https://docs.python.org/2/library/multiprocessing.html#windows for
# details.

def setup(data_file):
    # read data in file to global variable
    global data, algorithms, hashlib, time, file_name

    import hashlib, time
    data = open(data_file, 'rb').read()  # read file in to memory; data_file can now be deleted
    file_name = data_file
    if sys.version_info.major > 2:
        algorithms = list(hashlib.algorithms_guaranteed)
    else:
        algorithms = hashlib.algorithms
    algorithms = [alg for alg in algorithms if (not alg.startswith('shake'))]
    # if running under Windows, modules can't be global, as they are not
    # serializable; instead, they must be loaded in 'compute' (jobs); under
    # Posix (Linux, OS X and other Unix variants), modules declared global in
    # 'setup' will be available in 'compute'

    # 'os' module is already available (loaded by dispynode)
    if os.name == 'nt':  # remove modules under Windows
        del hashlib, time
    return 0

# 'cleanup' should have same argument as 'setup'
def cleanup(data_file):
    global data, algorithms, hashlib, time, file_name
    del data, algorithms, file_name
    if os.name != 'nt':
        del hashlib, time

def compute(n):
    global hashlib, time
    if os.name == 'nt': # Under Windows modules must be loaded in jobs
        import hashlib, time
    # 'data' and 'algorithms' global variables are initialized in 'setup'
    alg = algorithms[n % len(algorithms)]
    csum = getattr(hashlib, alg)()
    csum.update(data)
    time.sleep(2)
    return (dispy_node_ip_addr, file_name, alg, csum.hexdigest())

if __name__ == '__main__':
    import dispy, sys, os, glob, time

    # each node processes a file in 'data_files' with 'NodeAllocate.allocate'
    data_files = glob.glob(os.path.join(os.path.abspath(os.path.dirname(sys.argv[0])), '*.py'))
    node_id = 0

    # sub-class NodeAllocate to use node (and computation) specific 'depends'
    # and 'setup_args'
    class NodeAllocate(dispy.NodeAllocate):
        def allocate(self, cluster, ip_addr, name, cpus, avail_info=None, platform='*'):
            global node_id
            data_file = data_files[node_id % len(data_files)]
            node_id += 1
            print('Node %s (%s) processes "%s"' % (ip_addr, name, data_file))
            self.depends = [data_file] # 'depends' must be a list
            # files are saved at server under computation's directory so send just basename
            self.setup_args = (os.path.basename(data_file),) # 'setup_args' must be a tuple
            return cpus

    cluster = dispy.JobCluster(compute, nodes=[NodeAllocate('*')], setup=setup, cleanup=cleanup)
    # wait until (enough) nodes to be setup; alternately, 'cluster_status' can
    # be used to process nodes found
    time.sleep(5)
    jobs = []
    for n in range(10):
        job = cluster.submit(n)
        jobs.append(job)

    for job in jobs:
        job()
        if job.status == dispy.DispyJob.Finished:
            print('%s: "%s" %s: %s' % job.result)
        else:
            print(job.exception)
    cluster.print_status()
    cluster.close()
