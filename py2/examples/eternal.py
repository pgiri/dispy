# computation runs until terminated and return appropriate "result"
def compute():
    import time, random
    # generate random numbers between 0 and 1 and return best value close to 1
    best = 0
    while 1:
        # when a job is cancelled, KeyboardInterrupt is raised in job first and about 2 seconds
        # later, job is terminated. So jobs can process KeyboardInterrupt to take appropriate
        # action; in this case, best result is sent
        try:
            v = random.uniform(0, 1)
            if v > best:
                best = v
                # ProvisionalResult feature can also be used to send intermediate results
            time.sleep(2)
        except KeyboardInterrupt:  # job being terminated
            break
    return best

# cluster status callback to submit jobs on initialized nodes
def status_cb(status, node, job):
    global jobs
    if status == dispy.DispyNode.Initialized:
        print('node %s initialized with %s cpus' % (node.ip_addr, node.cpus))
        # submit jobs at this node
        for i in range(node.cpus):
            job = cluster.submit_node(node)
            if isinstance(job, dispy.DispyJob):
                jobs.add(job)
            else:
                print('failed to submit job at %s' % node.ip_addr)
    # elif status == dispy.DispyJob.Finished:
    #     print('job at %s computed: %s' % (job.ip_addr, job.result))
    elif status == dispy.DispyJob.Abandoned:
        print('%s failed: %s' % (job.ip_addr, job.exception))
        jobs.discard(job)
        
    else:
        return

if __name__ == '__main__':
    import sys, dispy

    # although not required in this example, NodeAllocate is used here to allocate at most 2 CPUs
    class NodeAllocate(dispy.NodeAllocate):
        def allocate(self, cluster, ip_addr, name, cpus, avail_info=None, platform=''):
            print('Allocate node for %s: %s / %s / %s' % (cluster.name, ip_addr, name, cpus))
            # use at most 2 cpus per node
            return min(cpus, 2)

    jobs = set()
    cluster = dispy.JobCluster(compute, nodes=[NodeAllocate(host='*')], cluster_status=status_cb)

    if sys.version_info.major < 3:
        input = raw_input

    while 1:
        try:
            cmd = input('Enter "quit" or "exit" or Ctrl-C to terminate: ').strip().lower()
            if cmd in ('quit', 'exit'):
                break
        except KeyboardInterrupt:
            break

    # cancel (terminate) jobs
    for job in jobs:
        cluster.cancel(job)

    best = 0
    for job in jobs:
        result = job()
        if job.status == dispy.DispyJob.Finished:
            print('job at %s computed %s' % (job.ip_addr, job.result))
            if result > best:
                best = result
    print('best estimate: %s' % best)
    cluster.print_status()
    exit(0)
