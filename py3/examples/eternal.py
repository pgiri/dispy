# This example shows how long running computations or computations that on their own can't
# determine result of a computation. In this case, computations send "guess" values to client
# using dispy_provisional_result feature. Client determines when such a result is acceptable and
# cancels jobs. It is also possible for client to cancel jobs (e.g., as done here, when "quit"
# command is given). In either case, when a job is canceled, the computations send current best
# "guess" as result (by handling KeyboardInterrupt exception).

# This scenario is useful in optimizations where searching for global min/max may take very long
# time and client/user can determine when current values are acceptable. Or when computations can
# take very long time so they can be terminated (e.g., when cloud computing charges may go up)
# while saving current state of each computation and then later rerun by resuming from saved
# state.

# computation runs until terminated and return appropriate "result"
def compute():
    import time, random
    # generate random numbers between 0 and 1 and return best value close to 1
    best = 0
    updated = 0  # when last provisional result was sent to client
    while 1:
        # when a job is canceled, first KeyboardInterrupt is raised in job and about 5 seconds
        # later, job is terminated. So jobs can process KeyboardInterrupt to take appropriate
        # action before job is terminated; in this case, best result is sent
        try:
            v = random.uniform(0, 1)
            if v > best:
                best = v
                # if last update was more than 10 seconds ago, send provisional result;
                # alternately, save current state in a file and send it to client
                # (with 'dispy_send_file'), e.g., to resume computations later
                if (time.time() - updated) > 10:
                    dispy_provisional_result(best)
                    updated = time.time()
            time.sleep(2)  # simulate computation
        except KeyboardInterrupt:  # job being terminated
            break
    return best


# process cluster status messages to submit jobs on initialized nodes
def cluster_status(status, node, job):
    global jobs
    if status == dispy.DispyJob.ProvisionalResult:
        if (1.0 - job.result) < 1e-2:  # enough accuracy, quit
            import signal
            if os.name == 'nt':
                signum = signal.CTRL_BREAK
            else:
                signum = signal.SIGINT
            os.kill(os.getpid(), signum)  # raises KeyboardInterrupt in '__main__'
    elif status == dispy.DispyNode.Initialized:
        print('node %s initialized with %s cpus' % (node.ip_addr, node.cpus))
        # submit jobs at this node
        for i in range(node.cpus):
            job = cluster.submit_node(node)
            if isinstance(job, dispy.DispyJob):
                jobs.add(job)
            else:
                print('failed to submit job at %s' % node.ip_addr)
    # elif job.status == dispy.DispyJob.Finished:
    #     print('job at %s computed: %s' % (job.ip_addr, job.result))
    elif status == dispy.DispyJob.Abandoned:
        print('%s failed: %s' % (job.ip_addr, job.exception))
        jobs.discard(job)


if __name__ == '__main__':
    import sys, os, dispy

    # although not required in this example, NodeAllocate is used here to allocate at most 2 CPUs
    class NodeAllocate(dispy.NodeAllocate):
        def allocate(self, cluster, ip_addr, name, cpus, avail_info=None, platform=''):
            print('Allocate node for %s: %s / %s / %s' % (cluster.name, ip_addr, name, cpus))
            # use at most 2 cpus per node
            return min(cpus, 2)

    jobs = set()
    cluster = dispy.JobCluster(compute, nodes=[NodeAllocate(host='*')],
                               cluster_status=cluster_status)

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
