# This program registers 'cluster_status' which schedules
# computations when a processor is available.

# job computation runs at dispynode servers
def compute(path):
    import hashlib, time, os
    csum = hashlib.sha1()
    with open(os.path.basename(path), 'rb') as fd:
        while True:
            data = fd.read(1024000)
            if not data:
                break
            csum.update(data)
    time.sleep(5)
    return csum.hexdigest()


# 'cluster_status' notification function. It is executed by dispy
# to indicate node / job status changes. Here node iniitialization and
# job done status are used to schedule jobs, so at most one job is
# running on a node (even if a node has more than one processor). Data
# files are assumed to be 'data000', 'data001' etc.
def cluster_status(status, node, job):
    if status == dispy.DispyJob.Finished:
        print('sha1sum for %s: %s' % (job.id, job.result))
    elif status == dispy.DispyJob.Terminated:
        print('sha1sum for %s failed: %s' % (job.id, job.exception))
    elif status == dispy.DispyNode.Initialized:
        print('node %s with %s CPUs available' % (node.ip_addr, node.avail_cpus))
    else:
        return

    global submitted
    data_file = 'data%03d' % submitted
    if os.path.isfile(data_file):
        submitted += 1
        # 'node' and 'dispy_job_depends' are consumed by dispy;
        # 'compute' is called with only 'data_file' as argument(s)
        job = cluster.submit_node(node, data_file, dispy_job_depends=[data_file])
        job.id = data_file
            

if __name__ == '__main__':
    import dispy, sys, os
    cluster = dispy.JobCluster(compute, cluster_status=cluster_status)
    submitted = 0
    while True:
        try:
            cmd = sys.stdin.readline().strip().lower()
        except KeyboardInterrupt:
            break
        if cmd == 'quit' or cmd == 'exit':
            break

    cluster.wait()
    cluster.print_status()
