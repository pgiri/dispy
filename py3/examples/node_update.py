# Example to illustrate using dispy to run given program on all nodes, e.g., to install / update
# software. The program 'node_update_script.py' in this example runs once on a node, not on all
# CPUs.

# 'cluster_status' function is called by dispy (client) to indicate node / job status
# changes.
def cluster_status(status, node, job):
    global nodes
    if status == dispy.DispyNode.Initialized:
        print('Node %s with %s CPUs available' % (node.ip_addr, node.avail_cpus))
        nodes.add(node.ip_addr)
        if cluster.submit_node(node):
            pass
        else:
            print('Failed to submit job at %s' % node.ip_addr)

    elif status == dispy.DispyJob.Finished:
        if job.stderr or job.exception:
            print('Node %s failed: %s / %s' % (job.ip_addr, job.stderr, job.exception))
        else:
            print('Node %s finished with %s' % (job.ip_addr, job.stdout.decode()))

    elif status == dispy.DispyJob.Terminated:
        print('Node %s failed: %s' % (job.ip_addr, job.stderr))

if __name__ == '__main__':
    import sys, os, time, dispy

    # usage: sys.argv[0] [<path to program to run on nodes>] [<number of nodes>]
    if len(sys.argv) < 2:
        script = os.path.join(os.path.dirname(sys.argv[0]), 'node_update_script.py')
    else:
        script = sys.argv[1]

    assert os.path.isfile(script)
    if len(sys.argv) > 2:
        num_nodes = int(sys.argv[2])
    else:
        num_nodes = 1

    nodes = set()
    cluster = dispy.JobCluster(script, cluster_status=cluster_status)
    while 1:
        # wait a bit for all nodes to be discovered
        time.sleep(5)
        if len(nodes) >= num_nodes:
            break
        cluster.discover_nodes(['*'])
    cluster.wait()
    cluster.print_status()
    cluster.close()
