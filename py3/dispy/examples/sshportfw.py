def compute(n): # function sent to remote nodes for execution
    time.sleep(n)
    return n

if __name__ == '__main__':
    import dispy
    # list remote nodes (here Amazon EC2 instance with external IP 54.204.242.185)
    nodes = ['54.204.242.185']
    # use ssh to forward port 51347 for each node; e.g.,
    # 'ssh -R 51347:localhost:51347 54.204.242.185'

    # start dispynode on each node with 'dispynode.py -i 54.204.242.185' (so dispynode
    # uses external IP address instead of default local IP address)
    cluster = dispy.JobCluster(compute, nodes=nodes, ip_addr='127.0.0.1')
    jobs = []
    for i in range(1, 10):
        job = cluster.submit(i)
        jobs.append(job)
    for job in jobs:
        print('result: %s' % job())
