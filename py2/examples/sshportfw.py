def compute(n): # function sent to remote nodes for execution
    time.sleep(n)
    return n

if __name__ == '__main__':
    import dispy
    # list remote nodes (here Amazon EC2 instance with external IP 54.172.166.83)
    nodes = ['54.172.166.83']
    # use ssh to forward port 9700 for each node; e.g.,
    # 'ssh -R 9700:localhost:9700 54.172.166.83'

    # start dispynode with 'dispynode.py --ext_host 54.172.166.83' (so dispynode
    # uses external IP address instead of default local IP address); do this for all nodes
    cluster = dispy.JobCluster(compute, nodes=nodes, ip_addr='127.0.0.1')
    jobs = []
    for i in range(1, 7):
        job = cluster.submit(i)
        jobs.append(job)
    for job in jobs:
        print('result: %s' % job())
