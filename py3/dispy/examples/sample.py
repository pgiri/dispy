# simple program that distributes 'compute' function' to each node running 'dispynode'
def compute(n):
    import time
    time.sleep(n)
    # dispy_node_name is name of server where this computation is being executed
    return (dispy_node_name, n)

if __name__ == '__main__':
    import dispy, random
    cluster = dispy.JobCluster(compute)
    jobs = []
    for i in range(10):
        # schedule execution of 'compute' on a node (running 'dispynode')
        # with a parameter (random number in this case)
        job = cluster.submit(random.randint(5,20))
        jobs.append(job)
    # cluster.wait() # wait for all scheduled jobs to finish
    for job in jobs:
        host, n = job() # waits for job to finish and returns results
        print('%s (%s) executed job %s at %s with %s' % (host, job.ip_addr, job.id,
                                                         job.start_time, n))
        # other fields of 'job' that may be useful:
        # print(job.stdout, job.stderr, job.exception, job.ip_addr, job.start_time, job.end_time)
    cluster.print_status()
