# Simple program that distributes 'delegate' function' to each node running 'dispynode'
# Client sends a function with each job and 'delegate' executes that function.
def delegate(func_name, n):
    # get function with given name (this function would've been sent with
    # 'dispy_job_depends' and available in global scope)
    func = globals()[func_name]
    return func(n)

# in this case two different functions (the only difference is in return value)
# are executed
def func1(n):
    import time
    time.sleep(n)
    return (dispy_node_name + ': func1', n)

def func2(n):
    import time
    time.sleep(n)
    return (dispy_node_name + ': func2', n)

if __name__ == '__main__':
    import dispy, random, time
    cluster = dispy.JobCluster(delegate, loglevel=dispy.logger.DEBUG)
    jobs = []
    for i in range(4):
        # send a function as 'dispy_job_depends'; this function is specific to
        # this job - it is discarded when job is over
        func_name = 'func' + str(i % 2 + 1)
        job = cluster.submit(func_name, random.randint(5,10),
                             dispy_job_depends=[globals()[func_name]])
        if not job:
            print('Failed to create job %s' % i)
            continue
        job.id = i # optionally associate an ID to job (if needed later)
        jobs.append(job)
    # cluster.wait() # wait for all scheduled jobs to finish
    for job in jobs:
        host, n = job() # waits for job to finish and returns results
        print('%s executed job %s at %s with %s' % (host, job.id, job.start_time, n))
        # other fields of 'job' that may be useful:
        # print(job.stdout, job.stderr, job.exception, job.ip_addr, job.start_time, job.end_time)
    cluster.print_status()
    dispy.logger.debug('jobs run: %s' % len(jobs))
