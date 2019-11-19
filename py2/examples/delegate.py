# Simple program that distributes 'delegate' function' to execute different
# computations with jobs. In this example, client sends a function with each job
# and 'delegate' executes that function.

# 'delegate' is sent to nodes with cluster initialization
def delegate(func_name, n):
    # get function with given name (this function would've been sent with
    # 'dispy_job_depends' and available in global scope)
    func = globals()[func_name]
    return func(n)

# in this case two different functions (the only difference is in return value)
# are sent with jobs and executed at nodes (with 'delegate')
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
    # above functions can be sent with 'depends' so they are available for jobs
    # always; instead, here, requird function is sent with 'dispy_job_depends'
    # to illustrate how to send functions with 'submit' (dynamically)
    cluster = dispy.JobCluster(delegate, loglevel=dispy.logger.DEBUG)
    jobs = []
    for i in range(4):
        # run above functions (computations) alternately
        if i % 2 == 0:
            func = func1
        else:
            func = func2
        # send function with 'dispy_job_depends'; this function is specific to
        # this job - it is discarded when job is over
        job = cluster.submit(func.__name__, random.randint(5, 10), dispy_job_depends=[func])
        if not job:
            print('Failed to create job %s' % i)
            continue
        jobs.append(job)

    for job in jobs:
        host, n = job() # waits for job to finish and returns results
        print('%s executed job %s at %s with %s' % (host, job.id, job.start_time, n))
        # other fields of 'job' that may be useful:
        # print(job.stdout, job.stderr, job.exception, job.ip_addr, job.start_time, job.end_time)
    cluster.print_status()
