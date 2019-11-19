# Example program that uses 'setup' and 'cleanup' functions to
# initialize/de-initialize global variables on each node before
# computations are executed. Computations (jobs) on a node update
# shared variable 'shvar' using multiprocessing's locks (so no two
# jobs update the variable simultaneously).

# Under Windows global variables must be serializable, so modules
# can't be global variables: See
# https://docs.python.org/2/library/multiprocessing.html#windows for
# details. This example is implemented to load 'random' module in
# computation (for each job). When executing in posix (Linux, OS X and
# other Unix variants), 'random' can be declared global variable and
# module loaded in 'setup' (and deleted in 'cleanup').

def setup():
    import multiprocessing, multiprocessing.sharedctypes
    global shvar
    lock = multiprocessing.Lock()
    shvar = multiprocessing.sharedctypes.Value('i', 1, lock=lock)
    return 0

def cleanup():
    global shvar
    del shvar

def compute():
    import random
    r = random.randint(1, 10)
    global shvar
    shvar.value += r
    return shvar.value

if __name__ == '__main__':
    import dispy
    cluster = dispy.JobCluster(compute, setup=setup, cleanup=cleanup)
    jobs = []
    for n in range(10):
        job = cluster.submit()
        jobs.append(job)

    for job in jobs:
        job()
        if job.status != dispy.DispyJob.Finished:
            print('job %s failed: %s' % (job.id, job.exception))
        else:
            print('%s: %s' % (job.id, job.result))
    cluster.print_status()
    cluster.close()
