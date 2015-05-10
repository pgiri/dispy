# example program that uses 'setup' and 'cleanup' functions to initialize/de-initialize
# global variables so computations (jobs) on a node can share variables
# (for read or read/write). These variables are not visible to computations on other nodes.
# It works on Unix variants (Linux, OS X etc.) but not Microsoft Windows.
def setup():
    import multiprocessing, multiprocessing.sharedctypes
    # import 'random' into global scope, create global shared variable
    global random, shvar
    import random
    lock = multiprocessing.Lock()
    shvar = multiprocessing.sharedctypes.Value('i', 1, lock=lock)
    return 0

def cleanup():
    del globals()['shvar']
    # unload 'random' module (doesn't undo everything import does)
    sys.modules.pop('random')
    del globals()['random']

def compute():
    r = random.randint(1, 10)
    global shvar
    shvar.value += r
    return shvar.value

if __name__ == '__main__':
    import dispy
    cluster = dispy.JobCluster(compute, depends=['file.dat'], setup=setup, cleanup=cleanup)
    jobs = []
    for n in range(10):
        job = cluster.submit()
        job.id = n
        jobs.append(job)

    for job in jobs:
        job()
        if job.status != dispy.DispyJob.Finished:
            print('job %s failed: %s' % (job.id, job.exception))
        else:
            print('%s: %s' % (job.id, job.result))
    cluster.stats()
    cluster.close()
