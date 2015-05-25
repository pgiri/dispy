# example program that uses 'setup' and 'cleanup' functions to initialize/de-initialize
# global variables on each node before computations are executed.
# It works on Unix variants (Linux, OS X etc.) but not Microsoft Windows.
def setup():
    # read data in file to global variable
    global data, algorithms, hashlib
    import hashlib
    data = bytes(open('node_setup.py').read(), 'ascii')
    algorithms = list(hashlib.algorithms_guaranteed)
    return 0

def cleanup():
    for var in ['data', 'algorithms', 'hashlib']:
        del globals()[var]

def compute(n):
    import hashlib
    alg = algorithms[n % len(algorithms)]
    csum = getattr(hashlib, alg)()
    # 'data' global variable has file data
    csum.update(data)
    return (alg, csum.hexdigest())

if __name__ == '__main__':
    import dispy, sys
    cluster = dispy.JobCluster(compute, depends=[sys.argv[0]], setup=setup, cleanup=cleanup)
    jobs = []
    for n in range(5):
        job = cluster.submit(n)
        job.id = n
        jobs.append(job)

    for job in jobs:
        job()
        if job.exception:
            print('job %s failed: %s' % (job.id, job.exception))
        else:
            print('%s: %s' % (job.id, job.result))
    cluster.stats()
    cluster.close()
