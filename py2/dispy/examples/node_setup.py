# example program that uses 'setup' and 'cleanup' functions to initialize/de-initialize
# global variables on each node before computations are executed.
# It works on Unix variants (Linux, OS X etc.) but not Microsoft Windows.
def setup():
    # read data in file to global variable
    global data
    data = open('file.dat').read()
    return 0

def cleanup():
    del globals()['data']

def compute(n):
    import hashlib
    alg = hashlib.algorithms[n % len(hashlib.algorithms)]
    csum = getattr(hashlib, alg)()
    # 'data' global variable has file data
    csum.update(data)
    return (alg, csum.hexdigest())

if __name__ == '__main__':
    import dispy
    cluster = dispy.JobCluster(compute, depends=['file.dat'], setup=setup, cleanup=cleanup)
    jobs = []
    for n in range(10):
        job = cluster.submit(n)
        job.id = n
        jobs.append(job)

    for job in jobs:
        job()
        print('%s: %s : %s' % (job.id, job.result[0], job.result[1]))
    cluster.stats()
    cluster.close()
