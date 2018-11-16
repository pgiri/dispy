# Example program that uses 'setup' and 'cleanup' functions to
# initialize/de-initialize global variables on each node before
# computations are executed. Computations use data in global variables
# instead of reading input for each job.

# Under Windows global variables must be serializable, so modules
# can't be global variables: See
# https://docs.python.org/2/library/multiprocessing.html#windows for
# details.

def setup(data_file):
    # read data in file to global variable
    global data, algorithms, hashlib

    import hashlib
    data = open(data_file).read()  # read file in to memory; data_file can now be deleted
    if sys.version_info.major > 2:
        data = data.encode() # convert to bytes
        algorithms = list(hashlib.algorithms_guaranteed)
    else:
        algorithms = hashlib.algorithms
    # if running under Windows, modules can't be global, as they are not
    # serializable; instead, they must be loaded in 'compute' (jobs); under
    # Posix (Linux, OS X and other Unix variants), modules declared global in
    # 'setup' will be available in 'compute'

    # 'os' module is already available (loaded by dispynode)
    if os.name == 'nt':  # remove modules under Windows
        del hashlib
    return 0

def cleanup():
    global data, algorithms, hashlib
    del data, algorithms
    if os.name != 'nt':
        del hashlib

def compute(n):
    global hashlib
    if os.name == 'nt': # Under Windows modules must be loaded in jobs
        import hashlib
    # 'data' and 'algorithms' global variables are initialized in 'setup'
    alg = algorithms[n % len(algorithms)]
    csum = getattr(hashlib, alg)()
    csum.update(data)
    return (alg, csum.hexdigest())

if __name__ == '__main__':
    import dispy, sys, functools
    # if no data file name is given, use this file as data file
    data_file = sys.argv[1] if len(sys.argv) > 1 else sys.argv[0]
    cluster = dispy.JobCluster(compute, depends=[data_file],
                               setup=functools.partial(setup, data_file), cleanup=cleanup)
    jobs = []
    for n in range(10):
        job = cluster.submit(n)
        jobs.append(job)

    for job in jobs:
        job()
        if job.status == dispy.DispyJob.Finished:
            print('%s: %s : %s' % (job.id, job.result[0], job.result[1]))
        else:
            print(job.exception)
    cluster.print_status()
    cluster.close()
