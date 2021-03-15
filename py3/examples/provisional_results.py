# Example to demonstrate a computation sending multiple "provisional"
# results to the client. This feature can be used in optimizations
# where each computation knows local optima and the client can collect
# these as they are received to compute global optimal value

import random, dispy

def compute(n): # executed on nodes
    import random, time, socket
    name = socket.gethostname()
    cur_best = 1
    for i in range(0, n):
        r = random.uniform(0, 1)
        if r <= cur_best:
            # possible result (local optimal value)
            dispy_provisional_result((name, r))
            cur_best = r
        time.sleep(0.1)
    # final result
    return (name, cur_best)

# dispy calls this function to indicate change in job status
def job_status(job): # executed at the client
    if (job.status == dispy.DispyJob.ProvisionalResult or
        job.status == dispy.DispyJob.Finished):
        print('best value from %s: %s' % (job.result[0], job.result[1]))
        if job.result[1] < 0.005:
            # acceptable result; terminate jobs
            print('%s computed: %s' % (job.result[0], job.result[1]))
            # 'jobs' and 'cluster' are created in '__main__' below
            for j in jobs:
                if j.status in [dispy.DispyJob.Created, dispy.DispyJob.Running,
                                dispy.DispyJob.ProvisionalResult]:
                    cluster.cancel(j)

if __name__ == '__main__':
    cluster = dispy.JobCluster(compute, job_status=job_status)
    jobs = []
    for n in range(4):
        job = cluster.submit(random.randint(50,100))
        if job is None:
            print('creating job %s failed!' % n)
            continue
        jobs.append(job)
    cluster.wait()
    cluster.print_status()
    cluster.close()
