# When jobs are submitted, they are stored in scheduler's queue. Thus, jobs'
# arguments are kept in memory until all references to jobs and DispyJob
# instances (which also store arguments) are gone from both dispy scheduler and
# client program (if it saves DispyJob instances). This can take up memory at
# the client, causing issues especially when submitting many jobs, or jobs with
# large arguments.

# Instead of submitting all jobs at once, this program submits between
# 'lower_bound' and 'upper_bound' jobs. If number of pending jobs drops below
# 'lower_bound', more jobs are submitted, up to 'upper_bound'. Adjust bounds as
# appropriate; e.g., 'lower_bound' should be at least as many CPUs available in
# the cluster, and 'uppper_bound' to, say, 3x that. Use NodeAlloate or
# cluster_status to dynamically update bounds depending on available
# CPUs in cluster.

# Note also that submitting even not that many jobs but with large arguemnts
# (e.g., arrays, lists) can also be a problem with memory. In that case,
# consider saving argument data in a file and use 'dispy_job_depends' to send
# the file and have computation load from that file.

def compute(n):  # executed on nodes
    import time
    time.sleep(n)
    return n

# dispy calls this function to indicate change in job status
def job_status(job): # executed at the client
    global pending_jobs, jobs_cond
    if (job.status == dispy.DispyJob.Finished  # most usual case
        or job.status in (dispy.DispyJob.Terminated, dispy.DispyJob.Cancelled,
                          dispy.DispyJob.Abandoned)):
        # 'pending_jobs' is shared between two threads, so access it with
        # 'jobs_cond' (see below)
        jobs_cond.acquire()
        if job.id: # job may have finished before 'main' assigned id
            pending_jobs.pop(job.id)
            # dispy.logger.info('job "%s" done with %s: %s', job.id, job.result, len(pending_jobs))
            # if job's result is no longer needed, set it to None to reduce memory footprint
            job.result = None
            if len(pending_jobs) <= lower_bound:
                jobs_cond.notify()
        jobs_cond.release()

if __name__ == '__main__':
    import dispy, threading, random

    # set lower and upper bounds as appropriate; assuming there are 30
    # processors in a cluster, bounds are set to 50 to 100; using sub-class
    # instances of NodeAllocate as 'nodes' or using 'cluster_status' allows to
    # find number of actual processors dispy uses
    lower_bound, upper_bound = 50, 100
    # use Condition variable to protect access to pending_jobs, as
    # 'job_status' is executed in another thread
    jobs_cond = threading.Condition()
    cluster = dispy.JobCluster(compute, job_status=job_status)
    pending_jobs = {}
    # submit 1000 jobs
    for i in range(1000):
        job = cluster.submit(random.uniform(3, 7))
        jobs_cond.acquire()
        # there is a chance the job may have finished and job_status called by
        # this time, so put it in 'pending_jobs' only if job is pending
        if job.status == dispy.DispyJob.Created or job.status == dispy.DispyJob.Running:
            pending_jobs[i] = job
            # dispy.logger.info('job "%s" submitted: %s', i, len(pending_jobs))
            if len(pending_jobs) >= upper_bound:
                while len(pending_jobs) > lower_bound:
                    jobs_cond.wait()
        jobs_cond.release()
        
    cluster.wait()
    cluster.print_status()
    cluster.close()
