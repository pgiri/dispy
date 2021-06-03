# example program that sends object instances in local program
# as arguments to distributed computation
# Also shows use of nodeinit_ feature to ensure that cmd module is immediately imported so that class C can extend cmd.Cmd on the node
import cmd

class C(cmd.Cmd):
    def __init__(self, i, n):
        self.i = i
        self.n = n

    def show(self):
        print('%s: %.2f' % (self.i, self.n))

def compute(obj):
    # obj is an instance of C
    import time
    time.sleep(obj.n)
    obj.show()
    return obj.n

if __name__ == '__main__':
    import random, dispy
    def nodeinit_setup():
        global cmd
        import cmd
        return 0
    cluster = dispy.JobCluster(compute, depends=[C], setup=nodeinit_setup)
    jobs = []
    for i in range(10):
        c = C(i, random.uniform(1, 3)) # create object of C
        job = cluster.submit(c) # it is sent to a node for executing 'compute'
        job.id = c # store this object for later use
        jobs.append(job)
    for job in jobs:
        job() # wait for job to finish
        print('%s: %.2f / %s' % (job.id.i, job.result, job.stdout))
