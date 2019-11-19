#!/usr/bin/env python

# example to create http server so cluster can be monitored / managed
# with a web browser

# program sends object instances in local program as arguments to
# distributed computation
class C(object):
    def __init__(self, s, i):
        self.s = s
        self.i = i
    # provide __str__ so instances of C are shown with useful info
    # when monitoring jobs in 'Node' page
    def __str__(self):
        return 'C(%s, %.4f)' % (self.s, self.i)

def compute(obj, n=5):
    # obj is an instance of C
    import time
    time.sleep(n)
    return n * obj.i

if __name__ == '__main__':
    import dispy, random

    # create cluster
    cluster = dispy.JobCluster(compute, depends=[C])

    # import dispy's httpd module, create http server
    import dispy.httpd
    http_server = dispy.httpd.DispyHTTPServer(cluster)

    # cluster can now be monitored / managed in web browser at
    # http://<host>:8181 where <host> is name or IP address of
    # computer running this program

    for i in range(8):
        c = C(str(i), random.uniform(1, 9))
        job = cluster.submit(c, n=random.randint(5, 20))

    cluster.wait() # wait for all jobs to finish
    http_server.shutdown() # this waits until browser gets all updates
    cluster.close()
