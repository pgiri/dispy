DispyPort = 9700
HTTPServerPort = 8181
IPv6MulticastGroup = 'ff05::b409:3171:9705:5134:6159'
IPv4MulticastGroup = '239.255.61.59'
MsgTimeout = 10
MaxFileSize = 0
# Settings below are evaluated so must be expressions
ClientPort = 'dispy.config.DispyPort'
NodePort = 'dispy.config.DispyPort + 1'
SharedSchedulerPort = 'dispy.config.DispyPort + 2'
