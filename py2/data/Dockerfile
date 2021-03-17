# File to build docker (https://www.docker.com) images to run dispynode containers.

# This file builds dispy (https://dispy.org) with Python 2 using latest Ubuntu Linux.

FROM ubuntu:latest

RUN apt-get update && \
  DEBIAN_FRONTEND=noninteractive apt-get install -yq libpython2.7-dev python-pip && \
  apt-get clean && \
  rm -rf /var/lib/apt/lists/* && \
  pip install dispy psutil netifaces

CMD ["/usr/local/bin/dispynode.py"]
