# File to build docker (https://www.docker.com) images to run dispynode containers.

# This file builds dispy (https://dispy.org) with Python 3 using latest Ubuntu Linux.

FROM ubuntu:latest

RUN apt-get update && \
  DEBIAN_FRONTEND=noninteractive apt-get install -yq libpython3-dev python3-pip && \
  apt-get clean && \
  rm -rf /var/lib/apt/lists/* && \
  pip3 install dispy psutil netifaces

CMD ["/usr/local/bin/dispynode.py"]
