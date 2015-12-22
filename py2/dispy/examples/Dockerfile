FROM ubuntu:latest

RUN apt-get update && \
  DEBIAN_FRONTEND=noninteractive apt-get install -yq libpython2.7-dev python-pip && \
  apt-get clean && \
  rm -rf /var/lib/apt/lists/* && \
  pip install dispy psutil

CMD ["/usr/local/bin/dispynode.py"]
