FROM ubuntu:latest

RUN apt-get update && \
  DEBIAN_FRONTEND=noninteractive apt-get install -yq libpython3-dev python3-pip && \
  apt-get clean && \
  rm -rf /var/lib/apt/lists/* && \
  pip3 install dispy psutil

CMD ["/usr/local/bin/dispynode.py"]
