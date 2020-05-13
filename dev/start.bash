#!/usr/bin/env bash

if [[ -z ${GCR_PROJECT} ]]; then
    echo "Please set GCR_PROJECT variable to the name of your Google Container Registry project"
    exit 1
fi

docker run --rm -it \
  -p 5432:5432 \
  -p 5888:5888 \
  -p 8000:8000 \
  -p 5005:5005 \
  -p 8020:8020 \
  -p 9000:9000 \
  -p 9090:9090 \
  -p 50070:50070 \
  -w /home/gpadmin/workspace \
  -v ~/workspace/pxf:/home/gpadmin/workspace/pxf \
  gcr.io/${GCR_PROJECT}/gpdb-pxf-dev/gpdb6-centos7-test-pxf-hdp2:latest /bin/bash -c \
  "/home/gpadmin/workspace/pxf/dev/bootstrap.bash && su - gpadmin"