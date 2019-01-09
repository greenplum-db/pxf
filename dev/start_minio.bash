#!/bin/bash

echo 'Adding test bucket gpdb-ud-scratch to Minio ...'
sudo mkdir -p /opt/minio/data/gpdb-ud-scratch

export MINIO_ACCESS_KEY=admin
export MINIO_SECRET_KEY=password
echo "Minio credentials: accessKey=${MINIO_ACCESS_KEY} secretKey=${MINIO_SECRET_KEY}"

echo 'Starting Minio ...'
sudo /opt/minio/bin/minio server /opt/minio/data &

# export minio credentials as AWS environment variables
export AWS_ACCESS_KEY_ID=${MINIO_ACCESS_KEY}
export AWS_SECRET_ACCESS_KEY=${MINIO_SECRET_KEY}

# set protocol as minio for automation testing
export PROTOCOL=minio