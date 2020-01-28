#!/bin/bash

GOOGLE_PROJECT_ID=${GOOGLE_PROJECT_ID:-data-gpdb-ud}
GOOGLE_ZONE=${GOOGLE_ZONE:-us-central1-a}

yum install -y -d1 openssh openssh-clients
mkdir -p ~/.ssh/
ssh-keygen -b 4096 -t rsa -f ~/.ssh/google_compute_engine -N "" -C "ccp-ci-service"

cat << EOF > /tmp/gsc-ci-service-account.key.json
${GOOGLE_CREDENTIALS}
EOF

gcloud auth activate-service-account \
  --key-file=/tmp/gsc-ci-service-account.key.json

set -x

HADOOP_HOSTNAME="ccp-$(cat terraform_dataproc/name)-m"

gcloud compute instances add-metadata "${HADOOP_HOSTNAME}" \
  --metadata ssh-keys="ccp-ci-service:$(cat ~/.ssh/google_compute_engine.pub)" \
  --zone "us-central1-a"

ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no \
  -t -i ~/.ssh/google_compute_engine "ccp-ci-service@${HADOOP_HOSTNAME}.${GOOGLE_ZONE}.c.${GOOGLE_PROJECT_ID}.internal" \
  "hadoop distcp gs://data-gpdb-ud-tpch/${SCALE}/lineitem_data/*.tbl /tmp/lineitem_read/" || exit 1
