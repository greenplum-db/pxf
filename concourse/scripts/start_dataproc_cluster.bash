#!/bin/bash

set -euo pipefail

# defaults
ENV_FILES_DIR=${ENV_FILES_DIR:-dataproc_env_files}
HADOOP_USER=${HADOOP_USER:-gpadmin}
IMAGE_VERSION=${IMAGE_VERSION:-1.3}
INITIALIZATION_SCRIPT=${INITIALIZATION_SCRIPT:-gs://pxf-perf/scripts/initialization-for-kerberos.sh}
KERBEROS=${KERBEROS:-false}
KEYRING=${KEYRING:-dataproc-kerberos}
KEY=${KEY:-dataproc-kerberos-test}
MACHINE_TYPE=${MACHINE_TYPE:-n1-standard-2}
NUM_WORKERS=${NUM_WORKERS:-2}
PROJECT=${GOOGLE_PROJECT_ID:-}
REGION=${GOOGLE_ZONE%-*} # lop off '-a', '-b', etc. from $GOOGLE_ZONE
REGION=${REGION:-us-central1}
SECRETS_BUCKET=${SECRETS_BUCKET:-data-gpdb-ud-pxf-secrets}
SUBNETWORK=${SUBNETWORK:-dynamic}
ZONE=${GOOGLE_ZONE:-us-central1-a}

pip install petname
yum install -y -d1 openssh openssh-clients
mkdir -p ~/.ssh
ssh-keygen -b 4096 -t rsa -f ~/.ssh/google_compute_engine -N "" -C "$HADOOP_USER"

gcloud config set project "$PROJECT"
gcloud auth activate-service-account --key-file=<(echo "$GOOGLE_CREDENTIALS")

set -x

PLAINTEXT=$(mktemp)
PLAINTEXT_NAME=$(basename "$PLAINTEXT")
PETNAME=ccp-$(petname)

# Initialize the dataproc service
GCLOUD_COMMAND=(gcloud beta dataproc clusters
  "--region=$REGION" create "$PETNAME"
  --initialization-actions "$INITIALIZATION_SCRIPT"
  --no-address
  --subnet "projects/${PROJECT}/regions/${REGION}/subnetworks/$SUBNETWORK"
  "--master-machine-type=$MACHINE_TYPE"
  "--worker-machine-type=$MACHINE_TYPE"
  "--zone=$ZONE"
  "--tags=bosh-network,outbound-through-nat,tag-concourse-dynamic"
  "--num-workers=$NUM_WORKERS"
  --image-version "$IMAGE_VERSION"
  --properties 'core:hadoop.proxyuser.gpadmin.hosts=*,core:hadoop.proxyuser.gpadmin.groups=*')

if [[ $KERBEROS == true ]]; then
    # Generate a random password
    date +%s | sha256sum | base64 | head -c 64 > "$PLAINTEXT"

    # Encrypt password file with the KMS key
    gcloud kms encrypt \
      --location "$REGION" \
      --keyring "$KEYRING" \
      --key "$KEY" \
      --plaintext-file "$PLAINTEXT" \
      --ciphertext-file "${PLAINTEXT}.enc"

    # Copy the encrypted file to gs
    gsutil cp "${PLAINTEXT}.enc" "gs://${SECRETS_BUCKET}/"

    GCLOUD_COMMAND+=(--kerberos-root-principal-password-uri
      "gs://${SECRETS_BUCKET}/${PLAINTEXT_NAME}.enc"
       --kerberos-kms-key "$KEY"
       --kerberos-kms-key-keyring "$KEYRING"
       --kerberos-kms-key-location "$REGION"
       --kerberos-kms-key-project "$PROJECT")
fi

"${GCLOUD_COMMAND[@]}"

HADOOP_HOSTNAME=${PETNAME}-m

gcloud compute instances add-metadata "$HADOOP_HOSTNAME" \
  --metadata ssh-keys="$HADOOP_USER:$(cat ~/.ssh/google_compute_engine.pub)" \
  --zone "$ZONE"

for ((i=0; i < NUM_WORKERS; i++));
do
  gcloud compute instances add-metadata "${PETNAME}-w-${i}" \
    --metadata ssh-keys="$HADOOP_USER:$(cat ~/.ssh/google_compute_engine.pub)" \
    --zone "$ZONE"
done

echo "$HADOOP_HOSTNAME" > "${ENV_FILES_DIR}/name"

mkdir -p "${ENV_FILES_DIR}/conf"

SSH_OPTS=(-o 'UserKnownHostsFile=/dev/null' -o 'StrictHostKeyChecking=no' -i ~/.ssh/google_compute_engine)

scp "${SSH_OPTS[@]}" \
  "${HADOOP_USER}@${HADOOP_HOSTNAME}:/etc/hadoop/conf/*-site.xml" \
  "${ENV_FILES_DIR}/conf"

scp "${SSH_OPTS[@]}" \
  "${HADOOP_USER}@${HADOOP_HOSTNAME}:/etc/hive/conf/*-site.xml" \
  "${ENV_FILES_DIR}/conf"

ssh "${SSH_OPTS[@]}" -t \
  "${HADOOP_USER}@${HADOOP_HOSTNAME}" \
  "sudo systemctl restart hadoop-hdfs-namenode" || exit 1

cp ~/.ssh/google_compute_engine* "$ENV_FILES_DIR"

if [[ $KERBEROS == true ]]; then
  ssh "${SSH_OPTS[@]}" -t "${HADOOP_USER}@${HADOOP_HOSTNAME}" \
    'set -euo pipefail
    grep default_realm /etc/krb5.conf | awk '"'"'{print $3}'"'"' > ~/REALM
    sudo kadmin.local -q "addprinc -pw pxf gpadmin"
    sudo kadmin.local -q "xst -k ${HOME}/pxf.service.keytab gpadmin"
    sudo klist -e -k -t "${HOME}/pxf.service.keytab"
    sudo chown gpadmin "${HOME}/pxf.service.keytab"
    sudo addgroup gpadmin hdfs
    sudo addgroup gpadmin hadoop
    '

  scp "${SSH_OPTS[@]}" "${HADOOP_USER}@${HADOOP_HOSTNAME}":{~/{REALM,pxf.service.keytab},/etc/krb5.conf} "$ENV_FILES_DIR"
fi
