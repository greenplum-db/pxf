#!/bin/bash

set -euo pipefail

# defaults
HADOOP_USER=${HADOOP_USER:-gpadmin}
IMAGE_VERSION=${IMAGE_VERSION:-1.3}
INITIALIZATION_SCRIPT=${INITIALIZATION_SCRIPT:-gs://pxf-perf/scripts/initialization-for-kerberos.sh}
INSTANCE_TAGS=${INSTANCE_TAGS:-bosh-network,outbound-through-nat,tag-concourse-dynamic}
KERBEROS=${KERBEROS:-false}
KEYRING=${KEYRING:-dataproc-kerberos}
KEY=${KEY:-dataproc-kerberos-test}
MACHINE_TYPE=${MACHINE_TYPE:-n1-standard-2}
NO_ADDRESS=${NO_ADDRESS:-true}
NUM_WORKERS=${NUM_WORKERS:-2}
PROJECT=${GOOGLE_PROJECT_ID:-}
PROXY_USER=${PROXY_USER:-gpadmin}
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
  --subnet "projects/${PROJECT}/regions/${REGION}/subnetworks/$SUBNETWORK"
  "--master-machine-type=$MACHINE_TYPE"
  "--worker-machine-type=$MACHINE_TYPE"
  "--zone=$ZONE"
  "--tags=$INSTANCE_TAGS"
  "--num-workers=$NUM_WORKERS"
  --image-version "$IMAGE_VERSION"
  --properties "core:hadoop.proxyuser.${PROXY_USER}.hosts=*,core:hadoop.proxyuser.${PROXY_USER}.groups=*")

if [[ $NO_ADDRESS == true ]]; then
    GCLOUD_COMMAND+=(--no-address)
fi

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
  --metadata "ssh-keys=$HADOOP_USER:$(< ~/.ssh/google_compute_engine.pub)" \
  --zone "$ZONE"

for ((i=0; i < NUM_WORKERS; i++));
do
  gcloud compute instances add-metadata "${PETNAME}-w-${i}" \
    --metadata "ssh-keys=$HADOOP_USER:$(< ~/.ssh/google_compute_engine.pub)" \
    --zone "$ZONE"
done

echo "$HADOOP_HOSTNAME" > "dataproc_env_files/name"

mkdir -p "dataproc_env_files/conf"

gcloud compute scp \
  "${HADOOP_USER}@${HADOOP_HOSTNAME}:/etc/hadoop/conf/*-site.xml" \
  "dataproc_env_files/conf"

gcloud compute scp \
  "${HADOOP_USER}@${HADOOP_HOSTNAME}:/etc/hive/conf/*-site.xml" \
  "dataproc_env_files/conf"

gcloud compute ssh "${HADOOP_USER}@${HADOOP_HOSTNAME}" \
  --command="sudo systemctl restart hadoop-hdfs-namenode" || exit 1

cp ~/.ssh/google_compute_engine* "dataproc_env_files"

if [[ $KERBEROS == true ]]; then
  gcloud compute ssh "${HADOOP_USER}@${HADOOP_HOSTNAME}" \
    -- -t \
    'set -euo pipefail
    grep default_realm /etc/krb5.conf | awk '"'"'{print $3}'"'"' > ~/REALM
    sudo kadmin.local -q "addprinc -pw pxf gpadmin"
    sudo kadmin.local -q "xst -k ${HOME}/pxf.service.keytab gpadmin"
    sudo klist -e -k -t "${HOME}/pxf.service.keytab"
    sudo chown gpadmin "${HOME}/pxf.service.keytab"
    sudo addgroup gpadmin hdfs
    sudo addgroup gpadmin hadoop
    '

  gcloud compute scp \
    "${HADOOP_USER}@${HADOOP_HOSTNAME}":{~/{REALM,pxf.service.keytab},/etc/krb5.conf} \
    "dataproc_env_files"
fi
