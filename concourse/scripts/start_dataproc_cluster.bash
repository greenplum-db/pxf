#!/bin/bash

set -euo pipefail

# defaults
CCP_REAP_MINUTES=${ccp_reap_minutes:-120}
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

pip3 install petname

CLUSTER_NAME=${CLUSTER_NAME:-ccp-$(petname)}
# remove any . in the value and lower case it as dataproc names can not contain dots or capital letters
CLUSTER_NAME=${CLUSTER_NAME//./}
CLUSTER_NAME=$(echo ${CLUSTER_NAME} | tr '[:upper:]' '[:lower:]')

mkdir -p ~/.ssh
ssh-keygen -b 4096 -t rsa -f ~/.ssh/google_compute_engine -N "" -C "$HADOOP_USER"

gcloud config set project "$PROJECT"
gcloud auth activate-service-account --key-file=<(echo "$GOOGLE_CREDENTIALS")

set -x

PLAINTEXT=$(mktemp)
PLAINTEXT_NAME=$(basename "$PLAINTEXT")

# Initialize the dataproc service
GCLOUD_COMMAND=(gcloud dataproc clusters
  create "$CLUSTER_NAME"
  "--region=$REGION"
  --initialization-actions "$INITIALIZATION_SCRIPT"
  --subnet "projects/${PROJECT}/regions/${REGION}/subnetworks/$SUBNETWORK"
  "--master-machine-type=$MACHINE_TYPE"
  "--worker-machine-type=$MACHINE_TYPE"
  "--zone=$ZONE"
  "--tags=$INSTANCE_TAGS"
  "--num-workers=$NUM_WORKERS"
  --image-version "$IMAGE_VERSION"
  --properties "core:hadoop.proxyuser.${PROXY_USER}.hosts=*,core:hadoop.proxyuser.${PROXY_USER}.groups=*")

if [[ -n "$CCP_REAP_MINUTES" ]]; then
    GCLOUD_COMMAND+=(--max-age "${CCP_REAP_MINUTES}m")
fi

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

HADOOP_HOSTNAME=${CLUSTER_NAME}-m

gcloud compute instances add-metadata "$HADOOP_HOSTNAME" \
  --metadata "ssh-keys=$HADOOP_USER:$(< ~/.ssh/google_compute_engine.pub)" \
  --zone "$ZONE"

for ((i=0; i < NUM_WORKERS; i++));
do
  gcloud compute instances add-metadata "${CLUSTER_NAME}-w-${i}" \
    --metadata "ssh-keys=$HADOOP_USER:$(< ~/.ssh/google_compute_engine.pub)" \
    --zone "$ZONE"

  HADOOP_IP_ADDRESS=$(gcloud compute instances describe "${CLUSTER_NAME}-w-${i}" \
    --format='get(networkInterfaces[0].networkIP)' \
    --zone "$ZONE")

  echo "${HADOOP_IP_ADDRESS} ${CLUSTER_NAME}-w-${i} ${CLUSTER_NAME}-w-${i}.c.${PROJECT}.internal" >> dataproc_env_files/etc_hostfile
done

echo "$HADOOP_HOSTNAME" > "dataproc_env_files/name"

mkdir -p "dataproc_env_files/conf"

SSH_OPTS=(-o 'UserKnownHostsFile=/dev/null' -o 'StrictHostKeyChecking=no' -i ~/.ssh/google_compute_engine)

HADOOP_IP_ADDRESS=$(gcloud compute instances describe "${HADOOP_HOSTNAME}" \
  --format='get(networkInterfaces[0].networkIP)' \
  --zone "$ZONE")

echo "${HADOOP_IP_ADDRESS} ${HADOOP_HOSTNAME} ${HADOOP_HOSTNAME}.c.${PROJECT}.internal" >> dataproc_env_files/etc_hostfile

scp "${SSH_OPTS[@]}" \
  "${HADOOP_USER}@${HADOOP_IP_ADDRESS}:/etc/hadoop/conf/*-site.xml" \
  "dataproc_env_files/conf"

scp "${SSH_OPTS[@]}" \
  "${HADOOP_USER}@${HADOOP_IP_ADDRESS}:/etc/hive/conf/*-site.xml" \
  "dataproc_env_files/conf"

ssh "${SSH_OPTS[@]}" -t \
  "${HADOOP_USER}@${HADOOP_IP_ADDRESS}" \
  "sudo systemctl restart hadoop-hdfs-namenode" || exit 1

cp ~/.ssh/google_compute_engine* "dataproc_env_files"

if [[ $KERBEROS == true ]]; then
  ssh "${SSH_OPTS[@]}" -t "${HADOOP_USER}@${HADOOP_IP_ADDRESS}" \
    "set -euo pipefail
    grep default_realm /etc/krb5.conf | awk '{print \$3}' > ~/REALM
    sudo kadmin.local -q 'addprinc -pw pxf ${HADOOP_USER}'
    sudo kadmin.local -q \"xst -k \${HOME}/pxf.service.keytab ${HADOOP_USER}\"
    sudo klist -e -k -t \"\${HOME}/pxf.service.keytab\"
    sudo chown ${HADOOP_USER} \"\${HOME}/pxf.service.keytab\"
    sudo addgroup ${HADOOP_USER} hdfs
    sudo addgroup ${HADOOP_USER} hadoop
    "

  scp "${SSH_OPTS[@]}" "${HADOOP_USER}@${HADOOP_IP_ADDRESS}":{~/{REALM,pxf.service*.keytab},/etc/krb5.conf} \
    "dataproc_env_files"
fi
