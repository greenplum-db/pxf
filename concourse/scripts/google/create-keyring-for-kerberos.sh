#!/bin/bash

set -ex

cat << EOF
usage: ./create-keyring-for-kerberos.sh

Use this script to create the keyring and cryptographic keys to
be used by the kerberized dataproc cluster. You need to export
the following parameters:

export PROJECT=[YOUR_GCP_PROJECT]
export REGION=us-central1 # For example us-central1
export KEYRING=dataproc-kerberos # For example dataproc-kerberos
export KEY=dataproc-kerberos-key # For example dataproc-kerberos-key
export SERVICE_ACCOUNT=[YOUR_SERVICE_ACCOUNT]

EOF

: "${KEYRING?"KEYRING is required. export KEYRING=[YOUR_KEYRING]"}"
: "${KEY?"KEY is required. export KEYRING=[YOUR_KEY_NAME]"}"
: "${PROJECT?"PROJECT is required. export KEYRING=[YOUR_GCP_PROJECT]"}"
: "${REGION?"REGION is required. export REGION=[REGION]"}"
: "${SERVICE_ACCOUNT?"SERVICE_ACCOUNT is required. export SERVICE_ACCOUNT=[YOUR_SERVICE_ACCOUNT]"}"

gcloud --project "${PROJECT}" \
  kms keyrings create "${KEYRING}" \
  --location "${REGION}"

gcloud --project "${PROJECT}" \
  kms keys create "${KEY}" \
  --location "${REGION}" \
  --keyring "${KEYRING}" \
  --purpose encryption

gcloud --project "${PROJECT}" \
  kms keys add-iam-policy-binding dataproc-kerberos-key \
  --location "${REGION}" \
  --keyring "${KEYRING}" \
  --member serviceAccount:"${SERVICE_ACCOUNT}" \
  --role roles/cloudkms.cryptoKeyEncrypterDecrypter
