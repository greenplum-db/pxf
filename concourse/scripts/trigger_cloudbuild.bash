#!/bin/bash

set -euo pipefail

COMMIT_SHA=$(cat pxf_src/.git/ref)
echo "Trigger builds with PXF SHA-1: ${COMMIT_SHA}"

mkdir -p metadata
echo "${COMMIT_SHA}" > metadata/git-sha-1

gcloud config set project "$GOOGLE_PROJECT_ID"
gcloud auth activate-service-account --key-file=<(echo "$GOOGLE_CREDENTIALS")

# Don't quote $TRIGGER_LIST to allow array
TRIGGER_NAMES=(${TRIGGER_LIST})

for trigger in "${TRIGGER_NAMES[@]}"
do
  echo "Trigger cloud build for ${trigger}"
  gcloud beta builds triggers run --quiet "${trigger}" --sha="${COMMIT_SHA}"
done
