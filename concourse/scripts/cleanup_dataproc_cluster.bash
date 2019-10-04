#!/bin/bash

set -euo pipefail

# defaults
ENV_FILES_DIR=${ENV_FILES_DIR:-dataproc_env_files}
PROJECT=${GOOGLE_PROJECT_ID:-}
REGION=${GOOGLE_ZONE%-*} # lop off '-a', '-b', etc. from $GOOGLE_ZONE
REGION=${REGION:-us-central1}

gcloud config set project "$PROJECT"
gcloud auth activate-service-account --key-file=<(echo "$GOOGLE_CREDENTIALS")

set -x

PETNAME=$(< "${ENV_FILES_DIR}/name")
# --quiet to avoid interactive prompts
gcloud beta dataproc clusters --quiet \
  "--region=$REGION" delete "${PETNAME%-m}" # lop off trailing '-m'

