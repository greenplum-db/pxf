#!/bin/bash

set -euo pipefail

PROJECT=${GOOGLE_PROJECT_ID:-}

gcloud config set project "$PROJECT"
gcloud auth activate-service-account --key-file=<(echo "$GOOGLE_CREDENTIALS")

gsutil cp -r gs://data-gpdb-ud/configuration/ambari-cloud/ ambari_env_files