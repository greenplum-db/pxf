#!/bin/bash

# The ambari configuration is a static configuration residing on a google cloud
# bucket. All the required configuration resides in the bucket including:
# - /etc/hadoop/conf/*-site.xml
# - /etc/hive/conf/*-site.xml
# - keytab
# - REALM (a file with the name of the REALM for this kerberized cluster)
# - HADOOP_USER (a file with the name of the hadoop user)
# - etc_hostfile (a file with the /etc/hosts configuration for this cluster)
# - krb5.conf
# To load the static configuration, prepare all the files above and
#    gsutil cp -r configuration gs://data-gpdb-ud/configuration/ambari-cloud/

set -euo pipefail

PROJECT=${GOOGLE_PROJECT_ID:-}

gcloud config set project "$PROJECT"
gcloud auth activate-service-account --key-file=<(echo "$GOOGLE_CREDENTIALS")

gsutil cp -r gs://data-gpdb-ud/configuration/ambari-cloud/* ambari_env_files