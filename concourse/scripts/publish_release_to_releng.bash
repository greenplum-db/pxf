#!/usr/bin/env bash

set -e

: "${GOOGLE_CREDENTIALS:?GOOGLE_CREDENTIALS must be set}"
: "${GCS_RELEASES_BUCKET:?GCS_RELEASES_BUCKET must be set}"
: "${GCS_RELEASES_PATH:?GCS_RELEASES_PATH must be set}"
: "${GCS_OSL_PATH:?GCS_OSL_PATH must be set}"
: "${RELENG_RELEASES_BUCKET:?RELENG_RELEASES_BUCKET must be set}"
: "${RELENG_RELEASES_PATH:?RELENG_RELEASES_PATH must be set}"

copy_to_releng() {
  local source=gs://${GCS_RELEASES_BUCKET}/${GCS_RELEASES_PATH}/gp${1}/${2}
  local destination=gs://${RELENG_RELEASES_BUCKET}/${RELENG_RELEASES_PATH}/${2}
  if gsutil ls "${source}" >/dev/null 2>&1 && ! gsutil ls "${destination}" >/dev/null 2>&1; then
    echo "Copying ${source} to ${destination}..."
    gsutil cp "${source}" "${destination}"
    gsutil ls "${destination}"
  fi
}

echo "Authenticating with Google service account..."
gcloud auth activate-service-account --key-file=<(echo "${GOOGLE_CREDENTIALS}") >/dev/null 2>&1

mapfile -t osls < <(gsutil ls "gs://${GCS_RELEASES_BUCKET}/${GCS_OSL_PATH}" | tail +2)

for osl in "${osls[@]}"; do
  : "${osl##*pxf-}"
  version=${_%-OSL*}
  copy_to_releng 5 "pxf-gp5-${version}-1.el6.x86_64.rpm"
  copy_to_releng 5 "pxf-gp5-${version}-1.el7.x86_64.rpm"
  copy_to_releng 6 "pxf-gp6-${version}-1.el7.x86_64.rpm"
done
