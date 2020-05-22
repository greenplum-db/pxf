#!/usr/bin/env bash

set -e

: "${GOOGLE_CREDENTIALS:?GOOGLE_CREDENTIALS must be set}"
: "${GCS_RELEASES_BUCKET:?GCS_RELEASES_BUCKET must be set}"
: "${GCS_OSL_PATH:?GCS_OSL_PATH must be set}"
: "${RELENG_BUCKET:?RELENG_BUCKET must be set}"
: "${RELENG_RELEASE_PATH:?RELENG_RELEASE_PATH must be set}"

echo "Authenticating with Google service account..."
gcloud auth activate-service-account --key-file=<(echo "${GOOGLE_CREDENTIALS}") >/dev/null 2>&1

mapfile -t osls < <(gsutil ls "gs://${GCS_BUCKET}/${GCS_OSL_PATH}" | tail +2)

sources=()
destinations=()
for osl in "${osls[@]}"; do
	: "${osl#pxf-}"
	version=${_%-OSL*}
	sources+=("gs://${GCS_RELEASES_BUCKET}/${GCS_RELEASES_PATH}/gp5/pxf-gp5-${version}-1.el6.x86_64.rpm")
	sources+=("gs://${GCS_RELEASES_BUCKET}/${GCS_RELEASES_PATH}/gp5/pxf-gp5-${version}-1.el7.x86_64.rpm")
	sources+=("gs://${GCS_RELEASES_BUCKET}/${GCS_RELEASES_PATH}/gp6/pxf-gp6-${version}-1.el7.x86_64.rpm")
	destinations+=("gs://${RELENG_BUCKET}/${RELENG_RELEASE_PATH}/pxf-gp5-${version}-1.el6.x86_64.rpm")
	destinations+=("gs://${RELENG_BUCKET}/${RELENG_RELEASE_PATH}/pxf-gp5-${version}-1.el7.x86_64.rpm")
	destinations+=("gs://${RELENG_BUCKET}/${RELENG_RELEASE_PATH}/pxf-gp6-${version}-1.el7.x86_64.rpm")
done

for ((i = 0; i < ${#sources[@]}; i++)); do
	if gsutil ls "${sources[$i]}" && ! gsutil ls "${destinations[$i]}"; then
		echo "Copying ${sources[$i]} to ${destinations[$i]}..."
		gsutil cp "${sources[$i]}" "${destinations[$i]}"
	fi
done
