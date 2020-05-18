#!/usr/bin/env bash

set -exo pipefail

: "${GOOGLE_CREDENTIALS:?GOOGLE_CREDENTIALS must be set}"
: "${GP_VER:?GP_VER must be set}"
: "${PXF_CERTIFICATION_FOLDER:?PXF_CERTIFICATION_FOLDER must be set}"

if [[ ! -f certification/certification.txt ]]; then
	echo 'ERROR: certification.txt file is not found.'
	exit 1
fi

certification=$(< certification/certification.txt)
echo "Found certification: $certification"

echo "Authenticating with Google service account..."
gcloud auth activate-service-account --key-file=<(echo "${GOOGLE_CREDENTIALS}") >/dev/null 2>&1

full_certification_dir="gs://${PXF_CERTIFICATION_FOLDER/gp${GP_VER}"
# find if the certification already exist to avoid error on duplicate publishing
existing_certification=$(gsutil list "${full_certification_dir}" | grep ${certification})
if [[ -n ${existing_certification} ]]; then
	echo "Found existing certification: ${existing_certification}"
	echo "Skipping upload, exiting successfully"
	exit 0
fi

full_certification_path="${full_certification_dir}/${certification}"
echo $(date) > /tmp/now
echo "Uploading certification to ${full_certification_path}"
gsutil cp /tmp/now "${full_certification_path}"

echo
echo "*****************************************************************************************"
echo "Successfully uploaded certification ${certification}"
echo "*****************************************************************************************"
echo
echo "Available certifications for Greenplum-${GP_VER}:"
echo "-----------------------------------------"
gsutil list ${full_certification_dir}

