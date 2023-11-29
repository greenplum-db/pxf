#!/usr/bin/env bash

set -e

: "${GCS_RELEASES_BUCKET:?GCS_RELEASES_BUCKET must be set}"
: "${GCS_RELEASES_PATH:?GCS_RELEASES_PATH must be set}"
: "${GCS_OSL_PATH:?GCS_OSL_PATH must be set}"
: "${GCS_ODP_PATH:?GCS_ODP_PATH must be set}"
: "${GOOGLE_CREDENTIALS:?GOOGLE_CREDENTIALS must be set}"


function fail() {
  echo "Error: $1"
  exit 1
}

check_artifacts() {
  for artifact in "${artifacts[@]}"; do
    gsutil ls "gs://${GCS_RELEASES_BUCKET}/${artifact}" >/dev/null 2>&1 || fail "Expected artifact not found: ${artifact}"
  done
}

copy_artifacts_to_local() {
  for artifact in "${artifacts[@]}"; do
    local source="gs://${GCS_RELEASES_BUCKET}/${artifact}"
    echo "Copying ${source} to ${destination_dir} ..."
    gsutil cp "${source}" "${destination_dir}"
  done
}

# authenticate to Google
echo "Authenticating with Google service account..."
gcloud auth activate-service-account --key-file=<(echo "${GOOGLE_CREDENTIALS}") >/dev/null 2>&1

# determine PXF version to ship
[[ -f pxf_shipit_file/version ]] || fail "Expected shipit file not found"
version=$(<pxf_shipit_file/version)

echo "Ship directive for PXF-${version} from : $(<pxf_shipit_file/*.txt)"

# define artifacts to copy
artifacts=(
  "${GCS_RELEASES_PATH}/gp5/pxf-gp5-${version}-1.el7.x86_64.rpm"
  "${GCS_RELEASES_PATH}/gp6/pxf-gp6-${version}-1.el7.x86_64.rpm"
  "${GCS_RELEASES_PATH}/gp6/pxf-gp6-${version}-1.el8.x86_64.rpm"
  "${GCS_RELEASES_PATH}/gp6/pxf-gp6-${version}-1.el9.x86_64.rpm"
  "${GCS_RELEASES_PATH}/gp6/pxf-gp6-${version}-1-ubuntu18.04-amd64.deb"
  "${GCS_RELEASES_PATH}/gp7/pxf-gp7-${version}-1.el8.x86_64.rpm"
  "${GCS_RELEASES_PATH}/gp7/pxf-gp7-${version}-1.el9.x86_64.rpm"
  "${GCS_OSL_PATH}/open_source_license_VMware_Greenplum_Platform_Extension_Framework_${version}_GA.txt"
  "${GCS_ODP_PATH}/VMware-greenplum-pxf-${version}-ODP.tar.gz"
)

# check and copy artifacts to local destination
destination_dir=pxf_artifacts
check_artifacts
copy_artifacts_to_local

echo "Contents of the destination directory ${destination_dir}:"
ls -al ${destination_dir}
