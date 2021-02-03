#!/usr/bin/env bash

set -e

: "${PXF_OSL_FILE_PREFIX:?PXF_OSL_FILE_PREFIX must be set}"
: "${PXF_ODP_FILE_PREFIX:?PXF_ODP_FILE_PREFIX must be set}"
: "${RELENG_GP5_DROP_URL:?RELENG_GP5_DROP_URL must be set}"
: "${RELENG_GP6_DROP_URL:?RELENG_GP6_DROP_URL must be set}"
: "${RELENG_OSL_DROP_URL:?RELENG_OSL_DROP_URL must be set}"
: "${RELENG_ODP_DROP_URL:?RELENG_ODP_DROP_URL must be set}"

function fail() {
  echo "Error: $1"
  exit 1
}

# determine PXF version to ship
[[ -f pxf_shipit_file/version ]] || fail "Expected shipit file not found"
version=$(<pxf_shipit_file/version)

# compute artifact URLs
pxf_gp5_el7_releng_url="${RELENG_GP5_DROP_URL}/pxf-gp5-${version}-2.el7.x86_64.rpm"
pxf_gp6_el7_releng_url="${RELENG_GP6_DROP_URL}/pxf-gp6-${version}-2.el7.x86_64.rpm"
pxf_gp6_ubuntu18_releng_url="${RELENG_GP6_DROP_URL}/pxf-gp6-${version}-2-ubuntu18.04-amd64.deb"
pxf_osl_releng_url="${RELENG_OSL_DROP_URL}/${PXF_OSL_FILE_PREFIX}_${version}_GA.txt"
pxf_odp_releng_url="${RELENG_ODP_DROP_URL}/${PXF_ODP_FILE_PREFIX}-${version}-ODP.tar.gz"
pxf_gp5_tarball_releng_url="${RELENG_GP5_DROP_URL}/pxf-${version}.tar.gz"
pxf_gp6_tarball_releng_url="${RELENG_GP6_DROP_URL}/pxf-${version}.tar.gz"

echo "Generating Releng Email"

# generate email subject
cat > pxf_artifacts/email_subject.txt << EOF
PXF Release ${version} is ready to be published to Tanzu Network
EOF

# generate email body
cat > pxf_artifacts/email_body.txt << EOF
Hi GPDB Releng Team,

The new PXF release ${version} is ready to be published to VMware Tanzu Network.

We have uploaded PXF release artifacts to the following RelEng locations:

${pxf_gp5_tarball_releng_url}
${pxf_gp5_el7_releng_url}
${pxf_gp6_tarball_releng_url}
${pxf_gp6_el7_releng_url}
${pxf_gp6_ubuntu18_releng_url}
${pxf_osl_releng_url}
${pxf_odp_releng_url}

The OSL file is also attached to this email.

Can you please upload the artifacts and the OSL / ODP files to the Greenplum Tanzu Network Release for our product, PXF?
The OSL file should appear as "Open Source Licenses for PXF ${version}".

Thank you,
PXF Team (pvtl-gp-ud@vmware.com)

---
Generated by UD CI build job: ${BUILD_PIPELINE_NAME}/jobs/${BUILD_JOB_NAME}/builds/${BUILD_NAME}
UD CI build job link: ${ATC_EXTERNAL_URL}/teams/main/pipelines/${BUILD_PIPELINE_NAME}/jobs/${BUILD_JOB_NAME}/builds/${BUILD_NAME}

EOF

echo "Subject: $(< pxf_artifacts/email_subject.txt)"
cat pxf_artifacts/email_body.txt
