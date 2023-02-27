#!/usr/bin/env bash

set -e

: "${PXF_OSL_FILE_PREFIX:?PXF_OSL_FILE_PREFIX must be set}"
: "${PXF_ODP_FILE_PREFIX:?PXF_ODP_FILE_PREFIX must be set}"
: "${RELENG_DROP_URL:?RELENG_DROP_URL must be set}"
: "${RELENG_OSL_DROP_URL:?RELENG_OSL_DROP_URL must be set}"
: "${RELENG_ODP_DROP_URL:?RELENG_ODP_DROP_URL must be set}"
: "${PXF_SLACK_CHANNEL_NAME:?PXF_SLACK_CHANNEL_NAME must be set}"
: "${PXF_SLACK_CHANNEL_LINK:?PXF_SLACK_CHANNEL_LINK must be set}"

function fail() {
  echo "Error: $1"
  exit 1
}

# determine PXF version to ship
[[ -f pxf_shipit_file/version ]] || fail "Expected shipit file not found"
version=$(<pxf_shipit_file/version)

# compute artifact URLs for GPDB5
pxf_gp5_tarball_releng_url="${RELENG_DROP_URL}/gpdb5/pxf-gp5-${version}-el7.x86_64.tar.gz"
pxf_gp5_el7_releng_url="${RELENG_DROP_URL}/gpdb5/pxf-gp5-${version}-2.el7.x86_64.rpm"
pxf_gp5_osl_releng_url="${RELENG_OSL_DROP_URL}/gpdb5/${PXF_OSL_FILE_PREFIX}_${version}_GA.txt"
pxf_gp5_odp_releng_url="${RELENG_ODP_DROP_URL}/gpdb5/${PXF_ODP_FILE_PREFIX}-${version}-ODP.tar.gz"

# compute artifact URLs for GPDB6
pxf_gp6_tarball_releng_url="${RELENG_DROP_URL}/gpdb6/pxf-gp6-${version}-el7.x86_64.tar.gz"
pxf_gp6_el7_releng_url="${RELENG_DROP_URL}/gpdb6/pxf-gp6-${version}-2.el7.x86_64.rpm"
pxf_gp6_el8_releng_url="${RELENG_DROP_URL}/gpdb6/pxf-gp6-${version}-2.el8.x86_64.rpm"
pxf_gp6_ubuntu18_releng_url="${RELENG_DROP_URL}/gpdb6/pxf-gp6-${version}-2-ubuntu18.04-amd64.deb"
pxf_gp6_osl_releng_url="${RELENG_OSL_DROP_URL}/gpdb6/${PXF_OSL_FILE_PREFIX}_${version}_GA.txt"
pxf_gp6_odp_releng_url="${RELENG_ODP_DROP_URL}/gpdb6/${PXF_ODP_FILE_PREFIX}-${version}-ODP.tar.gz"

# compute artifact URLs for GPDB7
pxf_gp7_el8_releng_url="${RELENG_DROP_URL}/gpdb7/pxf-gp7-${version}-2.el8.x86_64.rpm"
pxf_gp7_osl_releng_url="${RELENG_OSL_DROP_URL}/gpdb7/${PXF_OSL_FILE_PREFIX}_${version}_GA.txt"
pxf_gp7_odp_releng_url="${RELENG_ODP_DROP_URL}/gpdb7/${PXF_ODP_FILE_PREFIX}-${version}-ODP.tar.gz"

echo "Generating Slack Message"

# generate Slack message
tee pxf_artifacts/slack_message.txt << EOF
Hi @gp-releng,

The new PXF release ${version} is ready to be published to VMware Tanzu Network.

We have uploaded PXF release artifacts to the following RelEng locations:

The GPDB5 artifacts are:
* ${pxf_gp5_tarball_releng_url}
* ${pxf_gp5_el7_releng_url}
* ${pxf_gp5_osl_releng_url}
* ${pxf_gp5_odp_releng_url}

The GPDB6 artifacts are:
* ${pxf_gp6_tarball_releng_url}
* ${pxf_gp6_el7_releng_url}
* ${pxf_gp6_el8_releng_url}
* ${pxf_gp6_ubuntu18_releng_url}
* ${pxf_gp6_osl_releng_url}
* ${pxf_gp6_odp_releng_url}

The GPDB7 artifacts are:
* ${pxf_gp7_el8_releng_url}
* ${pxf_gp7_osl_releng_url}
* ${pxf_gp7_odp_releng_url}

Can you please upload the artifacts and the OSL / ODP files to the Greenplum Tanzu Network Release for our product, PXF?
The OSL file should appear as "Open Source Licenses for PXF ${version}".

Once the artifacts are published to the Tanzu Network site, please post a message in the #${PXF_SLACK_CHANNEL_NAME} slack channel:
${PXF_SLACK_CHANNEL_LINK}

EOF
