#!/bin/bash

set -eoux pipefail

GPHOME=/usr/local/greenplum-db-devel
PXF_ARTIFACTS_DIR=${PWD}/${OUTPUT_ARTIFACT_DIR}

# use a login shell for setting environment
bash --login -c "
	export PXF_HOME=${GPHOME}/pxf
	make -C '${PWD}/pxf_src' test install
"

# Create tarball for PXF
tar -C "${GPHOME}" -czf "${PXF_ARTIFACTS_DIR}/pxf.tar.gz" pxf
