#!/bin/bash -l

set -eox pipefail

GPHOME="/usr/local/greenplum-db-devel"
export PXF_ARTIFACTS_DIR=$(pwd)/${OUTPUT_ARTIFACT_DIR}

_main() {
	export TERM=xterm
	export BUILD_NUMBER="${TARGET_OS}"
	export PXF_HOME="${GPHOME}/pxf"
	export JAVA_TOOL_OPTIONS=-Dfile.encoding=UTF8
	yum install -y go
	export PATH="$PATH:$(cd ~; pwd)/go/bin"
	go get github.com/golang/dep/cmd/dep
	go get github.com/onsi/ginkgo/ginkgo
	pushd pxf_src/server
		make install
		make version > ${PXF_HOME}/version
	popd
	# Create tarball for PXF
	pushd ${GPHOME}
		tar -czf ${PXF_ARTIFACTS_DIR}/pxf.tar.gz pxf
	popd
}

_main
