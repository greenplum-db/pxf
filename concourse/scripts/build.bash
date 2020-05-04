#!/usr/bin/env bash

set -eox pipefail

: "${TARGET_OS:?TARGET_OS must be set}"

GPDB_PKG_DIR=gpdb_package
GPDB_VERSION=$(<"${GPDB_PKG_DIR}/version")
GPHOME=/usr/local/greenplum-db-${GPDB_VERSION}

function install_gpdb() {
    if [[ ${TARGET_OS} == rhel* ]]; then
	    rpm --quiet -ivh "${GPDB_PKG_DIR}/greenplum-db-${GPDB_VERSION}"-rhel*-x86_64.rpm
    elif [[ ${TARGET_OS} == ubuntu* ]]; then
	    # apt wants a full path
	    apt install -qq "${PWD}/${GPDB_PKG_DIR}/greenplum-db-${GPDB_VERSION}-ubuntu18.04-amd64.deb"
    else
	    echo "Unsupported operating system ${TARGET_OS}. Exiting..."
	    exit 1
    fi
}

function compile_pxf_protocol_extension() {
    source "${GPHOME}/greenplum_path.sh"
    if [[ ${TARGET_OS} == "rhel6" ]]; then
        source /opt/gcc_env.sh
    fi
    bash -c "
        source ~/.pxfrc
        make -C '${PWD}/pxf_src' tar
    "
}

function package_pxf_protocol_extension() {
    # verify contents
    ls -al pxf_src/build/dist
    tar -tvzf pxf_src/build/dist/pxf-*.tar.gz
    cp pxf_src/build/dist/pxf-*.tar.gz dist
}

install_gpdb
compile_pxf_protocol_extension
package_pxf_protocol_extension
