#!/usr/bin/env bash

set -eox pipefail

: "${TARGET_OS:?TARGET_OS must be set}"

CWDIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source "${CWDIR}/pxf_common.bash"

GPDB_PKG_DIR=gpdb_package
GPDB_VERSION=$(<"${GPDB_PKG_DIR}/version")
GPHOME=/usr/local/greenplum-db-${GPDB_VERSION}

function install_gpdb() {
    local pkg_file
    if command -v rpm; then
	    pkg_file=$(find "${GPDB_PKG_DIR}" -name "greenplum-db-${GPDB_VERSION}-rhel*-x86_64.rpm")
	    echo "Installing RPM ${pkg_file}..."
	    rpm --quiet -ivh "${pkg_file}" >/dev/null
    elif command -v apt; then
	    # apt wants a full path
	    pkg_file=$(find "${PWD}/${GPDB_PKG_DIR}" -name "greenplum-db-${GPDB_VERSION}-ubuntu18.04-amd64.deb")
	    echo "Installing DEB ${pkg_file}..."
	    apt install -qq "${pkg_file}" >/dev/null
    else
	    echo "Unsupported operating system ${TARGET_OS}. Exiting..."
	    exit 1
    fi
}

function compile_pxf() {
    source "${GPHOME}/greenplum_path.sh"
    if [[ ${TARGET_OS} == "rhel6" ]]; then
        source /opt/gcc_env.sh
    fi
    bash -c "
        source ~/.pxfrc
        VENDOR='${VENDOR}' LICENSE='${LICENSE}' make -C '${PWD}/pxf_src' rpm-tar
    "
}

function package_pxf() {
    # verify contents
    ls -al pxf_src/build/distrpm
    tar -tvzf pxf_src/build/distrpm/pxf-*.tar.gz
    cp pxf_src/build/distrpm/pxf-*.tar.gz dist
}

install_gpdb
inflate_dependencies
compile_pxf
package_pxf
