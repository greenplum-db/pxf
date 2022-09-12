#!/usr/bin/env bash

set -eox pipefail

CWDIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source "${CWDIR}/pxf_common.bash"

GPDB_PKG_DIR=gpdb_package
GPDB_VERSION=$(<"${GPDB_PKG_DIR}/version")

function install_gpdb() {
    local pkg_file
    if command -v rpm; then
        pkg_file=$(find "${GPDB_PKG_DIR}" -name "greenplum-db-${GPDB_VERSION}-rhel*-x86_64.rpm")
        echo "Installing RPM ${pkg_file}..."
        rpm --quiet -ivh "${pkg_file}" >/dev/null
    elif command -v apt-get; then
        # apt-get wants a full path
        pkg_file=$(find "${PWD}/${GPDB_PKG_DIR}" -name "greenplum-db-${GPDB_VERSION}-ubuntu18.04-amd64.deb")
        echo "Installing DEB ${pkg_file}..."
        apt-get install -qq "${pkg_file}" >/dev/null
    else
        echo "Unsupported operating system '$(source /etc/os-release && echo "${PRETTY_NAME}")'. Exiting..."
        exit 1
    fi
}

function compile_pxf() {
    source "${GPHOME}/greenplum_path.sh"

    # CentOS releases contain a /etc/redhat-release which is symlinked to /etc/centos-release
    if [[ -f /etc/redhat-release ]]; then
        MAKE_TARGET="rpm-tar"
    elif [[ -f /etc/debian_version ]]; then
        MAKE_TARGET="deb-tar"
    else
        echo "Unsupported operating system '$(source /etc/os-release && echo "${PRETTY_NAME}")'. Exiting..."
        exit 1
    fi

    bash -c "
        source ~/.pxfrc
        VENDOR='${VENDOR}' LICENSE='${LICENSE}' make -C '${PWD}/pxf_src' ${MAKE_TARGET}
    "
}

function package_pxf() {
    # verify contents
    if [[ -f /etc/redhat-release ]]; then
        DIST_DIR=distrpm
    elif [[ -f /etc/debian_version ]]; then
        DIST_DIR=distdeb
    else
        echo "Unsupported operating system '$(source /etc/os-release && echo "${PRETTY_NAME}")'. Exiting..."
        exit 1
    fi

    ls -al pxf_src/build/${DIST_DIR}
    tar -tvzf pxf_src/build/${DIST_DIR}/pxf-*.tar.gz
    cp pxf_src/build/${DIST_DIR}/pxf-*.tar.gz dist
}

function package_pxf_fdw() {
    # verify contents
    if [[ -f /etc/redhat-release ]]; then
        DIST_DIR=distrpm
    elif [[ -f /etc/debian_version ]]; then
        DIST_DIR=distdeb
    else
        echo "Unsupported operating system '$(source /etc/os-release && echo "${PRETTY_NAME}")'. Exiting..."
        exit 1
    fi

    # build PXF FDW extension separately
    bash -c "
        source ~/.pxfrc
        make -C '${PWD}/pxf_src/fdw' stage
    "
    # get the filename of previously built main PXF tarball to use its full name as a suffix
    local pxf_tarball=$(ls pxf_src/build/${DIST_DIR}/pxf-*.tar.gz | xargs -n 1 basename)
    local pxf_fdw_tarball="pxf_src/build/${DIST_DIR}/pxf-fdw${pxf_tarball#pxf}"

    # build the tarball and copy it to the output directory
    ls -al pxf_src/fdw/build/stage/fdw
    tar -cvzf "${pxf_fdw_tarball}" -C pxf_src/fdw/build/stage/fdw .
    cp pxf_src/build/${DIST_DIR}/pxf-fdw-*.tar.gz dist
}

install_gpdb
# installation of GPDB from RPM/DEB doesn't ensure that the installation location will match the version
# given in the gpdb_package, so set the GPHOME after installation
GPHOME=$(find /usr/local/ -name "greenplum-db-${GPDB_VERSION}*")

# To Be Removed
# This is temporary to change. Since The PR to incorporate these changes isn't merged in GPDB yet.

HEADER_FILE_GP7=pxf_gp7_headerfile
if [[ ${GPDB_VERSION:0:1} -ge 7 ]]; then
 #PROJECT=${GOOGLE_PROJECT_ID:-}

 #gcloud config set project "$PROJECT"
 #gcloud auth activate-service-account --key-file=<(echo "$GOOGLE_CREDENTIALS")

  mkdir ${GPHOME}/include/postgresql/server/extension/gp_exttable_fdw
  cp ${HEADER_FILE_GP7}/extaccess.h  ${GPHOME}/include/postgresql/server/extension/gp_exttable_fdw
fi

inflate_dependencies
compile_pxf
package_pxf
# package FDW extension for GP6 separately for downstream testing as it is not shipped
if [[ ${GPDB_VERSION%%.*} == 6 ]]; then
    package_pxf_fdw
fi
