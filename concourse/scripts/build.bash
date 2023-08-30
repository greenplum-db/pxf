#!/usr/bin/env bash

set -eox pipefail

echo "Count of files under /usr/local/go"
find /usr/local/go -type f | wc -l

chmod --recursive a-w /usr/local/go

CWDIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source "${CWDIR}/pxf_common.bash"

GPDB_PKG_DIR=gpdb_package
GPDB_VERSION=$(<"${GPDB_PKG_DIR}/version")

function install_gpdb() {
    local pkg_file
    if command -v rpm; then
        # For GP7 and above, a new rhel8 & rocky8 distro identifier
        # (el8) has been introduced.
        if [[ ${GPDB_VERSION%%.*} -ge 7 ]]; then
            DISTRO_MATCHING_PATTERN="el"
        else
            DISTRO_MATCHING_PATTERN="r"
        fi
        pkg_file=$(find "${GPDB_PKG_DIR}" -name "greenplum-db-${GPDB_VERSION}-${DISTRO_MATCHING_PATTERN}*-x86_64.rpm")

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
inflate_dependencies
compile_pxf
package_pxf
# package FDW extension for GP6 separately for downstream testing as it is not shipped
if [[ ${GPDB_VERSION%%.*} == 6 ]]; then
    package_pxf_fdw
fi
