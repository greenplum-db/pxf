#!/usr/bin/env bash

CWDIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
PPE_VERSION=$(<"${CWDIR}/version")

function check_gphome() {
    if [[ -z "${GPHOME}" ]]; then
        echo "Error: environment variable GPHOME is not set"
        exit 1
    fi

    if [[ ! -f ${GPHOME}/greenplum_path.sh ]]; then
        echo "Error: environment variable GPHOME must be set to a valid Greenplum installation"
        exit 1
    fi
}

function remove_old_component() {
    echo "Removing old PXF Protocol Extension ..."

}

function install_new_component() {
    echo "Installing new PXF Protocol Extension ..."
    cp -av ${CWDIR}/{lib,share} ${GPHOME}

    local error_code=$?
    if [ ${error_code} -ne 0 ]; then
        echo "Error: Installation failed"
        exit error_code
    fi
}

if [[ $1 == "--help" ]]; then
    echo "Set GPHOME environment variable and run this script to install the PXF Protocol Extension"
    exit 0
fi

check_gphome
remove_old_component
install_new_component

echo "Successfully installed PXF Protocol Extension (version ${PPE_VERSION}) into ${GPHOME}"
