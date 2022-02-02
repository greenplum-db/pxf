#!/usr/bin/env bash

CWDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

pushd ${CWDIR}/../downloads > /dev/null

# CentOS releases contain a /etc/redhat-release which is symlinked to /etc/centos-release
if [[ -f /etc/redhat-release ]]; then
    major_version=$(cat /etc/redhat-release | tr -dc '0-9.'|cut -d \. -f1)
    ARTIFACT_OS="rhel${major_version}"
    LATEST_RPM=$(ls greenplum*${ARTIFACT_OS}*.rpm | sort -r | head -1)

    if [[ -z $LATEST_RPM ]]; then
        echo "ERROR: No greenplum RPM found in ${PWD}"
        popd > /dev/null
        exit 1
    fi

    echo "Installing GPDB from ${LATEST_RPM} ..."
    sudo rpm --quiet -ivh "${LATEST_RPM}"

elif [[ -f /etc/debian_version ]]; then
    ARTIFACT_OS="ubuntu"
    LATEST_DEB=$(ls *greenplum*ubuntu*.deb | sort -r | head -1)

    if [[ -z $LATEST_DEB ]]; then
        echo "ERROR: No greenplum DEB found in ${PWD}"
        popd > /dev/null
        exit 1
    fi

    echo "Installing GPDB from ${LATEST_DEB} ..."
    # apt-get wants a full path
    sudo apt-get install -qq "${PWD}/${LATEST_DEB}"
else
    echo "Unsupported operating system '$(source /etc/os-release && echo "${PRETTY_NAME}")'. Exiting..."
    exit 1
fi

popd > /dev/null
