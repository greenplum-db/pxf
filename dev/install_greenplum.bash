#!/usr/bin/env bash

CWDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

pushd ${CWDIR}/../downloads > /dev/null

if [[ -f /etc/centos-release ]]; then
    major_version=$(cat /etc/centos-release | tr -dc '0-9.'|cut -d \. -f1)
    ARTIFACT_OS="rhel${major_version}"
    LATEST_RPM=$(ls greenplum*${ARTIFACT_OS}*.rpm | sort -r | head -1)

    if [[ -z $LATEST_RPM ]]; then
        echo "ERROR: No greenplum RPM found in ${PWD}"
        popd > /dev/null
        exit 1
    fi

    echo "Installing GPDB from ${LATEST_RPM} ..."
    sudo rpm --quiet -ivh "${LATEST_RPM}"

else
    ARTIFACT_OS="ubuntu"
    LATEST_DEB=$(ls *greenplum*ubuntu*.deb | sort -r | head -1)

    if [[ -z $LATEST_DEB ]]; then
        echo "ERROR: No greenplum DEB found in ${PWD}"
        popd > /dev/null
        exit 1
    fi

    echo "Installing GPDB from ${LATEST_DEB} ..."
    # apt wants a full path
	sudo apt install -qq "${PWD}/${LATEST_DEB}"
fi

popd > /dev/null