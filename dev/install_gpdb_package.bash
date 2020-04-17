#!/usr/bin/env bash

CWDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# TODO: check if gpadmin-limits.conf already exists and bail out if it does
>/etc/security/limits.d/gpadmin-limits.conf cat <<-EOF
gpadmin soft core unlimited
gpadmin soft nproc 131072
gpadmin soft nofile 65536
EOF

>>/home/gpadmin/.bash_profile cat <<EOF
export PS1="[\u@\h \W]\$ "
source /opt/rh/devtoolset-6/enable
export HADOOP_ROOT=~/workspace/singlecluster
export PXF_HOME=/usr/local/greenplum-db-devel/pxf
export GPHD_ROOT=~/workspace/singlecluster
export BUILD_PARAMS="-x test"
export LANG=en_US.UTF-8
export JAVA_HOME=/etc/alternatives/java_sdk
export SLAVES=1
export GOPATH=/opt/go
export PATH=\${PXF_HOME}/bin:\${GPHD_ROOT}/hadoop/bin:\${GOPATH}/bin:/usr/local/go/bin:\$PATH
EOF

pushd ${CWDIR}/../downloads > /dev/null
echo $PWD
ls

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

sudo chown -R gpadmin:gpadmin /usr/local/greenplum-db*
popd