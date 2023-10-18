#!/bin/bash

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# setup environment for gpadmin
#export PS1="[\u@\h \W]\$ "
#export HADOOP_ROOT=~/workspace/singlecluster
#export PXF_JVM_OPTS="-Xmx512m -Xms256m"
#export BUILD_PARAMS="-x test"

export JAVA_HOME=/etc/alternatives/java_sdk

# install and init Greenplum as gpadmin user
su - gpadmin -c ${SCRIPT_DIR}/install_greenplum.bash

# now GPHOME should be discoverable by .pxfrc
source ~gpadmin/.pxfrc
chown -R gpadmin:gpadmin ${GPHOME}

# remove existing PXF, if any, that could come pre-installed with Greenplum RPM
if [[ -d ${GPHOME}/pxf ]]; then
    echo; echo "=====> Removing PXF installed with GPDB <====="; echo
    rm -rf ${GPHOME}/pxf
    rm ${GPHOME}/lib/postgresql/pxf.so
    rm ${GPHOME}/share/postgresql/extension/pxf.control
    rm ${GPHOME}/share/postgresql/extension/pxf*.sql
fi

# prepare PXF_HOME for PXF installation
mkdir -p ${PXF_HOME}
chown -R gpadmin:gpadmin ${PXF_HOME}

# configure and start Hadoop single cluster
chmod a+w /singlecluster
SLAVES=1 ${SCRIPT_DIR}/init_hadoop.bash

su - gpadmin -c "
		source ~/.pxfrc &&
		env &&
		${SCRIPT_DIR}/init_greenplum.bash &&
		${SCRIPT_DIR}/install_pxf.bash
	"
