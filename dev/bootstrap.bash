#!/bin/bash

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

#mkdir -p /usr/local/greenplum-db
#chown -R gpadmin:gpadmin /usr/local/greenplum-db

# setup limits for gpadmin
if [[ ! -f /etc/security/limits.d/gpadmin-limits.conf ]]; then
>/etc/security/limits.d/gpadmin-limits.conf cat <<-EOF
gpadmin soft core unlimited
gpadmin soft nproc 131072
gpadmin soft nofile 65536
EOF
fi

# setup environment for gpadmin
>>/home/gpadmin/.bash_profile cat <<EOF
GPHOME=\$(find /usr/local/ -name 'greenplum-db-*')
if [[ -f \${GPHOME}/greenplum_path.sh ]]; then
    PYTHONHOME='' source \${GPHOME}/greenplum_path.sh
fi

export PS1="[\u@\h \W]\$ "
export HADOOP_ROOT=~/workspace/singlecluster
export PXF_HOME=\${GPHOME}/pxf
export PXF_CONF=~gpadmin/pxf
export PXF_JVM_OPTS="-Xmx512m -Xms256m"
export GPHD_ROOT=~/workspace/singlecluster
#export BUILD_PARAMS="-x test"
export LANG=en_US.UTF-8
export JAVA_HOME=/etc/alternatives/java_sdk
export SLAVES=1
export GOPATH=/opt/go
export PATH=~gpadmin/workspace/pxf/dev:\${PXF_HOME}/bin:\${GPHD_ROOT}/hadoop/bin:\${GOPATH}/bin:/usr/local/go/bin:\$PATH
export PGPORT=5432
EOF

# install and init Greenplum as gpadmin user
su - gpadmin -c ${SCRIPT_DIR}/install_greenplum.bash

# rename python distro shipped with Greenplum so that system python is used for Tinc tests
mv /usr/local/greenplum-db/ext/python/ /usr/local/greenplum-db/ext/python2

# remove existing PXF, if any, that could come pre-installed with Greenplum RPM
#source /usr/local/greenplum-db*/greenplum_path.sh

GPH=/usr/local/greenplum-db
if [[ -d ${GPH}/pxf ]]; then
    rm -rf ${GPH}/pxf
    rm ${GPH}/lib/postgresql/pxf.so
	rm ${GPH}/share/postgresql/extension/pxf.control
	rm ${GPH}/share/postgresql/extension/pxf*.sql
fi

su - gpadmin -c env

# initialize Greenplum instance
su - gpadmin -c ${SCRIPT_DIR}/init_greenplum.bash

# compile and install PXF into Greenplum
su - gpadmin -c ${SCRIPT_DIR}/install_pxf.bash

# configure and start Hadoop single cluster
su - gpadmin -c ${SCRIPT_DIR}/init_hadoop.bash

# run PXF smoke test
#su - gpadmin -c "cd ~/workspace/pxf/automation && make GROUP=smoke"
