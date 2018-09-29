#!/bin/bash -l

set -eo pipefail

GPHOME="/usr/local/greenplum-db-devel"
PXF_HOME="${GPHOME}/pxf"

function start_pxf_server() {
	pushd ${PXF_HOME} > /dev/null
	if [ "${IMPERSONATION}" == "false" ]; then
		echo "Impersonation is disabled, updating pxf-env.sh property"
		sed -i -e "s|^export PXF_USER_IMPERSONATION=.*$|export PXF_USER_IMPERSONATION=false|g" ${PXF_HOME}/conf/pxf-env.sh
	fi
	sed -i -e "s|^export PXF_JVM_OPTS=.*$|export PXF_JVM_OPTS=\"${PXF_JVM_OPTS}\"|g" ${PXF_HOME}/conf/pxf-env.sh
	echo "---------------------PXF environment -------------------------"
	cat ${PXF_HOME}/conf/pxf-env.sh
	echo "--------------------------------------------------------------"

	# Check if some other process is listening on 5888
	netstat -tlpna | grep 5888 || true
	su gpadmin -c "source ~gpadmin/.bash_profile && ./bin/pxf init && ./bin/pxf start"
	popd > /dev/null
}

function setup_hadoop_client() {
	local hadoop_ip=$1

    sed -i -e "s/\(0.0.0.0\|localhost\|127.0.0.1\)/${hadoop_ip}/g" ${GPHOME}/pxf/conf/*-site.xml
    sed -i -e 's/edw0/hadoop/' /etc/hosts
}

function _main() {

	tar -xzf pxf_tarball/pxf.tar.gz -C ${GPHOME}
	chown -R gpadmin:gpadmin ${GPHOME}/pxf

	setup_hadoop_client ${1}
	start_pxf_server
}

_main "$@"
