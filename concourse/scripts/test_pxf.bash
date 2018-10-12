#!/bin/bash -l

set -exo pipefail

CWDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "${CWDIR}/pxf_common.bash"

export GPHOME=${GPHOME:-"/usr/local/greenplum-db-devel"}
export PXF_HOME="${GPHOME}/pxf"
export JAVA_HOME="${JAVA_HOME}"

function run_pxf_automation() {
	# Let's make sure that automation/singlecluster directories are writeable
	chmod a+w pxf_src/automation /singlecluster
	find pxf_src/automation/tinc* -type d -exec chmod a+w {} \;

	ln -s ${PWD}/pxf_src /home/gpadmin/pxf_src
	su gpadmin -c "source ${GPHOME}/greenplum_path.sh && psql -p 15432 -d template1 -c \"CREATE EXTENSION PXF\""

	cat > /home/gpadmin/run_pxf_automation_test.sh <<-EOF
	set -exo pipefail

	source ${GPHOME}/greenplum_path.sh

	export PATH=\$PATH:${GPHD_ROOT}/bin:${HADOOP_ROOT}/bin:${HBASE_ROOT}/bin:${HIVE_ROOT}/bin:${ZOOKEEPER_ROOT}/bin
	export GPHD_ROOT=/singlecluster
	export PXF_HOME=${PXF_HOME}
	export PGPORT=15432

	# JAVA_HOME for Centos and Ubuntu has different suffix in our Docker images
	export JAVA_HOME=$(ls -d /usr/lib/jvm/java-1.8.0-openjdk* | head -1)

	cd pxf_src/automation
	make GROUP=${GROUP}

	exit 0
	EOF

	chown gpadmin:gpadmin /home/gpadmin/run_pxf_automation_test.sh
	chmod a+x /home/gpadmin/run_pxf_automation_test.sh
	su gpadmin -c "bash /home/gpadmin/run_pxf_automation_test.sh"
}

function setup_gpadmin_user() {
  ./gpdb_src/concourse/scripts/setup_gpadmin_user.bash
}

function setup_hadoop() {
	local hdfsrepo=$1

	if [ -n "${GROUP}" ]; then
		export SLAVES=1
	    setup_impersonation ${hdfsrepo}
		start_hadoop_services ${hdfsrepo}
	fi
}

function _main() {
	# Install GPDB
	install_gpdb_binary
	setup_gpadmin_user

	# Install PXF
	install_pxf_client
	chown -R gpadmin:gpadmin ${PXF_HOME}

	# Setup Hadoop before creating GPDB cluster to use system python for yum install
	setup_hadoop /singlecluster

	create_gpdb_cluster
	add_remote_user_access_for_gpdb "testuser"
	start_pxf_server

	# Run Tests
	if [ -n "${GROUP}" ]; then
		time run_pxf_automation
	fi
}

_main
