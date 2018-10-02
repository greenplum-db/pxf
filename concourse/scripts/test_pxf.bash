#!/bin/bash -l

set -exo pipefail

CWDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "${CWDIR}/pxf_common.bash"

export GPHOME=${GPHOME:-"/usr/local/greenplum-db-devel"}
export PXF_HOME="${GPHOME}/pxf"

function run_pxf_automation() {
	# Let's make sure that automation/singlecluster directories are writeable
	chmod a+w pxf_src/automation /singlecluster
	find pxf_src/automation/tinc* -type d -exec chmod a+w {} \;

	su gpadmin -c "source ${GPHOME}/greenplum_path.sh && psql -p 15432 -d template1 -c \"CREATE EXTENSION PXF\""

	cat > /home/gpadmin/run_pxf_automation_test.sh <<-EOF
	set -exo pipefail

	source ${GPHOME}/greenplum_path.sh

	export GPHD_ROOT=/singlecluster
	export PXF_HOME=${PXF_HOME}
	export PGPORT=15432

	cd pxf_src/automation
	make GROUP=${GROUP}

	exit 0
	EOF

	chown gpadmin:gpadmin /home/gpadmin/run_pxf_automation_test.sh
	chmod a+x /home/gpadmin/run_pxf_automation_test.sh
	su gpadmin -c "bash /home/gpadmin/run_pxf_automation_test.sh"
}

function setup_hadoop() {
	local hdfsrepo=$1

	case ${HADOOP_CLIENT} in
		CDH|HDP)
			cp ${hdfsrepo}/hadoop/etc/hadoop/{core,hdfs,mapred,yarn}-site.xml /etc/hadoop/conf
			cp ${hdfsrepo}/hive/conf/hive-site.xml /etc/hive/conf
			cp ${hdfsrepo}/hbase/conf/hbase-site.xml /etc/hbase/conf
			;;
		TAR)
			# TAR-based setup, edit the properties in pxf-env.sh to specify HADOOP_ROOT value
			sed -i -e "s|^[[:blank:]]*export HADOOP_ROOT=.*$|export HADOOP_ROOT=${hdfsrepo}|g" ${PXF_HOME}/conf/pxf-env.sh
			;;
		*)
			echo "FATAL: Unknown HADOOP_CLIENT=${HADOOP_CLIENT} parameter value"
			exit 1
			;;
	esac

	if [ -n "${GROUP}" ]; then
		export SLAVES=1
	    setup_impersonation ${hdfsrepo}
		start_hadoop_services ${hdfsrepo}
	fi
}

function _main() {
	# Reserve port 5888 for PXF service
	echo "pxf             5888/tcp               # PXF Service" >> /etc/services

	# Install GPDB
	install_gpdb_binary
	setup_gpadmin_user

	# Install PXF Client (pxf.so file)
	install_pxf_client

	# Install PXF Server
	if [ -d pxf_tarball ]; then
		# untar pxf server only if necessary
		if [ -d ${PXF_HOME} ]; then
			echo "Skipping pxf_tarball..."
		else
			tar -xzf pxf_tarball/pxf.tar.gz -C ${GPHOME}
		fi
	else
		install_pxf_server
	fi
	chown -R gpadmin:gpadmin ${PXF_HOME}

	# Install Hadoop and Hadoop Client
	# Doing this before making GPDB cluster to use system python for yum install
	setup_hadoop /singlecluster

	add_jdbc_jar_to_pxf_public_classpath /singlecluster
	create_gpdb_cluster
	add_remote_user_access_for_gpdb "testuser"
	start_pxf_server

	# Run Tests
	if [ -n "${GROUP}" ]; then
		time run_pxf_automation /singlecluster
	fi
}

_main
