#!/bin/bash -l

set -exo pipefail

CWDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "${CWDIR}/pxf_common.bash"

export GPHOME=${GPHOME:-"/usr/local/greenplum-db-devel"}
export PXF_HOME="${GPHOME}/pxf"

function run_pxf_automation() {
	cat > /home/gpadmin/run_pxf_automation_test.sh <<-EOF
	set -exo pipefail

	source ${GPHOME}/greenplum_path.sh
	source \${1}/gpdb_src/gpAux/gpdemo/gpdemo-env.sh

	# set variables needed by PXF Automation and Parot to run in GPDB mode with SingleCluster and standalone PXF
	export PG_MODE=GPDB
	export GPHD_ROOT=$1
	export PXF_HOME=${PXF_HOME}

	# Copy PSI package from system python to GPDB as automation test requires it
	psi_dir=\$(find /usr/lib64 -name psi | sort -r | head -1)
	cp -r \${psi_dir} ${GPHOME}/lib/python
	psql -d template1 -c "CREATE EXTENSION PXF"
	cd \${1}/pxf_src/automation
	make GROUP=${GROUP}

	exit 0
	EOF

	chown gpadmin:gpadmin /home/gpadmin/run_pxf_automation_test.sh
	chmod a+x /home/gpadmin/run_pxf_automation_test.sh
	su gpadmin -c "bash /home/gpadmin/run_pxf_automation_test.sh $(pwd)"
}

function setup_singlecluster() {

	local hdfsrepo=$1
	if [ -n "${GROUP}" ]; then

		# enable impersonation by gpadmin user
		if [ "${IMPERSONATION}" == "true" ]; then
			echo 'Impersonation is enabled, adding support for gpadmin proxy user'
			pushd ${hdfsrepo}/hadoop/etc/hadoop
			cat > proxy-config.xml <<-EOF
			<property>
				<name>hadoop.proxyuser.gpadmin.hosts</name>
				<value>*</value>
			</property>
			<property>
				<name>hadoop.proxyuser.gpadmin.groups</name>
				<value>*</value>
			</property>
			<property>
				<name>hadoop.security.authorization</name>
				<value>true</value>
			</property>
			<property>
				<name>hbase.security.authorization</name>
				<value>true</value>
			</property>
			<property>
				<name>hbase.rpc.protection</name>
				<value>authentication</value>
			</property>
			<property>
				<name>hbase.coprocessor.master.classes</name>
				<value>org.apache.hadoop.hbase.security.access.AccessController</value>
			</property>
			<property>
				<name>hbase.coprocessor.region.classes</name>
				<value>org.apache.hadoop.hbase.security.access.AccessController,org.apache.hadoop.hbase.security.access.SecureBulkLoadEndpoint</value>
			</property>
			<property>
				<name>hbase.coprocessor.regionserver.classes</name>
				<value>org.apache.hadoop.hbase.security.access.AccessController</value>
			</property>
			EOF
			sed -i -e '/<configuration>/r proxy-config.xml' core-site.xml
			sed -i -e '/<configuration>/r proxy-config.xml' ../../../hbase/conf/hbase-site.xml
			rm proxy-config.xml
			popd
		elif [ "${IMPERSONATION}" == "false" ]; then
			echo 'Impersonation is disabled, updating pxf-env.sh property'
			su gpadmin -c "sed -i -e 's|^[[:blank:]]*export PXF_USER_IMPERSONATION=.*$|export PXF_USER_IMPERSONATION=false|g' ${PXF_HOME}/conf/pxf-env.sh"
		else
			echo "ERROR: Invalid or missing CI property value: IMPERSONATION=${IMPERSONATION}"
			exit 1
		fi

		pushd ${hdfsrepo}/bin
		export SLAVES=1
		./init-gphd.sh
		# zookeeper required for HBase
		./start-zookeeper.sh
		./start-hdfs.sh
		./start-yarn.sh
		./start-hbase.sh
		./start-hive.sh

		# list running Hadoop daemons
		jps

		# grant gpadmin user admin privilege for feature tests to be able to run on secured cluster
		if [ "${IMPERSONATION}" == "true" ]; then
			echo 'Granting gpadmin user admin privileges for HBase'
			echo "grant 'gpadmin', 'RWXCA'" | hbase shell
		fi

		popd
	fi
}

function setup_hadoop_client() {
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

	echo "Contents of ${PXF_HOME}/conf/pxf-env.sh :"
	cat ${PXF_HOME}/conf/pxf-env.sh
}

function _main() {
	# Reserve port 5888 for PXF service
	echo "pxf             5888/tcp               # PXF Service" >> /etc/services

	# Install GPDB
	install_gpdb
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
			chown -R gpadmin:gpadmin ${PXF_HOME}
		fi
	else
		install_pxf_server
	fi

	# Install Hadoop and Hadoop Client
	# Doing this before making GPDB cluster to use system python for yum install
	setup_singlecluster /singlecluster
	setup_hadoop_client /singlecluster

	add_jdbc_jar_to_pxf_public_classpath /singlecluster

	make_cluster
	add_user_access "testuser"
	start_pxf_server

	# Let's make sure that automation/singlecluster directories are writeable
	chmod a+w pxf_src/automation /singlecluster
	find pxf_src/automation/tinc* -type d -exec chmod a+w {} \;

	# Run Tests
#	time run_regression_test
	if [ -n "${GROUP}" ]; then
		time run_pxf_automation /singlecluster
	fi
}

_main "$@"
