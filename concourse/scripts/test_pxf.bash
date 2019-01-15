#!/bin/bash -l

set -exo pipefail

CWDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "${CWDIR}/pxf_common.bash"

export GPHOME=${GPHOME:-"/usr/local/greenplum-db-devel"}
export PXF_HOME="${GPHOME}/pxf"
export JAVA_HOME="${JAVA_HOME}"
export JAVA_TOOL_OPTIONS=-Dfile.encoding=UTF8

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

function setup_hadoop() {
	local hdfsrepo=$1

	if [[ -n ${GROUP} ]]; then
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
	install_pxf_server

	if [[ ${PROTOCOL} == "s3" ]]; then
		echo Using S3 protocol
	elif [[ ${PROTOCOL} == "minio" ]]; then
		echo Using Minio with S3 protocol
		setup_minio
	elif [[ ${PROTOCOL} == "gs" ]]; then
		echo Using GS protocol
		cat << EOF > /tmp/gsc-ci-service-account.key.json
${GOOGLE_CREDENTIALS}
EOF
	elif [[ ${HADOOP_CLIENT} == "MAPR" ]]; then
		echo Using MAPR
		# start mapr services
		sudo /root/init-script
		# Copy mapr specific jars to $PXF_CONF/lib
		cp /opt/mapr/hadoop/hadoop-2.7.0/share/hadoop/common/lib/maprfs-5.2.2-mapr.jar ${PXF_CONF_DIR}/lib/
		cp /opt/mapr/hadoop/hadoop-2.7.0/share/hadoop/common/lib/hadoop-auth-2.7.0-mapr-1707.jar ${PXF_CONF_DIR}/lib/
		cp /opt/mapr/hadoop/hadoop-2.7.0/share/hadoop/common/hadoop-common-2.7.0-mapr-1707.jar ${PXF_CONF_DIR}/lib/
		# Create fat jar for automation
		mkdir -p /tmp/fatjar
		pushd /tmp/fatjar
		    jar -xf ${PXF_CONF_DIR}/lib/maprfs-5.2.2-mapr.jar
		    jar -xf ${PXF_CONF_DIR}/lib/hadoop-auth-2.7.0-mapr-1707.jar
		    jar -xf ${PXF_CONF_DIR}/lib/hadoop-common-2.7.0-mapr-1707.jar
		    jar -cf pxf-extras.jar *
		    cp pxf-extras.jar
		popd
		# Copy *-site.xml files
		cp /opt/mapr/hadoop/hadoop-2.7.0/etc/hadoop/*-site.xml ${PXF_CONF}/servers/default/
		# Set mapr port to 7222 in default.xml (sut)
		pushd pxf_src/automation
			sed -i 's|<port>8020</port>|<port>7222</port>|' src/test/resources/sut/default.xml
		popd
	elif [[ -z "${PROTOCOL}" ]]; then
		# Setup Hadoop before creating GPDB cluster to use system python for yum install
		setup_hadoop /singlecluster
	fi

	create_gpdb_cluster
	add_remote_user_access_for_gpdb "testuser"
	init_and_configure_pxf_server
	if [[ -z "${PROTOCOL}" ]]; then
		configure_pxf_default_server
		configure_pxf_s3_server
	fi
	start_pxf_server

	if [[ ${ACCEPTANCE} == "true" ]]; then
		echo Acceptance test pipeline
		exit 1
	fi

	# Run Tests
	if [[ -n ${GROUP} ]]; then
		time run_pxf_automation
	fi
}

_main
