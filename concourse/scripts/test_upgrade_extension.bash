#!/bin/bash

set -exo pipefail

CWDIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source "${CWDIR}/update_pxf_minor_version.bash"

CWDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# make sure GP_VER is set so that we know what PXF_HOME will be
: "${GP_VER:?GP_VER must be set}"

# set our own GPHOME for RPM-based installs before sourcing common script
export GPHOME=/usr/local/greenplum-db
export PXF_HOME=/usr/local/pxf-gp${GP_VER}

source "${CWDIR}/pxf_common.bash"
PG_REGRESS=${PG_REGRESS:-false}

export GOOGLE_PROJECT_ID=${GOOGLE_PROJECT_ID:-data-gpdb-ud}
export JAVA_TOOL_OPTIONS=-Dfile.encoding=UTF8
export HADOOP_HEAPSIZE=512
export YARN_HEAPSIZE=512
export GPHD_ROOT=/singlecluster
export PGPORT=${PGPORT:-5432}

PXF_GIT_URL="https://github.com/greenplum-db/pxf.git"

function run_pxf_automation() {
	# Let's make sure that automation/singlecluster directories are writeable
	chmod a+w pxf_src/automation /singlecluster || true
	find pxf_src/automation/tinc* -type d -exec chmod a+w {} \;

	local extension_name="pxf"
	if [[ ${USE_FDW} == "true" ]]; then
		extension_name="pxf_fdw"
	fi

	#TODO: remove once exttable tests with GP7 are set
	if [[ ${GROUP} == fdw_gpdb_schedule ]]; then
		extension_name="pxf_fdw"
	fi

	su gpadmin -c "
		source '${GPHOME}/greenplum_path.sh' &&
		psql -p ${PGPORT} -d template1 -c 'CREATE EXTENSION ${extension_name}'
	"
	# prepare certification output directory
	mkdir -p certification
	chmod a+w certification

	cat > ~gpadmin/run_pxf_automation_test.sh <<-EOF
		#!/usr/bin/env bash
		set -exo pipefail

		source ~gpadmin/.pxfrc

		export PATH=\$PATH:${GPHD_ROOT}/bin
		export GPHD_ROOT=${GPHD_ROOT}
		export PXF_HOME=${PXF_HOME}
		export PGPORT=${PGPORT}
		export USE_FDW=${USE_FDW}

		cd pxf_src/automation
		time make GROUP=${GROUP} test

		# if the test is successful, create certification file
		gpdb_build_from_sql=\$(psql -c 'select version()' | grep Greenplum | cut -d ' ' -f 6,8)
		gpdb_build_clean=\${gpdb_build_from_sql%)}
		pxf_version=\$(< ${PXF_HOME}/version)
		echo "GPDB-\${gpdb_build_clean/ commit:/-}-PXF-\${pxf_version}" > "${PWD}/certification/certification.txt"
		echo
		echo '****************************************************************************************************'
		echo "Wrote certification : \$(< ${PWD}/certification/certification.txt)"
		echo '****************************************************************************************************'
	EOF

	chown gpadmin:gpadmin ~gpadmin/run_pxf_automation_test.sh
	chmod a+x ~gpadmin/run_pxf_automation_test.sh

	if [[ ${ACCEPTANCE} == true ]]; then
		echo 'Acceptance test pipeline'
		exit 1
	fi

	su gpadmin -c ~gpadmin/run_pxf_automation_test.sh
}

function generate_extras_fat_jar() {
	mkdir -p /tmp/fatjar
	pushd /tmp/fatjar
		find "${BASE_DIR}/lib" -name '*.jar' -exec jar -xf {} \;
		jar -cf "/tmp/pxf-extras-1.0.0.jar" .
		chown -R gpadmin:gpadmin "/tmp/pxf-extras-1.0.0.jar"
	popd
}

function setup_hadoop() {
	local hdfsrepo=$1

	[[ -z ${GROUP} ]] && return 0

	export SLAVES=1
	setup_impersonation "${hdfsrepo}"
	if grep 'hadoop-3' "${hdfsrepo}/versions.txt"; then
		adjust_for_hadoop3 "${hdfsrepo}"
	fi
	start_hadoop_services "${hdfsrepo}"
}

function configure_sut() {
	[[ -d /tmp/build/ ]] && AMBARI_DIR=$(find /tmp/build/ -name ambari_env_files)
	if [[ -n $AMBARI_DIR ]]; then
		REALM=$(< "$AMBARI_DIR"/REALM)
		HADOOP_IP=$(grep < "$AMBARI_DIR"/etc_hostfile ambari-1 | awk '{print $1}')
		HADOOP_USER=$(< "$AMBARI_DIR"/HADOOP_USER)
		HBASE_IP=$(grep < "$AMBARI_DIR"/etc_hostfile ambari-3 | awk '{print $1}')
		HIVE_IP=$(grep < "$AMBARI_DIR"/etc_hostfile ambari-2 | awk '{print $1}')
		HIVE_HOSTNAME=$(grep < "$AMBARI_DIR"/etc_hostfile ambari-2 | awk '{print $2}')
		KERBERIZED_HADOOP_URI="hive/${HIVE_HOSTNAME}.c.${GOOGLE_PROJECT_ID}.internal@${REALM};saslQop=auth" # quoted because of semicolon
		# Add ambari hostfile to /etc/hosts
		sudo tee --append /etc/hosts < "$AMBARI_DIR"/etc_hostfile
		sudo cp "$AMBARI_DIR"/krb5.conf /etc/krb5.conf
		# Replace host, principal, and root path values in the SUT file
		sed -i \
			-e "/<hdfs>/,/<\/hdfs/ s|<host>localhost</host>|<host>${HADOOP_IP}</host>|g" \
			-e "/<hive>/,/<\/hive/ s|<host>localhost</host>|<host>${HIVE_IP}</host>|g" \
			-e "/<hbase>/,/<\/hbase/ s|<host>localhost</host>|<host>${HBASE_IP}</host>|g" \
			-e "s|</hdfs>|<hadoopRoot>$AMBARI_DIR</hadoopRoot></hdfs>|g" \
			-e "s|</hbase>|<hbaseRoot>$AMBARI_DIR</hbaseRoot></hbase>|g" \
			-e "s|</cluster>|<hiveBaseHdfsDirectory>/warehouse/tablespace/managed/hive/</hiveBaseHdfsDirectory><testKerberosPrincipal>${HADOOP_USER}@${REALM}</testKerberosPrincipal></cluster>|g" \
			-e "s|</hive>|<kerberosPrincipal>${KERBERIZED_HADOOP_URI}</kerberosPrincipal><userName>hive</userName></hive>|g" \
			pxf_src/automation/src/test/resources/sut/default.xml
	fi
}

function _main() {
	# kill the sshd background process when this script exits. Otherwise, the
	# concourse build will run forever.
	# trap 'pkill sshd' EXIT

	# Ping is called by gpinitsystem, which must be run by gpadmin
	chmod u+s /bin/ping

	# Install GPDB
	install_gpdb_package

	install_pxf_package

	inflate_singlecluster
	if [[ ${HADOOP_CLIENT} != HDP_KERBEROS && -z ${PROTOCOL} ]]; then
		# Setup Hadoop before creating GPDB cluster to use system python for yum install
		# Must be after installing GPDB to transfer hbase jar
		setup_hadoop "${GPHD_ROOT}"
	fi

	# initialize GPDB as gpadmin user
	su gpadmin -c "${CWDIR}/initialize_gpdb.bash"

	add_remote_user_access_for_gpdb testuser
	configure_pxf_server

	local HCFS_BUCKET # team-specific bucket names
	case ${PROTOCOL} in
		s3)
			echo 'Using S3 protocol'
			;;
		minio)
			echo 'Using Minio with S3 protocol'
			setup_minio
			;;
		gs)
			echo 'Using GS protocol'
			echo "${GOOGLE_CREDENTIALS}" > /tmp/gsc-ci-service-account.key.json
			;;
		adl)
			echo 'Using ADL protocol'
			;;
		wasbs)
			echo 'Using WASBS protocol'
			;;
		*) # no protocol, presumably
			configure_pxf_default_server
			configure_pxf_s3_server
			;;
	esac

	start_pxf_server

	# Create fat jar for automation
	generate_extras_fat_jar

	configure_sut

	inflate_dependencies

	ln -s "${PWD}/pxf_src" ~gpadmin/pxf_src

	# Run tests
	if [[ -n ${INITIAL_GROUP} ]]; then
		if [[ $PG_REGRESS == true ]]; then
			run_pg_regress
		else
			run_pxf_automation
		fi
	fi

	# Upgrade to latest PXF
	upgrade_pxf

	# Run tests
	if [[ -n ${GROUP} ]]; then
		if [[ $PG_REGRESS == true ]]; then
			run_pg_regress
		else
			run_pxf_automation
		fi
	fi
}

_main
