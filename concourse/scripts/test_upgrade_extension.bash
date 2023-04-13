#!/bin/bash

set -exo pipefail

CWDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# make sure GP_VER is set so that we know what PXF_HOME will be
: "${GP_VER:?GP_VER must be set}"

# set our own GPHOME for RPM-based installs before sourcing common script
export GPHOME=/usr/local/greenplum-db
export PXF_HOME=/usr/local/pxf-gp${GP_VER}

source "${CWDIR}/pxf_common.bash"
source "${CWDIR}/update_pxf_minor_version.bash"

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
	# Setup Hadoop before creating GPDB cluster to use system python for yum install
	# Must be after installing GPDB to transfer hbase jar
	setup_hadoop "${GPHD_ROOT}"

	# initialize GPDB as gpadmin user
	su gpadmin -c "${CWDIR}/initialize_gpdb.bash"

	add_remote_user_access_for_gpdb testuser
	configure_pxf_server

	local HCFS_BUCKET # team-specific bucket names
	configure_pxf_default_server

	start_pxf_server

	# Create fat jar for automation
	generate_extras_fat_jar

	inflate_dependencies

	ln -s "${PWD}/pxf_src" ~gpadmin/pxf_src

	# Run tests
	if [[ -n ${FIRST_GROUP} ]]; then
		GROUP=${FIRST_GROUP}
		run_pxf_automation
	fi

	# Upgrade to latest PXF
	upgrade_pxf

	# Run tests
	if [[ -n ${SECOND_GROUP} ]]; then
		GROUP=${SECOND_GROUP}
		run_pxf_automation
	else
		echo "No second group to be tested"
	fi
}

_main
