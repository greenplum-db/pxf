#!/bin/bash

set -exo pipefail

CWDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# make sure GP_VER is set so that we know what PXF_HOME will be
: "${GP_VER:?GP_VER must be set}"

# set our own GPHOME for binary or RPM-based installs before sourcing common script
if [[ -d bin_gpdb ]]; then
	# forward compatibility pipeline works with Greenplum binary tarballs
	export GPHOME=/usr/local/greenplum-db-devel
else
	# build pipeline works with Greenplum RPMs
	export GPHOME=/usr/local/greenplum-db
fi
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

function run_pg_regress() {
	# run desired groups (below we replace commas with spaces in $GROUPS)
	cat > ~gpadmin/run_pxf_automation_test.sh <<-EOF
		#!/usr/bin/env bash
		set -euxo pipefail

		source ~gpadmin/.pxfrc
		source "\${GPHOME}/greenplum_path.sh"

		export GPHD_ROOT=${GPHD_ROOT}
		export PXF_HOME=${PXF_HOME} PXF_BASE=${BASE_DIR}
		export PGPORT=${PGPORT}
		export HCFS_CMD=${GPHD_ROOT}/bin/hdfs
		export HCFS_PROTOCOL=${PROTOCOL}
		export HBASE_CMD=${GPHD_ROOT}/bin/hbase
		export BEELINE_CMD=${GPHD_ROOT}/hive/bin/beeline
		export HCFS_BUCKET=${HCFS_BUCKET}
		# hive-specific vars
		# export HIVE_IS_REMOTE= HIVE_HOST= HIVE_PRINCIPAL=

		time make -C ${PWD}/pxf_src/regression ${GROUP//,/ }
	EOF

	# this prevents a Hive error about hive.log.dir not existing
	sed -ie 's/-hiveconf hive.log.dir=$LOGS_ROOT //' "${GPHD_ROOT}/hive/conf/hive-env.sh"
	# we need to be able to write files under regression
	# and may also need to create files like ~gpamdin/pxf/servers/s3/s3-site.xml
	chown -R gpadmin "${PWD}/pxf_src/regression"
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

function adjust_automation_code() {
	local pxf_src_version=$(< pxf_src/version)
	local pxf_home_version=$(< "${PXF_HOME}/version")
	if [[ "${pxf_src_version}" != "${pxf_home_version}" ]]; then
		echo "WARNING: PXF source is version=${pxf_src_version} but PXF_HOME version=${pxf_home_version}"
		echo "backing up current pxf_src directory as pxf_src_backup ..."
		cp -R pxf_src pxf_src_backup
		local pxf_home_sha=$(< "${PXF_HOME}/commit.sha")
		echo "Switching PXF source to SHA=${pxf_home_sha}"
		pushd pxf_src > /dev/null
		git checkout ${pxf_home_sha}
		popd > /dev/null
		echo "restoring original concourse scripts into pxf_src from pxf_src_backup ..."
		rm -rf pxf_src/concourse/scripts
		cp -R pxf_src_backup/concourse/scripts pxf_src/concourse
		pxf_src_version=$(< pxf_src/version)
		if [[ "${pxf_src_version}" != "${pxf_home_version}" ]]; then
			echo "ERROR: restored PXF source version=${pxf_src_version} still does not match PXF_HOME version=${pxf_home_version}"
			exit 1
		fi
	fi
	echo "PXF source version=${pxf_src_version} matches PXF_HOME version=${pxf_home_version}"
}

function _main() {
	# kill the sshd background process when this script exits. Otherwise, the
	# concourse build will run forever.
	# trap 'pkill sshd' EXIT

	# Ping is called by gpinitsystem, which must be run by gpadmin
	chmod u+s /bin/ping

	# Install GPDB
	if [[ -d bin_gpdb ]]; then
		# forward compatibility pipeline works with Greenplum binary tarballs, not RPMs
		install_gpdb_binary
		chown -R gpadmin:gpadmin "${GPHOME}"
	else
		install_gpdb_package
	fi

	# Install PXF
	if [[ -d pxf_package ]]; then
		# forward compatibility pipeline works with PXF rpms, not rpm tarballs
		install_pxf_package
	else
		install_pxf_tarball
	fi

	# Certification jobs might install non-latest PXF, make sure automation code corresponds to what is installed
	if [[ -f ${PXF_HOME}/commit.sha ]] && [[ ${ADJUST_AUTOMATION} != false ]]; then
		adjust_automation_code
	else
		echo "WARNING: no commit.sha file is found in PXF_HOME=${PXF_HOME}"
	fi

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
			[[ ${PG_REGRESS} == true ]] && setup_s3_for_pg_regress
			;;
		minio)
			echo 'Using Minio with S3 protocol'
			setup_minio
			[[ ${PG_REGRESS} == true ]] && setup_minio_for_pg_regress
			;;
		gs)
			echo 'Using GS protocol'
			echo "${GOOGLE_CREDENTIALS}" > /tmp/gsc-ci-service-account.key.json
			[[ ${PG_REGRESS} == true ]] && setup_gs_for_pg_regress
			;;
		abfss)
			echo 'Using ABFSS protocol'
			[[ ${PG_REGRESS} == true ]] && setup_abfss_for_pg_regress
			;;
		wasbs)
			echo 'Using WASBS protocol'
			[[ ${PG_REGRESS} == true ]] && setup_wasbs_for_pg_regress
			;;
		*) # no protocol, presumably
			configure_pxf_default_server
			configure_pxf_s3_server
			;;
	esac

	start_pxf_server

	# Create fat jar for automation
	generate_extras_fat_jar

	inflate_dependencies

	ln -s "${PWD}/pxf_src" ~gpadmin/pxf_src

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
