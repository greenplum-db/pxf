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
if [[ ${HADOOP_CLIENT} == MAPR ]]; then
	export GPHD_ROOT=/opt/mapr
fi
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

function run_pxf_automation() {
	# Let's make sure that automation/singlecluster directories are writeable
	chmod a+w pxf_src/automation /singlecluster || true
	find pxf_src/automation/tinc* -type d -exec chmod a+w {} \;

	su gpadmin -c "
		source '${GPHOME}/greenplum_path.sh' &&
		psql -p ${PGPORT} -d template1 -c 'CREATE EXTENSION PXF'
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

function configure_mapr_dependencies() {
	# Copy mapr specific jars to $BASE_DIR/lib
	HADOOP_COMMON=/opt/mapr/hadoop/hadoop-2.7.0/share/hadoop/common
	cp "${HADOOP_COMMON}/lib/maprfs-5.2.2-mapr.jar" \
		"${HADOOP_COMMON}/lib/hadoop-auth-2.7.0-mapr-1707.jar" \
		"${HADOOP_COMMON}/hadoop-common-2.7.0-mapr-1707.jar" "${BASE_DIR}/lib"
	# Copy *-site.xml files
	cp /opt/mapr/hadoop/hadoop-2.7.0/etc/hadoop/*-site.xml "${BASE_DIR}/servers/default"
	# Copy mapred-site.xml for recursive hdfs directories test
	# We need to do this step after PXF Server init
	cp "${PXF_HOME}/templates/mapred-site.xml" "${BASE_DIR}/servers/default/recursive-site.xml"
	# Set mapr port to 7222 in default.xml (sut)
	sed -i 's|<port>8020</port>|<port>7222</port>|' pxf_src/automation/src/test/resources/sut/default.xml
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

	if [[ ${HADOOP_CLIENT} == MAPR ]]; then
		# start mapr services before installing GPDB
		/root/init-script
	fi

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
	if [[ -f ${PXF_HOME}/commit.sha ]]; then
		adjust_automation_code
	else
		echo "WARNING: no commit.sha file is found in PXF_HOME=${PXF_HOME}"
	fi

	if [[ ${HADOOP_CLIENT} != MAPR ]]; then
		inflate_singlecluster
		if [[ ${HADOOP_CLIENT} != HDP_KERBEROS && -z ${PROTOCOL} ]]; then
			# Setup Hadoop before creating GPDB cluster to use system python for yum install
			# Must be after installing GPDB to transfer hbase jar
			setup_hadoop "${GPHD_ROOT}"
		fi
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
		adl)
			echo 'Using ADL protocol'
			[[ ${PG_REGRESS} == true ]] && setup_adl_for_pg_regress
			;;
		wasbs)
			echo 'Using WASBS protocol'
			[[ ${PG_REGRESS} == true ]] && setup_wasbs_for_pg_regress
			;;
		*) # no protocol, presumably
			if [[ ${HADOOP_CLIENT} == MAPR ]]; then
				configure_mapr_dependencies
			else
				configure_pxf_default_server
				configure_pxf_s3_server
			fi
			;;
	esac

	start_pxf_server

	# Create fat jar for automation
	generate_extras_fat_jar

	configure_sut

	inflate_dependencies

	# To run Tinc against GP7 we need to modify PYTHONPATH in $GPHOME/greenplum_path.sh since Tinc calls that script
	# we will set PYTHONPATH to point to the set of python libs compiled with Python2 for GP6
	if [[ ${GP_VER} == 7 ]]; then
	  local gp6_python_libs=~gpadmin/python
	  sed -i "/PYTHONPATH=/ s/=.*/=${gp6_python_libs}/" /usr/local/greenplum-db/greenplum_path.sh
	fi

	ln -s "${PWD}/pxf_src" ~gpadmin/pxf_src

	# Run tests
	if [[ -n ${GROUP} ]]; then
	  run_pxf_automation
#		if [[ $PG_REGRESS == true ]]; then
#			run_pg_regress
#		else
#			run_pxf_automation
#		fi
	fi
}

_main
