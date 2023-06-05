#!/bin/bash

set -exo pipefail

CWDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "${CWDIR}/pxf_common.bash"
PG_REGRESS=${PG_REGRESS:-false}

export GOOGLE_PROJECT_ID=${GOOGLE_PROJECT_ID:-data-gpdb-ud}
export GPHOME=${GPHOME:-/usr/local/greenplum-db-devel}
export PXF_HOME=${GPHOME}/pxf
export JAVA_HOME=${JAVA_HOME}
export JAVA_TOOL_OPTIONS=-Dfile.encoding=UTF8
export HADOOP_HEAPSIZE=512
export YARN_HEAPSIZE=512
export GPHD_ROOT=/singlecluster
if [[ ${HADOOP_CLIENT} == MAPR ]]; then
	export GPHD_ROOT=/opt/mapr
fi
export PGPORT=${PGPORT:-15432}

function run_pg_regress() {
	# run desired groups (below we replace commas with spaces in $GROUPS)
	cat > ~gpadmin/run_pxf_automation_test.sh <<-EOF
		#!/usr/bin/env bash
		set -euxo pipefail

		source ${GPHOME}/greenplum_path.sh

		export GPHD_ROOT=${GPHD_ROOT}
		export PXF_HOME=${PXF_HOME} PXF_CONF=${PXF_CONF_DIR}
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
	ln -s "${PWD}/pxf_src" ~gpadmin/pxf_src

	if [[ $PG_REGRESS == true ]]; then
		run_pg_regress
		return $?
	fi

	# Let's make sure that automation/singlecluster directories are writeable
	chmod a+w pxf_src/automation /singlecluster || true
	find pxf_src/automation/tinc* -type d -exec chmod a+w {} \;

	su gpadmin -c "
		source '${GPHOME}/greenplum_path.sh' &&
		psql -p ${PGPORT} -d template1 -c 'CREATE EXTENSION PXF'
	"

	cat > ~gpadmin/run_pxf_automation_test.sh <<-EOF
		set -exo pipefail

		source ${GPHOME}/greenplum_path.sh

		export PATH=\$PATH:${GPHD_ROOT}/bin
		export GPHD_ROOT=${GPHD_ROOT}
		export PXF_HOME=${PXF_HOME}
		export PGPORT=${PGPORT}

		# JAVA_HOME is from pxf_common.bash
		export JAVA_HOME=${JAVA_HOME}

		cd pxf_src/automation
		make GROUP=${GROUP}
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
	mkdir -p /tmp/fatjar "${PXF_HOME}/tmp"
	pushd /tmp/fatjar
		find "${PXF_CONF_DIR}/lib" -name '*.jar' -exec jar -xf {} \;
		jar -cf "${PXF_HOME}/lib/pxf-extras-1.0.0.jar" .
		chown -R gpadmin:gpadmin "${PXF_HOME}/lib/pxf-extras-1.0.0.jar"
	popd
}

function configure_mapr_dependencies() {
	# Copy mapr specific jars to $PXF_CONF_DIR/lib
	HADOOP_COMMON=/opt/mapr/hadoop/hadoop-2.7.0/share/hadoop/common
	cp "${HADOOP_COMMON}/lib/maprfs-5.2.2-mapr.jar" \
		"${HADOOP_COMMON}/lib/hadoop-auth-2.7.0-mapr-1707.jar" \
		"${HADOOP_COMMON}/hadoop-common-2.7.0-mapr-1707.jar" "${PXF_CONF_DIR}/lib"
	# Copy *-site.xml files
	cp /opt/mapr/hadoop/hadoop-2.7.0/etc/hadoop/*-site.xml "${PXF_CONF_DIR}/servers/default"
	# Copy mapred-site.xml for recursive hdfs directories test
	# We need to do this step after PXF Server init
	cp "${PXF_CONF_DIR}/templates/mapred-site.xml" "${PXF_CONF_DIR}/servers/default/recursive-site.xml"
	# Set mapr port to 7222 in default.xml (sut)
	sed -i 's|<port>8020</port>|<port>7222</port>|' pxf_src/automation/src/test/resources/sut/default.xml
}

function configure_sut() {
	AMBARI_DIR=$(find /tmp/build/ -name ambari_env_files)
	if [[ -n $AMBARI_DIR  ]]; then
		REALM=$(cat "$AMBARI_DIR"/REALM)
		HADOOP_IP=$(grep < "$AMBARI_DIR"/etc_hostfile ambari-1 | awk '{print $1}')
		HADOOP_USER=$(cat "$AMBARI_DIR"/HADOOP_USER)
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
	trap 'pkill sshd' EXIT

	# Ping is called by gpinitsystem, which must be run by gpadmin
	chmod u+s /bin/ping

	if [[ ${HADOOP_CLIENT} == MAPR ]]; then
		# start mapr services before installing GPDB
		/root/init-script
	fi

	# Install GPDB
	install_gpdb_binary
	setup_gpadmin_user

	# Install PXF
	install_pxf_client
	install_pxf_server

	if [[ -z ${PROTOCOL} && ${HADOOP_CLIENT} != MAPR && ${HADOOP_CLIENT} != HDP_KERBEROS ]]; then
		# Setup Hadoop before creating GPDB cluster to use system python for yum install
		# Must be after installing GPDB to transfer hbase jar
		setup_hadoop "${GPHD_ROOT}"
	fi

	create_gpdb_cluster
	add_remote_user_access_for_gpdb testuser
	configure_pxf_server

	local HCFS_BUCKET # team-specific bucket names
	case ${PROTOCOL} in
		s3)
			echo 'Using S3 protocol'
			[[ ${PG_REGRESS} != false ]] && setup_s3_for_pg_regress
			;;
		minio)
			echo 'Using Minio with S3 protocol'
			setup_minio
			[[ ${PG_REGRESS} != false ]] && setup_minio_for_pg_regress
			;;
		gs)
			echo 'Using GS protocol'
			echo "${GOOGLE_CREDENTIALS}" > /tmp/gsc-ci-service-account.key.json
			[[ ${PG_REGRESS} != false ]] && setup_gs_for_pg_regress
			;;
		adl)
			echo 'Using ADL protocol'
			[[ ${PG_REGRESS} != false ]] && setup_adl_for_pg_regress
			;;
		wasbs)
			echo 'Using WASBS protocol'
			[[ ${PG_REGRESS} != false ]] && setup_wasbs_for_pg_regress
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

	# Run Tests
	if [[ -n ${GROUP} ]]; then
		time run_pxf_automation
	fi
}

_main
