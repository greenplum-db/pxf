#!/bin/bash

set -exuo pipefail

CWDIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
# shellcheck source=/dev/null
source "${CWDIR}/pxf_common.bash"

SSH_OPTS=(-i cluster_env_files/private_key.pem -o 'StrictHostKeyChecking=no')
HADOOP_SSH_OPTS=(-o 'StrictHostKeyChecking=no')
GOOGLE_ZONE=${GOOGLE_ZONE:-us-central1-a}
IMPERSONATION=${IMPERSONATION:-true}
LOCAL_GPHD_ROOT=/singlecluster
PROXY_USER=${PROXY_USER:-pxfuser}

function configure_local_hdfs() {
	sed -i -e "s|hdfs://0.0.0.0:8020|hdfs://${HADOOP_HOSTNAME}:8020|" \
	${LOCAL_GPHD_ROOT}/hadoop/etc/hadoop/core-site.xml ${LOCAL_GPHD_ROOT}/hbase/conf/hbase-site.xml
}

function run_multinode_smoke_test() {
	NO_OF_FILES=${1:-300}
	echo "Running multinode smoke test with ${NO_OF_FILES} files"
	time ssh "${HADOOP_SSH_OPTS[@]}" "${HADOOP_USER}@${HADOOP_HOSTNAME}" "
		export JAVA_HOME=/etc/alternatives/jre_1.8.0_openjdk
		command -v kinit && kinit gpadmin -kt pxf.service.keytab
		${HDFS_BIN}/hdfs dfs -mkdir -p /tmp && mkdir -p /tmp/pxf_test &&
		for ((i=1; i<=${NO_OF_FILES}; i++)); do
			cat > /tmp/pxf_test/test_\${i}.txt <<-EOF
				1
				2
				3
			EOF
		done &&
		${HDFS_BIN}/hdfs dfs -copyFromLocal /tmp/pxf_test/ /tmp
	"

	echo "Found $("${LOCAL_GPHD_ROOT}/bin/hdfs" dfs -ls /tmp/pxf_test | grep -c pxf_test) items in /tmp/pxf_test"
	expected_output=$((3 * NO_OF_FILES))

	time ssh "${SSH_OPTS[@]}" gpadmin@mdw "
		source ${GPHOME}/greenplum_path.sh
		psql -d template1 -c \"
			CREATE EXTERNAL TABLE pxf_multifile_test (b TEXT)
				LOCATION ('pxf://tmp/pxf_test?PROFILE=HdfsTextSimple')
				FORMAT 'CSV';
		\"
		num_rows=\$(psql -d template1 -t -c 'SELECT COUNT(*) FROM pxf_multifile_test;' | head -1)
		if (( num_rows == ${expected_output} )); then
			echo 'Received expected output'
		else
			echo \"Error. Expected output '${expected_output}' does not match actual '\${num_rows}'\"
			exit 1
		fi
	"
}

function update_pghba_conf() {
	local sdw_ips=("$@")
	for ip in "${sdw_ips[@]}"; do
		echo "host	all	gpadmin		$ip/32	trust"
	done | ssh "${SSH_OPTS[@]}" gpadmin@mdw "
		cat >> /data/gpdata/master/gpseg-1/pg_hba.conf &&
		cat /data/gpdata/master/gpseg-1/pg_hba.conf
	"
}

function setup_pxf_on_cluster() {
	# drop named query file for JDBC test to gpadmin's home on mdw
	scp "${SSH_OPTS[@]}" pxf_src/automation/src/test/resources/{,hive-}report.sql gpadmin@mdw:

	# init all PXFs using cluster command, configure PXF on master, sync configs and start pxf
	ssh "${SSH_OPTS[@]}" gpadmin@mdw "
		source ${GPHOME}/greenplum_path.sh &&
		if [[ ! -d ${PXF_CONF_DIR} ]]; then
			PXF_CONF=${PXF_CONF_DIR} ${GPHOME}/pxf/bin/pxf cluster init
			cp ${PXF_CONF_DIR}/templates/{hdfs,mapred,yarn,core,hbase,hive}-site.xml ${PXF_CONF_DIR}/servers/default
			sed -i -e 's/\(0.0.0.0\|localhost\|127.0.0.1\)/${hadoop_ip}/g' ${PXF_CONF_DIR}/servers/default/*-site.xml
		else
			cp ${PXF_CONF_DIR}/templates/mapred{,1}-site.xml
		fi &&
		mkdir -p ${PXF_CONF_DIR}/servers/s3{,-invalid} &&
		cp ${PXF_CONF_DIR}/templates/s3-site.xml ${PXF_CONF_DIR}/servers/s3 &&
		cp ${PXF_CONF_DIR}/templates/s3-site.xml ${PXF_CONF_DIR}/servers/s3-invalid &&
		sed -i  -e \"s|YOUR_AWS_ACCESS_KEY_ID|${ACCESS_KEY_ID}|\" \
			-e \"s|YOUR_AWS_SECRET_ACCESS_KEY|${SECRET_ACCESS_KEY}|\" \
			${PXF_CONF_DIR}/servers/s3/s3-site.xml &&
		mkdir -p ${PXF_CONF_DIR}/servers/database &&
		cp ${PXF_CONF_DIR}/templates/jdbc-site.xml ${PXF_CONF_DIR}/servers/database/ &&
		sed -i  -e 's|YOUR_DATABASE_JDBC_DRIVER_CLASS_NAME|org.postgresql.Driver|' \
			-e 's|YOUR_DATABASE_JDBC_URL|jdbc:postgresql://mdw:5432/pxfautomation|' \
			-e 's|YOUR_DATABASE_JDBC_USER|gpadmin|' \
			-e 's|YOUR_DATABASE_JDBC_PASSWORD||' \
			${PXF_CONF_DIR}/servers/database/jdbc-site.xml &&
		cp ~gpadmin/report.sql ${PXF_CONF_DIR}/servers/database &&
		cp ${PXF_CONF_DIR}/servers/database/jdbc-site.xml ${PXF_CONF_DIR}/servers/database/testuser-user.xml &&
		sed -i 's|pxfautomation|template1|' ${PXF_CONF_DIR}/servers/database/testuser-user.xml &&
		mkdir -p ${PXF_CONF_DIR}/servers/db-session-params &&
		cp ${PXF_CONF_DIR}/templates/jdbc-site.xml ${PXF_CONF_DIR}/servers/db-session-params &&
		sed -i  -e 's|YOUR_DATABASE_JDBC_DRIVER_CLASS_NAME|org.postgresql.Driver|' \
			-e 's|YOUR_DATABASE_JDBC_URL|jdbc:postgresql://mdw:5432/pxfautomation|' \
			-e 's|YOUR_DATABASE_JDBC_USER||' \
			-e 's|YOUR_DATABASE_JDBC_PASSWORD||' \
			-e 's|</configuration>|<property><name>jdbc.session.property.client_min_messages</name><value>debug1</value></property></configuration>|' \
			-e 's|</configuration>|<property><name>jdbc.session.property.default_statistics_target</name><value>123</value></property></configuration>|' \
			${PXF_CONF_DIR}/servers/db-session-params/jdbc-site.xml &&
		mkdir -p ${PXF_CONF_DIR}/servers/db-hive &&
		cp ${PXF_CONF_DIR}/templates/jdbc-site.xml ${PXF_CONF_DIR}/servers/db-hive &&
		sed -i  -e 's|YOUR_DATABASE_JDBC_DRIVER_CLASS_NAME|org.apache.hive.jdbc.HiveDriver|' \
			-e \"s|YOUR_DATABASE_JDBC_URL|jdbc:hive2://${HADOOP_HOSTNAME}:10000/default|\" \
			-e 's|YOUR_DATABASE_JDBC_USER||' \
			-e 's|YOUR_DATABASE_JDBC_PASSWORD||' \
			${PXF_CONF_DIR}/servers/db-hive/jdbc-site.xml &&
		cp ~gpadmin/hive-report.sql ${PXF_CONF_DIR}/servers/db-hive &&
		if [[ ${IMPERSONATION} == true ]]; then
			cp -r ${PXF_CONF_DIR}/servers/default ${PXF_CONF_DIR}/servers/default-no-impersonation

			if [[ ! -f ${PXF_CONF_DIR}/servers/default-no-impersonation/pxf-site.xml ]]; then
				cp ${PXF_CONF_DIR}/templates/pxf-site.xml ${PXF_CONF_DIR}/servers/default-no-impersonation/pxf-site.xml
			fi
			sed -i \
			-e '/<name>pxf.service.user.impersonation<\/name>/ {n;s|<value>.*</value>|<value>false</value>|g;}' \
			-e '/<name>pxf.service.user.name<\/name>/ {n;s|<value>.*</value>|<value>foobar</value>|g;}' \
			${PXF_CONF_DIR}/servers/default-no-impersonation/pxf-site.xml
		fi &&
		${GPHOME}/pxf/bin/pxf cluster sync
	"
}

function setup_pxf_kerberos_on_cluster() {
	DATAPROC_DIR=$(find /tmp/build/ -name dataproc_env_files)
	REALM=$(< "${DATAPROC_DIR}/REALM")
	REALM=${REALM^^} # make sure REALM is up-cased, down-case below for hive principal
	KERBERIZED_HADOOP_URI="hive/${HADOOP_HOSTNAME}@${REALM};saslQop=auth-conf" # quoted because of semicolon
	sed -i  -e "s|</hdfs>|<hadoopRoot>$DATAPROC_DIR</hadoopRoot></hdfs>|g" \
		-e "s|</cluster>|<testKerberosPrincipal>gpadmin@${REALM}</testKerberosPrincipal></cluster>|g" \
		-e "s|</hive>|<kerberosPrincipal>${KERBERIZED_HADOOP_URI}</kerberosPrincipal><userName>gpadmin</userName></hive>|g" \
		"$multiNodesCluster"
	ssh gpadmin@mdw "
		cp ${PXF_CONF_DIR}/templates/pxf-site.xml ${PXF_CONF_DIR}/servers/db-hive/pxf-site.xml &&
		sed -i 's|gpadmin/_HOST@EXAMPLE.COM|gpadmin@${REALM}|g' ${PXF_CONF_DIR}/servers/db-hive/pxf-site.xml &&
		sed -i 's|</configuration>|<property><name>hadoop.security.authentication</name><value>kerberos</value></property></configuration>|g' \
			${PXF_CONF_DIR}/servers/db-hive/jdbc-site.xml &&
		sed -i -e 's|\(jdbc:hive2://${HADOOP_HOSTNAME}:10000/default\)|\1;principal=${KERBERIZED_HADOOP_URI}|g' \
			${PXF_CONF_DIR}/servers/db-hive/jdbc-site.xml &&
		${GPHOME}/pxf/bin/pxf cluster sync
	"
	sudo mkdir -p /etc/security/keytabs
	sudo cp "${DATAPROC_DIR}/pxf.service.keytab" /etc/security/keytabs/gpadmin.headless.keytab
	sudo chown gpadmin:gpadmin /etc/security/keytabs/gpadmin.headless.keytab
	scp centos@mdw:/etc/krb5.conf /tmp/krb5.conf
	sudo cp /tmp/krb5.conf /etc/krb5.conf

	# Add foreign dataproc hostfile to /etc/hosts
	sudo tee --append /etc/hosts < "${DATAPROC_DIR}/etc_hostfile"

	if [[ -d dataproc_2_env_files ]]; then
		# Create the second hdfs-secure cluster configuration
		GPDB_CLUSTER_NAME_BASE=$(grep < cluster_env_files/etc_hostfile edw0 | awk '{print substr($3, 1, length($3)-2)}')
		HADOOP_2_HOSTNAME=$(< dataproc_2_env_files/name)
		HADOOP_2_USER=gpuser
		HADOOP_2_SSH_OPTS=(-o 'UserKnownHostsFile=/dev/null' -o 'StrictHostKeyChecking=no' -i dataproc_2_env_files/google_compute_engine)
		DATAPROC_2_DIR=$(find /tmp/build/ -name dataproc_2_env_files)
		REALM2=$(< "${DATAPROC_2_DIR}/REALM")
		REALM2=${REALM2^^} # make sure REALM2 is up-cased, down-case below for hive principal
		KERBERIZED_HADOOP_2_URI="hive/${HADOOP_2_HOSTNAME}@${REALM2};saslQop=auth-conf" # quoted because of semicolon
		ssh gpadmin@mdw "
			mkdir -p ${PXF_CONF_DIR}/servers/hdfs-secure &&
			cp ${PXF_CONF_DIR}/templates/pxf-site.xml ${PXF_CONF_DIR}/servers/hdfs-secure &&
			sed -i -e \"s|>gpadmin/_HOST@EXAMPLE.COM<|>${HADOOP_2_USER}/_HOST@${REALM2}<|g\" ${PXF_CONF_DIR}/servers/hdfs-secure/pxf-site.xml &&
			sed -i -e 's|/pxf.service.keytab<|/pxf.service.2.keytab<|g' ${PXF_CONF_DIR}/servers/hdfs-secure/pxf-site.xml
		"
		scp dataproc_2_env_files/conf/*-site.xml "gpadmin@mdw:${PXF_CONF_DIR}/servers/hdfs-secure"
		ssh gpadmin@mdw "${GPHOME}/pxf/bin/pxf cluster sync"

		sed -i  -e "s|</hdfs2>|<hadoopRoot>$DATAPROC_2_DIR</hadoopRoot><testKerberosPrincipal>${HADOOP_2_USER}@${REALM2}</testKerberosPrincipal></hdfs2>|g" \
			-e "s|</hive2>|<kerberosPrincipal>${KERBERIZED_HADOOP_2_URI}</kerberosPrincipal><userName>${HADOOP_2_USER}</userName></hive2>|g" \
			"$multiNodesCluster"

		# Create the db-hive-kerberos server configuration
		ssh "${SSH_OPTS[@]}" gpadmin@mdw "
			mkdir -p ${PXF_CONF_DIR}/servers/db-hive-kerberos &&
			cp ${PXF_CONF_DIR}/templates/jdbc-site.xml ${PXF_CONF_DIR}/servers/db-hive-kerberos &&
			sed -i -e 's|YOUR_DATABASE_JDBC_DRIVER_CLASS_NAME|org.apache.hive.jdbc.HiveDriver|' \
				-e \"s|YOUR_DATABASE_JDBC_URL|jdbc:hive2://${HADOOP_2_HOSTNAME}:10000/default;principal=${KERBERIZED_HADOOP_2_URI}|\" \
				-e 's|YOUR_DATABASE_JDBC_USER||' \
				-e 's|YOUR_DATABASE_JDBC_PASSWORD||' \
				${PXF_CONF_DIR}/servers/db-hive-kerberos/jdbc-site.xml &&
			cp ~gpadmin/hive-report.sql ${PXF_CONF_DIR}/servers/db-hive-kerberos &&
			cp ${PXF_CONF_DIR}/templates/pxf-site.xml ${PXF_CONF_DIR}/servers/db-hive-kerberos/pxf-site.xml &&
			sed -i 's|gpadmin/_HOST@EXAMPLE.COM|${HADOOP_2_USER}/_HOST@${REALM2}|g' ${PXF_CONF_DIR}/servers/db-hive-kerberos/pxf-site.xml &&
			sed -i -e 's|/pxf.service.keytab<|/pxf.service.2.keytab<|g' ${PXF_CONF_DIR}/servers/db-hive-kerberos/pxf-site.xml &&
			sed -i -e 's|\${pxf.service.user.impersonation.enabled}|false|g' ${PXF_CONF_DIR}/servers/db-hive-kerberos/pxf-site.xml &&
			sed -i 's|</configuration>|<property><name>hadoop.security.authentication</name><value>kerberos</value></property></configuration>|g' \
				${PXF_CONF_DIR}/servers/db-hive-kerberos/jdbc-site.xml &&
			${GPHOME}/pxf/bin/pxf cluster sync
		"

		# Add foreign dataproc hostfile to /etc/hosts
		sudo tee --append /etc/hosts < dataproc_2_env_files/etc_hostfile

		ssh "${HADOOP_2_SSH_OPTS[@]}" -t "${HADOOP_2_USER}@${HADOOP_2_HOSTNAME}" \
			"set -euo pipefail
			sudo kadmin.local -q 'addprinc -pw pxf ${HADOOP_2_USER}/${GPDB_CLUSTER_NAME_BASE}-0.c.${GOOGLE_PROJECT_ID}.internal'
			sudo kadmin.local -q 'addprinc -pw pxf ${HADOOP_2_USER}/${GPDB_CLUSTER_NAME_BASE}-1.c.${GOOGLE_PROJECT_ID}.internal'
			sudo kadmin.local -q 'addprinc -pw pxf ${HADOOP_2_USER}/${GPDB_CLUSTER_NAME_BASE}-2.c.${GOOGLE_PROJECT_ID}.internal'
			sudo kadmin.local -q \"xst -k \${HOME}/pxf.service-mdw.keytab ${HADOOP_2_USER}/${GPDB_CLUSTER_NAME_BASE}-0.c.${GOOGLE_PROJECT_ID}.internal\"
			sudo kadmin.local -q \"xst -k \${HOME}/pxf.service-sdw1.keytab ${HADOOP_2_USER}/${GPDB_CLUSTER_NAME_BASE}-1.c.${GOOGLE_PROJECT_ID}.internal\"
			sudo kadmin.local -q \"xst -k \${HOME}/pxf.service-sdw2.keytab ${HADOOP_2_USER}/${GPDB_CLUSTER_NAME_BASE}-2.c.${GOOGLE_PROJECT_ID}.internal\"
			sudo chown ${HADOOP_2_USER} \"\${HOME}/pxf.service-mdw.keytab\"
			sudo chown ${HADOOP_2_USER} \"\${HOME}/pxf.service-sdw1.keytab\"
			sudo chown ${HADOOP_2_USER} \"\${HOME}/pxf.service-sdw2.keytab\"
			"
		scp "${HADOOP_2_SSH_OPTS[@]}" "${HADOOP_2_USER}@${HADOOP_2_HOSTNAME}":~/pxf.service-*.keytab \
			/tmp/

		scp /tmp/pxf.service-*.keytab gpadmin@mdw:~/dataproc_2_env_files/

		# Add foreign dataproc hostfile to /etc/hosts on all nodes and copy keytab
		ssh gpadmin@mdw "
			source ${GPHOME}/greenplum_path.sh &&
			gpscp -f ~gpadmin/hostfile_all -v -r -u centos ~/dataproc_2_env_files/etc_hostfile =:/tmp/etc_hostfile &&
			gpssh -f ~gpadmin/hostfile_all -v -u centos -s -e 'sudo tee --append /etc/hosts < /tmp/etc_hostfile' &&
			gpscp -h mdw -v -r -u gpadmin ~/dataproc_2_env_files/pxf.service-mdw.keytab =:/home/gpadmin/pxf/keytabs/pxf.service.2.keytab &&
			gpscp -h sdw1 -v -r -u gpadmin ~/dataproc_2_env_files/pxf.service-sdw1.keytab =:/home/gpadmin/pxf/keytabs/pxf.service.2.keytab &&
			gpscp -h sdw2 -v -r -u gpadmin ~/dataproc_2_env_files/pxf.service-sdw2.keytab =:/home/gpadmin/pxf/keytabs/pxf.service.2.keytab
		"
		sudo cp "${DATAPROC_2_DIR}/pxf.service.keytab" /etc/security/keytabs/gpuser.headless.keytab
		sudo chown gpadmin:gpadmin /etc/security/keytabs/gpuser.headless.keytab
		sed -i "s/>second-hadoop</>${HADOOP_2_HOSTNAME}</g" "$multiNodesCluster"
	fi

	# Create the non-secure cluster configuration
	NON_SECURE_HADOOP_IP=$(grep < cluster_env_files/etc_hostfile edw0 | awk '{print $1}')
	ssh gpadmin@mdw "
		mkdir -p ${PXF_CONF_DIR}/servers/db-hive-non-secure &&
		cp ${PXF_CONF_DIR}/templates/jdbc-site.xml ${PXF_CONF_DIR}/servers/db-hive-non-secure &&
		sed -i -e 's|YOUR_DATABASE_JDBC_DRIVER_CLASS_NAME|org.apache.hive.jdbc.HiveDriver|' \
			-e \"s|YOUR_DATABASE_JDBC_URL|jdbc:hive2://${NON_SECURE_HADOOP_IP}:10000/default|\" \
			-e 's|YOUR_DATABASE_JDBC_USER||' \
			-e 's|YOUR_DATABASE_JDBC_PASSWORD||' \
			${PXF_CONF_DIR}/servers/db-hive-non-secure/jdbc-site.xml &&
		cp ~gpadmin/hive-report.sql ${PXF_CONF_DIR}/servers/db-hive-non-secure &&
		mkdir -p ${PXF_CONF_DIR}/servers/hdfs-non-secure &&
		cp ${PXF_CONF_DIR}/templates/{hdfs,mapred,yarn,core,hbase,hive,pxf}-site.xml ${PXF_CONF_DIR}/servers/hdfs-non-secure &&
		sed -i -e 's/\(0.0.0.0\|localhost\|127.0.0.1\)/${NON_SECURE_HADOOP_IP}/g' ${PXF_CONF_DIR}/servers/hdfs-non-secure/*-site.xml &&
		sed -i -e 's|\${user.name}|${PROXY_USER}|g' ${PXF_CONF_DIR}/servers/hdfs-non-secure/pxf-site.xml &&
		${GPHOME}/pxf/bin/pxf cluster sync
	"
	sed -i "s/>non-secure-hadoop</>${NON_SECURE_HADOOP_IP}</g" "$multiNodesCluster"

	# Create a secured server configuration with invalid principal name
	ssh gpadmin@mdw "
		mkdir -p ${PXF_CONF_DIR}/servers/secure-hdfs-invalid-principal &&
		cp ${PXF_CONF_DIR}/servers/default/*-site.xml ${PXF_CONF_DIR}/servers/secure-hdfs-invalid-principal &&
		cp ${PXF_CONF_DIR}/templates/pxf-site.xml ${PXF_CONF_DIR}/servers/secure-hdfs-invalid-principal &&
		sed -i -e 's|>gpadmin/_HOST@EXAMPLE.COM<|>foobar/_HOST@INVALID.REALM.INTERNAL<|g' ${PXF_CONF_DIR}/servers/secure-hdfs-invalid-principal/pxf-site.xml &&
		${GPHOME}/pxf/bin/pxf cluster sync
	"

	# Create a secured server configuration with invalid keytab
	ssh gpadmin@mdw "
		mkdir -p ${PXF_CONF_DIR}/servers/secure-hdfs-invalid-keytab &&
		cp ${PXF_CONF_DIR}/servers/default/*-site.xml ${PXF_CONF_DIR}/servers/secure-hdfs-invalid-keytab &&
		cp ${PXF_CONF_DIR}/templates/pxf-site.xml ${PXF_CONF_DIR}/servers/secure-hdfs-invalid-keytab &&
		sed -i -e 's|/pxf.service.keytab<|/non.existent.keytab<|g' ${PXF_CONF_DIR}/servers/secure-hdfs-invalid-keytab/pxf-site.xml &&
		${GPHOME}/pxf/bin/pxf cluster sync
	"

	# Configure the principal for the default-no-impersonation server
	ssh gpadmin@mdw "
	if [[ ${IMPERSONATION} == true ]]; then
		sed -i -e 's|gpadmin/_HOST@EXAMPLE.COM|gpadmin@${REALM}|g' ${PXF_CONF_DIR}/servers/default-no-impersonation/pxf-site.xml &&
		${GPHOME}/pxf/bin/pxf cluster sync
	fi
	"
}

function run_pxf_automation() {
	local multiNodesCluster=pxf_src/automation/src/test/resources/sut/MultiNodesCluster.xml

	if [[ $KERBEROS == true ]]; then
		multiNodesCluster=pxf_src/automation/src/test/resources/sut/MultiHadoopMultiNodesCluster.xml
	fi

	if (( HIVE_VERSION == 2 )); then
		local search='<hiveBaseHdfsDirectory>/hive/warehouse/</hiveBaseHdfsDirectory>'
		local replace='<hiveBaseHdfsDirectory>/user/hive/warehouse/</hiveBaseHdfsDirectory>'
		sed -i "s|${search}|${replace}|g" "$multiNodesCluster"
	fi

	# point the tests at remote Hadoop and GPDB
	sed -i "s/>hadoop</>${HADOOP_HOSTNAME}</g" "$multiNodesCluster"
	sed -i "/<class>org.greenplum.pxf.automation.components.gpdb.Gpdb<\/class>/ {n; s/localhost/mdw/}" \
		"$multiNodesCluster"

	if [[ $KERBEROS == true ]]; then
		setup_pxf_kerberos_on_cluster
		sed -i 's/sutFile=default.xml/sutFile=MultiHadoopMultiNodesCluster.xml/g' pxf_src/automation/jsystem.properties
	else
		sed -i 's/sutFile=default.xml/sutFile=MultiNodesCluster.xml/g' pxf_src/automation/jsystem.properties
	fi
	chown -R gpadmin:gpadmin ~gpadmin/{.ssh,pxf} pxf_src/automation

	cat > ~gpadmin/run_pxf_automation_test.sh <<-EOF
		set -exuo pipefail

		source ${GPHOME}/greenplum_path.sh

		export PXF_HOME=\${GPHOME}/pxf
		export PGHOST=mdw
		export PGPORT=5432

		export ACCESS_KEY_ID='${ACCESS_KEY_ID}' SECRET_ACCESS_KEY='${SECRET_ACCESS_KEY}'

		cd ${PWD}/pxf_src/automation
		make -C ${PWD}/pxf_src/automation GROUP=${GROUP}
	EOF

	chown gpadmin:gpadmin ~gpadmin/run_pxf_automation_test.sh
	chmod a+x ~gpadmin/run_pxf_automation_test.sh

	if [[ ${ACCEPTANCE} == true ]]; then
		echo 'Acceptance test pipeline'
		exit 1
	fi

	# needs to be login shell for UTF-8 tests (LOCALE)
	su --login gpadmin -c ~gpadmin/run_pxf_automation_test.sh
}

function _main() {
	cp -R cluster_env_files/.ssh/* /root/.ssh
	# make an array, gpdb_segments, containing hostnames that contain 'sdw'
	mapfile -t gpdb_segments < <(grep < cluster_env_files/etc_hostfile -e sdw | awk '{print $1}')
	if [[ -d dataproc_env_files ]]; then
		HADOOP_HOSTNAME=$(< dataproc_env_files/name)
		HADOOP_USER=gpadmin
		hadoop_ip=$(getent hosts "${HADOOP_HOSTNAME}" | awk '{ print $1 }')
		HADOOP_SSH_OPTS+=(-i dataproc_env_files/google_compute_engine)
		HDFS_BIN=/usr/bin
	else
		HADOOP_HOSTNAME=hadoop
		HADOOP_USER=centos
		HDFS_BIN=~centos/singlecluster/bin
		hadoop_ip=$(grep < cluster_env_files/etc_hostfile edw0 | awk '{print $1}')
		# tell hbase where to find zookeeper
		sed -i "/<name>hbase.zookeeper.quorum<\/name>/ {n; s/127.0.0.1/${hadoop_ip}/}" \
			"${LOCAL_GPHD_ROOT}/hbase/conf/hbase-site.xml"
	fi

	install_gpdb_binary # Installs the GPDB Binary on the container
	setup_gpadmin_user
	install_pxf_server
	init_and_configure_pxf_server
	remote_access_to_gpdb

	configure_local_hdfs

	# widen access to mdw to all nodes in the cluster for JDBC test
	update_pghba_conf "${gpdb_segments[@]}"

	setup_pxf_on_cluster

	if [[ $KERBEROS != true ]]; then
		run_multinode_smoke_test 1000
	fi

	run_pxf_automation
}

_main
