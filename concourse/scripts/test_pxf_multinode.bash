#!/bin/bash

set -exuo pipefail

CWDIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

export GPHOME=/usr/local/greenplum-db-devel
# whether PXF is being installed from a new component-based packaging
PXF_COMPONENT=${PXF_COMPONENT:=false}
if [[ ${PXF_COMPONENT} == "true" ]]; then
	PXF_HOME=/usr/local/pxf-gp${GP_VER}
else
	PXF_HOME=${GPHOME}/pxf
fi
export PXF_HOME

# shellcheck source=/dev/null
source "${CWDIR}/pxf_common.bash"

SSH_OPTS=(-i cluster_env_files/private_key.pem -o 'StrictHostKeyChecking=no')
HADOOP_SSH_OPTS=(-o 'StrictHostKeyChecking=no')
IMPERSONATION=${IMPERSONATION:-true}
LOCAL_GPHD_ROOT=/singlecluster
PROTOCOL=${PROTOCOL:-}
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

	time ssh "${SSH_OPTS[@]}" gpadmin@cdw "
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
	done | ssh "${SSH_OPTS[@]}" gpadmin@cdw "
		cat >> /data/gpdata/coordinator/gpseg-1/pg_hba.conf &&
		cat /data/gpdata/coordinator/gpseg-1/pg_hba.conf
	"
}

function add_testing_encoding() {
	# install new encoding and restart Greenplum so that the new encoding is picked up by Greenplum
	ssh "${SSH_OPTS[@]}" gpadmin@cdw "
		source ${GPHOME}/greenplum_path.sh &&
		gpssh -f ~gpadmin/hostfile_all -v -u centos -s -e 'sudo localedef -c -i ru_RU -f CP1251 ru_RU.CP1251' &&
		export MASTER_DATA_DIRECTORY=/data/gpdata/coordinator/gpseg-1 &&
		gpstop -air
	"
}

function setup_pxf_on_cluster() {
	# drop named query file for JDBC test to gpadmin's home on cdw
	scp "${SSH_OPTS[@]}" pxf_src/automation/src/test/resources/{,hive-}report.sql gpadmin@cdw:

	if [[ "${PROTOCOL}" == "file" ]]; then
		# drop pxf-profiles.xml file with sequence file profiles for file protocol
		cat > /tmp/pxf-profiles.xml <<-EOF
<?xml version="1.0" encoding="UTF-8"?>
<profiles>
    <profile>
        <name>file:AvroSequenceFile</name>
        <plugins>
            <fragmenter>org.greenplum.pxf.plugins.hdfs.HdfsDataFragmenter</fragmenter>
            <accessor>org.greenplum.pxf.plugins.hdfs.SequenceFileAccessor</accessor>
            <resolver>org.greenplum.pxf.plugins.hdfs.AvroResolver</resolver>
        </plugins>
    </profile>
    <profile>
        <name>file:SequenceFile</name>
        <plugins>
            <fragmenter>org.greenplum.pxf.plugins.hdfs.HdfsDataFragmenter</fragmenter>
            <accessor>org.greenplum.pxf.plugins.hdfs.SequenceFileAccessor</accessor>
            <resolver>org.greenplum.pxf.plugins.hdfs.WritableResolver</resolver>
        </plugins>
    </profile>
</profiles>
EOF

		scp "${SSH_OPTS[@]}" /tmp/pxf-profiles.xml "gpadmin@cdw:${BASE_DIR}/conf/pxf-profiles.xml"
	fi

	# configure PXF on coordinator, sync configs and start pxf
	ssh "${SSH_OPTS[@]}" gpadmin@cdw "
		source ${GPHOME}/greenplum_path.sh &&
		${PXF_HOME}/bin/pxf cluster register
		if [[ ! -d ${BASE_DIR} ]]; then
			if [[ ${PXF_VERSION} == 5 ]]; then
				GPHOME=${GPHOME} PXF_CONF=${BASE_DIR} ${PXF_HOME}/bin/pxf cluster init
			fi
			cp ${TEMPLATES_DIR}/templates/{hdfs,mapred,yarn,core,hbase,hive}-site.xml ${BASE_DIR}/servers/default
			sed -i -e 's/\(0.0.0.0\|localhost\|127.0.0.1\)/${hadoop_ip}/g' ${BASE_DIR}/servers/default/*-site.xml
		else
			cp ${TEMPLATES_DIR}/templates/mapred{,1}-site.xml
		fi &&
		mkdir -p ${BASE_DIR}/servers/s3{,-invalid} &&
		cp ${TEMPLATES_DIR}/templates/s3-site.xml ${BASE_DIR}/servers/s3 &&
		cp ${TEMPLATES_DIR}/templates/s3-site.xml ${BASE_DIR}/servers/s3-invalid &&
		sed -i  -e \"s|YOUR_AWS_ACCESS_KEY_ID|${ACCESS_KEY_ID}|\" \
			-e \"s|YOUR_AWS_SECRET_ACCESS_KEY|${SECRET_ACCESS_KEY}|\" \
			${BASE_DIR}/servers/s3/s3-site.xml &&
		mkdir -p ${BASE_DIR}/servers/database &&
		cp ${TEMPLATES_DIR}/templates/jdbc-site.xml ${BASE_DIR}/servers/database/ &&
		sed -i  -e 's|YOUR_DATABASE_JDBC_DRIVER_CLASS_NAME|org.postgresql.Driver|' \
			-e 's|YOUR_DATABASE_JDBC_URL|jdbc:postgresql://cdw:5432/pxfautomation|' \
			-e 's|YOUR_DATABASE_JDBC_USER|gpadmin|' \
			-e 's|YOUR_DATABASE_JDBC_PASSWORD||' \
			${BASE_DIR}/servers/database/jdbc-site.xml &&
		cp ~gpadmin/report.sql ${BASE_DIR}/servers/database &&
		cp ${BASE_DIR}/servers/database/jdbc-site.xml ${BASE_DIR}/servers/database/testuser-user.xml &&
		sed -i 's|pxfautomation|template1|' ${BASE_DIR}/servers/database/testuser-user.xml &&
		mkdir -p ${BASE_DIR}/servers/db-session-params &&
		cp ${TEMPLATES_DIR}/templates/jdbc-site.xml ${BASE_DIR}/servers/db-session-params &&
		sed -i  -e 's|YOUR_DATABASE_JDBC_DRIVER_CLASS_NAME|org.postgresql.Driver|' \
			-e 's|YOUR_DATABASE_JDBC_URL|jdbc:postgresql://cdw:5432/pxfautomation|' \
			-e 's|YOUR_DATABASE_JDBC_USER||' \
			-e 's|YOUR_DATABASE_JDBC_PASSWORD||' \
			-e 's|</configuration>|<property><name>jdbc.session.property.client_min_messages</name><value>debug1</value></property></configuration>|' \
			-e 's|</configuration>|<property><name>jdbc.session.property.default_statistics_target</name><value>123</value></property></configuration>|' \
			${BASE_DIR}/servers/db-session-params/jdbc-site.xml &&
		mkdir -p ${BASE_DIR}/servers/db-hive &&
		cp ${TEMPLATES_DIR}/templates/jdbc-site.xml ${BASE_DIR}/servers/db-hive &&
		sed -i  -e 's|YOUR_DATABASE_JDBC_DRIVER_CLASS_NAME|org.apache.hive.jdbc.HiveDriver|' \
			-e \"s|YOUR_DATABASE_JDBC_URL|jdbc:hive2://${HADOOP_HOSTNAME}:10000/default|\" \
			-e 's|YOUR_DATABASE_JDBC_USER||' \
			-e 's|YOUR_DATABASE_JDBC_PASSWORD||' \
			${BASE_DIR}/servers/db-hive/jdbc-site.xml &&
		cp ~gpadmin/hive-report.sql ${BASE_DIR}/servers/db-hive &&
		if [[ \"${PROTOCOL}\" == \"file\" ]]; then
			mkdir -p ${BASE_DIR}/servers/file
			cp ${TEMPLATES_DIR}/templates/mapred-site.xml ${BASE_DIR}/servers/file
			cp ${TEMPLATES_DIR}/templates/pxf-site.xml ${BASE_DIR}/servers/file
			sed -i \
			-e 's|</configuration>|<property><name>pxf.fs.basePath</name><value>${BASE_PATH}</value></property></configuration>|g' \
			-e '/<name>pxf.service.user.impersonation<\/name>/ {n;s|<value>.*</value>|<value>false</value>|g;}' \
			${BASE_DIR}/servers/file/pxf-site.xml
		fi &&
		if [[ ${IMPERSONATION} == true ]]; then
			cp -r ${BASE_DIR}/servers/default ${BASE_DIR}/servers/default-no-impersonation
			if [[ ! -f ${BASE_DIR}/servers/default-no-impersonation/pxf-site.xml ]]; then
				cp ${TEMPLATES_DIR}/templates/pxf-site.xml ${BASE_DIR}/servers/default-no-impersonation/pxf-site.xml
			fi
			sed -i \
			-e '/<name>pxf.service.user.impersonation<\/name>/ {n;s|<value>.*</value>|<value>false</value>|g;}' \
			-e 's|</configuration>|<property><name>pxf.service.user.name</name><value>foobar</value></property></configuration>|g' \
			${BASE_DIR}/servers/default-no-impersonation/pxf-site.xml
		fi &&
		echo 'export PXF_LOADER_PATH=file:/tmp/publicstage/pxf' >> ${BASE_DIR}/conf/pxf-env.sh && \
		echo -e '\npxf.profile.dynamic.regex=test:.*' >> ${BASE_DIR}/conf/pxf-application.properties && \
		PXF_BASE=${BASE_DIR} ${PXF_HOME}/bin/pxf cluster sync
	"
}

function setup_pxf_kerberos_on_cluster() {
	DATAPROC_DIR=$(find /tmp/build/ -name dataproc_env_files)
	REALM=$(< "${DATAPROC_DIR}/REALM")
	REALM=${REALM^^} # make sure REALM is up-cased, down-case below for hive principal
	KERBERIZED_HADOOP_URI="hive/${HADOOP_HOSTNAME}.${REALM,,}@${REALM};saslQop=auth-conf" # quoted because of semicolon
	sed -i  -e "s|</hdfs>|<hadoopRoot>$DATAPROC_DIR</hadoopRoot></hdfs>|g" \
		-e "s|</cluster>|<testKerberosPrincipal>gpadmin@${REALM}</testKerberosPrincipal></cluster>|g" \
		-e "s|</hive>|<kerberosPrincipal>${KERBERIZED_HADOOP_URI}</kerberosPrincipal><userName>gpadmin</userName></hive>|g" \
		"$multiNodesCluster"
	ssh gpadmin@cdw "
		cp ${TEMPLATES_DIR}/templates/pxf-site.xml ${BASE_DIR}/servers/db-hive/pxf-site.xml &&
		sed -i 's|gpadmin/_HOST@EXAMPLE.COM|gpadmin@${REALM}|g' ${BASE_DIR}/servers/db-hive/pxf-site.xml &&
		sed -i 's|</configuration>|<property><name>hadoop.security.authentication</name><value>kerberos</value></property></configuration>|g' \
			${BASE_DIR}/servers/db-hive/jdbc-site.xml &&
		sed -i -e 's|\(jdbc:hive2://${HADOOP_HOSTNAME}:10000/default\)|\1;principal=${KERBERIZED_HADOOP_URI}|g' \
			${BASE_DIR}/servers/db-hive/jdbc-site.xml &&
		PXF_BASE=${BASE_DIR} ${PXF_HOME}/bin/pxf cluster sync
	"
	sudo mkdir -p /etc/security/keytabs
	sudo cp "${DATAPROC_DIR}/pxf.service.keytab" /etc/security/keytabs/gpadmin.headless.keytab
	sudo chown gpadmin:gpadmin /etc/security/keytabs/gpadmin.headless.keytab
	scp centos@cdw:/etc/krb5.conf /tmp/krb5.conf
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
		KERBERIZED_HADOOP_2_URI="hive/${HADOOP_2_HOSTNAME}.${REALM2,,}@${REALM2};saslQop=auth-conf" # quoted because of semicolon
		ssh gpadmin@cdw "
			mkdir -p ${BASE_DIR}/servers/hdfs-secure &&
			cp ${TEMPLATES_DIR}/templates/pxf-site.xml ${BASE_DIR}/servers/hdfs-secure &&
			sed -i -e \"s|>gpadmin/_HOST@EXAMPLE.COM<|>${HADOOP_2_USER}/_HOST@${REALM2}<|g\" ${BASE_DIR}/servers/hdfs-secure/pxf-site.xml &&
			sed -i -e 's|/pxf.service.keytab<|/pxf.service.2.keytab<|g' ${BASE_DIR}/servers/hdfs-secure/pxf-site.xml
		"
		scp dataproc_2_env_files/conf/*-site.xml "gpadmin@cdw:${BASE_DIR}/servers/hdfs-secure"
		ssh gpadmin@cdw "PXF_BASE=${BASE_DIR} ${PXF_HOME}/bin/pxf cluster sync"

		sed -i  -e "s|</hdfs2>|<hadoopRoot>$DATAPROC_2_DIR</hadoopRoot><testKerberosPrincipal>${HADOOP_2_USER}@${REALM2}</testKerberosPrincipal></hdfs2>|g" \
			-e "s|</hive2>|<kerberosPrincipal>${KERBERIZED_HADOOP_2_URI}</kerberosPrincipal><userName>${HADOOP_2_USER}</userName></hive2>|g" \
			"$multiNodesCluster"

		# Create the db-hive-kerberos server configuration
		ssh "${SSH_OPTS[@]}" gpadmin@cdw "
			mkdir -p ${BASE_DIR}/servers/db-hive-kerberos &&
			cp ${TEMPLATES_DIR}/templates/jdbc-site.xml ${BASE_DIR}/servers/db-hive-kerberos &&
			sed -i -e 's|YOUR_DATABASE_JDBC_DRIVER_CLASS_NAME|org.apache.hive.jdbc.HiveDriver|' \
				-e \"s|YOUR_DATABASE_JDBC_URL|jdbc:hive2://${HADOOP_2_HOSTNAME}:10000/default;principal=${KERBERIZED_HADOOP_2_URI}|\" \
				-e 's|YOUR_DATABASE_JDBC_USER||' \
				-e 's|YOUR_DATABASE_JDBC_PASSWORD||' \
				${BASE_DIR}/servers/db-hive-kerberos/jdbc-site.xml &&
			cp ~gpadmin/hive-report.sql ${BASE_DIR}/servers/db-hive-kerberos &&
			cp ${TEMPLATES_DIR}/templates/pxf-site.xml ${BASE_DIR}/servers/db-hive-kerberos/pxf-site.xml &&
			sed -i 's|gpadmin/_HOST@EXAMPLE.COM|${HADOOP_2_USER}/_HOST@${REALM2}|g' ${BASE_DIR}/servers/db-hive-kerberos/pxf-site.xml &&
			sed -i -e 's|/pxf.service.keytab<|/pxf.service.2.keytab<|g' ${BASE_DIR}/servers/db-hive-kerberos/pxf-site.xml &&
			if [[ ${PXF_VERSION} == 5 ]]; then
				sed -i -e 's|\${pxf.service.user.impersonation.enabled}|false|g' ${BASE_DIR}/servers/db-hive-kerberos/pxf-site.xml
			else
				sed -i -e 's|<value>true</value>|<value>false</value>|g' ${BASE_DIR}/servers/db-hive-kerberos/pxf-site.xml
			fi &&
			sed -i 's|</configuration>|<property><name>hadoop.security.authentication</name><value>kerberos</value></property></configuration>|g' \
				${BASE_DIR}/servers/db-hive-kerberos/jdbc-site.xml &&
			PXF_BASE=${BASE_DIR} ${PXF_HOME}/bin/pxf cluster sync
		"

		# Add foreign dataproc hostfile to /etc/hosts
		sudo tee --append /etc/hosts < dataproc_2_env_files/etc_hostfile

		ssh "${HADOOP_2_SSH_OPTS[@]}" -t "${HADOOP_2_USER}@${HADOOP_2_HOSTNAME}" \
			"set -euo pipefail
			sudo kadmin.local -q 'addprinc -pw pxf ${HADOOP_2_USER}/${GPDB_CLUSTER_NAME_BASE}-0.c.${GOOGLE_PROJECT_ID}.internal'
			sudo kadmin.local -q 'addprinc -pw pxf ${HADOOP_2_USER}/${GPDB_CLUSTER_NAME_BASE}-1.c.${GOOGLE_PROJECT_ID}.internal'
			sudo kadmin.local -q 'addprinc -pw pxf ${HADOOP_2_USER}/${GPDB_CLUSTER_NAME_BASE}-2.c.${GOOGLE_PROJECT_ID}.internal'
			sudo kadmin.local -q \"xst -k \${HOME}/pxf.service-cdw.keytab ${HADOOP_2_USER}/${GPDB_CLUSTER_NAME_BASE}-0.c.${GOOGLE_PROJECT_ID}.internal\"
			sudo kadmin.local -q \"xst -k \${HOME}/pxf.service-sdw1.keytab ${HADOOP_2_USER}/${GPDB_CLUSTER_NAME_BASE}-1.c.${GOOGLE_PROJECT_ID}.internal\"
			sudo kadmin.local -q \"xst -k \${HOME}/pxf.service-sdw2.keytab ${HADOOP_2_USER}/${GPDB_CLUSTER_NAME_BASE}-2.c.${GOOGLE_PROJECT_ID}.internal\"
			sudo chown ${HADOOP_2_USER} \"\${HOME}/pxf.service-cdw.keytab\"
			sudo chown ${HADOOP_2_USER} \"\${HOME}/pxf.service-sdw1.keytab\"
			sudo chown ${HADOOP_2_USER} \"\${HOME}/pxf.service-sdw2.keytab\"
			"
		scp "${HADOOP_2_SSH_OPTS[@]}" "${HADOOP_2_USER}@${HADOOP_2_HOSTNAME}":~/pxf.service-*.keytab \
			/tmp/

		scp /tmp/pxf.service-*.keytab gpadmin@cdw:~/dataproc_2_env_files/

		# Add foreign dataproc hostfile to /etc/hosts on all nodes and copy keytab
		ssh gpadmin@cdw "
			source ${GPHOME}/greenplum_path.sh &&
			gpscp -f ~gpadmin/hostfile_all -v -r -u centos ~/dataproc_2_env_files/etc_hostfile =:/tmp/etc_hostfile &&
			gpssh -f ~gpadmin/hostfile_all -v -u centos -s -e 'sudo tee --append /etc/hosts < /tmp/etc_hostfile' &&
			gpscp -h cdw -v -r -u gpadmin ~/dataproc_2_env_files/pxf.service-cdw.keytab =:${BASE_DIR}/keytabs/pxf.service.2.keytab &&
			gpscp -h sdw1 -v -r -u gpadmin ~/dataproc_2_env_files/pxf.service-sdw1.keytab =:${BASE_DIR}/keytabs/pxf.service.2.keytab &&
			gpscp -h sdw2 -v -r -u gpadmin ~/dataproc_2_env_files/pxf.service-sdw2.keytab =:${BASE_DIR}/keytabs/pxf.service.2.keytab
		"
		sudo cp "${DATAPROC_2_DIR}/pxf.service.keytab" /etc/security/keytabs/gpuser.headless.keytab
		sudo chown gpadmin:gpadmin /etc/security/keytabs/gpuser.headless.keytab
		sed -i "s/>second-hadoop</>${HADOOP_2_HOSTNAME}</g" "$multiNodesCluster"
	fi

	if [[ -d ipa_env_files ]]; then
		# create the third hdfs-ipa cluster configuration
		HADOOP_3_HOSTNAME="$(< ipa_env_files/nn01)"
		HIVE_3_HOSTNAME="$(< ipa_env_files/nn02)"
		# see ansible/ipa-multinode-hadoop/tasks/ipa-server.yml
		HADOOP_3_USER=stout
		PXF_3_USER=porter
		HADOOP_3_SSH_OPTS=(-o 'UserKnownHostsFile=/dev/null' -o 'StrictHostKeyChecking=no' -i ipa_env_files/google_compute_engine)
		HADOOP_3_DIR=$(find /tmp/build/ -name ipa_env_files)
		DOMAIN3="$(< "${HADOOP_3_DIR}/domain")"
		REALM3="$(< "${HADOOP_3_DIR}/REALM")"
		REALM3="${REALM3^^}" # make sure REALM3 is upper-case
		ssh gpadmin@cdw "
			mkdir -p ${BASE_DIR}/servers/hdfs-ipa &&
			cp ${TEMPLATES_DIR}/templates/pxf-site.xml ${BASE_DIR}/servers/hdfs-ipa &&
			sed -i \
				-e \"s|>gpadmin/_HOST@EXAMPLE.COM<|>${PXF_3_USER}@${REALM3}<|g\" \
				-e 's|/pxf.service.keytab<|/pxf.service.3.keytab<|g' \
				-e '/pxf.service.kerberos.constrained-delegation/{n;s|<value>.*</value>|<value>true</value>|}' ${BASE_DIR}/servers/hdfs-ipa/pxf-site.xml
		"

		scp ipa_env_files/conf/*-site.xml "gpadmin@cdw:${BASE_DIR}/servers/hdfs-ipa"

		# optionally create a non-impersonation configuration servers with/without a service user for the IPA cluster for the proxy test
		ssh gpadmin@cdw "
			if [[ ${IMPERSONATION} == true ]]; then
				cp -r ${BASE_DIR}/servers/hdfs-ipa ${BASE_DIR}/servers/hdfs-ipa-no-impersonation
				sed -i \
				-e '/<name>pxf.service.user.impersonation<\/name>/ {n;s|<value>.*</value>|<value>false</value>|g;}' \
				-e 's|</configuration>|<property><name>pxf.service.user.name</name><value>foobar</value></property></configuration>|g' \
				-e '/pxf.service.kerberos.constrained-delegation/{n;s|<value>.*</value>|<value>true</value>|}' \
				${BASE_DIR}/servers/hdfs-ipa-no-impersonation/pxf-site.xml
				cp -r ${BASE_DIR}/servers/hdfs-ipa ${BASE_DIR}/servers/hdfs-ipa-no-impersonation-no-svcuser
				sed -i \
				-e '/<name>pxf.service.user.impersonation<\/name>/ {n;s|<value>.*</value>|<value>false</value>|g;}' \
				-e '/pxf.service.kerberos.constrained-delegation/{n;s|<value>.*</value>|<value>true</value>|}' \
				${BASE_DIR}/servers/hdfs-ipa-no-impersonation-no-svcuser/pxf-site.xml
			fi
		"

		# sync up PXF server configuration
		ssh gpadmin@cdw "PXF_BASE=${BASE_DIR} ${PXF_HOME}/bin/pxf cluster sync"

		# add configuration information to the SUT file for the automation suite
		sed -i \
			-e "s|</hdfsIpa>|<hadoopRoot>${HADOOP_3_DIR}</hadoopRoot></hdfsIpa>|g" \
			-e "s|</hdfsIpa>|<testKerberosPrincipal>${HADOOP_3_USER}@${REALM3}</testKerberosPrincipal></hdfsIpa>|g" \
			-e "s|</hdfsIpa>|<testKerberosKeytab>${HADOOP_3_DIR}/hadoop.user.keytab</testKerberosKeytab></hdfsIpa>|g" \
			-e "s|</hdfsIpa>|<sshUserName>hdfs</sshUserName></hdfsIpa>|g" \
			-e "s|</hdfsIpa>|<sshPrivateKey>${HADOOP_3_DIR}/google_compute_engine</sshPrivateKey></hdfsIpa>|g" \
			-e "s|hive-ipa-host|${HIVE_3_HOSTNAME}.${DOMAIN3}|g" \
			-e "s|IPA-REALM|${REALM3}|g" \
			"$multiNodesCluster"

		# add foreign Hadoop and IPA KDC hostfile to /etc/hosts
		sudo tee --append /etc/hosts < ipa_env_files/etc_hostfile

		# add foreign Hadoop and IPA KDC hostfile to /etc/hosts on all nodes
		ssh gpadmin@cdw "
			source ${GPHOME}/greenplum_path.sh &&
			gpscp -f ~gpadmin/hostfile_all -v -r -u centos ~/ipa_env_files/etc_hostfile =:/tmp/etc_hostfile &&
			gpssh -f ~gpadmin/hostfile_all -v -u centos -s -e 'sudo tee --append /etc/hosts < /tmp/etc_hostfile' &&
			gpscp -f ~gpadmin/hostfile_all -v -r -u gpadmin ~/ipa_env_files/pxf.service.keytab =:${BASE_DIR}/keytabs/pxf.service.3.keytab
			gpscp -f ~gpadmin/hostfile_all -v -r -u gpadmin ~/ipa_env_files/hadoop.user.keytab =:${BASE_DIR}/keytabs/hadoop.user.3.keytab
		"

		sudo cp "${HADOOP_3_DIR}/hadoop.user.keytab" "/etc/security/keytabs/${HADOOP_3_USER}.headless.keytab"
		sudo chown gpadmin:gpadmin "/etc/security/keytabs/${HADOOP_3_USER}.headless.keytab"
		sudo chown gpadmin:gpadmin "${HADOOP_3_DIR}/google_compute_engine"
		sudo chown gpadmin:gpadmin "${HADOOP_3_DIR}/hadoop.user.keytab"
		sed -i "s/>ipa-hadoop</>${HADOOP_3_HOSTNAME}</g" "$multiNodesCluster"

	fi

	# Create the non-secure cluster configuration
	NON_SECURE_HADOOP_IP=$(grep < cluster_env_files/etc_hostfile edw0 | awk '{print $1}')
	ssh gpadmin@cdw "
		mkdir -p ${BASE_DIR}/servers/db-hive-non-secure &&
		cp ${TEMPLATES_DIR}/templates/jdbc-site.xml ${BASE_DIR}/servers/db-hive-non-secure &&
		sed -i -e 's|YOUR_DATABASE_JDBC_DRIVER_CLASS_NAME|org.apache.hive.jdbc.HiveDriver|' \
			-e \"s|YOUR_DATABASE_JDBC_URL|jdbc:hive2://${NON_SECURE_HADOOP_IP}:10000/default|\" \
			-e 's|YOUR_DATABASE_JDBC_USER||' \
			-e 's|YOUR_DATABASE_JDBC_PASSWORD||' \
			${BASE_DIR}/servers/db-hive-non-secure/jdbc-site.xml &&
		cp ~gpadmin/hive-report.sql ${BASE_DIR}/servers/db-hive-non-secure &&
		mkdir -p ${BASE_DIR}/servers/hdfs-non-secure &&
		cp ${TEMPLATES_DIR}/templates/{hdfs,mapred,yarn,core,hbase,hive,pxf}-site.xml ${BASE_DIR}/servers/hdfs-non-secure &&
		sed -i -e 's/\(0.0.0.0\|localhost\|127.0.0.1\)/${NON_SECURE_HADOOP_IP}/g' ${BASE_DIR}/servers/hdfs-non-secure/*-site.xml &&
		sed -i -e 's|</configuration>|<property><name>pxf.service.user.name</name><value>${PROXY_USER}</value></property></configuration>|g' ${BASE_DIR}/servers/hdfs-non-secure/pxf-site.xml &&
		PXF_BASE=${BASE_DIR} ${PXF_HOME}/bin/pxf cluster sync
	"
	sed -i "s/>non-secure-hadoop</>${NON_SECURE_HADOOP_IP}</g" "$multiNodesCluster"

	# Create a secured server configuration with invalid principal name
	ssh gpadmin@cdw "
		mkdir -p ${BASE_DIR}/servers/secure-hdfs-invalid-principal &&
		cp ${BASE_DIR}/servers/default/*-site.xml ${BASE_DIR}/servers/secure-hdfs-invalid-principal &&
		cp ${TEMPLATES_DIR}/templates/pxf-site.xml ${BASE_DIR}/servers/secure-hdfs-invalid-principal &&
		sed -i -e 's|>gpadmin/_HOST@EXAMPLE.COM<|>foobar/_HOST@INVALID.REALM.INTERNAL<|g' ${BASE_DIR}/servers/secure-hdfs-invalid-principal/pxf-site.xml &&
		PXF_BASE=${BASE_DIR} ${PXF_HOME}/bin/pxf cluster sync
	"

	# Create a secured server configuration with invalid keytab
	ssh gpadmin@cdw "
		mkdir -p ${BASE_DIR}/servers/secure-hdfs-invalid-keytab &&
		cp ${BASE_DIR}/servers/default/*-site.xml ${BASE_DIR}/servers/secure-hdfs-invalid-keytab &&
		cp ${TEMPLATES_DIR}/templates/pxf-site.xml ${BASE_DIR}/servers/secure-hdfs-invalid-keytab &&
		sed -i -e 's|/pxf.service.keytab<|/non.existent.keytab<|g' ${BASE_DIR}/servers/secure-hdfs-invalid-keytab/pxf-site.xml &&
		PXF_BASE=${BASE_DIR} ${PXF_HOME}/bin/pxf cluster sync
	"

	# Configure the principal for the default-no-impersonation server
	ssh gpadmin@cdw "
	if [[ ${IMPERSONATION} == true ]]; then
		sed -i -e 's|gpadmin/_HOST@EXAMPLE.COM|gpadmin@${REALM}|g' ${BASE_DIR}/servers/default-no-impersonation/pxf-site.xml &&
		PXF_BASE=${BASE_DIR} ${PXF_HOME}/bin/pxf cluster sync
	fi
	"
}

function configure_nfs() {
	echo "install the NFS client"
	yum install -y -q -e 0 nfs-utils

	echo "check available NFS shares in cdw"
	showmount -e cdw

	echo "create mount point and mount it"
	mkdir -p "${BASE_PATH}"
	mount -o nolock -t nfs cdw:/var/nfs "${BASE_PATH}"
	chown -R gpadmin:gpadmin "${BASE_PATH}"
	chmod -R 755 "${BASE_PATH}"

	echo "verify the mount worked"
	mount | grep nfs
	df -hT

	echo "write a test file to make sure it worked"
	sudo runuser -l gpadmin -c "touch ${BASE_PATH}/$(hostname)-test"
	ls -l "${BASE_PATH}"
}

function run_pxf_automation() {
	local multiNodesCluster=pxf_src/automation/src/test/resources/sut/MultiNodesCluster.xml

	if [[ $KERBEROS == true ]]; then
		if [[ -d ipa_env_files ]]; then
			# use SUT file with configuration that includes IPA hadoop cluster
			multiNodesCluster=pxf_src/automation/src/test/resources/sut/MultiHadoopIPAMultiNodesCluster.xml
		else
			multiNodesCluster=pxf_src/automation/src/test/resources/sut/MultiHadoopMultiNodesCluster.xml
		fi
	fi

	if (( HIVE_VERSION == 2 )); then
		local search='<hiveBaseHdfsDirectory>/hive/warehouse/</hiveBaseHdfsDirectory>'
		local replace='<hiveBaseHdfsDirectory>/user/hive/warehouse/</hiveBaseHdfsDirectory>'
		sed -i "s|${search}|${replace}|g" "$multiNodesCluster"
	fi

	# adjust GPHOME in SUT files
	if [[ ${PXF_COMPONENT} == "true" ]]; then
		sed -i "s/greenplum-db-devel/greenplum-db/g" "$multiNodesCluster"
	fi

	# point the tests at remote Hadoop and GPDB
	sed -i "s/>hadoop</>${HADOOP_HOSTNAME}</g" "$multiNodesCluster"
	sed -i "/<class>org.greenplum.pxf.automation.components.gpdb.Gpdb<\/class>/ {n; s/localhost/cdw/}" \
		"$multiNodesCluster"

	if [[ $KERBEROS == true ]]; then
		setup_pxf_kerberos_on_cluster
	fi
	sed -i "s/sutFile=default.xml/sutFile=$(basename $multiNodesCluster)/g" pxf_src/automation/jsystem.properties

	chown -R gpadmin:gpadmin ~gpadmin/.ssh pxf_src/automation

	cat > ~gpadmin/run_pxf_automation_test.sh <<-EOF
		#!/usr/bin/env bash
		set -exuo pipefail

		source ${GPHOME}/greenplum_path.sh

		# set explicit GPHOME here for consistency across container / CCP
		export GPHOME=${GPHOME}
		export PXF_HOME=${PXF_HOME}
		export PGHOST=cdw
		export PGPORT=5432

		export ACCESS_KEY_ID='${ACCESS_KEY_ID}' SECRET_ACCESS_KEY='${SECRET_ACCESS_KEY}'

		export BASE_PATH='${BASE_PATH}'
		export PROTOCOL=${PROTOCOL}

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

	remote_access_to_gpdb
	if [[ "${PROTOCOL}" == "file" ]]; then
		# ensure user id and group id match the VM id on the container to be able
		# to read and write files
		usermod -u  "$(ssh cdw 'id -u gpadmin')" gpadmin
		groupmod -g "$(ssh cdw 'id -g gpadmin')" gpadmin
	fi

	cp -R cluster_env_files/.ssh/* /root/.ssh
	# make an array, gpdb_segments, containing hostnames that contain 'sdw'
	mapfile -t gpdb_segments < <(grep < cluster_env_files/etc_hostfile -e sdw | awk '{print $1}')
	if [[ "${PROTOCOL}" == "file" ]]; then
		HADOOP_HOSTNAME=localhost
		HADOOP_USER=gpadmin
		HDFS_BIN=/singlecluster/bin
		hadoop_ip=127.0.0.1
	elif [[ -d dataproc_env_files ]]; then
		HADOOP_HOSTNAME=$(< dataproc_env_files/name)
		HADOOP_USER=gpadmin
		hadoop_ip=$(getent hosts "${HADOOP_HOSTNAME}.c.${GOOGLE_PROJECT_ID}.internal" | awk '{ print $1 }')
		HADOOP_SSH_OPTS+=(-i dataproc_env_files/google_compute_engine)
		HDFS_BIN=/usr/bin
	elif grep "edw0" cluster_env_files/etc_hostfile; then
		HADOOP_HOSTNAME=hadoop
		HADOOP_USER=centos
		HDFS_BIN=~centos/singlecluster/bin
		hadoop_ip=$(grep < cluster_env_files/etc_hostfile edw0 | awk '{print $1}')
		# tell hbase where to find zookeeper
		sed -i "/<name>hbase.zookeeper.quorum<\/name>/ {n; s/127.0.0.1/${hadoop_ip}/}" \
			"${LOCAL_GPHD_ROOT}/hbase/conf/hbase-site.xml"
	fi

	if [[ ${PXF_COMPONENT} == "true" ]]; then
		install_gpdb_package
		setup_gpadmin_user
		if [[ -d pxf_tarball ]]; then
			install_pxf_tarball
		elif [[ -d pxf_artifact ]]; then
			cp pxf_artifact/*.rpm /tmp/pxf_installer
			cp pxf_src/package/install_rpm /tmp/pxf_installer/install_component
		else
			echo "Unable to find a suitable PXF installer"
			exit 1
		fi
	else
		install_gpdb_binary # Installs the GPDB Binary on the container
		setup_gpadmin_user
		install_pxf_server
	fi
	if [[ ${PXF_VERSION} == "5" ]]; then
		init_pxf
	fi
	configure_pxf_server

	inflate_singlecluster
	configure_local_hdfs

	# widen access to cdw to all nodes in the cluster for JDBC test
	update_pghba_conf "${gpdb_segments[@]}"

	# Add the ru_RU.CP1251 encoding for testing
	add_testing_encoding

	setup_pxf_on_cluster

	if [[ "$PROTOCOL" == "file" ]]; then
		configure_nfs # configures NFS on the container
	fi

	if [[ "$PROTOCOL" != "file" ]] && [[ $KERBEROS != true ]]; then
		run_multinode_smoke_test 1000
	fi

	inflate_dependencies
	run_pxf_automation
}

_main
