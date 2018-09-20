#!/bin/bash -l

set -exo pipefail

GPHOME="/usr/local/greenplum-db-devel"
PXF_HOME="${GPHOME}/pxf"
AMBARI_PREFIX="http://${NODE}:8080/api/v1"
CURL_OPTS="-u admin:admin -H X-Requested-By:ambari"
CWDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "${CWDIR}/pxf_common.bash"

function run_pxf_smoke_secure() {
	cat > /home/gpadmin/run_pxf_smoke_secure_test.sh <<-EOF
	set -exo pipefail

	source ${GPHOME}/greenplum_path.sh
	source \${1}/gpdb_src/gpAux/gpdemo/gpdemo-env.sh

	echo "Reading external table from Hadoop to Greenplum using PXF"
	psql -d template1 -c "CREATE EXTENSION PXF"
	psql -d template1 -c "CREATE EXTERNAL TABLE test (name TEXT) LOCATION ('pxf://tmp/test.txt?profile=HdfsTextSimple') FORMAT 'TEXT';"
	psql -d template1 -c "SELECT * FROM test"
	psql -d template1 -c "CREATE WRITABLE EXTERNAL TABLE test_output (name TEXT) LOCATION ('pxf://tmp/test_output?profile=HdfsTextSimple') FORMAT 'TEXT';"
	psql -d template1 -c "CREATE TABLE test_internal (name TEXT);"
	psql -d template1 -c "INSERT INTO test_internal SELECT * FROM test"
	echo "Writing to external table from Greenplum to Hadoop using PXF"
	psql -d template1 -c "INSERT INTO test_output SELECT * FROM test_internal"
	num_rows=\$(psql -d template1 -t -c "SELECT COUNT(*) FROM test" | tr -d \'[:space:]\')
	echo "Found \${num_rows} records"

	wrote_rows=\$(hdfs dfs -cat /tmp/test_output/* | wc -l)
	echo "Wrote \${wrote_rows} records to external HDFS"

	if [ \${num_rows} != \${wrote_rows} ]; then
		echo 'The number of read/writteng rows does not match'
		exit 1
	fi

	exit 0
	EOF

	cat > /tmp/test.txt <<-EOF
Alex
Ben
Divya
Francisco
Kong
Lav
Shivram
EOF

	su - gpadmin -c "kinit -kt /etc/security/keytabs/gpadmin-krb5.keytab gpadmin/${NODE}@${REALM}"
	echo "Adding text file to hdfs"
	su - gpadmin -c "hdfs dfs -put /tmp/test.txt /tmp"
	su - gpadmin -c "hdfs dfs -ls /tmp"

	chown gpadmin:gpadmin /home/gpadmin/run_pxf_smoke_secure_test.sh
	chmod a+x /home/gpadmin/run_pxf_smoke_secure_test.sh
	su gpadmin -c "bash /home/gpadmin/run_pxf_smoke_secure_test.sh $(pwd)"
}

function set_hostname() {
	sed "s/$HOSTNAME/${NODE} $HOSTNAME/g" /etc/hosts > /tmp/hosts
	echo y | cp /tmp/hosts /etc/hosts
}

function secure_pxf() {
	echo -e "export PXF_KEYTAB=\"/etc/security/keytabs/gpadmin-krb5.keytab\"\nexport PXF_PRINCIPAL=\"gpadmin/_HOST@${REALM}\"" >> /usr/local/greenplum-db-devel/pxf/conf/pxf-env.sh
}

function wait_for_hadoop_services() {
	local retries=20 # Total wait time is 300 seconds
	local sleep_time=15
	local request_id=${1}

	echo "Waiting for Hadoop services to start"

	local status=IN_PROGRESS

	while [ ${status} != COMPLETED ] && [ ${retries} -gt 0 ]; do
		sleep ${sleep_time}
		status=$(curl -s -u admin:admin ${AMBARI_PREFIX}/clusters/${CLUSTER_NAME}/requests/${request_id} | jq --raw-output '.Requests.request_status')
		retries=$((retries-1))
	done

	if [ ${status} != COMPLETED ]; then
		echo "Unable to start hadoop services"
		exit 1
	fi
}

function start_hadoop_services() {
	echo "Starting hadoop services"

	local request_id=$(curl ${CURL_OPTS} -X PUT -d '{"ServiceInfo": {"state" : "STARTED"}}' ${AMBARI_PREFIX}/clusters/${CLUSTER_NAME}/services | jq '.Requests.id')

	if [ -z "${request_id}" ]; then
		echo -e "Unable to get request id... why did cluster start command not get caught by set -e or set -o pipefail?" >&2
		exit 1
	fi

	wait_for_hadoop_services ${request_id}
}

function start_hadoop_secure() {

	echo 'Starting kerberos services'
	service kadmin start
	service krb5kdc start

	echo 'Creating principal and keytab for gpadmin user'

	/usr/sbin/kadmin.local -q "addprinc -randkey -pw changeme gpadmin/${NODE}@${REALM}"
	/usr/sbin/kadmin.local -q "xst -norandkey -k /etc/security/keytabs/gpadmin-krb5.keytab gpadmin/${NODE}@${REALM}"
	chown gpadmin:gpadmin /etc/security/keytabs/gpadmin-krb5.keytab

	echo 'Start ambari services'
	ambari-agent start
	ambari-server start
	sleep 30 # wait until ambari's webserver is ready to process requests
	# Start cluster
	start_hadoop_services
	# We run the start request twice, in case the first request failts
	start_hadoop_services
}

function _main() {
	if [ -z "${TARGET_OS}" ]; then
		echo "FATAL: TARGET_OS is not set"
		exit 1
	fi

	if [ "${TARGET_OS}" != "centos" -a "${TARGET_OS}" != "sles" ]; then
		echo "FATAL: TARGET_OS is set to an unsupported value: ${TARGET_OS}"
		echo "Configure TARGET_OS to be centos or sles"
		exit 1
	fi

	# Reserve port 5888 for PXF service
	echo "pxf             5888/tcp               # PXF Service" >> /etc/services

	time set_hostname
	time install_gpdb
	time setup_gpadmin_user
	time setup_sshd
	# setup hadoop before making GPDB cluster
	time start_hadoop_secure
	source ${GPHOME}/greenplum_path.sh
	time install_pxf_client

	# untar pxf server only if necessary
	if [ -d ${PXF_HOME} ]; then
		echo "Skipping pxf_tarball..."
	else
		tar -xzf pxf_tarball/pxf.tar.gz -C ${GPHOME}
	fi
	chown -R gpadmin:gpadmin ${GPHOME}/pxf

	time secure_pxf
	time make_cluster
	time add_user_access "gpadmin"
	time start_pxf_server
	# Let's make sure that automation directories are writeable
	chmod a+w pxf_src/automation
	find pxf_src/automation/tinc* -type d -exec chmod a+w {} \;
	#time run_regression_test
	if [ -n "${GROUP}" ]; then
		time run_pxf_smoke_secure ${PWD}
	fi
}

_main "$@"
