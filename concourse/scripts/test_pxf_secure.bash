#!/bin/bash

set -exo pipefail

CWDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "${CWDIR}/pxf_common.bash"

AMBARI_PREFIX=http://${NODE}:8080/api/v1
CURL_OPTS=(-u admin:admin -H X-Requested-By:ambari)

function run_pxf_smoke_secure() {
	cat > /home/gpadmin/run_pxf_smoke_secure_test.sh <<-EOF
		set -exo pipefail

		source '${GPHOME}/greenplum_path.sh'
		source '${PWD}/gpdb_src/gpAux/gpdemo/gpdemo-env.sh'

		export PATH=\$PATH:${GPHD_ROOT}/bin
		echo 'Reading external table from Hadoop to Greenplum using PXF'
		psql -d template1 -c "
		   CREATE EXTENSION PXF;
		   CREATE EXTERNAL TABLE test (name TEXT)
		      LOCATION ('pxf://tmp/test.txt?profile=HdfsTextSimple')
		      FORMAT 'TEXT';
		   SELECT * FROM test;
		"
		echo 'Writing to external table from Greenplum to Hadoop using PXF'
		psql -d template1 -c "
		   CREATE WRITABLE EXTERNAL TABLE test_output (name TEXT)
		      LOCATION ('pxf://tmp/test_output?profile=HdfsTextSimple')
		      FORMAT 'TEXT';
		   CREATE TABLE test_internal (name TEXT);
		   INSERT INTO test_internal SELECT * FROM test;
		   INSERT INTO test_output SELECT * FROM test_internal;
		"
		num_rows=\$(psql -d template1 -t -c 'SELECT COUNT(*) FROM test' | tr -d '[:space:]')
		echo "Found \${num_rows} records"

		rows_written=\$(hdfs dfs -cat /tmp/test_output/* | wc -l)
		echo "Wrote \${rows_written} records to external HDFS"

		if [[ \${num_rows} != \${rows_written} ]]; then
		   echo 'The number of read/writing rows does not match'
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

	echo 'Adding text file to hdfs'
	su - gpadmin -c "
		kinit -kt /etc/security/keytabs/gpadmin-krb5.keytab gpadmin/${NODE}@${REALM}
		hdfs dfs -put /tmp/test.txt /tmp
		hdfs dfs -ls /tmp
	"

	chown gpadmin:gpadmin ~gpadmin/run_pxf_smoke_secure_test.sh
	chmod a+x ~gpadmin/run_pxf_smoke_secure_test.sh
	su gpadmin -c ~gpadmin/run_pxf_smoke_secure_test.sh
}

function set_hostname() {
	echo 'Setting host name'
	sed "s/$HOSTNAME/${NODE} $HOSTNAME/g" /etc/hosts > /tmp/hosts
	cp /tmp/hosts /etc
}

function secure_pxf() {
	hadoop_authentication='<property><name>hadoop.security.authentication</name><value>kerberos</value></property>'
	hadoop_authorization='<property><name>hadoop.security.authorization</name><value>true</value></property>'
	yarn_principal='<property><name>yarn.resourcemanager.principal</name><value>rm/_HOST@AMBARI.APACHE.ORG</value></property>'

	echo -e "export PXF_KEYTAB=/etc/security/keytabs/gpadmin-krb5.keytab\nexport PXF_PRINCIPAL='gpadmin/_HOST@${REALM}'" >> "${PXF_CONF_DIR}/conf/pxf-env.sh"
	sed -i -e "s|<configuration>|<configuration>\n${hadoop_authentication}\n${hadoop_authorization}|g" "${PXF_CONF_DIR}/servers/default/core-site.xml"
	sed -i -e "s|<configuration>|<configuration>\n${yarn_principal}|g" "${PXF_CONF_DIR}/servers/default/yarn-site.xml"
}

function wait_for_hadoop_services() {
	# Total wait time is 300 seconds
	local retries=20 sleep_time=15 request_id=${1}

	echo 'Waiting for Hadoop services to start'
	local status=IN_PROGRESS
	while [[ ${status} != COMPLETED ]] && (( retries > 0 )); do
		sleep ${sleep_time}
		status=$(curl -s -u admin:admin "${AMBARI_PREFIX}/clusters/${CLUSTER_NAME}/requests/${request_id}" | jq --raw-output '.Requests.request_status')
		((retries--))
	done

	if [[ ${status} != COMPLETED ]]; then
		echo 'Unable to start hadoop services'
		exit 1
	fi
}

function start_hadoop_services() {
	echo 'Starting hadoop services'
	local request_id
	request_id=$(curl "${CURL_OPTS[@]}" -X PUT -d '{"ServiceInfo": {"state" : "STARTED"}}' "${AMBARI_PREFIX}/clusters/${CLUSTER_NAME}/services" | jq --raw-output '.Requests.id')
	if [[ -z ${request_id} ]]; then
		echo 'Unable to get request id... why did cluster start command not get caught by set -e or set -o pipefail?' >&2
		exit 1
	fi

	wait_for_hadoop_services "${request_id}"
}

function start_hadoop_secure() {
	echo 'Starting kerberos services'
	service kadmin start
	service krb5kdc start

	echo 'Creating principal and keytab for gpadmin user'
	local KEYTABS_DIR=/etc/security/keytabs
	mkdir -p "${KEYTABS_DIR}"
	/usr/sbin/kadmin.local -q "addprinc -randkey -pw changeme gpadmin/${NODE}@${REALM}"
	/usr/sbin/kadmin.local -q "xst -norandkey -k ${KEYTABS_DIR}/gpadmin-krb5.keytab gpadmin/${NODE}@${REALM}"
	chown gpadmin:gpadmin "${KEYTABS_DIR}/gpadmin-krb5.keytab"

	echo 'Start ambari services'
	ambari-agent start
	ambari-server start
	sleep 45 # wait until ambari's webserver is ready to process requests

	# Start cluster
	start_hadoop_services
	# We run the start request twice, in case the first request failts
	start_hadoop_services
}

function _main() {
	set_hostname
	install_gpdb_binary
	setup_gpadmin_user

	# setup hadoop before creating GPDB cluster
	start_hadoop_secure
	install_pxf_client
	install_pxf_server
	init_and_configure_pxf_server
	configure_pxf_default_server
	secure_pxf

	# clean up pxf-site.xml
	rm "${PXF_CONF_DIR}/servers/default/pxf-site.xml"

	create_gpdb_cluster
	add_remote_user_access_for_gpdb testuser
	start_pxf_server

	time run_regression_test

	if [[ ${ACCEPTANCE} == true ]]; then
		echo 'Acceptance test pipeline'
		exit 1
	fi

	if [[ -n ${GROUP} ]]; then
		time run_pxf_smoke_secure
	fi
}

_main
