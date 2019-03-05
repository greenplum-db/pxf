#!/bin/bash -l

set -exo pipefail

CWDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "${CWDIR}/pxf_common.bash"

SSH_OPTS="-i cluster_env_files/private_key.pem -o StrictHostKeyChecking=no"
GPHD_ROOT="/singlecluster"

function configure_local_hdfs() {

	sed -i -e 's|hdfs://0.0.0.0:8020|hdfs://hadoop:8020|' \
	${GPHD_ROOT}/hadoop/etc/hadoop/core-site.xml ${GPHD_ROOT}/hbase/conf/hbase-site.xml
	sed -i -e "s/>tez/>mr/g" ${GPHD_ROOT}/hive/conf/hive-site.xml
}

function run_multinode_smoke_test() {

	echo "Running multinode smoke test with ${NO_OF_FILES} files"
	time ssh hadoop "export JAVA_HOME=/etc/alternatives/jre_1.8.0_openjdk
	${GPHD_ROOT}/bin/hdfs dfs -mkdir -p /tmp && mkdir -p /tmp/pxf_test && \
	for i in \$(seq 1 ${NO_OF_FILES}); do \
	cat > /tmp/pxf_test/test_\${i}.txt <<-EOF
	1
	2
	3
	EOF
	done && \
	${GPHD_ROOT}/bin/hdfs dfs -copyFromLocal /tmp/pxf_test/ /tmp && \
	${GPHD_ROOT}/bin/hdfs dfs -chown -R gpadmin:gpadmin /tmp/pxf_test"

	echo "Found $(${GPHD_ROOT}/bin/hdfs dfs -ls /tmp/pxf_test | grep pxf_test | wc -l) items in /tmp/pxf_test"
	expected_output=$((3 * ${NO_OF_FILES}))

	time ssh ${SSH_OPTS} gpadmin@mdw "source ${GPHOME}/greenplum_path.sh
	psql -d template1 -c \"
	CREATE EXTERNAL TABLE pxf_multifile_test (b TEXT) LOCATION ('pxf://tmp/pxf_test?PROFILE=HdfsTextSimple') FORMAT 'CSV';\"
	num_rows=\$(psql -d template1 -t -c \"SELECT COUNT(*) FROM pxf_multifile_test;\" | head -1)
	if [[ \${num_rows} == ${expected_output} ]] ; then
		echo \"Received expected output\"
	else
		echo \"Error. Expected output ${expected_output} does not match actual \${num_rows}\"
		exit 1
	fi"
}

function open_ssh_tunnels() {

	# https://stackoverflow.com/questions/2241063/bash-script-to-setup-a-temporary-ssh-tunnel
	ssh-keyscan hadoop >> /root/.ssh/known_hosts
	ssh -fNT -M -S /tmp/mdw5432 -L 5432:mdw:5432 gpadmin@mdw
	ssh -fNT -M -S /tmp/hadoop2181 -L 2181:hadoop:2181 root@hadoop
	ssh -S /tmp/mdw5432 -O check gpadmin@mdw
	ssh -S /tmp/hadoop2181 -O check root@hadoop
}

function close_ssh_tunnels() {
	ssh -S /tmp/mdw5432 -O exit gpadmin@mdw
	ssh -S /tmp/hadoop2181 -O exit root@hadoop
}

function update_pghba_conf() {

    local sdw_ips=("$@")
    for ip in ${sdw_ips[@]}; do
        echo "host     all         gpadmin         $ip/32    trust" >> pg_hba.patch
    done
    scp ${SSH_OPTS} pg_hba.patch gpadmin@mdw:

    ssh ${SSH_OPTS} gpadmin@mdw "
        cat pg_hba.patch >> /data/gpdata/master/gpseg-1/pg_hba.conf &&
        cat /data/gpdata/master/gpseg-1/pg_hba.conf"
}

function setup_pxf_on_segment {
    local segment=${1}
    scp -r ${SSH_OPTS} pxf_tarball centos@${segment}:
    scp ${SSH_OPTS} cluster_env_files/etc_hostfile centos@${segment}:

    # install PXF as superuser
    ssh ${SSH_OPTS} centos@${segment} "
        sudo sed -i -e 's/edw0/hadoop/' /etc/hosts &&
        sudo yum install -y -d 1 java-1.8.0-openjdk-devel &&
        echo 'export JAVA_HOME=/usr/lib/jvm/jre' | sudo tee -a ~gpadmin/.bashrc &&
        echo 'export JAVA_HOME=/usr/lib/jvm/jre' | sudo tee -a ~centos/.bashrc &&
        sudo tar -xzf pxf_tarball/pxf.tar.gz -C ${GPHOME} &&
        sudo chown -R gpadmin:gpadmin ${GPHOME}/pxf"
}

function setup_pxf_on_cluster() {
    # untar pxf on all nodes in the cluster
    for node in ${gpdb_nodes}; do
        setup_pxf_on_segment ${node} &
    done
    wait
    # init all PXFs using cluster command, configure PXF on master, sync configs and start pxf
    ssh ${SSH_OPTS} gpadmin@mdw "source ${GPHOME}/greenplum_path.sh &&
        PXF_CONF=${PXF_CONF_DIR} ${GPHOME}/pxf/bin/pxf cluster init &&
        cp ${PXF_CONF_DIR}/templates/{hdfs,mapred,yarn,core,hbase,hive}-site.xml ${PXF_CONF_DIR}/servers/default/ &&
        mkdir -p ${PXF_CONF_DIR}/servers/s3 && mkdir -p ${PXF_CONF_DIR}/servers/s3-invalid &&
        cp ${PXF_CONF_DIR}/templates/s3-site.xml ${PXF_CONF_DIR}/servers/s3/ &&
        cp ${PXF_CONF_DIR}/templates/s3-site.xml ${PXF_CONF_DIR}/servers/s3-invalid/ &&
        sed -i \"s|YOUR_AWS_ACCESS_KEY_ID|${ACCESS_KEY_ID}|\" ${PXF_CONF_DIR}/servers/s3/s3-site.xml &&
        sed -i \"s|YOUR_AWS_SECRET_ACCESS_KEY|${SECRET_ACCESS_KEY}|\" ${PXF_CONF_DIR}/servers/s3/s3-site.xml &&
        sed -i -e 's/\(0.0.0.0\|localhost\|127.0.0.1\)/${hadoop_ip}/g' ${PXF_CONF_DIR}/servers/default/*-site.xml &&
        if [ ${IMPERSONATION} == false ]; then
            echo 'export PXF_USER_IMPERSONATION=false' >> ${PXF_CONF_DIR}/conf/pxf-env.sh
        fi &&
        echo 'export PXF_JVM_OPTS=\"${PXF_JVM_OPTS}\"' >> ${PXF_CONF_DIR}/conf/pxf-env.sh &&
        ${GPHOME}/pxf/bin/pxf cluster sync &&
        ${GPHOME}/pxf/bin/pxf cluster start"
}

function run_pxf_automation() {

	${GPHD_ROOT}/bin/hdfs dfs -chown gpadmin:gpadmin /tmp
	sed -i 's/sutFile=default.xml/sutFile=MultiNodesCluster.xml/g' pxf_src/automation/jsystem.properties
	chown -R gpadmin:gpadmin /home/gpadmin pxf_src/automation

	cat > /home/gpadmin/run_pxf_automation_test.sh <<EOF
set -exo pipefail

source ${GPHOME}/greenplum_path.sh

export PATH=\$PATH:${GPHD_ROOT}/bin:${HADOOP_ROOT}/bin:${HBASE_ROOT}/bin:${HIVE_ROOT}/bin:${ZOOKEEPER_ROOT}/bin
export GPHOME=/usr/local/greenplum-db-devel
export PXF_HOME=${GPHOME}/pxf
export PGHOST=localhost
export PGPORT=5432

cd pxf_src/automation
make GROUP=${GROUP}

exit 0
EOF

	chown gpadmin:gpadmin /home/gpadmin/run_pxf_automation_test.sh
	chmod a+x /home/gpadmin/run_pxf_automation_test.sh

	if [[ ${ACCEPTANCE} == true ]]; then
		echo Acceptance test pipeline
		close_ssh_tunnels
		exit 1
	fi

	su gpadmin -c "bash /home/gpadmin/run_pxf_automation_test.sh"
}

function _main() {

	cp -R cluster_env_files/.ssh/* /root/.ssh
	gpdb_nodes=$( < cluster_env_files/etc_hostfile grep -e "sdw\|mdw" | awk '{print $1}')
	gpdb_segments=$( < cluster_env_files/etc_hostfile grep -e "sdw" | awk '{print $1}')
	hadoop_ip=$( < cluster_env_files/etc_hostfile grep "edw0" | awk '{print $1}')

	install_gpdb_binary
	setup_gpadmin_user
	install_pxf_server
	init_and_configure_pxf_server
	remote_access_to_gpdb

	open_ssh_tunnels
	configure_local_hdfs

	# widen access to mdw to all nodes in the cluster for JDBC test
	update_pghba_conf "${gpdb_segments[@]}"

	setup_pxf_on_cluster

	run_pxf_automation
	close_ssh_tunnels
}

_main
