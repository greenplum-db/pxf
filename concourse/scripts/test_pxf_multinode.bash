#!/bin/bash -l

set -exo pipefail

CWDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "${CWDIR}/pxf_common.bash"

SSH_OPTS="-i cluster_env_files/private_key.pem -o StrictHostKeyChecking=no"

function configure_local_hdfs() {

    sed -i -e 's|hdfs://0.0.0.0:8020|hdfs://hadoop:8020|' /etc/hadoop/conf/core-site.xml /etc/hbase/conf/hbase-site.xml
    sed -i -e "s/>tez/>mr/g" /etc/hive/conf/hive-site.xml
}

function ssh_access_to_hadoop() {

    ssh ${SSH_OPTS} centos@hadoop "sudo mkdir -p /root/.ssh && \
        sudo cp /home/centos/.ssh/authorized_keys /root/.ssh && \
        sudo sed -i 's/PermitRootLogin no/PermitRootLogin yes/' /etc/ssh/sshd_config && \
        sudo service sshd restart"
}

function run_multinode_smoke_test() {

    echo "Running multinode smoke test with ${NO_OF_FILES} files"
    time ssh hadoop "export JAVA_HOME=/etc/alternatives/jre_1.8.0_openjdk
    /home/centos/singlecluster/bin/hdfs dfs -mkdir -p /tmp && mkdir -p /tmp/pxf_test && \
    for i in \$(seq 1 ${NO_OF_FILES}); do \
    cat > /tmp/pxf_test/test_\${i}.txt <<-EOF
	1
	2
	3
	EOF
    done && \
    /home/centos/singlecluster/bin/hdfs dfs -copyFromLocal /tmp/pxf_test/ /tmp && \
    /home/centos/singlecluster/bin/hdfs dfs -chown -R gpadmin:gpadmin /tmp/pxf_test"

    echo "Found $(hdfs dfs -ls /tmp/pxf_test | grep pxf_test | wc -l) items in /tmp/pxf_test"
    expected_output=$((3 * ${NO_OF_FILES}))

    time ssh ${SSH_OPTS} gpadmin@mdw "source ${GPHOME}/greenplum_path.sh
	psql -d template1 -c \"
	CREATE EXTERNAL TABLE pxf_multifile_test (b TEXT) LOCATION ('pxf://tmp/pxf_test?PROFILE=HdfsTextSimple') FORMAT 'CSV';\"
	num_rows=\$(psql -d template1 -t -c \"SELECT COUNT(*) FROM pxf_multifile_test;\" | head -1)
	if [ \${num_rows} == ${expected_output} ] ; then
		echo \"Received expected output\"
	else
		echo \"Error. Expected output ${expected_output} does not match actual \${num_rows}\"
		exit 1
	fi"
}

function open_ssh_tunnels() {

    # https://stackoverflow.com/questions/2241063/bash-script-to-setup-a-temporary-ssh-tunnel
    ssh -fNT -M -S /tmp/mdw5432 -L 5432:mdw:5432 gpadmin@mdw
    ssh -fNT -M -S /tmp/hadoop2181 -L 2181:hadoop:2181 root@hadoop
    ssh -S /tmp/mdw5432 -O check gpadmin@mdw
    ssh -S /tmp/hadoop2181 -O check root@hadoop
}

function close_ssh_tunnels() {

    ssh -S /tmp/mdw5432 -O exit gpadmin@mdw
    ssh -S /tmp/hadoop2181 -O exit root@hadoop
}

function run_pxf_automation() {

    hdfs dfs -chown gpadmin:gpadmin /tmp
    sed -i 's/sutFile=default.xml/sutFile=MultiNodesCluster.xml/g' pxf_src/automation/jsystem.properties
    chown -R gpadmin:gpadmin /home/gpadmin pxf_src/automation

    cat > /home/gpadmin/run_pxf_automation_test.sh <<-EOF
	set -exo pipefail

	source ${GPHOME}/greenplum_path.sh
	export GPHOME=/usr/local/greenplum-db-devel
	export PXF_HOME=\${GPHOME}/pxf
	export PGHOST=localhost
	export PGPORT=5432

	cd pxf_src/automation
	make GROUP=gpdb

	exit 0
	EOF

	chown gpadmin:gpadmin /home/gpadmin/run_pxf_automation_test.sh
	chmod a+x /home/gpadmin/run_pxf_automation_test.sh
	su gpadmin -c "bash /home/gpadmin/run_pxf_automation_test.sh"
}

function _main() {

    install_gpdb_binary
    setup_gpadmin_user
    remote_access_to_gpdb
    ssh_access_to_hadoop

    open_ssh_tunnels
    configure_local_hdfs
    run_multinode_smoke_test
    run_pxf_automation
    close_ssh_tunnels
}

_main
