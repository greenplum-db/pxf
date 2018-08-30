#!/bin/bash -l

set -eo pipefail

GPHOME="/usr/local/greenplum-db-devel"
SSH_OPTS="-i cluster_env_files/private_key.pem -o StrictHostKeyChecking=no"

function setup_gpadmin_user() {

    groupadd -g 1000 gpadmin && useradd -u 1000 -g 1000 -M gpadmin
    echo "gpadmin  ALL=(ALL)       NOPASSWD: ALL" > /etc/sudoers.d/gpadmin
    groupadd supergroup && usermod -a -G supergroup gpadmin
    echo -e "password\npassword" | passwd gpadmin 2> /dev/null
    echo -e "gpadmin soft core unlimited" >> /etc/security/limits.d/gpadmin-limits.conf
    echo -e "gpadmin soft nproc 131072" >> /etc/security/limits.d/gpadmin-limits.conf
    echo -e "gpadmin soft nofile 65536" >> /etc/security/limits.d/gpadmin-limits.conf
    echo -e "export JAVA_HOME=/usr/lib/jvm/java-1.7.0-openjdk.x86_64" >> /home/gpadmin/.bashrc
    ln -s ${PWD}/pxf_src /home/gpadmin/pxf_src
}

function setup_sshd() {

    ssh-keygen -f /etc/ssh/ssh_host_key -N '' -t rsa1
    ssh-keygen -f /etc/ssh/ssh_host_rsa_key -N '' -t rsa
    ssh-keygen -f /etc/ssh/ssh_host_dsa_key -N '' -t dsa
    sed -i -e 's|Defaults    requiretty|#Defaults    requiretty|' /etc/sudoers
    sed -ri 's/UsePAM yes/UsePAM no/g;s/PasswordAuthentication yes/PasswordAuthentication no/g' /etc/ssh/sshd_config
    sed -ri 's@^HostKey /etc/ssh/ssh_host_ecdsa_key$@#&@;s@^HostKey /etc/ssh/ssh_host_ed25519_key$@#&@' /etc/ssh/sshd_config
    service sshd start
}

function configure_hdfs() {

    sed -i -e 's|hdfs://0.0.0.0:8020|hdfs://hadoop:8020|' /etc/hadoop/conf/core-site.xml /etc/hbase/conf/hbase-site.xml
    sed -i -e "s/>tez/>mr/g" /etc/hive/conf/hive-site.xml
}

function ssh_access_to_gpdb() {

    cp -R cluster_env_files/.ssh /root/.ssh
    cat cluster_env_files/etc_hostfile >> /etc/hosts
    sed 's/mdw/hadoop/' hdp_cluster_env_files/etc_hostfile >> /etc/hosts
    gpdb_master=$( < cluster_env_files/etc_hostfile grep "mdw.*" | awk '{print $1}')
    ssh ${SSH_OPTS} gpadmin@mdw "source /usr/local/greenplum-db-devel/greenplum_path.sh && \
      export MASTER_DATA_DIRECTORY=/data/gpdata/master/gpseg-1 && \
      echo 'host all all 192.168.0.0/16 trust' >> /data/gpdata/master/gpseg-1/pg_hba.conf && \
      psql -d template1 -c 'CREATE EXTENSION pxf;' && \
      psql -d template1 -c 'CREATE DATABASE gpadmin;' && \
      psql -d template1 -c 'CREATE ROLE root LOGIN;' && \
      gpstop -u"
    cp /root/.ssh/*.pem /root/.ssh/id_rsa
    cp cluster_env_files/public_key.openssh /root/.ssh/authorized_keys
    passwd -u root
}

function ssh_access_to_hadoop() {

    { ssh-keyscan localhost; ssh-keyscan 0.0.0.0; ssh-keyscan hadoop; } >> /root/.ssh/known_hosts
    HDP_SSH_OPTS="-i hdp_cluster_env_files/private_key.pem -o StrictHostKeyChecking=no "
    scp ${HDP_SSH_OPTS} cluster_env_files/public_key.openssh centos@hadoop:/tmp
    ssh ${HDP_SSH_OPTS} centos@hadoop "sudo mkdir -p /root/.ssh && \
        sudo cp /tmp/public_key.openssh /root/.ssh/authorized_keys && \
        sudo sed -i 's/PermitRootLogin no/PermitRootLogin yes/' /etc/ssh/sshd_config && \
        sudo service sshd restart"
}

function run_multinode_smoke_test() {

    hdfs dfs -mkdir -p /tmp
    mkdir -p /tmp/pxf_test

    echo "Number of files = ${NO_OF_FILES}"
    for i in $(seq 1 ${NO_OF_FILES}); do
    cat > /tmp/pxf_test/test_${i}.txt <<-EOF
	1
	2
	3
	EOF
    done

    hdfs dfs -copyFromLocal /tmp/pxf_test/ /tmp
    hdfs dfs -chown -R gpadmin:gpadmin /tmp/pxf_test
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
	fi
	"
}

function setup_local_gpdb() {

    mkdir -p ${GPHOME}
    tar -xzf gpdb_binary/bin_gpdb.tar.gz -C ${GPHOME}
    tar -xzf pxf_tarball/pxf.tar.gz -C ${GPHOME}
    psi_dir=$(find /usr/lib64 -name psi | sort -r | head -1)
    cp -r ${psi_dir} ${GPHOME}/lib/python
    cp -r cluster_env_files/.ssh /home/gpadmin/.ssh
    cp /home/gpadmin/.ssh/*.pem /home/gpadmin/.ssh/id_rsa
    cp cluster_env_files/public_key.openssh /home/gpadmin/.ssh/authorized_keys
    { ssh-keyscan localhost; ssh-keyscan 0.0.0.0; ssh-keyscan hadoop; } >> /home/gpadmin/.ssh/known_hosts
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
    sed -i 's/sutFile=default.xml/sutFile=MultiNodesCluster.xml/g' pxf_src/pxf_automation/jsystem.properties
    chown -R gpadmin:gpadmin /home/gpadmin pxf_src/pxf_automation

    cat > /home/gpadmin/run_pxf_automation_test.sh <<-EOF
	set -exo pipefail

	export GPHOME=/usr/local/greenplum-db-devel
	export PXF_HOME=\${GPHOME}/pxf
	export PG_MODE=GPDB
	export PGHOST=localhost
	export PGPORT=5432
	source \${GPHOME}/greenplum_path.sh

	cd pxf_src/pxf_automation
	make GROUP=gpdb

	exit 0
	EOF

	chown gpadmin:gpadmin /home/gpadmin/run_pxf_automation_test.sh
	chmod a+x /home/gpadmin/run_pxf_automation_test.sh
	su gpadmin -c "bash /home/gpadmin/run_pxf_automation_test.sh"
}

function _main() {

    setup_gpadmin_user
    setup_sshd
    ssh_access_to_gpdb
    ssh_access_to_hadoop
    setup_local_gpdb

    open_ssh_tunnels
    configure_hdfs
    run_multinode_smoke_test
    run_pxf_automation
    close_ssh_tunnels
}

_main "$@"
