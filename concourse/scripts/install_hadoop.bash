#!/bin/bash -l

set -exo pipefail

CWDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "${CWDIR}/pxf_common.bash"

SSH_OPTS="-i cluster_env_files/private_key.pem"
GPHD_ROOT="/singlecluster"

function install_hadoop_single_cluster() {

    local hadoop_ip=${1}
    ssh ${SSH_OPTS} centos@edw0 "sudo mkdir -p /root/.ssh &&
        sudo cp /home/centos/.ssh/authorized_keys /root/.ssh &&
        sudo sed -i 's/PermitRootLogin no/PermitRootLogin yes/' /etc/ssh/sshd_config &&
        sudo service sshd restart"

    tar -xzf pxf_tarball/pxf.tar.gz -C /tmp
    cp /tmp/pxf/lib/pxf-hbase-*.jar /singlecluster/hbase/lib

    scp ${SSH_OPTS} cluster_env_files/etc_hostfile root@edw0:
    scp ${SSH_OPTS} -rq /singlecluster root@edw0:/
    scp ${SSH_OPTS} pxf_src/concourse/scripts/pxf_common.bash root@edw0:

    ssh ${SSH_OPTS} root@edw0 "
        source pxf_common.bash &&
        export IMPERSONATION=${IMPERSONATION} &&
        export GPHD_ROOT=${GPHD_ROOT} &&
        sed -i 's/edw0/hadoop/' /etc/hosts &&
        sed -i -e 's/>tez/>mr/g' -e 's/localhost/${hadoop_ip}/g' \${GPHD_ROOT}/hive/conf/hive-site.xml &&
        sed -i -e 's/0.0.0.0/${hadoop_ip}/g' \${GPHD_ROOT}/hadoop/etc/hadoop/{core,hdfs,yarn}-site.xml &&

        yum install -y -d 1 java-1.8.0-openjdk-devel &&
        echo 'export JAVA_HOME=/usr/lib/jvm/jre' >> ~/.bashrc &&
        export JAVA_HOME=/etc/alternatives/jre_1.8.0_openjdk &&
        export HADOOP_ROOT=${GPHD_ROOT} &&
        export HBASE_ROOT=${GPHD_ROOT}/hbase &&
        export HIVE_ROOT=${GPHD_ROOT}/hive &&
        export ZOOKEEPER_ROOT=${GPHD_ROOT}/zookeeper &&
        export PATH=\$PATH:${GPHD_ROOT}/bin:\${HADOOP_ROOT}/bin:\${HBASE_ROOT}/bin:\${HIVE_ROOT}/bin:\${ZOOKEEPER_ROOT}/bin &&

        groupadd supergroup && usermod -a -G supergroup gpadmin &&
        setup_impersonation ${GPHD_ROOT} &&
        start_hadoop_services ${GPHD_ROOT}"
}

function _main() {

    cp -R cluster_env_files/.ssh/* /root/.ssh

    hadoop_ip=$( < cluster_env_files/etc_hostfile grep "edw0" | awk '{print $1}')
    install_hadoop_single_cluster ${hadoop_ip}

}

_main
