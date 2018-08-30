#!/bin/bash -l

set -exo pipefail

SSH_OPTS="-i cluster_env_files/private_key.pem"

function setup_pxf {

    local segment=${1}
    local hadoop_ip=${2}
    scp -r ${SSH_OPTS} pxf_tarball centos@"${segment}":
    scp ${SSH_OPTS} pxf_src/concourse/setup_pxf_on_segment.sh centos@${segment}:
    scp ${SSH_OPTS} /singlecluster/hadoop/etc/hadoop/{core,hdfs,mapred}-site.xml centos@${segment}:
    scp ${SSH_OPTS} /singlecluster/hive/conf/hive-site.xml centos@${segment}:
    scp ${SSH_OPTS} /singlecluster/hbase/conf/hbase-site.xml centos@${segment}:
    scp ${SSH_OPTS} hdp_cluster_env_files/etc_hostfile centos@${segment}:
    sed 's/mdw/hadoop/' hdp_cluster_env_files/etc_hostfile >> /etc/hosts
    ssh ${SSH_OPTS} centos@${segment} "sudo bash -c \"\
        cd /home/centos && IMPERSONATION=${IMPERSONATION} PXF_JVM_OPTS='${PXF_JVM_OPTS}' ./setup_pxf_on_segment.sh ${hadoop_ip}
        \""
}

function _main() {

	cp -R cluster_env_files/.ssh /root/.ssh
    gpdb_segments=$( < cluster_env_files/etc_hostfile grep -e "[sdw|mdw]" | awk '{print $1}')
    hadoop_ip=$( < hdp_cluster_env_files/etc_hostfile grep "mdw.*" | awk '{print $1}')
    for segment in ${gpdb_segments}; do
        setup_pxf ${segment} ${hadoop_ip} &
    done
    wait
}

_main "$@"
