#!/bin/bash -l

function _main() {

	ccp_src/scripts/download_tf_state.sh
    NUMBER_OF_NODES=$(jq -r .cluster_host_list[] ./terraform*/metadata | wc -l)
    ccp_src/scripts/generate_env_files.sh ${NUMBER_OF_NODES} false
    ccp_src/scripts/generate_ssh_files.sh ${NUMBER_OF_NODES} false
    hadoop_ip=$( < cluster_env_files/etc_hostfile grep "mdw.*" | awk '{print $1}')

    SSH_OPTS="-i cluster_env_files/private_key.pem"

    scp ${SSH_OPTS} singlecluster/singlecluster-HDP.tar.gz centos@mdw:
    scp ${SSH_OPTS} pxf_src/concourse/setup_hadoop_single_cluster.sh centos@mdw:
    scp ${SSH_OPTS} cluster_env_files/etc_hostfile centos@mdw:

    ssh ${SSH_OPTS} centos@mdw "sudo bash -c \"\
        cd /home/centos && IMPERSONATION=${IMPERSONATION} NO_OF_FILES=${NO_OF_FILES} ./setup_hadoop_single_cluster.sh ${hadoop_ip}
    \""
}

_main "$@"
