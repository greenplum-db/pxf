#!/bin/bash -l

set -exo pipefail

GPHOME="/usr/local/greenplum-db-devel"
SSH_OPTS="-i cluster_env_files/private_key.pem"

function install_hadoop_client {

    local segment=${1}
    scp -r ${SSH_OPTS} hdp.repo centos@${segment}:~
    ssh ${SSH_OPTS} centos@${segment} "
        sudo yum install -y -d 1 java-1.8.0-openjdk-devel &&
	    echo 'export JAVA_HOME=/usr/lib/jvm/jre' | sudo tee -a ~gpadmin/.bash_profile &&
	    echo 'export JAVA_HOME=/usr/lib/jvm/jre' | sudo tee -a ~centos/.bash_profile &&
	    sudo mv /home/centos/hdp.repo /etc/yum.repos.d &&
	    sudo yum install -y -d 1 hadoop-client hive hbase"
}

function setup_pxf {

    local segment=${1}
    local hadoop_ip=${2}
    scp -r ${SSH_OPTS} pxf_tarball centos@"${segment}":
    scp ${SSH_OPTS} pxf_src/concourse/setup_pxf_on_segment.sh centos@${segment}:
    scp ${SSH_OPTS} /singlecluster/hadoop/etc/hadoop/{core,hdfs,mapred}-site.xml centos@${segment}:
    scp ${SSH_OPTS} /singlecluster/hive/conf/hive-site.xml centos@${segment}:
    scp ${SSH_OPTS} /singlecluster/hbase/conf/hbase-site.xml centos@${segment}:
    scp ${SSH_OPTS} /singlecluster/jdbc/postgresql-jdbc*.jar centos@${segment}:
    scp ${SSH_OPTS} cluster_env_files/etc_hostfile centos@${segment}:
    ssh ${SSH_OPTS} centos@${segment} "sudo bash -c \"\
        cd /home/centos && IMPERSONATION=${IMPERSONATION} PXF_JVM_OPTS='${PXF_JVM_OPTS}' ./setup_pxf_on_segment.sh ${hadoop_ip}
        \""
}

function install_hadoop_single_cluster() {

    local hadoop_ip=${1}
    tar -xzf pxf_tarball/pxf.tar.gz -C /tmp
    cp /tmp/pxf/lib/pxf-hbase-*.jar /singlecluster/hbase/lib
    scp ${SSH_OPTS} cluster_env_files/etc_hostfile centos@edw0:
    scp ${SSH_OPTS} -rq /singlecluster centos@edw0:
    scp ${SSH_OPTS} pxf_src/concourse/setup_hadoop_single_cluster.sh centos@edw0:

    ssh ${SSH_OPTS} centos@edw0 "sudo bash -c \"\
        cd /home/centos && IMPERSONATION=${IMPERSONATION} ./setup_hadoop_single_cluster.sh ${hadoop_ip}
    \""
}

function update_pghba_and_restart_gpdb() {
    local sdw_ips=("$@")

	for ip in ${sdw_ips}; do
        echo "host     all         gpadmin         $ip/32    trust" >> pg_hba.patch
    done
    scp ${SSH_OPTS} pg_hba.patch gpadmin@mdw:

	ssh ${SSH_OPTS} gpadmin@mdw "cat pg_hba.patch >> /data/gpdata/master/gpseg-1/pg_hba.conf;\
		cat /data/gpdata/master/gpseg-1/pg_hba.conf; \
		source /usr/local/greenplum-db-devel/greenplum_path.sh; \
		export MASTER_DATA_DIRECTORY=/data/gpdata/master/gpseg-1; \
		gpstop -u"
}

function _main() {

	cp -R cluster_env_files/.ssh/* /root/.ssh
    gpdb_nodes=$( < cluster_env_files/etc_hostfile grep -e "sdw\|mdw" | awk '{print $1}')
    gpdb_segments=$( < cluster_env_files/etc_hostfile grep -e "sdw" | awk '{print $1}')

    if [ "${SKIP_SINGLECLUSTER}" != "" ]; then
      hadoop_ip=$( < cluster_env_files/etc_hostfile grep "edw0" | awk '{print $1}')
      install_hadoop_single_cluster ${hadoop_ip} &
    else
      hadoop_ip="ccp-$(cat terraform_dataproc/name)-m"
    fi

    cat > hdp.repo <<-EOF
		#VERSION_NUMBER=2.6.5.0-292
		[HDP-2.6.5.0]
		name=HDP Version - HDP-2.6.5.0
		baseurl=http://public-repo-1.hortonworks.com/HDP/centos7/2.x/updates/2.6.5.0
		gpgcheck=1
		gpgkey=http://public-repo-1.hortonworks.com/HDP/centos7/2.x/updates/2.6.5.0/RPM-GPG-KEY/RPM-GPG-KEY-Jenkins
		enabled=1
		priority=1
	EOF
    for node in ${gpdb_nodes}; do
        install_hadoop_client ${node} &
    done
    wait
    for node in ${gpdb_nodes}; do
        setup_pxf ${node} ${hadoop_ip} &
    done
    wait

    # widen access to mdw to all nodes in the cluster for JDBC test
    update_pghba_and_restart_gpdb "${gpdb_segments[@]}"
}

_main "$@"
