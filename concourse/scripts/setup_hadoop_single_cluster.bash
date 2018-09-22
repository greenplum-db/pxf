#!/bin/bash -l

set -exo pipefail

CWDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "${CWDIR}/pxf_common.bash"

export GPHD_ROOT="/home/centos/singlecluster"

sed -i 's/edw0/hadoop/' /etc/hosts
sed -i -e "s/>tez/>mr/g" -e "s/localhost/${1}/g" ${GPHD_ROOT}/hive/conf/hive-site.xml
sed -i -e "s/0.0.0.0/${1}/g" ${GPHD_ROOT}/hadoop/etc/hadoop/core-site.xml
sed -i -e "s/0.0.0.0/${1}/g" ${GPHD_ROOT}/hadoop/etc/hadoop/hdfs-site.xml
sed -i -e "s/0.0.0.0/${1}/g" ${GPHD_ROOT}/hadoop/etc/hadoop/yarn-site.xml
echo 'export JAVA_HOME=/usr/lib/jvm/jre' >> ~/.bash_profile

yum install -y -d 1 java-1.8.0-openjdk-devel
export JAVA_HOME=/etc/alternatives/jre_1.8.0_openjdk
export HADOOP_ROOT=${GPHD_ROOT}
export HBASE_ROOT=${GPHD_ROOT}/hbase
export HIVE_ROOT=${GPHD_ROOT}/hive
export ZOOKEEPER_ROOT=${GPHD_ROOT}/zookeeper
export PATH=$PATH:${GPHD_ROOT}/bin:${HADOOP_ROOT}/bin:${HBASE_ROOT}/bin:${HIVE_ROOT}/bin:${ZOOKEEPER_ROOT}/bin

setup_impersonation ${GPHD_ROOT}
start_hadoop_services ${GPHD_ROOT}

