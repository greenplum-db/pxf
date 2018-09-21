#!/bin/bash -l

set -exo pipefail

export GPHD_ROOT="/home/centos/singlecluster"

sed -i 's/edw0/hadoop/' /etc/hosts
sed -i -e "s/>tez/>mr/g" -e "s/localhost/${1}/g" ${GPHD_ROOT}/hive/conf/hive-site.xml
sed -i -e "s/0.0.0.0/${1}/g" ${GPHD_ROOT}/hadoop/etc/hadoop/core-site.xml
sed -i -e "s/0.0.0.0/${1}/g" ${GPHD_ROOT}/hadoop/etc/hadoop/hdfs-site.xml
sed -i -e "s/0.0.0.0/${1}/g" ${GPHD_ROOT}/hadoop/etc/hadoop/yarn-site.xml
echo 'export JAVA_HOME=/usr/lib/jvm/jre' >> ~/.bash_profile

if [ "${IMPERSONATION}" == "true" ]; then
     echo 'Impersonation is enabled, adding support for gpadmin proxy user'
     pushd ${GPHD_ROOT}/hadoop/etc/hadoop
     cat > proxy-config.xml <<-EOF
     <property>
         <name>hadoop.proxyuser.gpadmin.hosts</name>
         <value>*</value>
     </property>
     <property>
         <name>hadoop.proxyuser.gpadmin.groups</name>
         <value>*</value>
     </property>
     <property>
         <name>hadoop.security.authorization</name>
         <value>true</value>
     </property>
     <property>
         <name>hbase.security.authorization</name>
         <value>true</value>
     </property>
     <property>
         <name>hbase.rpc.protection</name>
         <value>authentication</value>
     </property>
     <property>
         <name>hbase.coprocessor.master.classes</name>
         <value>org.apache.hadoop.hbase.security.access.AccessController</value>
     </property>
     <property>
         <name>hbase.coprocessor.region.classes</name>
         <value>org.apache.hadoop.hbase.security.access.AccessController,org.apache.hadoop.hbase.security.access.SecureBulkLoadEndpoint</value>
     </property>
     <property>
         <name>hbase.coprocessor.regionserver.classes</name>
         <value>org.apache.hadoop.hbase.security.access.AccessController</value>
     </property>
EOF
     sed -i -e '/<configuration>/r proxy-config.xml' core-site.xml
     sed -i -e '/<configuration>/r proxy-config.xml' ../../../hbase/conf/hbase-site.xml
     rm proxy-config.xml
     popd
fi

yum install -y -d 1 java-1.8.0-openjdk-devel

export JAVA_HOME=/etc/alternatives/jre_1.8.0_openjdk
export HADOOP_ROOT=${GPHD_ROOT}
export HBASE_ROOT=${GPHD_ROOT}/hbase
export HIVE_ROOT=${GPHD_ROOT}/hive
export ZOOKEEPER_ROOT=${GPHD_ROOT}/zookeeper
export PATH=$PATH:${GPHD_ROOT}/bin:${HADOOP_ROOT}/bin:${HBASE_ROOT}/bin:${HIVE_ROOT}/bin:${ZOOKEEPER_ROOT}/bin

# Start all hadoop services
${GPHD_ROOT}/bin/init-gphd.sh
${GPHD_ROOT}/bin/start-hdfs.sh
${GPHD_ROOT}/bin/start-zookeeper.sh
${GPHD_ROOT}/bin/start-yarn.sh
${GPHD_ROOT}/bin/start-hbase.sh
${GPHD_ROOT}/bin/start-hive.sh

echo 'Granting gpadmin user admin privileges for HBase'
echo "grant 'gpadmin', 'RWXCA'" | hbase shell
