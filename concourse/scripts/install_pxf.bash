#!/bin/bash

set -euxo pipefail

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
GPHOME="/usr/local/greenplum-db-devel"
MASTER_HOSTNAME=$( < cluster_env_files/etc_hostfile grep "mdw.*" | awk '{print $2}')
HADOOP_HOSTNAME="ccp-$(cat terraform_dataproc/name)-m"
PXF_CONF_DIR="/home/gpadmin/pxf"

cat << EOF
  ############################
  #                          #
  #     PXF Installation     #
  #                          #
  ############################
EOF

function create_pxf_installer_scripts() {
cat > /tmp/configure_pxf.sh <<EOFF
#!/bin/bash

set -euxo pipefail

GPHOME=/usr/local/greenplum-db-devel
PXF_HOME="\${GPHOME}/pxf"
PXF_CONF_DIR="/home/gpadmin/pxf"

function setup_pxf_env() {
	pushd \${PXF_HOME} > /dev/null

	#Check if some other process is listening on 5888
	netstat -tlpna | grep 5888 || true

	if [ "${IMPERSONATION}" == "false" ]; then
		echo 'Impersonation is disabled, updating pxf-env.sh property'
		su gpadmin -c "sed -i -e 's|^[[:blank:]]*export PXF_USER_IMPERSONATION=.*$|export PXF_USER_IMPERSONATION=false|g' \${PXF_CONF_DIR}/conf/pxf-env.sh"
	fi

	if [[ ! -z "${PXF_JVM_OPTS}" ]]; then
		echo 'export PXF_JVM_OPTS="${PXF_JVM_OPTS}"' >> \${PXF_CONF_DIR}/conf/pxf-env.sh
	fi

	popd > /dev/null
}

function main() {
	rm -rf \$PXF_CONF_DIR/servers/default/core-site.xml \$PXF_CONF_DIR/servers/default/hdfs-site.xml
	cp /etc/hadoop/conf/core-site.xml /etc/hadoop/conf/hdfs-site.xml \$PXF_CONF_DIR/servers/default/
	chown -R gpadmin:gpadmin \$PXF_CONF_DIR/servers/default
	setup_pxf_env
}

main

EOFF

cat > /tmp/install_pxf_dependencies.sh <<EOFF
#!/bin/bash

set -euxo pipefail

GPHOME=/usr/local/greenplum-db-devel
PXF_HOME="\${GPHOME}/pxf"
PXF_CONF_DIR="/home/gpadmin/pxf"
export HADOOP_VER=2.6.5.0-292

function install_java() {
	yum install -y -d 1 java-1.8.0-openjdk-devel java-1.8.0-openjdk-devel-debug
	echo 'export JAVA_HOME=/usr/lib/jvm/jre' | sudo tee -a ~gpadmin/.bashrc
	echo 'export JAVA_HOME=/usr/lib/jvm/jre' | sudo tee -a ~centos/.bashrc
}

function install_hadoop_client() {
	cat > /etc/yum.repos.d/hdp.repo <<-EOF
[HDP-2.6.5.0]
name=HDP Version - HDP-2.6.5.0
baseurl=http://public-repo-1.hortonworks.com/HDP/centos7/2.x/updates/2.6.5.0
gpgcheck=1
gpgkey=http://public-repo-1.hortonworks.com/HDP/centos7/2.x/updates/2.6.5.0/RPM-GPG-KEY/RPM-GPG-KEY-Jenkins
enabled=1
priority=1
EOF
	yum install -y -d 1 hadoop-client
	echo "export HADOOP_VERSION=\${HADOOP_VER}" | sudo tee -a ~gpadmin/.bash_profile
	echo "export HADOOP_HOME=/usr/hdp/\${HADOOP_VER}" | sudo tee -a ~gpadmin/.bash_profile
	echo "export HADOOP_HOME=/usr/hdp/\${HADOOP_VER}" | sudo tee -a ~centos/.bash_profile
}

function setup_hadoop_client() {
	cat > /etc/hadoop/conf/core-site.xml <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
	<property>
		<name>fs.defaultFS</name>
		<value>hdfs://${HADOOP_HOSTNAME}:8020</value>
	</property>
	<property>
	   <name>hadoop.security.key.provider.path</name>
	   <value>kms://http@${HADOOP_HOSTNAME}:16000/kms</value>
	</property>
	<property>
		<name>ipc.ping.interval</name>
		<value>900000</value>
	</property>
</configuration>
EOF

	cat > /etc/hadoop/conf/hdfs-site.xml <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
   <property>
	  <name>dfs.permissions</name>
	  <value>true</value>
   </property>
	<property>
		<name>dfs.support.append</name>
		<value>true</value>
	</property>
   <property>
	   <name>dfs.block.local-path-access.user</name>
	   <value>gpadmin</value>
   </property>
	<property>
		<name>dfs.replication</name>
		<value>3</value>
	</property>
   <property>
	   <name>dfs.datanode.socket.write.timeout</name>
	   <value>0</value>
   </property>
	<property>
		<name>dfs.webhdfs.enabled</name>
		<value>true</value>
	</property>
	<property>
		<name>dfs.allow.truncate</name>
		<value>true</value>
	</property>
	<property>
		<name>dfs.encryption.key.provider.uri</name>
		<value>kms://http@${HADOOP_HOSTNAME}:16000/kms</value>
	</property>
</configuration>
EOF
}

function main() {
	install_java
	install_hadoop_client
	setup_hadoop_client
}

main

EOFF

cat > /tmp/install_pxf.sh <<EOFF
#!/bin/bash

set -euxo pipefail

GPHOME=/usr/local/greenplum-db-devel
PXF_HOME="\${GPHOME}/pxf"

function main() {
	# Reserve port 5888 for PXF service
	echo "pxf             5888/tcp               # PXF Service" >> /etc/services

	tar -xzf pxf_tarball/pxf.tar.gz -C \${GPHOME}
	chown -R gpadmin:gpadmin \${PXF_HOME}
}

main

EOFF

	chmod +x /tmp/install_pxf_dependencies.sh /tmp/configure_pxf.sh /tmp/install_pxf.sh
	scp /tmp/install_pxf_dependencies.sh /tmp/configure_pxf.sh /tmp/install_pxf.sh "${MASTER_HOSTNAME}:~gpadmin/"
}

function run_pxf_installer_scripts() {
	ssh "${MASTER_HOSTNAME}" "bash -c \"\
	source ${GPHOME}/greenplum_path.sh && \
	export JAVA_HOME=/usr/lib/jvm/jre && \
	export MASTER_DATA_DIRECTORY=/data/gpdata/master/gpseg-1/ && \
	gpconfig -c gp_hadoop_home -v '/usr/hdp/2.6.5.0-292' && \
	gpconfig -c gp_hadoop_target_version -v 'hdp' && gpstop -u && \
	gpscp -f ~gpadmin/hostfile_all -v -r ~gpadmin/pxf_tarball centos@=:/home/centos && \
	gpscp -f ~gpadmin/hostfile_all -v ~gpadmin/{install_pxf_dependencies,configure_pxf,install_pxf}.sh centos@=:/home/centos && \
	gpssh -f ~gpadmin/hostfile_all -v -u centos -s -e 'sudo /home/centos/install_pxf_dependencies.sh' && \
	gpssh -f ~gpadmin/hostfile_all -v -u centos -s -e 'sudo /home/centos/install_pxf.sh' && \
	PXF_CONF=${PXF_CONF_DIR} ${GPHOME}/pxf/bin/pxf cluster init && \
	gpssh -f ~gpadmin/hostfile_all -v -u centos -s -e 'sudo /home/centos/configure_pxf.sh' && \
	${GPHOME}/pxf/bin/pxf cluster start \
	\""
}

function _main() {
	scp -r pxf_tarball cluster_env_files/* "${MASTER_HOSTNAME}:~gpadmin/"
	create_pxf_installer_scripts
	run_pxf_installer_scripts
}

_main
