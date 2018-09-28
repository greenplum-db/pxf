#!/bin/bash -l

GPHOME="/usr/local/greenplum-db-devel"
PXF_HOME="${GPHOME}/pxf"

if [ -d gpAux/extensions/pxf ]; then
	PXF_EXTENSIONS_DIR=gpAux/extensions/pxf
else
	PXF_EXTENSIONS_DIR=gpcontrib/pxf
fi

function set_env() {
	export TERM=xterm-256color
	export TIMEFORMAT=$'\e[4;33mIt took %R seconds to complete this step\e[0m';
}

function run_regression_test() {
	cat > /home/gpadmin/run_regression_test.sh <<-EOF
	source /opt/gcc_env.sh
	source ${GPHOME}/greenplum_path.sh

	cd "\${1}/gpdb_src/gpAux"
	source gpdemo/gpdemo-env.sh

	cd "\${1}/gpdb_src/${PXF_EXTENSIONS_DIR}"
	make installcheck USE_PGXS=1

	[ -s regression.diffs ] && cat regression.diffs && exit 1

	exit 0
	EOF

	chown gpadmin:gpadmin /home/gpadmin/run_regression_test.sh
	chmod a+x /home/gpadmin/run_regression_test.sh
	su gpadmin -c "bash /home/gpadmin/run_regression_test.sh $(pwd)"
}

function install_gpdb_binary() {
    service sshd start
    mkdir -p ${GPHOME}
    tar -xzf bin_gpdb/bin_gpdb.tar.gz -C ${GPHOME}
    if [ -d pxf_tarball ]; then
        tar -xzf pxf_tarball/pxf.tar.gz -C ${GPHOME}
    fi
	# Copy PSI package from system python to GPDB as automation test requires it
    if [ ! -d ${GPHOME}/lib/python/psi ]; then
        psi_dir=$(find /usr/lib64 -name psi | sort -r | head -1)
        cp -r ${psi_dir} ${GPHOME}/lib/python
    fi
}

function remote_access_to_gpdb() {
    # Copy cluster keys to root user
    passwd -u root
    cp -Rf cluster_env_files/.ssh/* /root/.ssh
    cp -f cluster_env_files/private_key.pem /root/.ssh/id_rsa
    cp -f cluster_env_files/public_key.pem /root/.ssh/id_rsa.pub
    cp -f cluster_env_files/public_key.openssh /root/.ssh/authorized_keys
    sed 's/edw0/hadoop/' cluster_env_files/etc_hostfile >> /etc/hosts
    # Copy cluster keys to gpadmin user
    rm -rf /home/gpadmin/.ssh/*
    cp cluster_env_files/.ssh/* /home/gpadmin/.ssh
    cp cluster_env_files/.ssh/*.pem /home/gpadmin/.ssh/id_rsa
    cp cluster_env_files/public_key.openssh /home/gpadmin/.ssh/authorized_keys
    { ssh-keyscan localhost; ssh-keyscan 0.0.0.0; } >> /home/gpadmin/.ssh/known_hosts
    ssh ${SSH_OPTS} gpadmin@mdw "source ${GPHOME}/greenplum_path.sh && \
      export MASTER_DATA_DIRECTORY=/data/gpdata/master/gpseg-1 && \
      echo 'host all all 10.0.0.0/16 trust' >> /data/gpdata/master/gpseg-1/pg_hba.conf && \
      psql -d template1 -c 'CREATE EXTENSION pxf;' && \
      psql -d template1 -c 'CREATE DATABASE gpadmin;' && \
      psql -d template1 -c 'CREATE ROLE root LOGIN;' && \
      gpstop -u"
}


function create_gpdb_cluster() {
	pushd gpdb_src/gpAux/gpdemo
	su gpadmin -c "source ${GPHOME}/greenplum_path.sh && make create-demo-cluster"
	popd
}

function add_remote_user_access_for_gpdb() {
	local username=${1}
	# load local cluster configuration
	pushd gpdb_src/gpAux/gpdemo

	echo "Adding access entry for ${username} to pg_hba.conf"
	su gpadmin -c "source ./gpdemo-env.sh; echo 'local    all     ${username}     trust' >> \${MASTER_DATA_DIRECTORY}/pg_hba.conf"

	echo "Restarting GPDB for change to pg_hba.conf to take effect"
	su gpadmin -c "source ${GPHOME}/greenplum_path.sh; source ./gpdemo-env.sh; gpstop -u"
	popd
}

function setup_gpadmin_user() {

    groupadd -g 1000 gpadmin && useradd -u 1000 -g 1000 -M gpadmin
    echo "gpadmin  ALL=(ALL)       NOPASSWD: ALL" > /etc/sudoers.d/gpadmin
    groupadd supergroup && usermod -a -G supergroup gpadmin
    mkdir -p /home/gpadmin/.ssh
    ssh-keygen -t rsa -N "" -f /home/gpadmin/.ssh/id_rsa
    cat /home/gpadmin/.ssh/id_rsa.pub >> /home/gpadmin/.ssh/authorized_keys
    chmod 0600 /home/gpadmin/.ssh/authorized_keys
    { ssh-keyscan localhost; ssh-keyscan 0.0.0.0; } >> /home/gpadmin/.ssh/known_hosts
	chown -R gpadmin:gpadmin ${GPHOME} /home/gpadmin
    echo -e "password\npassword" | passwd gpadmin 2> /dev/null
    echo -e "gpadmin soft core unlimited" >> /etc/security/limits.d/gpadmin-limits.conf
    echo -e "gpadmin soft nproc 131072" >> /etc/security/limits.d/gpadmin-limits.conf
    echo -e "gpadmin soft nofile 65536" >> /etc/security/limits.d/gpadmin-limits.conf
    echo -e "export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk.x86_64" >> /home/gpadmin/.bashrc
    if [ -d gpdb_src/gpAux/gpdemo ]; then
        chown -R gpadmin:gpadmin gpdb_src/gpAux/gpdemo
    fi
    ln -s ${PWD}/gpdb_src /home/gpadmin/gpdb_src
    ln -s ${PWD}/pxf_src /home/gpadmin/pxf_src
}

function install_pxf_client() {
	# recompile pxf.so file for dev environments only
	if [ "${TEST_ENV}" == "dev" ]; then
		pushd gpdb_src > /dev/null
		source ${GPHOME}/greenplum_path.sh
		source /opt/gcc_env.sh

		cd ${PXF_EXTENSIONS_DIR}
		USE_PGXS=1 make install
		popd > /dev/null
	fi
}

function install_pxf_server() {
	export BUILD_NUMBER="${TARGET_OS}"
	export JAVA_TOOL_OPTIONS=-Dfile.encoding=UTF8
	pushd pxf_src/server
	make install -s DATABASE=gpdb
	popd
}

function setup_hdp_repo() {
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
}

function setup_impersonation() {
    local GPHD_ROOT=${1}

	# enable impersonation by gpadmin user
    if [ "${IMPERSONATION}" == "true" ]; then
         echo 'Impersonation is enabled, adding support for gpadmin proxy user'
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
         sed -i -e '/<configuration>/r proxy-config.xml' ${GPHD_ROOT}/hadoop/etc/hadoop/core-site.xml ${GPHD_ROOT}/hbase/conf/hbase-site.xml
         rm proxy-config.xml
    elif [ "${IMPERSONATION}" == "false" ]; then
        echo 'Impersonation is disabled, updating pxf-env.sh property'
        su gpadmin -c "sed -i -e 's|^[[:blank:]]*export PXF_USER_IMPERSONATION=.*$|export PXF_USER_IMPERSONATION=false|g' ${PXF_HOME}/conf/pxf-env.sh"
    else
        echo "ERROR: Invalid or missing CI property value: IMPERSONATION=${IMPERSONATION}"
        exit 1
    fi
}

function start_hadoop_services() {
    local GPHD_ROOT=${1}

    # Start all hadoop services
    ${GPHD_ROOT}/bin/init-gphd.sh
    ${GPHD_ROOT}/bin/start-hdfs.sh
    ${GPHD_ROOT}/bin/start-zookeeper.sh
    ${GPHD_ROOT}/bin/start-yarn.sh
    ${GPHD_ROOT}/bin/start-hbase.sh
    ${GPHD_ROOT}/bin/start-hive.sh

	# list running Hadoop daemons
	jps

	# grant gpadmin user admin privilege for feature tests to be able to run on secured cluster
	if [ "${IMPERSONATION}" == "true" ]; then
		echo 'Granting gpadmin user admin privileges for HBase'
		echo "grant 'gpadmin', 'RWXCA'" | hbase shell
	fi
}

function start_pxf_server() {
	pushd ${PXF_HOME} > /dev/null

	#Check if some other process is listening on 5888
	netstat -tlpna | grep 5888 || true

	echo 'Start PXF service'

	su gpadmin -c "./bin/pxf init && ./bin/pxf start"
	popd > /dev/null
}
