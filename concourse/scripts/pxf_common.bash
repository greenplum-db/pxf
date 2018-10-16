#!/bin/bash -l

if [ "${TARGET_OS}" == "ubuntu" ]; then
    GPHOME="/usr/local/gpdb"
else
    GPHOME="/usr/local/greenplum-db-devel"
fi

PXF_HOME="${GPHOME}/pxf"

JAVA_HOME=$(ls -d /usr/lib/jvm/java-1.8.0-openjdk* | head -1)

if [ -d gpdb_src/gpAux/extensions/pxf ]; then
	PXF_EXTENSIONS_DIR=gpdb_src/gpAux/extensions/pxf
else
	PXF_EXTENSIONS_DIR=gpdb_src/gpcontrib/pxf
fi

function set_env() {
	export TERM=xterm-256color
	export TIMEFORMAT=$'\e[4;33mIt took %R seconds to complete this step\e[0m';
}

function run_regression_test() {
	ln -s ${PWD}/gpdb_src /home/gpadmin/gpdb_src
	cat > /home/gpadmin/run_regression_test.sh <<-EOF
	source /opt/gcc_env.sh || true
	source ${GPHOME}/greenplum_path.sh
	source gpdb_src/gpAux/gpdemo/gpdemo-env.sh
	export PATH=\$PATH:${GPHD_ROOT}/bin:${HADOOP_ROOT}/bin:${HBASE_ROOT}/bin:${HIVE_ROOT}/bin:${ZOOKEEPER_ROOT}/bin

	cd "${PXF_EXTENSIONS_DIR}"
	make installcheck USE_PGXS=1

	[ -s regression.diffs ] && cat regression.diffs && exit 1

	exit 0
	EOF

	chown -R gpadmin:gpadmin ${PXF_EXTENSIONS_DIR}
	chown gpadmin:gpadmin /home/gpadmin/run_regression_test.sh
	chmod a+x /home/gpadmin/run_regression_test.sh
	su gpadmin -c "bash /home/gpadmin/run_regression_test.sh $(pwd)"
}

function install_gpdb_binary() {

    if [ ! -d ${GPHOME}/bin/psql ]; then
        mkdir -p ${GPHOME}
        tar -xzf bin_gpdb/*.tar.gz -C ${GPHOME}
    fi

    if [ "${TARGET_OS}" == "centos" ]; then
        service sshd start
        psi_dir=$(find /usr/lib64 -name psi | sort -r | head -1)
    elif [ "${TARGET_OS}" == "ubuntu" ]; then
        service ssh start
        pip install psi
        psi_dir=$(find /usr/local/lib -name psi | sort -r | head -1)
    fi

    # Copy PSI package from system python to GPDB as automation test requires it
    if [ ! -d ${GPHOME}/lib/python/psi ]; then
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
    echo -e "export JAVA_HOME=${JAVA_HOME}" >> /home/gpadmin/.bashrc
    if [ -d gpdb_src/gpAux/gpdemo ]; then
        chown -R gpadmin:gpadmin gpdb_src/gpAux/gpdemo
    fi
}

function install_pxf_client() {
	# recompile pxf.so file for dev environments only
	if [ "${TEST_ENV}" == "dev" ]; then
		source ${GPHOME}/greenplum_path.sh
		source /opt/gcc_env.sh || true

		pushd ${PXF_EXTENSIONS_DIR} > /dev/null
		USE_PGXS=1 make install
		popd > /dev/null
	fi
}

function install_pxf_server() {
    if [ ! -d ${PXF_HOME} ]; then
        if [ -d pxf_tarball ]; then
            tar -xzf pxf_tarball/pxf.tar.gz -C ${GPHOME}
        else
            export BUILD_NUMBER="${TARGET_OS}"
            export JAVA_TOOL_OPTIONS=-Dfile.encoding=UTF8
            pushd pxf_src/server
            make install
            popd
        fi
        chown -R gpadmin:gpadmin ${PXF_HOME}
    fi
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
    if [ ! -f ${GPHD_ROOT}/hbase/lib/pxf-hbase-*.jar ]; then
        cp ${PXF_HOME}/lib/pxf-hbase-*.jar ${GPHD_ROOT}/hbase/lib
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
    export PATH=$PATH:${GPHD_ROOT}/bin:${HADOOP_ROOT}/bin:${HBASE_ROOT}/bin:${HIVE_ROOT}/bin:${ZOOKEEPER_ROOT}/bin

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
