#!/bin/bash -l

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

	cd "\${1}/gpdb_src/gpAux/extensions/pxf"
	make installcheck USE_PGXS=1

	[ -s regression.diffs ] && cat regression.diffs && exit 1

	exit 0
	EOF

	chown gpadmin:gpadmin /home/gpadmin/run_regression_test.sh
	chmod a+x /home/gpadmin/run_regression_test.sh
	su gpadmin -c "bash /home/gpadmin/run_regression_test.sh $(pwd)"
}

function install_gpdb() {
	[ ! -d ${GPHOME} ] && mkdir -p ${GPHOME}
	tar -xzf bin_gpdb/bin_gpdb.tar.gz -C ${GPHOME}
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
    { ssh-keyscan localhost; ssh-keyscan 0.0.0.0; } >> /home/gpadmin/.ssh/known_hosts
}

function remote_access_to_gpdb() {

    ssh ${SSH_OPTS} gpadmin@mdw "source /usr/local/greenplum-db-devel/greenplum_path.sh && \
      export MASTER_DATA_DIRECTORY=/data/gpdata/master/gpseg-1 && \
      echo 'host all all 10.0.0.0/16 trust' >> /data/gpdata/master/gpseg-1/pg_hba.conf && \
      psql -d template1 -c 'CREATE EXTENSION pxf;' && \
      psql -d template1 -c 'CREATE DATABASE gpadmin;' && \
      psql -d template1 -c 'CREATE ROLE root LOGIN;' && \
      gpstop -u"
}

function setup_sshd() {

    service sshd start
    passwd -u root
    /bin/cp -Rf cluster_env_files/.ssh/* /root/.ssh
    /bin/cp -f cluster_env_files/private_key.pem /root/.ssh/id_rsa
    /bin/cp -f cluster_env_files/public_key.pem /root/.ssh/id_rsa.pub
    /bin/cp -f cluster_env_files/public_key.openssh /root/.ssh/authorized_keys
    sed 's/edw0/hadoop/' cluster_env_files/etc_hostfile >> /etc/hosts
}

function make_cluster() {
	pushd gpdb_src/gpAux/gpdemo
	su gpadmin -c "make create-demo-cluster"
	popd
}

function add_user_access() {
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
	source ${GPHOME}/greenplum_path.sh
	if [ "${TEST_ENV}" == "dev" ]; then
		pushd gpdb_src > /dev/null
		source /opt/gcc_env.sh
		cd gpAux/extensions/pxf
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

function add_jdbc_jar_to_pxf_public_classpath() {
	local singlecluster=${1}

	# append the full path to PostgreSQL JDBC JAR file to pxf_public.classpath for JDBC tests
	ls ${singlecluster}/jdbc/postgresql-jdbc*.jar >> ${PXF_HOME}/conf/pxf-public.classpath

	cat ${PXF_HOME}/conf/pxf-public.classpath
}

function start_pxf_server() {
	pushd ${PXF_HOME} > /dev/null

	#Check if some other process is listening on 5888
	netstat -tlpna | grep 5888 || true

	echo 'Start PXF service'

	su gpadmin -c "bash ./bin/pxf init"
	su gpadmin -c "bash ./bin/pxf start"
	popd > /dev/null
}
