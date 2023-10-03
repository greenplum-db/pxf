#!/bin/bash

GPHOME=${GPHOME:=/usr/local/greenplum-db-devel}
PXF_HOME=${PXF_HOME:=${GPHOME}/pxf}
CDD_VALUE=/data/gpdata/coordinator/gpseg-1
PXF_COMMON_SRC_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
PXF_VERSION=${PXF_VERSION:=6}
PROXY_USER=${PROXY_USER:-pxfuser}
PROTOCOL=${PROTOCOL:-}
GOOGLE_PROJECT_ID=${GOOGLE_PROJECT_ID:-data-gpdb-ud}
PXF_SRC=$(find /tmp/build -name pxf_src -type d)
CCP_OS_USER=""

# on purpose do not call this PXF_CONF|PXF_BASE so that it is not set during pxf operations
if [[ ${PXF_VERSION} == 5 ]]; then
	BASE_DIR=~gpadmin/pxf
	SHARE_DIR="${PXF_HOME}/lib"
	TEMPLATES_DIR=${BASE_DIR}
else
	BASE_DIR=${PXF_BASE_DIR:-$PXF_HOME}
	SHARE_DIR="${PXF_HOME}/share"
	TEMPLATES_DIR=${PXF_HOME}
fi

if [[ -f ~/.pxfrc ]]; then
	# shellcheck disable=SC1090
	source <(grep "export JAVA_HOME" ~/.pxfrc)
	echo "JAVA_HOME found in ${HOME}/.pxfrc, set to ${JAVA_HOME}..."
else
	JAVA_HOME=$(find /usr/lib/jvm -name 'java-1.8.0-openjdk*' | head -1)
fi

# java home for hadoop services
HADOOP_JAVA_HOME=${HADOOP_JAVA_HOME:-$JAVA_HOME}

if [[ -d gpdb_src/gpAux/extensions/pxf ]]; then
	PXF_EXTENSIONS_DIR=gpdb_src/gpAux/extensions/pxf
else
	PXF_EXTENSIONS_DIR=gpdb_src/gpcontrib/pxf
fi

function inflate_dependencies() {
	local tarballs=() files_to_link=()
	if [[ -f pxf-build-dependencies/pxf-build-dependencies.tar.gz ]]; then
		tarballs+=(pxf-build-dependencies/pxf-build-dependencies.tar.gz)
		files_to_link+=(~gpadmin/.{go-mod-cached-sources,gradle})
	fi
	if [[ -f pxf-automation-dependencies/pxf-automation-dependencies.tar.gz ]]; then
		tarballs+=(pxf-automation-dependencies/pxf-automation-dependencies.tar.gz)
		files_to_link+=(~gpadmin/.m2)
	fi
	if [[ -f regression-tools/regression-tools.tar.gz ]]; then
		tarballs+=(regression-tools/regression-tools.tar.gz)
	fi

	# when running automation against GP7, we need python2 dependencies shipped with gp6 to make Tinc work
	# if required, these libraries will be fetched by a CI pipeline under gp6-python-libs directory
	if [[ -f gp6-python-libs/gp6-python-libs.tar.gz ]]; then
		tarballs+=(gp6-python-libs/gp6-python-libs.tar.gz)
	fi
	(( ${#tarballs[@]} == 0 )) && return
	for t in "${tarballs[@]}"; do
		tar -xzf "${t}" -C ~gpadmin
	done
	ln -s "${files_to_link[@]}" ~root
	chown -R gpadmin:gpadmin ~gpadmin
}

function inflate_singlecluster() {
	local singlecluster=$(find singlecluster -name 'singlecluster*.tar.gz')
	if [[ ! -f ${singlecluster} ]]; then
		echo "Didn't find ${PWD}/singlecluster directory... skipping tarball inflation..."
		return
	fi
	tar zxf "${singlecluster}" -C /
	mv /singlecluster-* /singlecluster
	chmod a+w /singlecluster
	mkdir -p /etc/hadoop/conf /etc/hive/conf /etc/hbase/conf
	ln -s /singlecluster/hadoop/etc/hadoop/*-site.xml /etc/hadoop/conf
	ln -s /singlecluster/hive/conf/hive-site.xml /etc/hive/conf
	ln -s /singlecluster/hbase/conf/hbase-site.xml /etc/hbase/conf
}

function set_env() {
	export TERM=xterm-256color
	export TIMEFORMAT=$'\e[4;33mIt took %R seconds to complete this step\e[0m';
}

function run_pxf_automation() {
	# Let's make sure that automation/singlecluster directories are writeable
	chmod a+w pxf_src/automation /singlecluster || true
	find pxf_src/automation/tinc* -type d -exec chmod a+w {} \;

	local extension_name="pxf"
	if [[ ${USE_FDW} == "true" ]]; then
		extension_name="pxf_fdw"
	fi

	#TODO: remove once exttable tests with GP7 are set
	if [[ ${GROUP} == fdw_gpdb_schedule ]]; then
		extension_name="pxf_fdw"
	fi

	su gpadmin -c "
		source '${GPHOME}/greenplum_path.sh' &&
		psql -p ${PGPORT} -d template1 -c 'CREATE EXTENSION IF NOT EXISTS ${extension_name}'
	"
	# prepare certification output directory
	mkdir -p certification
	chmod a+w certification

	cat > ~gpadmin/run_pxf_automation_test.sh <<-EOF
		#!/usr/bin/env bash
		set -exo pipefail

		source ~gpadmin/.pxfrc

		export PATH=\$PATH:${GPHD_ROOT}/bin
		export GPHD_ROOT=${GPHD_ROOT}
		export PXF_HOME=${PXF_HOME}
		export PGPORT=${PGPORT}
		export USE_FDW=${USE_FDW}

		cd pxf_src/automation
		time make GROUP=${GROUP} test

		# if the test is successful, create certification file
		gpdb_build_from_sql=\$(psql -c 'select version()' | grep Greenplum | cut -d ' ' -f 6,8)
		gpdb_build_clean=\${gpdb_build_from_sql%)}
		pxf_version=\$(< ${PXF_HOME}/version)
		echo "GPDB-\${gpdb_build_clean/ commit:/-}-PXF-\${pxf_version}" > "${PWD}/certification/certification.txt"
		echo
		echo '****************************************************************************************************'
		echo "Wrote certification : \$(< ${PWD}/certification/certification.txt)"
		echo '****************************************************************************************************'
	EOF

	chown gpadmin:gpadmin ~gpadmin/run_pxf_automation_test.sh
	chmod a+x ~gpadmin/run_pxf_automation_test.sh

	if [[ ${ACCEPTANCE} == true ]]; then
		echo 'Acceptance test pipeline'
		exit 1
	fi

	su gpadmin -c ~gpadmin/run_pxf_automation_test.sh
}

function run_regression_test() {
	ln -s "${PWD}/gpdb_src" ~gpadmin/gpdb_src
	cat > ~gpadmin/run_regression_test.sh <<-EOF
		#!/bin/bash
		source /opt/gcc_env.sh || true
		source ${GPHOME}/greenplum_path.sh
		source gpdb_src/gpAux/gpdemo/gpdemo-env.sh
		export PATH=\$PATH:${GPHD_ROOT}/bin

		cd "${PXF_EXTENSIONS_DIR}"
		make installcheck USE_PGXS=1

		[[ -s regression.diffs ]] && cat regression.diffs && exit 1

		exit 0
	EOF

	chown -R gpadmin:gpadmin "${PXF_EXTENSIONS_DIR}"
	chown gpadmin:gpadmin ~gpadmin/run_regression_test.sh
	chmod a+x ~gpadmin/run_regression_test.sh
	su gpadmin -c ~gpadmin/run_regression_test.sh
}

function build_install_gpdb() {

	bash -c "
		source /opt/gcc_env.sh || true
		cd '${PWD}/gpdb_src' || return 1
		CC=\$(command -v gcc) CXX=\$(command -v g++) ./configure \
			--with-{perl,python,libxml,zstd} \
			--disable-{gpfdist,orca} \
			'--prefix=${GPHOME}'
		make -j4 -s
		make -s install
	"
}

function install_gpdb_binary() {
	# Ensure gpadmin owns its home directory
	chown -R gpadmin:gpadmin /home/gpadmin

	if [[ -d bin_gpdb ]]; then
		mkdir -p ${GPHOME}
		tar -xzf bin_gpdb/*.tar.gz -C ${GPHOME}
	else
		build_install_gpdb
	fi

	local gphome python_dir python_version=2.7 export_pythonpath='export PYTHONPATH=$PYTHONPATH'
	# CentOS releases contain a /etc/redhat-release which is symlinked to /etc/centos-release
	if [[ -f /etc/redhat-release ]]; then
		# We can't use service sshd restart as service is not installed on CentOS 7 or RHEL 8.
		/usr/sbin/sshd &
		python_dir=python${python_version}/site-packages
		export_pythonpath+=:/usr/lib/${python_dir}:/usr/lib64/$python_dir
	elif [[ -f /etc/debian_version ]]; then
		service ssh start
		python_dir=python${python_version}/dist-packages
		export_pythonpath+=:/usr/local/lib/$python_dir
	fi

	echo "$export_pythonpath" >> "${PXF_SRC}/automation/tinc/main/tinc_env.sh"
}

function install_gpdb_package() {
	# Ensure gpadmin owns its home directory
	chown -R gpadmin:gpadmin /home/gpadmin

	local gphome python_dir python_version=2.7 export_pythonpath='export PYTHONPATH=$PYTHONPATH' pkg_file version
	gpdb_package=${PWD}/${GPDB_PKG_DIR:-gpdb_package}

	if command -v rpm; then
		# install GPDB RPM
		pkg_file=$(find "${gpdb_package}" -name 'greenplum-db-*x86_64.rpm')
		if [[ -z ${pkg_file} ]]; then
			echo "Couldn't find RPM file in ${gpdb_package}. Skipping install..."
			return 1
		fi
		echo "Installing ${pkg_file}..."
		rpm --quiet -ivh "${pkg_file}" >/dev/null

		# We can't use service sshd restart as service is not installed on CentOS 7 or RHEL 8.
		/usr/sbin/sshd &
		python_dir=python${python_version}/site-packages
		export_pythonpath+=:/usr/lib/${python_dir}:/usr/lib64/${python_dir}
	elif command -v apt-get; then
		# install GPDB DEB, apt-get wants an absolute path
		pkg_file=$(find "${gpdb_package}" -name 'greenplum-db-*-ubuntu18.04-amd64.deb')
		if [[ -z ${pkg_file} ]]; then
			echo "Couldn't find DEB file in ${gpdb_package}. Skipping install..."
			return 1
		fi
		echo "Installing ${pkg_file}..."
		apt-get install -qq "${pkg_file}" >/dev/null

		service ssh start
		python_dir=python${python_version}/dist-packages
		export_pythonpath+=:/usr/local/lib/$python_dir
	else
		echo "Unsupported operating system '$(source /etc/os-release && echo "${PRETTY_NAME}")'. Exiting..."
		exit 1
	fi

	echo "$export_pythonpath" >> "${PXF_SRC}/automation/tinc/main/tinc_env.sh"

	# create symlink to allow pgregress to run (hardcoded to look for /usr/local/greenplum-db-devel/psql)
	rm -rf /usr/local/greenplum-db-devel
	# get version from the package file name
	: "${pkg_file#*greenplum-db-}"
	version=${_%%-*}
	gphome_dir=$(find /usr/local/ -name "greenplum-db-${version}*" -type d)
	ln -sf "${gphome_dir}" /usr/local/greenplum-db-devel
	# change permissions to gpadmin
	chown -R gpadmin:gpadmin /usr/local/greenplum-db*
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
	awk '{print "localhost", $1, $2; print "0.0.0.0", $1, $2}' /etc/ssh/ssh_host_rsa_key.pub >> /home/gpadmin/.ssh/known_hosts
	ssh "${SSH_OPTS[@]}" gpadmin@cdw "
		source ${GPHOME}/greenplum_path.sh &&
		export MASTER_DATA_DIRECTORY=${CDD_VALUE} &&
		echo 'host all all 10.0.0.0/16 trust' >> ${CDD_VALUE}/pg_hba.conf &&
		psql -d template1 <<-EOF && gpstop -u
			CREATE EXTENSION pxf;
			CREATE DATABASE gpadmin;
			CREATE ROLE root LOGIN;
		EOF
	"
}

function create_gpdb_cluster() {
	su gpadmin -c "source ${GPHOME}/greenplum_path.sh && make -C gpdb_src/gpAux/gpdemo create-demo-cluster"
}

function add_remote_user_access_for_gpdb() {
	local username=${1}

	# load local cluster configuration
	echo "Adding access entry for ${username} to pg_hba.conf and restarting GPDB for change to take effect"
	su gpadmin -c "
		if [[ -f gpdb_src/gpAux/gpdemo/gpdemo-env.sh ]]; then
		    source gpdb_src/gpAux/gpdemo/gpdemo-env.sh
		else
		    export MASTER_DATA_DIRECTORY=~gpadmin/data/master/gpseg-1
		fi
		echo 'local    all     ${username}     trust' >> \${MASTER_DATA_DIRECTORY}/pg_hba.conf
		source ${GPHOME}/greenplum_path.sh
		gpstop -u
	"
}

function setup_gpadmin_user() {

	# Don't create gpadmin user if already exists
	if ! id -u gpadmin; then
		groupadd -g 1000 gpadmin && useradd -u 1000 -g 1000 -M gpadmin
		echo "gpadmin  ALL=(ALL)	   NOPASSWD: ALL" > /etc/sudoers.d/gpadmin
		groupadd supergroup && usermod -a -G supergroup gpadmin
		mkdir -p ~gpadmin/.ssh
		ssh-keygen -t rsa -N "" -f ~gpadmin/.ssh/id_rsa
		cat /home/gpadmin/.ssh/id_rsa.pub >> ~gpadmin/.ssh/authorized_keys
		chmod 0600 /home/gpadmin/.ssh/authorized_keys
		awk '{print "localhost", $1, $2; print "0.0.0.0", $1, $2}' /etc/ssh/ssh_host_rsa_key.pub >> ~gpadmin/.ssh/known_hosts
		chown -R gpadmin:gpadmin ${GPHOME} ~gpadmin/.ssh # don't chown cached dirs ~/.m2, etc.
		echo -e "password\npassword" | passwd gpadmin 2> /dev/null
	fi
	cat <<-EOF >> /etc/security/limits.d/gpadmin-limits.conf
		gpadmin soft core unlimited
		gpadmin soft nproc 131072
		gpadmin soft nofile 65536
	EOF
	echo "export JAVA_HOME=${JAVA_HOME}" >> ~gpadmin/.bashrc
	if [[ -d gpdb_src/gpAux/gpdemo ]]; then
		chown -R gpadmin:gpadmin gpdb_src/gpAux/gpdemo
	fi

	if grep -i ubuntu /etc/os-release; then
		echo '[[ -f ~/.bashrc ]] && . ~/.bashrc' >> ~gpadmin/.bash_profile
		chown gpadmin:gpadmin ~gpadmin/.bash_profile
	fi
}

function install_pxf_client() {
	[[ ${TEST_ENV} == dev ]] || return 0
	# recompile pxf.so file for dev environments only
	bash -c "
		source '${GPHOME}/greenplum_path.sh'
		source /opt/gcc_env.sh || true
		USE_PGXS=1 make -C '${PXF_EXTENSIONS_DIR}' install
	"
}

function install_pxf_server() {
	if [[ ! -d ${PXF_HOME} ]]; then
		if [[ -d pxf_tarball ]]; then
			tar -xzf pxf_tarball/pxf.tar.gz -C ${GPHOME}
		else
			# requires login shell so that Go's dep is on PATH
			bash --login -c "
				export JAVA_TOOL_OPTIONS=-Dfile.encoding=UTF8
				make -C '${PWD}/pxf_src' install
			"
		fi
	fi
	chown -R gpadmin:gpadmin "${PXF_HOME}"
}

function install_pxf_tarball() {
	local tarball_dir=${PXF_PKG_DIR:-pxf_tarball}
	tar -xzf "${tarball_dir}/"pxf-gp*.tar.gz -C /tmp
	/tmp/pxf*/install_component
	chown -R gpadmin:gpadmin "${PXF_HOME}"

	# install separately built PXF FDW extension if it is available on the inputs
	local fdw_tarball_dir=${PXF_PKG_DIR:-pxf_fdw_tarball}
	if compgen -G "${fdw_tarball_dir}/pxf-fdw-*.tar.gz" > /dev/null; then
		tar -xzf "${fdw_tarball_dir}/"pxf-fdw-*.tar.gz -C /tmp
		chmod 777 /tmp
		ls -al /tmp
		/usr/bin/install -c -m 755 /tmp/pxf_fdw.so "$("${GPHOME}/bin/pg_config" --pkglibdir)"
		/usr/bin/install -c -m 644 /tmp/pxf_fdw.control "$("${GPHOME}/bin/pg_config" --sharedir)/extension/"
		/usr/bin/install -c -m 644 /tmp/pxf_fdw*.sql "$("${GPHOME}/bin/pg_config" --sharedir)/extension/"
	fi
}

function install_pxf_package() {
	if command -v rpm; then
		# install PXF RPM
		pkg_file=$(find pxf_package -name 'pxf-gp*.x86_64.rpm')
		if [[ -z ${pkg_file} ]]; then
			echo "Couldn't find PXF RPM file in pxf_package. Skipping install..."
			return 1
		fi
		echo "Installing ${pkg_file}..."
		rpm --quiet -ivh "${pkg_file}" >/dev/null
	elif command -v dpkg; then
		# install PXF DEB
		pkg_file=$(find pxf_package -name 'pxf-gp*amd64.deb')
		if [[ -z ${pkg_file} ]]; then
			echo "Couldn't find PXF DEB file in pxf_package. Skipping install..."
			return 1
		fi
		echo "Installing ${pkg_file}..."
		dpkg --install "${pkg_file}" >/dev/null
	fi

	chown -R gpadmin:gpadmin "${PXF_HOME}"
}

function setup_impersonation() {
	local GPHD_ROOT=${1}

	# enable impersonation by gpadmin user
	if [[ ${IMPERSONATION} == true ]]; then
		echo 'Impersonation is enabled, adding support for gpadmin proxy user'
		cat > proxy-config.xml <<-EOF
			<property>
			  <name>hadoop.proxyuser.${PROXY_USER}.hosts</name>
			  <value>*</value>
			</property>
			<property>
			  <name>hadoop.proxyuser.${PROXY_USER}.groups</name>
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
		sed -i -e '/<configuration>/r proxy-config.xml' "${GPHD_ROOT}/hadoop/etc/hadoop/core-site.xml" "${GPHD_ROOT}/hbase/conf/hbase-site.xml"
		rm proxy-config.xml
	elif [[ ${IMPERSONATION} == false ]]; then
		echo 'Impersonation is disabled, no proxy user setup performed.'
	else
		echo "ERROR: Invalid or missing CI property value: IMPERSONATION=${IMPERSONATION}"
		exit 1
	fi
	if ! find "${GPHD_ROOT}/hbase/lib" -name 'pxf-hbase-*.jar' | grep pxf-hbase; then
		cp "${SHARE_DIR}"/pxf-hbase-*.jar "${GPHD_ROOT}/hbase/lib"
	fi
}

function setup_hadoop() {
	local hdfsrepo=$1

	[[ -z ${GROUP} ]] && return 0

	export SLAVES=1
	setup_impersonation "${hdfsrepo}"
	if grep 'hadoop-3' "${hdfsrepo}/versions.txt"; then
		adjust_for_hadoop3 "${hdfsrepo}"
	fi
	start_hadoop_services "${hdfsrepo}"
}

function adjust_for_hadoop3() {
	local GPHD_ROOT=${1}

	# remove deprecated conf from hive-env.sh
	sed -i -e 's|-hiveconf hive.log.dir=$LOGS_ROOT ||g' "${GPHD_ROOT}/hive/conf/hive-env.sh"

	# add properties to hive-site.xml
	cat > patch.xml <<-EOF
		<property>
			<name>hive.tez.container.size</name>
			<value>2048</value>
		</property>
		<property>
			<name>datanucleus.schema.autoCreateAll</name>
			<value>True</value>
		</property>
		<property>
			<name>metastore.metastore.event.db.notification.api.auth</name>
			<value>false</value>
		</property>
	EOF
	sed -i -e '/<configuration>/r patch.xml' -e 's|>mr|>tez|g' "${GPHD_ROOT}/hive/conf/hive-site.xml"

	# add properties to tez-site.xml
	cat > patch.xml <<-EOF
		<property>
			<name>tez.use.cluster.hadoop-libs</name>
			<value>true</value>
		</property>
	EOF
	sed -i -e '/<configuration supports_final="true">/r patch.xml' "${GPHD_ROOT}/tez/conf/tez-site.xml"
	rm patch.xml

	# update properties in yarn-site.xml
	sed -i -e 's|HADOOP_CONF|HADOOP_CONF_DIR|g' \
	       -e 's|HADOOP_ROOT|HADOOP_HOME|g' "${GPHD_ROOT}/hadoop/etc/hadoop/yarn-site.xml"

}

function start_hadoop_services() {
	local GPHD_ROOT=${1}

	# Start all hadoop services
	JAVA_HOME=${HADOOP_JAVA_HOME} "${GPHD_ROOT}/bin/init-gphd.sh"
	JAVA_HOME=${HADOOP_JAVA_HOME} "${GPHD_ROOT}/bin/start-hdfs.sh"
	JAVA_HOME=${HADOOP_JAVA_HOME} "${GPHD_ROOT}/bin/start-zookeeper.sh"
	JAVA_HOME=${HADOOP_JAVA_HOME} "${GPHD_ROOT}/bin/start-yarn.sh" &
	JAVA_HOME=${HADOOP_JAVA_HOME} "${GPHD_ROOT}/bin/start-hbase.sh" &
	init_hive_metastore "${GPHD_ROOT}"
	JAVA_HOME=${HADOOP_JAVA_HOME} "${GPHD_ROOT}/bin/start-hive.sh" &
	wait
	export PATH=$PATH:${GPHD_ROOT}/bin

	# list running Hadoop daemons
	JAVA_HOME=${HADOOP_JAVA_HOME} jps

	# grant gpadmin user admin privilege for feature tests to be able to run on secured cluster
	if [[ ${IMPERSONATION} == true ]]; then
		echo 'Granting gpadmin user admin privileges for HBase'
		echo "grant 'gpadmin', 'RWXCA'" | hbase shell
	fi
}

# explicitly init the hive metastore to ensure necessary system tables have been created
function init_hive_metastore() {
	local GPHD_ROOT=${1}
	mkdir -p "${GPHD_ROOT}/storage/hive"
	pushd "${GPHD_ROOT}/storage/hive"
	JAVA_HOME=${HADOOP_JAVA_HOME}  ${GPHD_ROOT}/hive/bin/schematool -dbType derby -initSchema
	popd
}

function init_pxf() {
	echo 'Ensure pxf version can be run before pxf init'
	su gpadmin -c "${PXF_HOME}/bin/pxf version | grep -E '^PXF version [0-9]+.[0-9]+.[0-9]+'" || exit 1

	echo 'Initializing PXF service'
	# requires a login shell to source startup scripts (JAVA_HOME)
	su --login gpadmin -c "PXF_CONF=${BASE_DIR} ${PXF_HOME}/bin/pxf init"
}

function configure_pxf_server() {
	echo 'Ensure pxf version can be run before configuring pxf'
	su gpadmin -c "${PXF_HOME}/bin/pxf version | grep -E '^PXF version [0-9]+.[0-9]+.[0-9]+'" || exit 1

	echo 'Register PXF extension in Greenplum'
	# requires a login shell to source startup scripts (JAVA_HOME)
	su --login gpadmin -c "${PXF_HOME}/bin/pxf register"

	# prepare pxf if BASE_DIR is different from PXF_HOME
	if [[ "$BASE_DIR" != "$PXF_HOME" ]]; then
		echo "Prepare PXF in $BASE_DIR"
		su --login gpadmin -c "PXF_BASE=${BASE_DIR} pxf prepare"
		export PXF_BASE=${BASE_DIR}
		echo "export PXF_BASE=${BASE_DIR}" >> ~/.pxfrc
		echo "export PXF_BASE=${BASE_DIR}" >> ~gpadmin/.bashrc
	fi

	# update impersonation value based on CI parameter
	if [[ ! ${IMPERSONATION} == true ]]; then
		echo 'Impersonation is disabled, updating pxf-site.xml property'
		if [[ ! -f ${BASE_DIR}/servers/default/pxf-site.xml ]]; then
			cp ${PXF_HOME}/templates/pxf-site.xml ${BASE_DIR}/servers/default/pxf-site.xml
		fi
		sed -i -e "s|<value>true</value>|<value>false</value>|g" ${BASE_DIR}/servers/default/pxf-site.xml
	elif [[ -z ${PROTOCOL} ]]; then
		# Copy pxf-site.xml to the server configuration and update the
		# pxf.service.user.name property value to use the PROXY_USER
		# Only copy this file when testing against non-cloud
		if [[ ! -f ${BASE_DIR}/servers/default/pxf-site.xml ]]; then
			cp ${TEMPLATES_DIR}/templates/pxf-site.xml ${BASE_DIR}/servers/default/pxf-site.xml
			sed -i -e "s|</configuration>|<property><name>pxf.service.user.name</name><value>${PROXY_USER}</value></property></configuration>|g" ${BASE_DIR}/servers/default/pxf-site.xml
		fi
	fi

	# update runtime JDK value based on CI parameter
	RUN_JDK_VERSION=${RUN_JDK_VERSION:-8}
	if [[ $RUN_JDK_VERSION == 11 ]]; then
		echo 'JDK 11 requested for runtime, setting PXF JAVA_HOME=/usr/lib/jvm/jdk-11 in pxf-env.sh'
		su gpadmin -c "echo 'export JAVA_HOME=/usr/lib/jvm/jdk-11' >> ${BASE_DIR}/conf/pxf-env.sh"
	fi

	# add property to allow dynamic test: profiles that are used when testing against FDW
	echo -e "\npxf.profile.dynamic.regex=test:.*" >> "${BASE_DIR}/conf/pxf-application.properties"
}

function configure_hdfs_client_for_s3() {
	S3_CORE_SITE_XML=$(mktemp)
	cat <<-EOF > "${S3_CORE_SITE_XML}"
		<property>
		  <name>fs.s3a.access.key</name>
		  <value>${ACCESS_KEY_ID}</value>
		</property>
		<property>
		  <name>fs.s3a.secret.key</name>
		  <value>${SECRET_ACCESS_KEY}</value>
		</property>
	EOF
	sed -i -e "/<configuration>/r ${S3_CORE_SITE_XML}" "${GPHD_ROOT}/hadoop/etc/hadoop/core-site.xml"
}

function configure_hdfs_client_for_minio() {
	MINIO_CORE_SITE_XML=$(mktemp)
	cat <<-EOF > "${MINIO_CORE_SITE_XML}"
		<property>
		  <name>fs.s3a.endpoint</name>
		  <value>http://localhost:9000</value>
		</property>
		<property>
		  <name>fs.s3a.access.key</name>
		  <value>${ACCESS_KEY_ID}</value>
		</property>
		<property>
		  <name>fs.s3a.secret.key</name>
		  <value>${SECRET_ACCESS_KEY}</value>
		</property>
	EOF
	sed -i -e "/<configuration>/r ${MINIO_CORE_SITE_XML}" "${GPHD_ROOT}/hadoop/etc/hadoop/core-site.xml"
}

function configure_hdfs_client_for_gs() {
	cp "${PXF_HOME}/lib/shared/"gcs-connector-hadoop2-*-shaded.jar \
		"${GPHD_ROOT}/hadoop/share/hadoop/hdfs/lib"
	GS_CORE_SITE_XML=$(mktemp)
	cat <<-EOF > "${GS_CORE_SITE_XML}"
		<property>
		  <name>fs.AbstractFileSystem.gs.impl</name>
		  <value>com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS</value>
		  <description>The AbstractFileSystem for gs: uris.</description>
		</property>
		<property>
		  <name>google.cloud.auth.service.account.enable</name>
		  <value>true</value>
		  <description>
		    Whether to use a service account for GCS authorization.
		    Setting this property to \`false\` will disable use of service accounts for
		    authentication.
		  </description>
		</property>
		<property>
		  <name>google.cloud.auth.service.account.json.keyfile</name>
		  <value>${GOOGLE_KEYFILE}</value>
		  <description>
		    The JSON key file of the service account used for GCS
		    access when google.cloud.auth.service.account.enable is true.
		  </description>
		</property>
	EOF
	sed -i -e "/<configuration>/r ${GS_CORE_SITE_XML}" "${GPHD_ROOT}/hadoop/etc/hadoop/core-site.xml"
}

function configure_hdfs_client_for_adl() {
	cp "${PXF_HOME}/lib/shared/"azure-data-lake-store-sdk-*.jar \
		"${PXF_HOME}/lib/shared/"hadoop-azure-*.jar \
		"${PXF_HOME}/lib/shared/"hadoop-azure-datalake-*.jar \
		"${PXF_HOME}/lib/shared/"hadoop-common-*.jar \
		"${PXF_HOME}/lib/shared/"htrace-core4-*-incubating.jar \
		"${PXF_HOME}/lib/shared/"stax2-api-*.jar \
		"${PXF_HOME}/lib/shared/"woodstox-core-*.jar "${GPHD_ROOT}/hadoop/share/hadoop/common/lib"
	ADL_CORE_SITE_XML=$(mktemp)
	cat <<-EOF > "${ADL_CORE_SITE_XML}"
		<property>
		    <name>fs.adl.oauth2.access.token.provider.type</name>
		    <value>ClientCredential</value>
		</property>
		<property>
		    <name>fs.adl.oauth2.refresh.url</name>
		    <value>${ADL_OAUTH2_REFRESH_URL}</value>
		</property>
		<property>
		    <name>fs.adl.oauth2.client.id</name>
		    <value>${ADL_OAUTH2_CLIENT_ID}</value>
		</property>
		<property>
		    <name>fs.adl.oauth2.credential</name>
		    <value>${ADL_OAUTH2_CREDENTIAL}</value>
		</property>
	EOF
	sed -i -e "/<configuration>/r ${ADL_CORE_SITE_XML}" "${GPHD_ROOT}/hadoop/etc/hadoop/core-site.xml"
}

function configure_hdfs_client_for_wasbs() {
	WASBS_CORE_SITE_XML=$(mktemp)
	cat <<-EOF > "${WASBS_CORE_SITE_XML}"
		<property>
		  <name>fs.azure.account.key.${WASBS_ACCOUNT_NAME}.blob.core.windows.net</name>
		  <value>${WASBS_ACCOUNT_KEY}</value>
		</property>
	EOF
	sed -i -e "/<configuration>/r ${WASBS_CORE_SITE_XML}" "${GPHD_ROOT}/hadoop/etc/hadoop/core-site.xml"
}

function configure_pxf_gs_server() {
	mkdir -p ${BASE_DIR}/servers/gs
	GOOGLE_KEYFILE=$(mktemp)
	echo "${GOOGLE_CREDENTIALS}" > "${GOOGLE_KEYFILE}"
	chown gpadmin "${GOOGLE_KEYFILE}"
	sed -e "s|YOUR_GOOGLE_STORAGE_KEYFILE|${GOOGLE_KEYFILE}|" \
		${TEMPLATES_DIR}/templates/gs-site.xml >"${BASE_DIR}/servers/gs/gs-site.xml"
}

function configure_pxf_s3_server() {
	mkdir -p ${BASE_DIR}/servers/s3
	sed -e "s|YOUR_AWS_ACCESS_KEY_ID|${ACCESS_KEY_ID}|" \
		-e "s|YOUR_AWS_SECRET_ACCESS_KEY|${SECRET_ACCESS_KEY}|" \
		${TEMPLATES_DIR}/templates/s3-site.xml >${BASE_DIR}/servers/s3/s3-site.xml

	mkdir -p ${BASE_DIR}/servers/s3-invalid
	cp ${TEMPLATES_DIR}/templates/s3-site.xml ${BASE_DIR}/servers/s3-invalid/s3-site.xml
	chown -R gpadmin:gpadmin "${BASE_DIR}/servers/s3" "${BASE_DIR}/servers/s3-invalid"
}

function configure_pxf_minio_server() {
	mkdir -p ${BASE_DIR}/servers/minio
	sed -e "s|YOUR_AWS_ACCESS_KEY_ID|${ACCESS_KEY_ID}|" \
		-e "s|YOUR_AWS_SECRET_ACCESS_KEY|${SECRET_ACCESS_KEY}|" \
		-e "s|YOUR_MINIO_URL|http://localhost:9000|" \
		${TEMPLATES_DIR}/templates/minio-site.xml >${BASE_DIR}/servers/minio/minio-site.xml
}

function configure_pxf_adl_server() {
	mkdir -p "${BASE_DIR}/servers/adl"
	sed -e "s|YOUR_ADL_REFRESH_URL|${ADL_OAUTH2_REFRESH_URL}|g" \
		-e "s|YOUR_ADL_CLIENT_ID|${ADL_OAUTH2_CLIENT_ID}|g" \
		-e "s|YOUR_ADL_CREDENTIAL|${ADL_OAUTH2_CREDENTIAL}|g" \
		"${TEMPLATES_DIR}/templates/adl-site.xml" >"${BASE_DIR}/servers/adl/adl-site.xml"
}

function configure_pxf_wasbs_server() {
	mkdir -p ${BASE_DIR}/servers/wasbs
	sed -e "s|YOUR_AZURE_BLOB_STORAGE_ACCOUNT_NAME|${WASBS_ACCOUNT_NAME}|g" \
		-e "s|YOUR_AZURE_BLOB_STORAGE_ACCOUNT_KEY|${WASBS_ACCOUNT_KEY}|g" \
		"${TEMPLATES_DIR}/templates/wasbs-site.xml" >"${BASE_DIR}/servers/wasbs/wasbs-site.xml"
}

function configure_pxf_default_server() {
	AMBARI_DIR=$(find /tmp/build/ -name ambari_env_files)
	if [[ -n $AMBARI_DIR  ]]; then
	  AMBARI_KEYTAB_FILE=$(find "$AMBARI_DIR" -name "*.keytab")
		cp "${AMBARI_DIR}"/conf/*-site.xml "${BASE_DIR}/servers/default"

		if [[ -n $AMBARI_KEYTAB_FILE ]]; then
			REALM=$(cat "$AMBARI_DIR"/REALM)
			HADOOP_USER=$(cat "$AMBARI_DIR"/HADOOP_USER)
			cp ${TEMPLATES_DIR}/templates/mapred-site.xml ${BASE_DIR}/servers/default/mapred1-site.xml
			cp ${TEMPLATES_DIR}/templates/pxf-site.xml ${BASE_DIR}/servers/default/pxf-site.xml
			sed -i -e "s|gpadmin/_HOST@EXAMPLE.COM|${HADOOP_USER}@${REALM}|g" ${BASE_DIR}/servers/default/pxf-site.xml
			if [[ ${PXF_VERSION} == 5 ]]; then
				sed -i -e "s|\${pxf.conf}/keytabs/pxf.service.keytab|$AMBARI_KEYTAB_FILE|g" ${BASE_DIR}/servers/default/pxf-site.xml
			else
				sed -i -e "s|\${pxf.base}/keytabs/pxf.service.keytab|$AMBARI_KEYTAB_FILE|g" ${BASE_DIR}/servers/default/pxf-site.xml
			fi
			sed -i -e "s|\${user.name}||g" ${BASE_DIR}/servers/default/pxf-site.xml
			sudo mkdir -p /etc/security/keytabs/
			sudo cp "$AMBARI_KEYTAB_FILE" /etc/security/keytabs/"${HADOOP_USER}".headless.keytab
			sudo chown gpadmin:gpadmin /etc/security/keytabs/"${HADOOP_USER}".headless.keytab

			mkdir -p ${BASE_DIR}/servers/db-hive/
			cp ${BASE_DIR}/servers/default/pxf-site.xml ${BASE_DIR}/servers/db-hive/
			cp ${TEMPLATES_DIR}/templates/jdbc-site.xml ${BASE_DIR}/servers/db-hive/

			REALM=$(cat "$AMBARI_DIR"/REALM)
			HIVE_HOSTNAME=$(grep < "$AMBARI_DIR"/etc_hostfile ambari-2 | awk '{print $2}')
			KERBERIZED_HADOOP_URI="hive/${HIVE_HOSTNAME}.c.${GOOGLE_PROJECT_ID}.internal@${REALM};saslQop=auth" # quoted because of semicolon
			sed -i -e 's|YOUR_DATABASE_JDBC_DRIVER_CLASS_NAME|org.apache.hive.jdbc.HiveDriver|' \
				-e "s|YOUR_DATABASE_JDBC_URL|jdbc:hive2://${HIVE_HOSTNAME}:10000/default;principal=${KERBERIZED_HADOOP_URI}|" \
				-e 's|YOUR_DATABASE_JDBC_USER||' \
				-e 's|YOUR_DATABASE_JDBC_PASSWORD||' \
				-e 's|</configuration>|<property><name>hadoop.security.authentication</name><value>kerberos</value></property></configuration>|g' \
				${BASE_DIR}/servers/db-hive/jdbc-site.xml

			cp "${PXF_SRC}"/automation/src/test/resources/hive-report.sql ${BASE_DIR}/servers/db-hive/
		fi
	else
		# copy hadoop config files to BASE_DIR/servers/default
		if [[ -d /etc/hadoop/conf/ ]]; then
			cp /etc/hadoop/conf/*-site.xml "${BASE_DIR}/servers/default"
		fi
		if [[ -d /etc/hive/conf/ ]]; then
			cp /etc/hive/conf/*-site.xml "${BASE_DIR}/servers/default"
		fi
		if [[ -d /etc/hbase/conf/ ]]; then
			cp /etc/hbase/conf/*-site.xml "${BASE_DIR}/servers/default"
		fi
	fi

	if [[ ${IMPERSONATION} == true ]]; then
		cp -r ${BASE_DIR}/servers/default ${BASE_DIR}/servers/default-no-impersonation

		if [[ ! -f ${BASE_DIR}/servers/default-no-impersonation/pxf-site.xml ]]; then
			cp ${TEMPLATES_DIR}/templates/pxf-site.xml ${BASE_DIR}/servers/default-no-impersonation/pxf-site.xml
		fi

		sed -i \
			-e "/<name>pxf.service.user.impersonation<\/name>/ {n;s|<value>.*</value>|<value>false</value>|g;}" \
			-e "s|</configuration>|<property><name>pxf.service.user.name</name><value>foobar</value></property></configuration>|g" \
			${BASE_DIR}/servers/default-no-impersonation/pxf-site.xml
	fi
	chown -R gpadmin:gpadmin "${BASE_DIR}/servers"
}

function start_pxf_server() {
	# Check if some other process is listening on 5888
	netstat -tlpna | grep 5888 || true

	echo 'Starting PXF service'
	su --login gpadmin -c "${PXF_HOME}/bin/pxf start"
	# grep with regex to avoid catching grep process itself
	if [[ ${PXF_VERSION} == 5 ]]; then
		ps -aef | grep '[t]omcat'
	else
		ps -aef | grep '[p]xf-app'
	fi
}

function setup_s3_for_pg_regress() {
	configure_pxf_s3_server
	configure_hdfs_client_for_s3
	HCFS_BUCKET=gpdb-ud-scratch
}

function setup_gs_for_pg_regress() {
	configure_pxf_gs_server
	configure_hdfs_client_for_gs
	HCFS_BUCKET=data-gpdb-ud-tpch
}

function setup_adl_for_pg_regress() {
	configure_pxf_adl_server
	configure_hdfs_client_for_adl
	HCFS_BUCKET=${ADL_ACCOUNT}.azuredatalakestore.net
}

function setup_wasbs_for_pg_regress() {
	configure_pxf_wasbs_server
	configure_hdfs_client_for_wasbs
	HCFS_BUCKET=pxf-container@${WASBS_ACCOUNT_NAME}.blob.core.windows.net
}

function setup_minio_for_pg_regress() {
	configure_pxf_minio_server
	configure_hdfs_client_for_minio
	# this is set in setup_minio()
	HCFS_BUCKET=gpdb-ud-scratch
}

function setup_minio() {
	echo 'Adding test bucket gpdb-ud-scratch to Minio ...'
	mkdir -p /opt/minio/data/gpdb-ud-scratch

	export MINIO_ACCESS_KEY=admin MINIO_SECRET_KEY=password
	echo "Minio credentials: accessKey=${MINIO_ACCESS_KEY} secretKey=${MINIO_SECRET_KEY}"

	echo 'Starting Minio ...'
	MINIO_DOMAIN=localhost /opt/minio/bin/minio server /opt/minio/data &

	# export minio credentials as access environment variables
	export ACCESS_KEY_ID=${MINIO_ACCESS_KEY} SECRET_ACCESS_KEY=${MINIO_SECRET_KEY}
}

function set_ccp_os_user() {
    metadata_file="cluster_env_files/terraform/metadata"

    # TODO: Remove the jq installation from here once available in the base image.
    # Check if jq is installed
    if ! rpm -q jq &> /dev/null; then
        echo "jq is not installed. Installing jq..."
        sudo yum install -y jq
    fi

    # Check if the metadata file exists
    if [ ! -e "$metadata_file" ]; then
        echo "The $metadata_file file does not exist."
        exit 2
    fi

    # shellcheck disable=SC2034
    CCP_OS_USER=$(jq -r '.ami_default_user' "$metadata_file")
}
