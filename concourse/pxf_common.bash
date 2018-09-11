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
	./gpdb_src/concourse/scripts/setup_gpadmin_user.bash ${TARGET_OS}
}

function install_pxf_client() {
	# recompile pxf.so file for dev environments only
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
	pushd pxf_src/pxf
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
