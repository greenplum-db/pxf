#!/usr/bin/env bash

export GPHOME=/usr/local/greenplum-db
export PXF_HOME=$(find /usr/local/ -name "pxf-gp*" -type d)
export PATH=$PATH:${PXF_HOME}/bin

echo "Using GPHOME       : $GPHOME"
echo "Using PXF_HOME     : $PXF_HOME"

# export PXF_BASE_DIR
if [[ -n "${PXF_BASE}" ]]; then
  export PXF_BASE_DIR=${PXF_BASE}
  export PXF_BASE_OPTION="PXF_BASE=${PXF_BASE} "
  echo "Using PXF_BASE     : $PXF_BASE"
else
  # for tests only, pxf product scripts do not look at this variable
  export PXF_BASE_DIR=${PXF_HOME}
  export PXF_BASE_OPTION=""
fi
echo "Using PXF_BASE_DIR : $PXF_BASE_DIR"
echo

red="\033[0;31m"
green="\033[0;32m"
yellow="\033[0;33m"
white="\033[0;37;1m"
reset="\033[0m"

err_cnt=0
test_cnt=0
failed_tests_cnt=0

declare -a all_cluster_hosts
mapfile -t all_cluster_hosts <hostfile_all

run_test() {
	local usage='test <func> <message>'
	local func=${1:?${usage}} message="$((++test_cnt))) ${2:?${usage}}"
	echo -e "${yellow}${message}${white}:"
	# call the test function
	${func}
	# check if there were assertion errors
	if (( err_cnt == 0 )); then
		echo -e "${green}pass${reset}"
		echo
		return
	fi
	# update count of failed tests
	((failed_tests_cnt++))
	# reset error count back to 0 for the new test
	err_cnt=0
	echo -e "${red}fail${white}"
	echo
	echo -e "${reset}" && return 1
}

exit_with_err() {
	local usage='exit_with_err <test_name>'
	local test_name=${1:?${usage}}
	if (( failed_tests_cnt > 0 )); then
		echo -e "${red}${test_name}${white}: failed ${failed_tests_cnt}/${test_cnt} tests${reset}"
	else
		echo -e "${green}${test_name}${white}: all tests passed!${reset}"
	fi
	echo "failed_tests_cnt=${failed_tests_cnt}"
	exit "${failed_tests_cnt}"
}

assert_equals() {
	local usage='assert_equals <expected_text> <text_to_compare> <msg>'
	local expected=${1} text=${2} message="${3:?${usage}}"
	[[ "${expected}" == "${text//[$'\r']}" ]] && return
	assertion_error "${expected}" "${text}" "${message}"
}

assert_not_equals() {
	local usage='assert_not_equals <expected_text> <text_to_compare> <msg>'
	local expected=${1} text=${2} message="${3:?${usage}}"
	[[ "${expected}" != "${text//[$'\r']}" ]] && return
	assertion_error "${expected}" "${text}" "${message}"
}

assert_empty() {
	local usage='assert_empty <text_to_compare> <msg>'
	local text=${1} message="${2:?${usage}}"
	[[ -z "${text//[$'\r']}" ]] && return
	assertion_error "" "${text}" "${message}"
}

assert_not_empty() {
	local usage='assert_not_empty <text_to_compare> <msg>'
	local text=${1} message="${2:?${usage}}"
	[[ -n "${text//[$'\r']}" ]] && return
	assertion_error "<NOT_EMPTY>" "${text}" "${message}"
}

assertion_error() {
	local usage='assertion_error <expected_text> <text_to_compare> <msg>'
	local expected=${1} text=${2} message="${3:?${usage}}"
	((err_cnt++))
	echo -e "${red}--- assertion failed : ${yellow}${message}${white}"
	diff <(echo "${expected}") <(echo "${text}")
	cmp -b <(echo "${expected}") <(echo "${text}")
	echo -e "${reset}" && return 1
}

remove_remote_file() {
  ssh "${1}" "rm -rf "${2}""
}

list_remote_file() {
  ssh "${1}" "[[ -f ${2} ]] && ls ${2}"
}

echo_remote_dir() {
  ssh "${1}" "[[ -d ${2} ]] && echo ${2}"
}

list_remote_configs() {
	ssh "${1}" "
			[[ -d ${PXF_BASE_DIR}/servers/foo ]] && ls ${PXF_BASE_DIR}/servers/foo
			[[ -e ${PXF_BASE_DIR}/conf/foo.jar ]] && ls ${PXF_BASE_DIR}/conf/foo.jar
	"
}

remove_remote_configs() {
	ssh "${1}" "
			rm -rf ${PXF_BASE_DIR}/servers/foo
			rm -f  ${PXF_BASE_DIR}/conf/foo.jar
	"
}

cat_remote_file() {
  ssh "${1}" "[[ -f ${2} ]] && cat ${2}"
}

list_remote_pxf_running_pid() {
  ssh "${1}" "ps -aef | grep pxf | grep -v grep | tr -s ' ' | cut -d ' ' -f 2"
}

has_standby_coordinator() {
  grep -q scdw hostfile_all
}

get_host_from_version() {
  local mainHostName="master"
  gpdb_build_from_sql=$(psql -c 'select version()' | grep Greenplum | cut -d ' ' -f 6,8)
  GPDB_VERSION=${gpdb_build_from_sql:0:1}

   if [[ GPDB_VERSION -lt 7 ]]
      then
        mainHostName="master"
      else
        mainHostName="coordinator"
  fi

  echo "${mainHostName}"
}

get_cluster_description() {
  local cluster_description="$(get_host_from_version) host"
  if has_standby_coordinator; then
    cluster_description+=", standby $(get_host_from_version) host,"
  fi
  local num_segment_hosts
  num_segment_hosts="$(grep -c 'sdw\d+' hostfile_all)"
  cluster_description+=" and ${num_segment_hosts} segment hosts..."


  echo "${cluster_description}"
}

get_cluster_sync_description() {
  local cluster_sync_description="$(get_host_from_version) host to "
  if has_standby_coordinator; then
    cluster_sync_description+="standby $(get_host_from_version) host and "
  fi
  local num_segment_hosts
  num_segment_hosts="$(grep -c 'sdw\d+' hostfile_all)"
  cluster_sync_description+="${num_segment_hosts} segment hosts..."

  echo "${cluster_sync_description}"
}
