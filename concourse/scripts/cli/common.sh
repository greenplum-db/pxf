#!/usr/bin/env bash

#export PXF_CONF=~gpadmin/pxf
export GPHOME=/usr/local/greenplum-db
export PXF_HOME=$(find /usr/local/ -name "pxf-gp*" -type d)
export PATH=$PATH:${PXF_HOME}/bin

echo "Using GPHOME       :   $GPHOME"
echo "Using PXF_HOME     : $PXF_HOME"

# export PXF_BASE only if explicitly specified
if [[ -n "${PXF_BASE_DIR}" ]]; then
  export PXF_BASE=${PXF_BASE_DIR}
  echo "Using PXF_BASE     : $PXF_BASE"
else
  # for tests only, pxf product scripts do not look at this variable
  export PXF_BASE_DIR=${PXF_HOME}
fi
echo "Using PXF_BASE_DIR : $PXF_BASE_DIR"

red="\033[0;31m"
green="\033[0;32m"
yellow="\033[0;33m"
white="\033[0;37;1m"
reset="\033[0m"

err_cnt=0
test_cnt=0
failed_tests_cnt=0

run_test() {
	local usage='test <func> <message>'
	local func=${1:?${usage}} message="$(( ++test_cnt ))) ${2:?${usage}}"
	echo -e "${yellow}${msg}${white}:"
	# call the test function
	(${func})
	# check if there were assertion errors
	if (( err_cnt == 0 )); then
		echo -e "${green}pass${reset}"
		return
	fi
	# update count of failed tests
	(( failed_tests_cnt++ ))
	# reset error count back to 0 for the new test
	err_cnt=0
	echo -e "${red}fail${white}"
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
	exit "${failed_tests_cnt}"
}

assert_equals() {
	local usage='assert_equals <expected_text> <text_to_compare> <msg>'
	local expected=${1:?${usage}} text=${2:?${usage}} msg="${3:?${usage}}"
	if [[ ${expected} == "${text//
/}" ]]; then # clean up any carriage returns
		return
	fi
	((err_cnt++))
	echo -e "${red}--- assertion failed : ${yellow}${msg}${white}"
	diff <(echo "${expected}") <(echo "${text}")
	cmp -b <(echo "${expected}") <(echo "${text}")
	echo -e "${reset}" && return 1
}

remove_remote_file() {
  ssh "${1}" "rm -f "${2}""
}

list_remote_file() {
  ssh "${1}" "ls "${2}" 2>&1"
}
