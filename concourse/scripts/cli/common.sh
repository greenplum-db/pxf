#!/usr/bin/env bash

export PXF_CONF=~gpadmin/pxf
GPHOME=/usr/local/greenplum-db-devel
export PATH=$PATH:${GPHOME}/pxf/bin

red="\033[0;31m"
green="\033[0;32m"
yellow="\033[0;33m"
white="\033[0;37;1m"
reset="\033[0m"

err_cnt=0
test_cnt=0

compare() {
	local usage='compare <expected_text> <text_to_compare> <msg>'
	local expected=${1:?${usage}} text=${2:?${usage}} msg="$(( ++test_cnt ))) ${3:?${usage}}"
	echo -e "${yellow}${msg}${white}:"
	if [[ ${expected} == "${text///}" ]]; then # clean up any carriage returns
		echo -e "${green}pass${reset}"
		return
	fi
	((err_cnt++))
	echo -e "${red}fail${white}"
	diff <(echo "${expected}") <(echo "${text}")
	cmp -b <(echo "${expected}") <(echo "${text}")
	echo -e "${reset}" && return 1
}

exit_with_err() {
	local usage='exit_with_err <test_name>'
	local test_name=${1:?${usage}}
	if (( err_cnt > 0 )); then
		echo -e "${red}${test_name}${white}: failed ${err_cnt}/${test_cnt} tests${reset}"
	else
		echo -e "${green}${test_name}${white}: all tests passed!${reset}"
	fi
	exit "${err_cnt}"
}
