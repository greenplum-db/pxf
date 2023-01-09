#!/usr/bin/env bash

dir=$( cd "$(dirname "${BASH_SOURCE[0]}")" && pwd )
[[ -e ${dir}/common.sh ]] || exit 1
source "${dir}/common.sh"

cluster_description="$(get_cluster_description)"
num_hosts="${#all_cluster_hosts[@]}"

# ************************************************************************************************************
# ***** TEST Suite starts with PXF running on all nodes ******************************************************
# ************************************************************************************************************

# === Test "pxf cluster status (all running)" ================================================================
expected_status_message=\
"Checking status of PXF servers on ${cluster_description}
PXF is running on ${num_hosts} out of ${num_hosts} hosts"
test_status_succeeds_all_running() {
  # given:
  for host in "${all_cluster_hosts[@]}"; do
    #    : PXF is running
    local running_pid="$(list_remote_pxf_running_pid ${host})"
    echo "running pid=${running_pid}"
    assert_not_empty "${running_pid}" "PXF should be running on host ${host}"
    #    : AND the process pid file exists
    local file_pid="$(cat_remote_file ${host} "${PXF_BASE_DIR}"/run/pxf-app.pid)"
    echo "file pid=${file_pid}"
    assert_not_empty "${file_pid}" "PXF pid file should exist on host ${host}"
    #    : AND the pids match
    assert_equals "${running_pid}" "${file_pid}" "pid files should match on host ${host}"
  done
  # when : "pxf cluster status" command is run
  local result="$(pxf cluster status)"
  # then : it succeeds and prints the expected message
  assert_equals "${expected_status_message}" "${result}" "pxf cluster status should succeed"
}
run_test test_status_succeeds_all_running "pxf cluster status (all running) should succeed"
# ============================================================================================================

# === Test "pxf cluster stop (all running)" ==================================================================
expected_stop_message=\
"Stopping PXF on ${cluster_description}
PXF stopped successfully on ${num_hosts} out of ${num_hosts} hosts"
test_stop_succeeds_all_running() {
  # given:
  for host in "${all_cluster_hosts[@]}"; do
    #    : PXF is running
    local running_pid="$(list_remote_pxf_running_pid ${host})"
    echo "running pid=${running_pid}"
    assert_not_empty "${running_pid}" "PXF should be running on host ${host}"
    #    : AND the process pid file exists
    local file_pid="$(cat_remote_file ${host} "${PXF_BASE_DIR}"/run/pxf-app.pid)"
    echo "file pid=${file_pid}"
    assert_not_empty "${file_pid}" "PXF pid file should exist on host ${host}"
    #    : AND the pids match
    assert_equals "${running_pid}" "${file_pid}" "pid files should match on host ${host}"
  done
  # when : "pxf cluster stop" command is run
  local result="$(pxf cluster stop)"
  # then : it succeeds and prints the expected message
  assert_equals "${expected_stop_message}" "${result}" "pxf cluster stop should succeed"
  for host in "${all_cluster_hosts[@]}"; do
    #    : AND there are no PXF processes left running
    local running_pid="$(list_remote_pxf_running_pid ${host})"
    assert_empty "${running_pid}" "PXF should not be running on host ${host}"
    #    :AND the process pid file no longer exists
    local pid_file="$(list_remote_file ${host} "${PXF_BASE_DIR}"/run/pxf-app.pid)"
    assert_empty "${pid_file}" "PXF pid file should not exist on host ${host}"
  done
}
run_test test_stop_succeeds_all_running "pxf cluster stop (all running) should succeed"
# ============================================================================================================

# === Test "pxf cluster stop (none running)" =================================================================
expected_stop_message=\
"Stopping PXF on ${cluster_description}
PXF stopped successfully on ${num_hosts} out of ${num_hosts} hosts"
test_stop_succeeds_none_running() {
  # given:
  for host in "${all_cluster_hosts[@]}"; do
    #    : PXF is not running
    local running_pid="$(list_remote_pxf_running_pid ${host})"
    echo "running pid=${running_pid}"
    assert_empty "${running_pid}" "PXF should not be running on host ${host}"
    #    : AND the process pid file does not exist
    local file_pid="$(cat_remote_file ${host} "${PXF_BASE_DIR}"/run/pxf-app.pid)"
    echo "file pid=${file_pid}"
    assert_empty "${file_pid}" "PXF pid file should not exist on host ${host}"
  done
  # when : "pxf cluster stop" command is run
  local result="$(pxf cluster stop)"
  # then : it succeeds and prints the expected message
  assert_equals "${expected_stop_message}" "${result}" "pxf cluster stop should succeed"
  for host in "${all_cluster_hosts[@]}"; do
    #    : AND there are no PXF processes left running
    local running_pid="$(list_remote_pxf_running_pid ${host})"
    assert_empty "${running_pid}" "PXF should not be running on host ${host}"
    #    :AND the process pid file does not exist
    local pid_file="$(list_remote_file ${host} "${PXF_BASE_DIR}"/run/pxf-app.pid)"
    assert_empty "${pid_file}" "PXF pid file should not exist on host ${host}"
  done
}
run_test test_stop_succeeds_none_running "pxf cluster stop (none running) should succeed"
# ============================================================================================================

# === Test "pxf cluster status (none running)" ===============================================================
expected_status_message=\
"Checking status of PXF servers on ${cluster_description}\n\
ERROR: PXF is not running on ${num_hosts} out of ${num_hosts} hosts\n\
cdw ==> ${yellow}Checking if PXF is up and running...${reset}\n\
${red}ERROR: PXF is down - the application is not running${reset}...\n"

if has_standby_coordinator; then
  expected_status_message+="scdw ==> ${yellow}Checking if PXF is up and running...${reset}\n${red}ERROR: PXF is down - the application is not running${reset}...\n"
fi

expected_status_message+=\
"sdw1 ==> ${yellow}Checking if PXF is up and running...${reset}\n\
${red}ERROR: PXF is down - the application is not running${reset}...\n\
sdw2 ==> ${yellow}Checking if PXF is up and running...${reset}\n\
${red}ERROR: PXF is down - the application is not running${reset}..."
test_status_succeeds_none_running() {
  for host in "${all_cluster_hosts[@]}"; do
    #    : PXF is not running
    local running_pid="$(list_remote_pxf_running_pid ${host})"
    echo "running pid=${running_pid}"
    assert_empty "${running_pid}" "PXF should not be running on host ${host}"
    #    : AND the process pid file does not exist
    local file_pid="$(cat_remote_file ${host} "${PXF_BASE_DIR}"/run/pxf-app.pid)"
    echo "file pid=${file_pid}"
    assert_empty "${file_pid}" "PXF pid file should not exist on host ${host}"
  done
  # when : "pxf cluster status" command is run
  local result="$(pxf cluster status 2>&1)"
  # then : it succeeds and prints the expected message
  assert_equals "$(echo -e "${expected_status_message}")" "${result}" "pxf cluster status should succeed"
}
run_test test_status_succeeds_none_running "pxf cluster status (none running) should succeed"
# ============================================================================================================

# === Test "pxf cluster start (none running)" ================================================================
expected_start_message=\
"Starting PXF on ${cluster_description}
PXF started successfully on ${num_hosts} out of ${num_hosts} hosts"
test_start_succeeds_none_running() {
  # given:
  for host in "${all_cluster_hosts[@]}"; do
    #    : PXF is not running
    local running_pid="$(list_remote_pxf_running_pid ${host})"
    echo "running pid=${running_pid}"
    assert_empty "${running_pid}" "PXF should not be running on host ${host}"
    #    : AND the process pid file does not exist
    local file_pid="$(cat_remote_file ${host} "${PXF_BASE_DIR}"/run/pxf-app.pid)"
    echo "file pid=${file_pid}"
    assert_empty "${file_pid}" "PXF pid file should not exist on host ${host}"
  done
  # when : "pxf cluster start" command is run
  local result="$(pxf cluster start)"
  # then : it succeeds and prints the expected message
  assert_equals "${expected_start_message}" "${result}" "pxf cluster start should succeed"
  for host in "${all_cluster_hosts[@]}"; do
    #    : AND PXF is running
    local running_pid="$(list_remote_pxf_running_pid ${host})"
    echo "running pid=${running_pid}"
    assert_not_empty "${running_pid}" "PXF should be running on host ${host}"
    #    : AND the process pid file exists
    local file_pid="$(cat_remote_file ${host} "${PXF_BASE_DIR}"/run/pxf-app.pid)"
    echo "file pid=${file_pid}"
    assert_not_empty "${file_pid}" "PXF pid file should exist on host ${host}"
    #    : AND the pids match
    assert_equals "${running_pid}" "${file_pid}" "pid files should match on host ${host}"
  done
}
run_test test_start_succeeds_none_running "pxf cluster start (none running) should succeed"
# ============================================================================================================

# === Test "pxf cluster start (all running)" =================================================================
expected_start_message=\
"Starting PXF on ${cluster_description}
PXF started successfully on ${num_hosts} out of ${num_hosts} hosts"
test_start_succeeds_all_running() {
  local index=0
  declare -a old_pids
  # given:
  for host in "${all_cluster_hosts[@]}"; do
    #    : PXF is running
    local running_pid="$(list_remote_pxf_running_pid ${host})"
    echo "running pid=${running_pid}"
    assert_not_empty "${running_pid}" "PXF should be running on host ${host}"
    #    : AND the process pid file exists
    local file_pid="$(cat_remote_file ${host} "${PXF_BASE_DIR}"/run/pxf-app.pid)"
    echo "file pid=${file_pid}"
    assert_not_empty "${file_pid}" "PXF pid file should exist on host ${host}"
    #    : AND the pids match
    assert_equals "${running_pid}" "${file_pid}" "pid files should match on host ${host}"
    old_pids[${index}]="${running_pid}"
    ((index++))
  done
  # when : "pxf cluster start" command is run
  local result="$(pxf cluster start)"
  # then : it succeeds and prints the expected message
  assert_equals "${expected_start_message}" "${result}" "pxf cluster start should succeed"
  index=0
  for host in "${all_cluster_hosts[@]}"; do
    #    : AND PXF is running
    local running_pid="$(list_remote_pxf_running_pid ${host})"
    echo "running pid=${running_pid}"
    assert_not_empty "${running_pid}" "PXF should be running on host ${host}"
    #    : AND the process pid file exists
    local file_pid="$(cat_remote_file ${host} "${PXF_BASE_DIR}"/run/pxf-app.pid)"
    echo "file pid=${file_pid}"
    assert_not_empty "${file_pid}" "PXF pid file should exist on host ${host}"
    #    : AND the pids match
    assert_equals "${running_pid}" "${file_pid}" "pid files should match on host ${host}"
    #    : AND the process pid file content is the same as the previous one
    assert_equals "${old_pids[${index}]}" "${running_pid}" "PXF pid should be the same after start on host ${host}"
    ((index++))
  done
}
run_test test_start_succeeds_all_running "pxf cluster start (all running) should succeed"
# ============================================================================================================

# === Test "pxf cluster restart (all running)" ===============================================================
expected_restart_message=\
"Restarting PXF on ${cluster_description}
PXF restarted successfully on ${num_hosts} out of ${num_hosts} hosts"
test_restart_succeeds_all_running() {
  local index=0
  declare -a old_pids
  # given:
  for host in "${all_cluster_hosts[@]}"; do
    #    : PXF is running
    local running_pid="$(list_remote_pxf_running_pid ${host})"
    echo "running pid=${running_pid}"
    assert_not_empty "${running_pid}" "PXF should be running on host ${host}"
    #    : AND the process pid file exists
    local file_pid="$(cat_remote_file ${host} "${PXF_BASE_DIR}"/run/pxf-app.pid)"
    echo "file pid=${file_pid}"
    assert_not_empty "${file_pid}" "PXF pid file should exist on host ${host}"
    #    : AND the pids match
    assert_equals "${running_pid}" "${file_pid}" "pid files should match on host ${host}"
    old_pids[${index}]="${running_pid}"
    ((index++))
  done
  # when : "pxf cluster restart" command is run
  local result="$(pxf cluster restart)"
  # then : it succeeds and prints the expected message
  assert_equals "${expected_restart_message}" "${result}" "pxf cluster restart should succeed"
  index=0
  for host in "${all_cluster_hosts[@]}"; do
    #    : AND PXF is running
    local running_pid="$(list_remote_pxf_running_pid ${host})"
    echo "running pid=${running_pid}"
    assert_not_empty "${running_pid}" "PXF should be running on host ${host}"
    #    : AND the process pid file exists
    local file_pid="$(cat_remote_file ${host} "${PXF_BASE_DIR}"/run/pxf-app.pid)"
    echo "file pid=${file_pid}"
    assert_not_empty "${file_pid}" "PXF pid file should exist on host ${host}"
    #    : AND the pids match
    assert_equals "${running_pid}" "${file_pid}" "pid files should match on host ${host}"
    #    : AND the process pid file content is different from the previous one
    assert_not_equals "${old_pids[${index}]}" "${running_pid}" "PXF pid should change after restart on host ${host}"
    ((index++))
  done
}
run_test test_restart_succeeds_all_running "pxf cluster restart (all running) should succeed"
# ============================================================================================================

# === Test "pxf cluster restart (none running)" ==============================================================
expected_restart_message=\
"Restarting PXF on ${cluster_description}
PXF restarted successfully on ${num_hosts} out of ${num_hosts} hosts"
test_restart_succeeds_none_running() {
  # given:
  for host in "${all_cluster_hosts[@]}"; do
    #    : PXF is not running
    ssh "${host}" "${PXF_BASE_OPTION}${PXF_HOME}/bin/pxf stop"
    assert_empty "$(list_remote_pxf_running_pid ${host})" "PXF should not be running on host ${host}"
    #    : AND the process pid file does not exist
    local file_pid="$(cat_remote_file ${host} "${PXF_BASE_DIR}"/run/pxf-app.pid)"
    assert_empty "${file_pid}" "PXF pid file should not exist on host ${host}"
  done
  # when : "pxf cluster restart" command is run
  local result="$(pxf cluster restart)"
  # then : it succeeds and prints the expected message
  assert_equals "${expected_restart_message}" "${result}" "pxf cluster restart should succeed"
  for host in "${all_cluster_hosts[@]}"; do
    #    : AND PXF is running
    local running_pid="$(list_remote_pxf_running_pid ${host})"
    echo "running pid=${running_pid}"
    assert_not_empty "${running_pid}" "PXF should be running on host ${host}"
    #    : AND the process pid file exists
    local file_pid="$(cat_remote_file ${host} "${PXF_BASE_DIR}"/run/pxf-app.pid)"
    echo "file pid=${file_pid}"
    assert_not_empty "${file_pid}" "PXF pid file should exist on host ${host}"
    #    : AND the pids match
    assert_equals "${running_pid}" "${file_pid}" "pid files should match on host ${host}"
  done
}
run_test test_restart_succeeds_none_running "pxf cluster restart (none running) should succeed"
# ============================================================================================================

# === Test "pxf cluster restart (one running)" ===============================================================
expected_restart_message=\
"Restarting PXF on ${cluster_description}
PXF restarted successfully on ${num_hosts} out of ${num_hosts} hosts"
test_restart_succeeds_one_running() {
  # given: PXF is not running on coordinator, standby coordinator or segment host 2
  for host in "${all_cluster_hosts[@]}"; do
    [[ $host == sdw1 ]] && continue

    ssh ${host} "${PXF_BASE_OPTION}${PXF_HOME}/bin/pxf stop"
    assert_empty "$(list_remote_pxf_running_pid ${host})" "PXF should not be running on host ${host}"
  done
  #      : AND PXF is running on segment host 1
  local sdw1_pid="$(list_remote_pxf_running_pid sdw1)"
  assert_not_empty "${sdw1_pid}" "PXF should be running on host sdw1"
  # when : "pxf cluster restart" command is run
  local result="$(pxf cluster restart)"
  # then : it succeeds and prints the expected message
  assert_equals "${expected_restart_message}" "${result}" "pxf cluster restart should succeed"
  for host in "${all_cluster_hosts[@]}"; do
    #    : AND PXF is running
    local running_pid="$(list_remote_pxf_running_pid ${host})"
    echo "running pid=${running_pid}"
    assert_not_empty "${running_pid}" "PXF should be running on host ${host}"
    #    : AND the process pid file exists
    local file_pid="$(cat_remote_file ${host} "${PXF_BASE_DIR}"/run/pxf-app.pid)"
    echo "file pid=${file_pid}"
    assert_not_empty "${file_pid}" "PXF pid file should exist on host ${host}"
    #    : AND the pids match
    assert_equals "${running_pid}" "${file_pid}" "pid files should match on host ${host}"
    #    : AND the pid on sdw1 should not be the same (since restart is doing stop first)
    [[ $host == sdw1 ]] && assert_not_equals "${sdw1_pid}" "${running_pid}" "pid should change on host sdw1"
  done
}
run_test test_restart_succeeds_one_running "pxf cluster restart (one running) should succeed"
# ============================================================================================================

# === Test "pxf cluster start (one running)" =================================================================
expected_start_message=\
"Starting PXF on ${cluster_description}
PXF started successfully on ${num_hosts} out of ${num_hosts} hosts"
test_start_succeeds_one_running() {
  # given: PXF is running on segment host 1
  local sdw1_pid="$(list_remote_pxf_running_pid sdw1)"
  assert_not_empty "${sdw1_pid}" "PXF should be running on host sdw1"
  #      : AND PXF is not running on coordinator, standby coordinator or segment host 2
  for host in "${all_cluster_hosts[@]}"; do
    [[ $host == sdw1 ]] && continue

    ssh ${host} "${PXF_BASE_OPTION}${PXF_HOME}/bin/pxf stop"
    assert_empty "$(list_remote_pxf_running_pid ${host})" "PXF should not be running on host ${host}"
  done
  # when : "pxf cluster start" command is run
  local result="$(pxf cluster start)"
  # then : it succeeds and prints the expected message
  assert_equals "${expected_start_message}" "${result}" "pxf cluster start should succeed"
  for host in "${all_cluster_hosts[@]}"; do
    #    : AND PXF is running
    local running_pid="$(list_remote_pxf_running_pid ${host})"
    echo "running pid=${running_pid}"
    assert_not_empty "${running_pid}" "PXF should be running on host ${host}"
    #    : AND the process pid file exists
    local file_pid="$(cat_remote_file ${host} "${PXF_BASE_DIR}"/run/pxf-app.pid)"
    echo "file pid=${file_pid}"
    assert_not_empty "${file_pid}" "PXF pid file should exist on host ${host}"
    #    : AND the pids match
    assert_equals "${running_pid}" "${file_pid}" "pid files should match on host ${host}"
    #    : AND the pid on sdw1 should still be the same
    [[ $host == sdw1 ]] && assert_equals "${sdw1_pid}" "${running_pid}" "pid should not change on host sdw1"
  done
}
run_test test_start_succeeds_one_running "pxf cluster start (one running) should succeed"
# ============================================================================================================

# === Test "pxf cluster status (one running)" ================================================================
expected_status_message=\
"Checking status of PXF servers on ${cluster_description}\n\
ERROR: PXF is not running on $((num_hosts-1)) out of ${num_hosts} hosts\n\
cdw ==> ${yellow}Checking if PXF is up and running...${reset}\n\
${red}ERROR: PXF is down - the application is not running${reset}...\n"

if has_standby_coordinator; then
  expected_status_message+="scdw ==> ${yellow}Checking if PXF is up and running...${reset}\n${red}ERROR: PXF is down - the application is not running${reset}...\n"
fi

expected_status_message+=\
"sdw2 ==> ${yellow}Checking if PXF is up and running...${reset}\n\
${red}ERROR: PXF is down - the application is not running${reset}..."
test_status_succeeds_one_running() {
  # given: PXF is running on segment host 1
  local sdw1_pid="$(list_remote_pxf_running_pid sdw1)"
  assert_not_empty "${sdw1_pid}" "PXF should be running on host sdw1"
  #      : AND PXF is not running on other cluster hosts
  for host in "${all_cluster_hosts[@]}"; do
    [[ $host == sdw1 ]] && continue

    ssh ${host} "${PXF_BASE_OPTION}${PXF_HOME}/bin/pxf stop"
    assert_empty "$(list_remote_pxf_running_pid ${host})" "PXF should not be running on host ${host}"
  done
  # when : "pxf cluster status" command is run
  local result="$(pxf cluster status 2>&1)"
  # then : it succeeds and prints the expected message
  assert_equals "$(echo -e "${expected_status_message}")" "${result}" "pxf cluster status should succeed"
}
run_test test_status_succeeds_one_running "pxf cluster status (one running) should succeed"
# ===========================================================================================================

# === Test "pxf cluster stop (one running)" =================================================================
expected_stop_message=\
"Stopping PXF on ${cluster_description}
PXF stopped successfully on ${num_hosts} out of ${num_hosts} hosts"
test_stop_succeeds_one_running() {
  # given: PXF is not running on coordinator, standby coordinator or segment host 2
  for host in "${all_cluster_hosts[@]}"; do
    [[ ${host} == sdw1 ]] && continue

    ssh ${host} "${PXF_BASE_OPTION}${PXF_HOME}/bin/pxf stop"
    assert_empty "$(list_remote_pxf_running_pid ${host})" "PXF should not be running on host ${host}"
  done
  #      : AND PXF is running on segment host 1
  local sdw1_pid="$(list_remote_pxf_running_pid sdw1)"
  assert_not_empty "${sdw1_pid}" "PXF should be running on host sdw1"
  # when : "pxf cluster stop" command is run
  local result="$(pxf cluster stop)"
  # then : it succeeds and prints the expected message
  assert_equals "${expected_stop_message}" "${result}" "pxf cluster stop should succeed"
  for host in "${all_cluster_hosts[@]}"; do
    #    : AND there are no PXF processes left running
    local running_pid="$(list_remote_pxf_running_pid ${host})"
    assert_empty "${running_pid}" "PXF should not be running on host ${host}"
    #    : AND the process pid file does not exist
    local pid_file="$(list_remote_file ${host} "${PXF_BASE_DIR}"/run/pxf-app.pid)"
    assert_empty "${pid_file}" "PXF pid file should not exist on host ${host}"
  done
}
run_test test_stop_succeeds_one_running "pxf cluster stop (one running) should succeed"
# ============================================================================================================

exit_with_err "${BASH_SOURCE[0]}"
