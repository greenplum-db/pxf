#!/usr/bin/env bash

dir=$( cd "$(dirname "${BASH_SOURCE[0]}")" && pwd )
[[ -e ${dir}/common.sh ]] || exit 1
source "${dir}/common.sh"

# === Test "pxf cluster stop" ================================================================================
expected_stop_message=\
"Stopping PXF on 2 segment hosts...
PXF stopped successfully on 2 out of 2 hosts"
test_stop_succeeds() {
  # given:
  for host in sdw{1,2}; do
    #    : pxf is running
    local running_pid="$(list_remote_pxf_running_pid ${host})"
    echo "running pid=${running_pid}"
    assert_not_empty "${running_pid}" "PXF should be running on host ${host}"
    #    :AND the process pid file exists
    local file_pid="$(cat_remote_file ${host} "${PXF_BASE_DIR}"/run/pxf-app.pid)"
    echo "file pid=${file_pid}"
    assert_not_empty "${file_pid}" "PXF pid file should exist on host ${host}"
    #    :AND the pids match
    assert_equals "${running_pid}" "${file_pid}" "pid files should match on host ${host}"
  done
  # when : "pxf cluster stop" command is run
  local result="$(pxf cluster stop)"
  # then : it succeeds and prints the expected message
  assert_equals "${expected_stop_message}" "${result}" "pxf cluster stop should succeed"
  for host in sdw{1,2}; do
    #    : AND there are no PXF processes left running
    local running_pid="$(list_remote_pxf_running_pid ${host})"
    assert_empty "${running_pid}" "PXF should be running on host ${host}"
    #    :AND the process pid file no longer exists
    local pid_file="$(list_remote_file ${host} "${PXF_BASE_DIR}"/run/pxf-app.pid)"
    assert_empty "${pid_file}" "PXF pid file should not exist on host ${host}"
  done
}
run_test test_stop_succeeds "pxf cluster stop should succeed"
# ============================================================================================================

# === Test "pxf cluster stop" ================================================================================
expected_start_message=\
"Starting PXF on 2 segment hosts...
PXF started successfully on 2 out of 2 hosts"
test_start_succeeds() {
  # given: there are no PXF processes running TODO
  #      : AND the process pid file does not exists TODO
  # when : "pxf cluster start" command is run
  local result="$(pxf cluster start)"
  # then : it succeeds and prints the expected message
  assert_equals "${expected_start_message}" "${result}" "pxf cluster start should succeed"
  #      : AND there are PXF processes running TODO
  #      : AND the process pid file is created TODO
}
run_test test_start_succeeds "pxf cluster start should succeed"
# ============================================================================================================

# === Test "pxf cluster restart" ================================================================================
expected_restart_message=\
"Restarting PXF on 2 segment hosts...
PXF restarted successfully on 2 out of 2 hosts"
test_restart_succeeds() {
  # given: pxf is running TODO
  #      : AND the process pid file exists TODO
  # when : "pxf cluster restart" command is run
  local result="$(pxf cluster restart)"
  # then : it succeeds and prints the expected message
  assert_equals "${expected_restart_message}" "${result}" "pxf cluster restart should succeed"
  #      : AND there are PXF processes running TODO
  #      : AND the process pid file content is different from the previous one TODO
}
run_test test_start_succeeds "pxf cluster restart should succeed"
# ============================================================================================================

compare "Tomcat stopped." "$(ssh sdw1 ${PXF_HOME}/bin/pxf stop)" "pxf stop on sdw1 should succeed"
sdw2_pid=$(ssh sdw2 "cat ${PXF_HOME}/run/catalina.pid")
expected_start_message="Starting PXF on 2 segment hosts...
ERROR: PXF failed to start on 1 out of 2 hosts
sdw2 ==> Existing PID file found during start.
Tomcat appears to still be running with PID ${sdw2_pid}. Start aborted...."
compare "${expected_start_message}" "$(pxf cluster start 2>&1)" "pxf cluster start should fail on sdw2"

compare "Tomcat stopped." "$(ssh sdw1 ${PXF_HOME}/bin/pxf stop)" "pxf stop on sdw1 should succeed"
expected_stop_message="Stopping PXF on 2 segment hosts...
ERROR: PXF failed to stop on 1 out of 2 hosts
sdw1 ==> \$CATALINA_PID was set but the specified file does not exist. Is Tomcat running? Stop aborted."
compare "${expected_stop_message}" "$(pxf cluster stop 2>&1)" "pxf cluster stop should fail on sdw1"

compare "${expected_restart_message}" "$(pxf cluster restart)" "pxf cluster restart should succeed"

expected_status_message="Checking status of PXF servers on 2 segment hosts...
PXF is running on 2 out of 2 hosts"
compare "${expected_status_message}" "$(pxf cluster status)" "pxf cluster status should succeed"

compare "Tomcat stopped." "$(ssh sdw1 ${PXF_HOME}/bin/pxf stop)" "pxf stop on sdw1 should succeed"
expected_status_message="Checking status of PXF servers on 2 segment hosts...
ERROR: PXF is not running on 1 out of 2 hosts
sdw1 ==> Checking if tomcat is up and running...
ERROR: PXF is down - tomcat is not running..."
compare "${expected_status_message}" "$(pxf cluster status 2>&1)" "pxf cluster status should fail on sdw1"

expected_start_message="Tomcat started.
Checking if tomcat is up and running...
Server: PXF Server
Checking if PXF webapp is up and running...
PXF webapp is listening on port 5888"
compare "${expected_start_message}" "$(ssh sdw1 ${PXF_HOME}/bin/pxf start)" "pxf start on sdw1 should succeed"

expected_status_message="Checking status of PXF servers on 2 segment hosts...
PXF is running on 2 out of 2 hosts"
compare "${expected_status_message}" "$(pxf cluster status)" "pxf cluster status should succeed"

exit_with_err "${BASH_SOURCE[0]}"
