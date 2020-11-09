#!/usr/bin/env bash

dir=$( cd "$(dirname "${BASH_SOURCE[0]}")" && pwd )
[[ -e ${dir}/common.sh ]] || exit 1
source "${dir}/common.sh"

expected_start_message="Starting PXF on 2 segment hosts...
PXF started successfully on 2 out of 2 hosts"
expected_stop_message="Stopping PXF on 2 segment hosts...
PXF stopped successfully on 2 out of 2 hosts"
expected_restart_message="Restarting PXF on 2 segment hosts...
PXF restarted successfully on 2 out of 2 hosts"

compare "${expected_stop_message}" "$(pxf cluster stop)" "pxf cluster stop should succeed"
compare "${expected_start_message}" "$(pxf cluster start)" "pxf cluster start should succeed"
compare "${expected_restart_message}" "$(pxf cluster restart)" "pxf cluster restart should succeed"

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
