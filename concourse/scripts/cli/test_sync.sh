#!/usr/bin/env bash

dir=$( cd "$(dirname "${BASH_SOURCE[0]}")" && pwd )
[[ -e ${dir}/common.sh ]] || exit 1
source "${dir}/common.sh"

list_cluster_configs() {
	for host in {s,}mdw sdw{1,2}; do
		echo ${host}:
		list_remote_configs "${host}"
	done
}


# === Test "pxf cluster sync " ===============================================================================
expected_sync_message=\
"Syncing PXF configuration files from master host to standby master host and 2 segment hosts...
PXF configs synced successfully on 3 out of 3 hosts"
expected_cluster_configs=\
"smdw:
1
2
3
${PXF_BASE_DIR}/conf/foo.jar
mdw:
1
2
3
${PXF_BASE_DIR}/conf/foo.jar
sdw1:
1
2
3
${PXF_BASE_DIR}/conf/foo.jar
sdw2:
1
2
3
${PXF_BASE_DIR}/conf/foo.jar"
test_sync_succeeds() {
  # given:
  for host in smdw sdw{1,2}; do
    #    : files do not exist on remote hosts
    remove_remote_configs "${host}"
    assert_empty "$(list_remote_configs ${host})" "config files should not exist on host ${host}"
  done
  #      : AND new files are created on master host
  rm -rf "${PXF_BASE_DIR}/servers/foo"
  rm -f  "${PXF_BASE_DIR}/conf/foo.jar"
  mkdir -p "${PXF_BASE_DIR}/servers/foo"
  touch ${PXF_BASE_DIR}/servers/foo/{1..3} "${PXF_BASE_DIR}/conf/foo.jar"
  # when : "pxf cluster sync" command is run
  local result="$(pxf cluster sync)"
  # then : it succeeds and prints the expected message
  assert_equals "${expected_sync_message}" "${result}" "pxf cluster sync should succeed"
  #      : AND files show be copied to remote hosts
  assert_equals "${expected_cluster_configs}" "$(list_cluster_configs)" "files should be copied to remote hosts"
}
run_test test_sync_succeeds "pxf cluster sync should succeed"
# ============================================================================================================

# === Test "pxf cluster sync (no delete)" ====================================================================
expected_sync_message=\
"Syncing PXF configuration files from master host to standby master host and 2 segment hosts...
PXF configs synced successfully on 3 out of 3 hosts"
expected_cluster_configs=\
"smdw:
1
2
3
${PXF_BASE_DIR}/conf/foo.jar
mdw:
1
2
3
sdw1:
1
2
3
${PXF_BASE_DIR}/conf/foo.jar
sdw2:
1
2
3
${PXF_BASE_DIR}/conf/foo.jar"
test_sync_succeeds_no_delete() {
  # given:
  for host in smdw sdw{1,2}; do
    #    : files exist on remote hosts
    assert_equals "${PXF_BASE_DIR}/conf/foo.jar" "$(list_remote_file ${host} "${PXF_BASE_DIR}/conf/foo.jar")" "foo.jar should exist on host ${host}"
  done
  #      : AND the file is removed from the master host
  rm -f "${PXF_BASE_DIR}/conf/foo.jar"
  # when : "pxf cluster sync" command is run
  local result="$(pxf cluster sync)"
  # then : it succeeds and prints the expected message
  assert_equals "${expected_sync_message}" "${result}" "pxf cluster sync should succeed"
  #      : AND foo.jar should still exist on remote hosts
  assert_equals "${expected_cluster_configs}" "$(list_cluster_configs)" "foo.jar should be missing from mdw"
}
run_test test_sync_succeeds_no_delete "pxf cluster sync (no delete) should succeed"
# ============================================================================================================

# === Test "pxf cluster sync (delete one host)" ==============================================================
expected_cluster_configs=\
"smdw:
1
2
3
${PXF_BASE_DIR}/conf/foo.jar
mdw:
1
2
3
sdw1:
1
2
3
sdw2:
1
2
3
${PXF_BASE_DIR}/conf/foo.jar"
test_sync_succeeds_delete_one_host() {
  # given:
  for host in smdw sdw{1,2}; do
    #    : files exist on remote hosts
    assert_equals "${PXF_BASE_DIR}/conf/foo.jar" "$(list_remote_file ${host} "${PXF_BASE_DIR}/conf/foo.jar")" "foo.jar should exist on host ${host}"
  done
  #      : AND the file is removed from the master host
  rm -f "${PXF_BASE_DIR}/conf/foo.jar"
  # when : "pxf sync --delete" command is run for one host
  local result1="$(pxf sync --delete sdw1)"
  #      : AND "pxf sync" command is run for another host
  local result2="$(pxf sync sdw2)"
  # then : they should succeed
  assert_empty "${result1}" "pxf sync --delete sdw1 should succeed"
  assert_empty "${result2}" "pxf sync sdw2 should succeed"
  #      : AND foo.jar should be removed from sdw1 only
  assert_equals "${expected_cluster_configs}" "$(list_cluster_configs)" "foo.jar should be missing from mdw and sdw1"
}
run_test test_sync_succeeds_delete_one_host "pxf cluster sync (delete one host) should succeed"
# ============================================================================================================

# === Test "pxf cluster sync (no delete server conf)" ========================================================
expected_sync_message=\
"Syncing PXF configuration files from master host to standby master host and 2 segment hosts...
PXF configs synced successfully on 3 out of 3 hosts"
expected_cluster_configs=\
"smdw:
1
2
3
${PXF_BASE_DIR}/conf/foo.jar
mdw:
2
3
sdw1:
1
2
3
sdw2:
1
2
3
${PXF_BASE_DIR}/conf/foo.jar"
test_sync_succeeds_no_delete_server_conf() {
  # given:
  for host in smdw sdw{1,2}; do
    #    : files exist on remote hosts
    assert_equals "${PXF_BASE_DIR}/servers/foo/1" "$(list_remote_file ${host} "${PXF_BASE_DIR}/servers/foo/1")" "servers/foo/1 should exist on host ${host}"
  done
  #      : AND the file is removed from the master host
  rm -f "${PXF_BASE_DIR}/servers/foo/1"
  # when : "pxf cluster sync" command is run
  local result="$(pxf cluster sync)"
  # then : it succeeds and prints the expected message
  assert_equals "${expected_sync_message}" "${result}" "pxf cluster sync should succeed"
  #      : AND servers/foo/1 should still exist on remote hosts
  assert_equals "${expected_cluster_configs}" "$(list_cluster_configs)" "servers/foo/1 should be missing from mdw"
}
run_test test_sync_succeeds_no_delete_server_conf "pxf cluster sync (no delete server conf) should succeed"
# ============================================================================================================

# === Test "pxf cluster sync (delete server conf)" ===========================================================
expected_sync_message=\
"Syncing PXF configuration files from master host to standby master host and 2 segment hosts...
PXF configs synced successfully on 3 out of 3 hosts"
expected_cluster_configs=\
"smdw:
2
3
mdw:
2
3
sdw1:
2
3
sdw2:
2
3"
test_sync_succeeds_delete_server_conf() {
  # given:
  for host in smdw sdw{1,2}; do
    #    : files exist on remote hosts
    assert_equals "${PXF_BASE_DIR}/servers/foo/1" "$(list_remote_file ${host} "${PXF_BASE_DIR}/servers/foo/1")" "servers/foo/1 should exist on host ${host}"
  done
  #      : AND the file is removed from the master host
  rm -f "${PXF_BASE_DIR}/servers/foo/1"
  # when : "pxf cluster sync --delete" command is run
  local result="$(pxf cluster sync --delete)"
  # then : it succeeds and prints the expected message
  assert_equals "${expected_sync_message}" "${result}" "pxf cluster sync should succeed"
  #      : AND servers/foo/1 should not exist on remote hosts
  assert_equals "${expected_cluster_configs}" "$(list_cluster_configs)" "servers/foo/1 should be missing from all hosts"
}
run_test test_sync_succeeds_delete_server_conf "pxf cluster sync (delete server conf) should succeed"
# ============================================================================================================

# === Test "pxf cluster sync (delete server)" ================================================================
expected_sync_message=\
"Syncing PXF configuration files from master host to standby master host and 2 segment hosts...
PXF configs synced successfully on 3 out of 3 hosts"
expected_cluster_configs=\
"smdw:
mdw:
sdw1:
sdw2:"
test_sync_succeeds_delete_server_conf() {
  # given:
  for host in smdw sdw{1,2}; do
    #    : server directory exists on remote hosts
    assert_equals "${PXF_BASE_DIR}/servers/foo" "$(echo_remote_dir ${host} "${PXF_BASE_DIR}/servers/foo")" "servers/foo should exist on host ${host}"
  done
  #      : AND the server directory is removed from the master host
  rm -rf "${PXF_BASE_DIR}/servers/foo"
  # when : "pxf cluster sync --delete" command is run
  local result="$(pxf cluster sync --delete)"
  # then : it succeeds and prints the expected message
  assert_equals "${expected_sync_message}" "${result}" "pxf cluster sync should succeed"
  #      : AND servers/foo should not exist on remote hosts
  assert_equals "${expected_cluster_configs}" "$(list_cluster_configs)" "servers/foo should be missing from all hosts"
}
run_test test_sync_succeeds_delete_server_conf "pxf cluster sync (delete server conf) should succeed"
# ============================================================================================================

# put standby master on sdw1
source "${GPHOME}/greenplum_path.sh"
gpinitstandby -ar >/dev/null
ssh sdw1 'mkdir /data/gpdata/master'
gpinitstandby -as sdw1 >/dev/null

# files should not sync to smdw anymore
expected_cluster_output="Syncing PXF configuration files from master host to 2 segment hosts...
PXF configs synced successfully on 2 out of 2 hosts"
touch ${PXF_CONF}/conf/foo.jar
compare "${expected_cluster_output}" "$(pxf cluster sync)" "pxf cluster sync should succeed"
expected="smdw:
mdw:
${PXF_CONF}/conf/foo.jar
sdw1:
${PXF_CONF}/conf/foo.jar
sdw2:
${PXF_CONF}/conf/foo.jar"
compare "${expected}" "$(get_status)" "all nodes but smdw should have ${PXF_CONF}/conf/foo.jar"

# put standby master back on smdw
gpinitstandby -ar >/dev/null
gpinitstandby -as smdw >/dev/null

exit_with_err "${BASH_SOURCE[0]}"
