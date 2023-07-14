#!/usr/bin/env bash

dir=$( cd "$(dirname "${BASH_SOURCE[0]}")" && pwd )
[[ -e ${dir}/common.sh ]] || exit 1
source "${dir}/common.sh"

cluster_description="$(get_cluster_description)"
cluster_sync_description="$(get_cluster_sync_description)"
num_hosts="${#all_cluster_hosts[@]}"
get_host_from_version="$(get_host_from_version)"

list_cluster_configs() {
	for host in "${all_cluster_hosts[@]}"; do
		echo ${host}:
		list_remote_configs "${host}"
	done
}


# === Test "pxf cluster sync " ===============================================================================
expected_sync_message=\
"Syncing PXF configuration files from ${cluster_sync_description}
PXF configs synced successfully on $((num_hosts-1)) out of $((num_hosts-1)) hosts"

expected_cluster_configs=\
"cdw:
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

if has_standby_coordinator; then
  printf -v expected_cluster_configs "%s\nscdw:\n1\n2\n3\n${PXF_BASE_DIR}/conf/foo.jar" "${expected_cluster_configs}"
fi

test_sync_succeeds() {
  # given:
  for host in "${all_cluster_hosts[@]}"; do
    [[ $host == cdw ]] && continue
    #    : files do not exist on remote hosts
    remove_remote_configs "${host}"
    assert_empty "$(list_remote_configs ${host})" "config files should not exist on host ${host}"
  done
  #      : AND new files are created on coordinator host
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
"Syncing PXF configuration files from ${cluster_sync_description}
PXF configs synced successfully on $((num_hosts-1)) out of $((num_hosts-1)) hosts"

expected_cluster_configs=\
"cdw:
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

if has_standby_coordinator; then
  printf -v expected_cluster_configs "%s\nscdw:\n1\n2\n3\n${PXF_BASE_DIR}/conf/foo.jar" "${expected_cluster_configs}"
fi

test_sync_succeeds_no_delete() {
  # given:
  for host in "${all_cluster_hosts[@]}"; do
    [[ $host == cdw ]] && continue
    #    : files exist on remote hosts
    assert_equals "${PXF_BASE_DIR}/conf/foo.jar" "$(list_remote_file ${host} "${PXF_BASE_DIR}/conf/foo.jar")" "foo.jar should exist on host ${host}"
  done
  #      : AND the file is removed from the coordinator host
  rm -f "${PXF_BASE_DIR}/conf/foo.jar"
  # when : "pxf cluster sync" command is run
  local result="$(pxf cluster sync)"
  # then : it succeeds and prints the expected message
  assert_equals "${expected_sync_message}" "${result}" "pxf cluster sync should succeed"
  #      : AND foo.jar should still exist on remote hosts
  assert_equals "${expected_cluster_configs}" "$(list_cluster_configs)" "foo.jar should be missing from cdw"
}
run_test test_sync_succeeds_no_delete "pxf cluster sync (no delete) should succeed"
# ============================================================================================================

# === Test "pxf cluster sync (delete one host)" ==============================================================
expected_cluster_configs=\
"cdw:
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

if has_standby_coordinator; then
  printf -v expected_cluster_configs "%s\nscdw:\n1\n2\n3\n${PXF_BASE_DIR}/conf/foo.jar" "${expected_cluster_configs}"
fi

test_sync_succeeds_delete_one_host() {
  # given:
  for host in "${all_cluster_hosts[@]}"; do
    [[ $host == cdw ]] && continue
    #    : files exist on remote hosts
    assert_equals "${PXF_BASE_DIR}/conf/foo.jar" "$(list_remote_file ${host} "${PXF_BASE_DIR}/conf/foo.jar")" "foo.jar should exist on host ${host}"
  done
  #      : AND the file is removed from the coordinator host
  rm -f "${PXF_BASE_DIR}/conf/foo.jar"
  # when : "pxf sync --delete" command is run for one host
  local result1="$(pxf sync --delete sdw1)"
  #      : AND "pxf sync" command is run for another host
  local result2="$(pxf sync sdw2)"
  # then : they should succeed
  assert_empty "${result1}" "pxf sync --delete sdw1 should succeed"
  assert_empty "${result2}" "pxf sync sdw2 should succeed"
  #      : AND foo.jar should be removed from sdw1 only
  assert_equals "${expected_cluster_configs}" "$(list_cluster_configs)" "foo.jar should be missing from cdw and sdw1"
}
run_test test_sync_succeeds_delete_one_host "pxf cluster sync (delete one host) should succeed"
# ============================================================================================================

# === Test "pxf cluster sync (no delete server conf)" ========================================================
expected_sync_message=\
"Syncing PXF configuration files from ${cluster_sync_description}
PXF configs synced successfully on $((num_hosts-1)) out of $((num_hosts-1)) hosts"
expected_cluster_configs=\
"cdw:
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

if has_standby_coordinator; then
  printf -v expected_cluster_configs "%s\nscdw:\n1\n2\n3\n${PXF_BASE_DIR}/conf/foo.jar" "${expected_cluster_configs}"
fi

test_sync_succeeds_no_delete_server_conf() {
  # given:
  for host in "${all_cluster_hosts[@]}"; do
    [[ $host == cdw ]] && continue
    #    : files exist on remote hosts
    assert_equals "${PXF_BASE_DIR}/servers/foo/1" "$(list_remote_file ${host} "${PXF_BASE_DIR}/servers/foo/1")" "servers/foo/1 should exist on host ${host}"
  done
  #      : AND the file is removed from the coordinator host
  rm -f "${PXF_BASE_DIR}/servers/foo/1"
  # when : "pxf cluster sync" command is run
  local result="$(pxf cluster sync)"
  # then : it succeeds and prints the expected message
  assert_equals "${expected_sync_message}" "${result}" "pxf cluster sync should succeed"
  #      : AND servers/foo/1 should still exist on remote hosts
  assert_equals "${expected_cluster_configs}" "$(list_cluster_configs)" "servers/foo/1 should be missing from cdw"
}
run_test test_sync_succeeds_no_delete_server_conf "pxf cluster sync (no delete server conf) should succeed"
# ============================================================================================================

# === Test "pxf cluster sync (delete server conf)" ===========================================================
expected_sync_message=\
"Syncing PXF configuration files from ${cluster_sync_description}
PXF configs synced successfully on $((num_hosts-1)) out of $((num_hosts-1)) hosts"
expected_cluster_configs=\
"cdw:
2
3
sdw1:
2
3
sdw2:
2
3"

if has_standby_coordinator; then
  printf -v expected_cluster_configs "%s\nscdw:\n2\n3" "${expected_cluster_configs}"
fi

test_sync_succeeds_delete_server_conf() {
  # given:
  for host in "${all_cluster_hosts[@]}"; do
    [[ $host == cdw ]] && continue
    #    : files exist on remote hosts
    assert_equals "${PXF_BASE_DIR}/servers/foo/1" "$(list_remote_file ${host} "${PXF_BASE_DIR}/servers/foo/1")" "servers/foo/1 should exist on host ${host}"
  done
  #      : AND the file is removed from the coordinator host
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
"Syncing PXF configuration files from ${cluster_sync_description}
PXF configs synced successfully on $((num_hosts-1)) out of $((num_hosts-1)) hosts"
expected_cluster_configs=\
"cdw:
sdw1:
sdw2:"

if has_standby_coordinator; then
  printf -v expected_cluster_configs "%s\nscdw:" "${expected_cluster_configs}"
fi

test_sync_succeeds_delete_server() {
  # given:
  for host in "${all_cluster_hosts[@]}"; do
    [[ $host == cdw ]] && continue
    #    : server directory exists on remote hosts
    assert_equals "${PXF_BASE_DIR}/servers/foo" "$(echo_remote_dir ${host} "${PXF_BASE_DIR}/servers/foo")" "servers/foo should exist on host ${host}"
  done
  #      : AND the server directory is removed from the coordinator host
  rm -rf "${PXF_BASE_DIR}/servers/foo"
  # when : "pxf cluster sync --delete" command is run
  local result="$(pxf cluster sync --delete)"
  # then : it succeeds and prints the expected message
  assert_equals "${expected_sync_message}" "${result}" "pxf cluster sync should succeed"
  #      : AND servers/foo should not exist on remote hosts
  assert_equals "${expected_cluster_configs}" "$(list_cluster_configs)" "servers/foo should be missing from all hosts"
}
run_test test_sync_succeeds_delete_server "pxf cluster sync (delete server) should succeed"
# ============================================================================================================

# === Test "pxf cluster sync (no standby)" ================================================================
expected_sync_message=\
"Syncing PXF configuration files from ${get_host_from_version} host to 2 segment hosts...
PXF configs synced successfully on 2 out of 2 hosts"
expected_cluster_configs=\
"cdw:
${PXF_BASE_DIR}/conf/foo.jar
sdw1:
${PXF_BASE_DIR}/conf/foo.jar
sdw2:
${PXF_BASE_DIR}/conf/foo.jar"

if has_standby_coordinator; then
  printf -v expected_cluster_configs "%s\nscdw:" "${expected_cluster_configs}"
fi

test_sync_succeeds_no_standby() {
  # given:
  for host in "${all_cluster_hosts[@]}"; do
    [[ $host == cdw ]] && continue
    #    : files do not exist on remote hosts
    remove_remote_configs "${host}"
    assert_empty "$(list_remote_configs ${host})" "config files should not exist on host ${host}"
  done
  #      : AND new file is created on coordinator host
  touch "${PXF_BASE_DIR}/conf/foo.jar"
  #      : AND the standby coordinator is no longer on scdw
  source "${GPHOME}/greenplum_path.sh"
  if has_standby_coordinator; then
    gpinitstandby -ar >/dev/null
  fi
  # when : "pxf cluster sync" command is run
  local result="$(pxf cluster sync)"
  # then : it succeeds and prints the expected message
  assert_equals "${expected_sync_message}" "${result}" "pxf cluster sync should succeed"
  #      : AND the files appear on segment hosts, but not on the former standby host
  assert_equals "${expected_cluster_configs}" "$(list_cluster_configs)" "foo.jar should be missing from scdw"
}
run_test test_sync_succeeds_no_standby "pxf cluster sync (no standby) should succeed"
# ============================================================================================================

# put standby coordinator back on scdw
if has_standby_coordinator; then
  gpinitstandby -as scdw >/dev/null
fi

exit_with_err "${BASH_SOURCE[0]}"
