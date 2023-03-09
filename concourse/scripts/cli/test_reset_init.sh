#!/usr/bin/env bash

dir=$( cd "$(dirname "${BASH_SOURCE[0]}")" && pwd )
[[ -e ${dir}/common.sh ]] || exit 1
source "${dir}/common.sh"

expected_extension_file="${GPHOME}/share/postgresql/extension/pxf.control"

cluster_description="$(get_cluster_description)"
num_hosts="${#all_cluster_hosts[@]}"

list_extension_files() {
	local usage='list_extension_files <host>' host=${1:?${usage}}
	ssh "${host}" "
		[[ -f "${GPHOME}/share/postgresql/extension/pxf.control" ]] && ls "${GPHOME}/share/postgresql/extension/pxf.control"
		[[ -f "${GPHOME}/share/postgresql/extension/pxf--1.0.sql" ]] && ls "${GPHOME}/share/postgresql/extension/pxf--1.0.sql"
		[[ -f "${GPHOME}/lib/postgresql/pxf.so" ]] && ls "${GPHOME}/lib/postgresql/pxf.so"
	"
}

remove_extension_files() {
	local usage='remove_extension_files <host>' host=${1:?${usage}}
	ssh "${host}" "
		rm -f ${GPHOME}/share/postgresql/extension/pxf.control
		rm -f ${GPHOME}/share/postgresql/extension/pxf--1.0.sql
		rm -f ${GPHOME}/lib/postgresql/pxf.so
	"
}

# === Test "pxf cluster reset" - does nothing on the remote nodes, PXF can be running, no state changes ======
successful_reset_message=\
"*****************************************************************************
* DEPRECATION NOTICE:
* The \"pxf cluster reset\" command is deprecated and will be removed
* in a future release of PXF.
*****************************************************************************

Resetting PXF on ${cluster_description}
PXF has been reset on ${num_hosts} out of ${num_hosts} hosts"
test_reset_succeeds() {
  # given: <nothing>
  # when : "pxf cluster reset" command is run
  local result="$(pxf cluster reset)"
  # then : it succeeds and prints the expected message
  assert_equals "${successful_reset_message}" "${result}" "pxf cluster reset should succeed"
}
run_test test_reset_succeeds "pxf cluster reset should succeed"
# ============================================================================================================

# === Test "pxf cluster init" - registers PXF extension into GPHOME, where extension does not yet exist ======
successful_init_message=\
"*****************************************************************************
* DEPRECATION NOTICE:
* The \"pxf cluster init\" command is deprecated and will be removed
* in a future release of PXF.
*
* Use the \"pxf cluster register\" command instead.
*
*****************************************************************************

Initializing PXF on ${cluster_description}
PXF initialized successfully on ${num_hosts} out of ${num_hosts} hosts"
test_init_no_extension_succeeds() {
  # given: extension files do not exist under GPHOME
  for host in "${all_cluster_hosts[@]}"; do
    remove_extension_files "${host}"
    assert_empty "$(list_extension_files ${host})" "extension files should NOT exist on host ${host}"
  done
  # when : "pxf cluster init" command is run
  local result="$(pxf cluster init)"
  # then : it succeeds and prints the expected message
  assert_equals "${successful_init_message}" "${result}" "pxf cluster init (no extension) should succeed"
  #      : AND the extension file is copied to GPHOME
  for host in "${all_cluster_hosts[@]}"; do
    assert_equals "${expected_extension_file}" "$(list_extension_files ${host})" "extension file should exist on host ${host}"
  done
}
run_test test_init_no_extension_succeeds "pxf cluster init (no extension) should succeed"
# ============================================================================================================

# === Test "pxf cluster init" - registers PXF extension into GPHOME, where extension already exist ======
successful_init_message=\
"*****************************************************************************
* DEPRECATION NOTICE:
* The \"pxf cluster init\" command is deprecated and will be removed
* in a future release of PXF.
*
* Use the \"pxf cluster register\" command instead.
*
*****************************************************************************

Initializing PXF on ${cluster_description}
PXF initialized successfully on ${num_hosts} out of ${num_hosts} hosts"

control_file_content=\
"directory = '/usr/local/pxf-gp6/gpextable/'
default_version = '2.1'
comment = 'Extension which allows to access unmanaged data'
module_pathname = '/usr/local/pxf-gp6/gpextable/pxf'
superuser = true
relocatable = false
schema = public"

test_init_extension_exists_succeeds() {
  # given: extension file exist under GPHOME
  for host in "${all_cluster_hosts[@]}"; do
    ssh "${host}" "echo 'placeholder' > ${GPHOME}/share/postgresql/extension/pxf.control"
    assert_equals "placeholder" "$(ssh ${host} cat ${GPHOME}/share/postgresql/extension/pxf.control)" "extension file should have placeholder content on host ${host}"
  done
  # when : "pxf cluster init" command is run
  local result="$(pxf cluster init)"
  # then : it succeeds and prints the expected message
  assert_equals "${successful_init_message}" "${result}" "pxf cluster init (extension exists) should succeed"
  #      : AND the extension file overrides existing one in GPHOME
  for host in "${all_cluster_hosts[@]}"; do
    assert_equals "${control_file_content}" "$(ssh ${host} cat ${GPHOME}/share/postgresql/extension/pxf.control)" "extension file should have valid content on host ${host}"
  done
}
run_test test_init_extension_exists_succeeds "pxf cluster init (extension exists) should succeed"
# ============================================================================================================

#
exit_with_err "${BASH_SOURCE[0]}"
