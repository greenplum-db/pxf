#!/usr/bin/env bash

SCRIPT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

cluster_env_files=$( cd "${SCRIPT_DIR}/../../../cluster_env_files" && pwd )

cp -r "${cluster_env_files}/.ssh" "${HOME}"

scp "${SCRIPT_DIR}/cli/common.sh" cdw:

err_cnt=0
for script in "${SCRIPT_DIR}/cli/"test_*.sh; do
	scp "${script}" cdw:
	script_short_name=${script##*/} # chop off path to script
	ssh cdw "source ${GPHOME}/greenplum_path.sh && ~gpadmin/${script_short_name}"
	((err_cnt+=$?))
done

exit "${err_cnt}"
