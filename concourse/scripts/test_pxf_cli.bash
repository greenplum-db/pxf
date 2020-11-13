#!/usr/bin/env bash

SCRIPT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

cluster_env_files=$( cd "${SCRIPT_DIR}/../../../cluster_env_files" && pwd )

cp -r "${cluster_env_files}/.ssh" "${HOME}"

scp "${SCRIPT_DIR}/cli/common.sh" mdw:

if [[ -n "${PXF_BASE_DIR}" ]]; then
  PXF_BASE_DIR_OPT="PXF_BASE_DIR=${PXF_BASE_DIR} "
fi

err_cnt=0
for script in "${SCRIPT_DIR}/cli/"test_*.sh; do
	scp "${script}" mdw:
	script_short_name=${script##*/} # chop off path to script
	ssh mdw "${PXF_BASE_DIR_OPT}~gpadmin/${script_short_name}"
	((err_cnt+=$?))
done

exit "${err_cnt}"
