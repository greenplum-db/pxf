#!/bin/bash

CWDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source "${CWDIR}/pxf_common.bash"

# we need word boundary in case of standby coordinator (scdw)
PXF_HOME=/usr/local/pxf-gp${GP_VER}
PXF_BASE_DIR=${PXF_BASE_DIR:-$PXF_HOME}

# ANSI Colors
echoRed() { echo $'\e[0;31m'"$1"$'\e[0m'; }
echoGreen() { echo $'\e[0;32m'"$1"$'\e[0m'; }

function upgrade_pxf() {
	existing_pxf_version=$(cat $PXF_HOME/version)
	echoGreen "Stopping PXF ${existing_pxf_version}"
	su gpadmin -c "${PXF_HOME}/bin/pxf version && ${PXF_HOME}/bin/pxf cluster stop"

	echoGreen "Installing Newer Version of PXF 6"
	install_pxf_tarball

	echoGreen "Check the PXF 6 version"
	su gpadmin -c "${PXF_HOME}/bin/pxf version"

	echoGreen "Register the PXF extension into Greenplum"
	su gpadmin -c "GPHOME=${GPHOME} ${PXF_HOME}/bin/pxf cluster register"

	if [[ "${PXF_BASE_DIR}" != "${PXF_HOME}" ]]; then
		echoGreen "Prepare PXF in ${PXF_BASE_DIR}"
		PXF_BASE=${PXF_BASE_DIR} ${PXF_HOME}/bin/pxf cluster prepare
		echo \"export PXF_BASE=${PXF_BASE_DIR}\" >> ~gpadmin/.bashrc
	fi
	updated_pxf_version=$(cat $PXF_HOME/version)

	echoGreen "Starting PXF ${updated_pxf_version}"

	if [[ ${existing_pxf_version} > ${updated_pxf_version} ]]; then
		echoRed "Existing version of PXF (${existing_pxf_version}) is greater than or equal to the new version (${updated_pxf_version})"
	fi

	su gpadmin -c "PXF_BASE=${PXF_BASE_DIR} ${PXF_HOME}/bin/pxf cluster start"

  # the new version of PXF brought in a new version of the extension. For databases that already had PXF installed,
  # we need to explicitly upgrade the PXF extension to the new version
	echoGreen "ALTER EXTENSION pxf UPDATE - for multibyte delimiter tests"
	su gpadmin -c "source ${GPHOME}/greenplum_path.sh && psql -d template1 -c 'ALTER EXTENSION pxf UPDATE' \
																							&& psql -d pxfautomation -c 'ALTER EXTENSION pxf UPDATE' \
																							&& psql -d pxfautomation_encoding -c 'SELECT * FROM pg_extension'"
}
