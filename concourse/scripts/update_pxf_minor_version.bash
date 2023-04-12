#!/bin/bash

set -euxo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

GPHOME=/usr/local/greenplum-db-devel

# we need word boundary in case of standby coordinator (scdw)
COORDINATOR_HOSTNAME="cdw"
PXF_HOME=/usr/local/pxf-gp${GP_VER}
PXF_BASE_DIR=${PXF_BASE_DIR:-$PXF_HOME}

# ANSI Colors
echoRed() { echo $'\e[0;31m'"$1"$'\e[0m'; }
echoGreen() { echo $'\e[0;32m'"$1"$'\e[0m'; }

function upgrade_pxf() {
	su gpadmin -c

  existing_pxf_version=$(cat $PXF_HOME/version)
	echoGreen "Stopping PXF ${existing_pxf_version}"
	${PXF_HOME}/bin/pxf version && ${PXF_HOME}/bin/pxf cluster stop

	echoGreen "Installing Newer Version of PXF 6"
	source ${GPHOME}/greenplum_path.sh &&
	export JAVA_HOME=/usr/lib/jvm/jre &&
	gpscp -f ~gpadmin/hostfile_all -v -u centos -r ~/pxf_tarball centos@=: &&
	gpssh -f ~gpadmin/hostfile_all -v -u centos -s -e 'tar -xzf ~centos/pxf_tarball/pxf-*.tar.gz -C /tmp' &&
	gpssh -f ~gpadmin/hostfile_all -v -u centos -s -e 'sudo GPHOME=${GPHOME} /tmp/pxf*/install_component'


	echoGreen "Change ownership of PXF 6 directory to gpadmin"
	ssh "${COORDINATOR_HOSTNAME}" "source ${GPHOME}/greenplum_path.sh && gpssh -f ~gpadmin/hostfile_all -v -u centos -s -e 'sudo chown -R gpadmin:gpadmin ${PXF_HOME}'"

	echoGreen "Check the PXF 6 version"
	ssh "${COORDINATOR_HOSTNAME}" "${PXF_HOME}/bin/pxf version"

	echoGreen "Register the PXF extension into Greenplum"
	ssh "${COORDINATOR_HOSTNAME}" "GPHOME=${GPHOME} ${PXF_HOME}/bin/pxf cluster register"

	if [[ "${PXF_BASE_DIR}" != "${PXF_HOME}" ]]; then
		echoGreen "Prepare PXF in ${PXF_BASE_DIR}"
		PXF_BASE=${PXF_BASE_DIR} ${PXF_HOME}/bin/pxf cluster prepare
		echo \"export PXF_BASE=${PXF_BASE_DIR}\" >> ~gpadmin/.bashrcz
	fi
  updated_pxf_version=$(cat $PXF_HOME/version)

	echoGreen "Starting PXF ${updated_pxf_version}"

	if [[ ${existing_pxf_version} >= ${updated_pxf_version} ]]; then
	  echoRed "Existing version of PXF (${existing_pxf_version}) is greater than or equal to the new version (${updated_pxf_version})"
	fi

	PXF_BASE=${PXF_BASE_DIR} ${PXF_HOME}/bin/pxf cluster start

  echoGreen "ALTER EXTENSION pxf UPDATE - for multibyte delimiter tests"
  source ${GPHOME}/greenplum_path.sh && psql -d template1 -c 'ALTER EXTENSION pxf UPDATE'
  source ${GPHOME}/greenplum_path.sh && psql -d pxfautomation -c 'ALTER EXTENSION pxf UPDATE'
  source ${GPHOME}/greenplum_path.sh && psql -d pxfautomation_encoding -c 'SELECT * FROM pg_extension'
}

function _main() {
	scp -r pxf_tarball "${COORDINATOR_HOSTNAME}:~gpadmin"
	upgrade_pxf
}

_main
