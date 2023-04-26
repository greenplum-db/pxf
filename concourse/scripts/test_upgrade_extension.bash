#!/bin/bash

set -exo pipefail

CWDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# make sure GP_VER is set so that we know what PXF_HOME will be
: "${GP_VER:?GP_VER must be set}"
# We run 2 sets of automation tests, once before upgrading and once after
# make sure that SECOND_GROUP is set so that we actually have a set of tests
# to run against after we upgrade PXF.
: "${SECOND_GROUP:?SECOND_GROUP must be set}"

# set our own GPHOME for RPM-based installs before sourcing common script
export GPHOME=/usr/local/greenplum-db
export PXF_HOME=/usr/local/pxf-gp${GP_VER}
export PXF_BASE_DIR=${PXF_BASE_DIR:-$PXF_HOME}

source "${CWDIR}/pxf_common.bash"

export GOOGLE_PROJECT_ID=${GOOGLE_PROJECT_ID:-data-gpdb-ud}
export JAVA_TOOL_OPTIONS=-Dfile.encoding=UTF8
export HADOOP_HEAPSIZE=512
export YARN_HEAPSIZE=512
export GPHD_ROOT=/singlecluster
export PGPORT=${PGPORT:-5432}

function upgrade_pxf() {
	existing_pxf_version=$(cat "${PXF_HOME}"/version)
	echo "Stopping PXF ${existing_pxf_version}"
	su gpadmin -c "${PXF_HOME}/bin/pxf version && ${PXF_HOME}/bin/pxf cluster stop"

	echo "Installing Newer Version of PXF 6"
	install_pxf_tarball

	echo "Check the PXF 6 version"
	su gpadmin -c "${PXF_HOME}/bin/pxf version"

	echo "Register the PXF extension into Greenplum"
	su gpadmin -c "GPHOME=${GPHOME} ${PXF_HOME}/bin/pxf cluster register"

	if [[ "${PXF_BASE_DIR}" != "${PXF_HOME}" ]]; then
		echo "Prepare PXF in ${PXF_BASE_DIR}"
		PXF_BASE="${PXF_BASE_DIR}" "${PXF_HOME}"/bin/pxf cluster prepare
		echo "export PXF_BASE=${PXF_BASE_DIR}" >> ~gpadmin/.bashrc
	fi
	updated_pxf_version=$(cat "${PXF_HOME}"/version)

	echo "Starting PXF ${updated_pxf_version}"

	if [[ "${existing_pxf_version}" > "${updated_pxf_version}" ]]; then
		echo "Existing version of PXF (${existing_pxf_version}) is greater than or equal to the new version (${updated_pxf_version})"
	fi

	su gpadmin -c "PXF_BASE=${PXF_BASE_DIR} ${PXF_HOME}/bin/pxf cluster start"

  # the new version of PXF brought in a new version of the extension. For databases that already had PXF installed,
  # we need to explicitly upgrade the PXF extension to the new version
	echo "ALTER EXTENSION pxf UPDATE - for multibyte delimiter tests"

	su gpadmin <<'EOSU'
  source ${GPHOME}/greenplum_path.sh &&
  psql --no-align --tuples-only --command "SELECT datname FROM pg_catalog.pg_database WHERE datname != 'template0';" | while read -r dbname; do
      echo -n "checking if database '${dbname}' has PXF extension installed... "
      if ! psql --dbname="${dbname}" --no-align --tuples-only --command "SELECT extname FROM pg_catalog.pg_extension WHERE extname = 'pxf'" | grep . &>/dev/null; then
          echo "skipping database '${dbname}'"
          continue
      fi
      echo "updating PXF extension in database '${dbname}'"
      psql --dbname="${dbname}" --set ON_ERROR_STOP=on --command "ALTER EXTENSION pxf UPDATE;"
    done
EOSU

}

function _main() {

	# Upgrade to latest PXF
	echo
	echo
	echo '****************************************************************************************************'
	echo "*                                       Upgrading PXF                                              *"
	echo '****************************************************************************************************'
	echo
	echo

	# Upgrade from older version of PXF to newer version of PXF present in the tarball
	upgrade_pxf

	# Run tests after upgrading PXF
	# second time running automation so we should be running the second group
	GROUP=${SECOND_GROUP}
	run_pxf_automation
}

_main
