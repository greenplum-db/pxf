#!/usr/bin/env bash

set -e

: "${GPDB_PKG_DIR:?GPDB_PKG_DIR must be set}"
: "${PXF_SRC:?PXF_SRC must be set}"

BASE_DIR=${PWD}

version=$(<"${GPDB_PKG_DIR}/version")
if command -v rpm; then
	rpm --quiet -ivh "${GPDB_PKG_DIR}/greenplum-db-${version}"-rhel*-x86_64.rpm
elif command -v apt; then
	# apt wants a full path
	apt install -qq "${PWD}/${GPDB_PKG_DIR}/greenplum-db-${version}-ubuntu18.04-amd64.deb"
else
	echo "Cannot install RPM or DEB from ${GPDB_PKG_DIR}, no rpm or apt command available in this environment. Exiting..."
	exit 1
fi

# create symlink to allow pgregress to run (hardcoded to look for /usr/local/greenplum-db-devel/psql)
rm -rf /usr/local/greenplum-db-devel
ln -sf "/usr/local/greenplum-db-${version}" /usr/local/greenplum-db-devel
