#!/usr/bin/env bash

set -e

: "${GP_VER:?GP_VER must be set}"

function fail() {
  echo "Error: $1"
  exit 1
}

# check if directory with artifacts is available
[[ -d pxf_artifacts ]] || fail "pxf_artifacts directory not found"

# check if the RPM for GP version and RHEL7 is available
rpm_file_name=$(find pxf_artifacts/licensed -type f -name "pxf-gp${GP_VER}-*-2.el7.x86_64.rpm")
[[ -f ${rpm_file_name} ]] || fail "pxf_artifacts/licensed/pxf-gp${GP_VER}-*-2.el7.x86_64.rpm not found"

# extract RPM
build_dir=${PWD}
pushd /
rpm2cpio "${build_dir}/${rpm_file_name}" | cpio -idm
popd
echo "listing installed directory /usr/local/pxf-gp${GP_VER}:"
ls -al "/usr/local/pxf-gp${GP_VER}"

# determine the PXF version
pxf_version=$(cat "/usr/local/pxf-gp${GP_VER}/version")

# copy installed PXF into a staging directory
mkdir -p /tmp/pxf_tarball_repackage
cp -R "/usr/local/pxf-gp${GP_VER}/" /tmp/pxf_tarball_repackage/pxf

# place gpextable into the appropriate locations when creating internal `pxf.tar.gz` tarball, so they are just extracted and no additional copying is required
mv /tmp/pxf_tarball_repackage/pxf/gpextable/* /tmp/pxf_tarball_repackage/
rm -rf /tmp/pxf_tarball_repackage/pxf/gpextable/

# list staging directory
echo "listing staging directory /tmp/pxf_tarball_repackage"
ls -al /tmp/pxf_tarball_repackage

# create the pxf.tar.gz that contains all files from the RPM installation
echo "create the pxf tarball"
mkdir -p /tmp/pxf_tarball
tar -czf /tmp/pxf_tarball/pxf.tar.gz -C /tmp/pxf_tarball_repackage .

# create the install_gpdb_component file
cat > /tmp/pxf_tarball/install_gpdb_component <<EOF
#!/bin/bash
set -x
CWDIR="\$( cd "\$( dirname "\${BASH_SOURCE[0]}" )" && pwd )"
: "\${GPHOME:?GPHOME must be set}"
tar xvzf "\${CWDIR}/pxf.tar.gz" -C \$GPHOME
EOF
chmod +x /tmp/pxf_tarball/install_gpdb_component

echo "create the pxf installer tarball in pxf_artifacts/licensed/gp${GP_VER}/pxf-gp${GP_VER}-${pxf_version}-el7.x86_64.tar.gz"
mkdir -p "pxf_artifacts/licensed/gp${GP_VER}/"
tar -czf "pxf_artifacts/licensed/gp${GP_VER}/pxf-gp${GP_VER}-${pxf_version}-el7.x86_64.tar.gz" -C /tmp/pxf_tarball .