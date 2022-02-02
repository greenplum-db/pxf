#!/usr/bin/env bash

set -e

: "${GP_VER:?GP_VER must be set}"

function fail() {
  echo "Error: $1"
  exit 1
}

# check if directory with artifacts is available
[[ -d pxf_artifacts ]] || fail "pxf_artifacts directory not found"

# check if the DEB for GP version and ubuntu18.04 is available
deb_file_name=$(find pxf_artifacts -type f -name "pxf-gp${GP_VER}-*-1-ubuntu18.04-amd64.deb" -printf "%f\n")
[[ -n ${deb_file_name} ]] || fail "pxf_artifacts/pxf-gp${GP_VER}-*-1-ubuntu18.04-amd64.deb not found"

# check that OSL file is available
osl_file_name=$(find pxf_artifacts -type f -name "open_source_license*.txt" -printf "%f\n")
[[ -n ${osl_file_name} ]] || fail "pxf_artifacts/open_source_license*.txt not found"

# unpackage existing debian
dpkg-deb --raw-extract "pxf_artifacts/${deb_file_name}" rebuild

# update the release number and copy over the osl file
sed -i -e 's/Version: \([^-]*\)-1/Version: \1-2/' rebuild/DEBIAN/control
cp pxf_artifacts/${osl_file_name} rebuild/usr/local/pxf-gp${GP_VER}/open_source_licenses.txt

# rebuild the debian package
dpkg-deb --build rebuild

# rebuild.deb should exist at this point
mv rebuild.deb pxf-gp${GP_VER}-$(dpkg-deb --field rebuild.deb Version)-ubuntu18.04-amd64.deb

# check if the new DEB for GP version and ubuntu18.04 is available
new_deb_file_name=$(find ./ -type f -name "pxf-gp${GP_VER}-*-2-ubuntu18.04-amd64.deb" -printf "%f\n")
[[ -n ${new_deb_file_name} ]] || fail "./pxf-gp${GP_VER}-*-2-ubuntu18.04-amd64.deb not found"

# install the new DEB, check that the OSL file is present
dpkg --install ${new_deb_file_name}
echo "listing installed directory /usr/local/pxf-gp${GP_VER}:"
ls -al /usr/local/pxf-gp${GP_VER}

# check that the OSL file is present
[[ -f /usr/local/pxf-gp${GP_VER}/open_source_licenses.txt ]] || fail "/usr/local/pxf-gp${GP_VER}/open_source_licenses.txt not found"
grep -q "Greenplum Platform Extension Framework" "/usr/local/pxf-gp${GP_VER}/open_source_licenses.txt" || \
  fail "/usr/local/pxf-gp${GP_VER}/open_source_licenses.txt has incorrect content"

# copy the new DEB to the output directory
echo "Copying ./${new_deb_file_name} to pxf_artifacts/licensed/${new_deb_file_name}"
mkdir -p pxf_artifacts/licensed
cp ${new_deb_file_name} pxf_artifacts/licensed/
