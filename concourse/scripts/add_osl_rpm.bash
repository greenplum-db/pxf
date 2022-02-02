#!/usr/bin/env bash

set -e

: "${GP_VER:?GP_VER must be set}"
: "${REDHAT_MAJOR_VERSION:?REDHAT_MAJOR_VERSION must be set}"

function fail() {
  echo "Error: $1"
  exit 1
}

# check if directory with artifacts is available
[[ -d pxf_artifacts ]] || fail "pxf_artifacts directory not found"

# check if the RPM for GP version and REDHAT_MAJOR_VERSION is available
rpm_file_name=$(find pxf_artifacts -type f -name "pxf-gp${GP_VER}-*-1.el${REDHAT_MAJOR_VERSION}.x86_64.rpm" -printf "%f\n")
[[ -n ${rpm_file_name} ]] || fail "pxf_artifacts/pxf-gp${GP_VER}-*-1.el${REDHAT_MAJOR_VERSION}.x86_64.rpm not found"

# check that OSL file is available
osl_file_name=$(find pxf_artifacts -type f -name "open_source_license*.txt" -printf "%f\n")
[[ -n ${osl_file_name} ]] || fail "pxf_artifacts/open_source_license*.txt not found"

# setup a script to add the OSL file to the RPM spec
mkdir -p /build/{scripts,temp,output}

# setup directories for RPMREBUILD
cat > /build/scripts/add_license_file_to_spec.sh <<-EOF
#!/usr/bin/env bash
while read line; do
    echo \$line
done
echo '%attr(0644, root, root) "/usr/local/pxf-gp${GP_VER}/open_source_licenses.txt"'
EOF
chmod a+x /build/scripts/add_license_file_to_spec.sh

# run rpmrebuild script to produce a new RPM
export RPMREBUILD_TMPDIR=/build/temp
rpmrebuild -d /build/output \
  --change-files="cp pxf_artifacts/${osl_file_name} /build/temp/work/root/usr/local/pxf-gp${GP_VER}/open_source_licenses.txt" \
  --change-spec-files="/build/scripts/add_license_file_to_spec.sh" \
  --release="2.el${REDHAT_MAJOR_VERSION}" \
  -p "pxf_artifacts/${rpm_file_name}"

# check if the RPM for GP version and REDHAT_MAJOR_VERSION is available
new_rpm_file_name=$(find /build/output/x86_64/ -type f -name "pxf-gp${GP_VER}-*-2.el${REDHAT_MAJOR_VERSION}.x86_64.rpm" -printf "%f\n")
[[ -n ${new_rpm_file_name} ]] || fail "/build/output/x86_64/pxf-gp${GP_VER}-*-2.el${REDHAT_MAJOR_VERSION}.x86_64.rpm not found"

# install the new RPM, check that the OSL file is present
rpm -ivh "/build/output/x86_64/${new_rpm_file_name}"
echo "listing installed directory /usr/local/pxf-gp${GP_VER}:"
ls -al /usr/local/pxf-gp${GP_VER}

# check that the OSL file is present
[[ -f /usr/local/pxf-gp${GP_VER}/open_source_licenses.txt ]] || fail "/usr/local/pxf-gp${GP_VER}/open_source_licenses.txt not found"
grep -q "Greenplum Platform Extension Framework" "/usr/local/pxf-gp${GP_VER}/open_source_licenses.txt" || \
  fail "/usr/local/pxf-gp${GP_VER}/open_source_licenses.txt has incorrect content"

# copy the new RPM to the output directory
echo "Copying /build/output/x86_64/${new_rpm_file_name} to pxf_artifacts/licensed/${new_rpm_file_name}"
mkdir -p pxf_artifacts/licensed
cp "/build/output/x86_64/${new_rpm_file_name}" pxf_artifacts/licensed/

