#!/usr/bin/env bash

set -e

: "${GOOGLE_CREDENTIALS:?GOOGLE_CREDENTIALS must be set}"
: "${GCS_BUCKET:?GCS_BUCKET must be set}"
: "${GCS_RELEASES_PATH:?GCS_RELEASES_PATH must be set}"
: "${GP_VER:?GP_VER must be set}"

gcloud auth activate-service-account --key-file=<(echo "${GOOGLE_CREDENTIALS}")

tarballs=(pxf_gp*_tarball_*/*gz)
if (( ${#tarballs[@]} < 1 )); then
	echo "Couldn't find any tarballs, check pipeline task inputs..."
	exit 1
fi

for tarball in "${tarballs[@]}"; do
	pkg_file=$(tar tf "${tarball}" | grep -E 'pxf-gp.*/pxf-gp.*(rpm|deb)')
	if [[ $pkg_file =~ -SNAPSHOT ]]; then
		echo "SNAPSHOT files detected in tarball '${tarball}': '${pkg_file}'... skipping upload to releases..."
		continue
	fi
	if [[ ${pkg_file##*/} =~ pxf-gp[0-9]+-([0-9.]+)-1\.(.*\.(deb|rpm)) ]]; then
		pxf_version=${BASH_REMATCH[1]}
		suffix=${BASH_REMATCH[2]}
		echo "Determined PXF version number to be '${pxf_version}' with suffix '${suffix}'..."
	else
		echo "Couldn't determine version number from file named '${pkg_file}', skipping upload to releases..."
		continue
	fi
	tar zxf "${tarball}"
	gsutil cp "${pkg_file}" "gs://${GCS_BUCKET}/${GCS_RELEASES_PATH}/pxf-gp${GP_VER}-${pxf_version}-1.${suffix}"
done
