#!/usr/bin/env bash

set -e

: "${GCS_RELEASES_BUCKET:?GCS_RELEASES_BUCKET must be set}"
: "${GCS_RELEASES_PATH:?GCS_RELEASES_PATH must be set}"
: "${GIT_BRANCH:?GIT_BRANCH must be set}"
: "${GIT_EMAIL:?GIT_EMAIL must be set}"
: "${GIT_REMOTE_URL:?GIT_REMOTE_URL must be set}"
: "${GIT_SSH_KEY:?GIT_SSH_KEY must be set}"
: "${GIT_USERNAME:?GIT_USERNAME must be set}"
: "${GOOGLE_CREDENTIALS:?GOOGLE_CREDENTIALS must be set}"

echo "Authenticating with Google service account..."
gcloud auth activate-service-account --key-file=<(echo "${GOOGLE_CREDENTIALS}") >/dev/null 2>&1

tarballs=(pxf-gp*-tarball-*/*gz)
if (( ${#tarballs[@]} < 1 )); then
	echo "Couldn't find any tarballs, check pipeline task inputs..."
	exit 1
fi

NOT_RELEASABLE=0
sources=()
destinations=()
for tarball in "${tarballs[@]}"; do
	pkg_file=$(tar tf "${tarball}" | grep -E 'pxf-gp.*/pxf-gp.*(rpm|deb)')
	if [[ ${pkg_file} =~ -SNAPSHOT ]]; then
		echo "SNAPSHOT files detected in tarball '${tarball}': '${pkg_file}'... skipping upload to releases..."
		((NOT_RELEASABLE+=1))
		continue
	fi
	if [[ ${pkg_file##*/} =~ pxf-gp[0-9]+-([0-9.]+)-1[\.-](.*\.(deb|rpm)) ]]; then
		pxf_version=${BASH_REMATCH[1]}
		suffix=${BASH_REMATCH[2]}
		echo "Determined PXF version number to be '${pxf_version}' with suffix '${suffix}'..."
		sources+=("${pkg_file}")
		: "${pkg_file#pxf-gp}"
		gp_ver=${_%%-*}
		destinations+=("gs://${GCS_RELEASES_BUCKET}/${GCS_RELEASES_PATH}/gp${gp_ver}/${pkg_file##*/}")
		echo "Expanding tarball '${tarball}'..."
		tar zxf "${tarball}"
	else
		echo "Couldn't determine version number from file named '${pkg_file}', skipping upload to releases..."
		exit 1
	fi
done

if (( NOT_RELEASABLE > 0 )); then
	# we should fail here if we have a mix of SNAPSHOT/release tarballs
	rc=1
	(( NOT_RELEASABLE == ${#tarballs[@]} )) && rc=0
	echo "${NOT_RELEASABLE} out of ${#tarballs[@]} tarballs are snapshots, exiting with rc=${rc}..."
	exit "${rc}"
fi

if [[ $(<pxf_src/version) != "${pxf_version}" ]]; then
	echo "PXF version from RPM/DEB doesn't match pxf_src/version: ${pxf_version} != $(<pxf_src/version), exiting..."
	exit 1
fi

for ((i = 0; i < "${#sources[@]}"; i++)); do
	echo "Copying '${sources[$i]}' to '${destinations[$i]}'..."
	gsutil cp "${sources[$i]}" "${destinations[$i]}"
done

# Tagging and Bumping of Version Number only happens in "prod" environment.
if [[ ${ENVIRONMENT} == prod ]]; then
	# configure ssh
	mkdir -p ~/.ssh
	ssh-keyscan github.com > ~/.ssh/known_hosts
	echo "${GIT_SSH_KEY}" > ~/.ssh/id_rsa
	chmod 600 ~/.ssh/id_rsa

	# configure git
	git config --global user.email "${GIT_EMAIL}"
	git config --global user.name "${GIT_USERNAME}"
	git -C pxf_src remote set-url origin "${GIT_REMOTE_URL}"
	# avoid detached HEAD state from Concourse checking out a SHA
	git -C pxf_src checkout "${GIT_BRANCH}"

	TAG=release-${pxf_version}
	echo "Creating new tag ${TAG}..."
	# create annotated tag
	git -C pxf_src tag -am "PXF Version ${TAG}" "${TAG}"

	patch=${pxf_version##*.}
	# bump patch and add -SNAPSHOT
	SNAPSHOT_VERSION=${pxf_version%${patch}}$((patch + 1))-SNAPSHOT

	echo "Changing version ${pxf_version} -> ${SNAPSHOT_VERSION} and committing change..."
	echo "${SNAPSHOT_VERSION}" > pxf_src/version
	git -C pxf_src add version
	git -C pxf_src commit -m "Bump version to ${SNAPSHOT_VERSION} [skip ci]"

	echo "Pushing new tag ${TAG} and new SNAPSHOT version ${SNAPSHOT_VERSION}"
	git -C pxf_src push --follow-tags
else
	echo "[Environment: ${ENVIRONMENT}] - Skipping Tagging and Bumping Version Number operations."
fi
