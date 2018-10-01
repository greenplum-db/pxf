#!/usr/bin/env bash

set -euxo pipefail
pushd pxf_src
VERSION=`git describe --tags`
popd
yum -y install jq
git clone --depth=1 gpdb_release gpdb_release_output
cd gpdb_release_output

if [ "$(cat components/component_manifest.json | jq '.platforms[].components[] | select(.name=="pxf")')" == "" ]; then
  cat components/component_manifest.json \
	| jq ".platforms[].components += [{\"name\":\"pxf\",\"version\":\"${VERSION}\",\"bucket\":\"gpdb-stable-concourse-builds\",\"path\":\"components/pxf\",\"filetype\":\"tar.gz\",\"artifact_version\":\"latest\"}]" \
	> /tmp/component_manifest.json
else
  cat components/component_manifest.json \
    | jq "(.platforms[].components[] | select(.name==\"pxf\").version) = \"${VERSION}\"" \
    > /tmp/component_manifest.json
fi
mv /tmp/component_manifest.json components/component_manifest.json

git config user.email "pxf_bot@example.com"
git config user.name "PXF_BOT"
git commit -am "Update PXF manifest to version ${VERSION}"
