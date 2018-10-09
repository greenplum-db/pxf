#!/usr/bin/env bash

set -euxo pipefail

yum -y install git

pushd pxf_src
VERSION=`git describe --tags`
popd
git clone --depth=1 gpdb_release gpdb_release_output
cd gpdb_release_output

cat components/component_manifest.json \
  | jq "(.platforms[].components[] | select(.name==\"pxf\").version) = \"${VERSION}\"" \
  > /tmp/component_manifest.json

mv /tmp/component_manifest.json components/component_manifest.json

git config user.email "pxf_bot@example.com"
git config user.name "PXF_BOT"
git commit -am "Update PXF manifest to version ${VERSION}"
