#!/usr/bin/env bash

set -euxo pipefail

yum -y install git

HERE=$(dirname $0)

source "$HERE/add_pxf_to_manifest.bash"

pushd pxf_src
VERSION=`git describe --tags`
popd
git clone --depth=1 gpdb_release gpdb_release_output
cd gpdb_release_output

add_pxf_to_manifest "$VERSION" components/component_manifest.json \
  > /tmp/component_manifest.json

mv /tmp/component_manifest.json components/component_manifest.json

git config user.email "pxf_bot@example.com"
git config user.name "PXF_BOT"
git commit -am "Update PXF manifest to version ${VERSION}"
