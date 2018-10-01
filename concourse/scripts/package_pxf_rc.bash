#!/usr/bin/env bash

set -x

pushd pxf_src
VERSION=`git describe --tags`
popd
cat > install_gpdb_component <<EOF
#!/bin/sh
set -x
tar xzf pxf.tar.gz -C \$GPHOME
EOF
chmod a+x install_gpdb_component
cat > smoke_test_gpdb_component <<EOF
#!/bin/sh
set -x
\$GPHOME/pxf/bin/pxf start &&
\$GPHOME/pxf/bin/pxf stop
EOF
chmod a+x smoke_test_gpdb_component
mv pxf_tarball/pxf.tar.gz .
tar -cvzf pxf_artifacts/pxf-${VERSION}.tar.gz pxf.tar.gz install_gpdb_component smoke_test_gpdb_component
