#!/bin/bash

set -exo pipefail

pushd pxf_src
VERSION=`git describe --tags`

popd

cat > install_gpdb_component <<EOF
#!/bin/bash
set -x
tar xvzf pxf.tar.gz -C \$GPHOME
EOF
cat > smoke_test_gpdb_component <<EOF
#!/bin/bash
set -x
\$GPHOME/pxf/bin/pxf version 2>&1 | grep "PXF version ${VERSION}" || exit 1
EOF
chmod +x install_gpdb_component smoke_test_gpdb_component
cp pxf_tarball/pxf.tar.gz .
tar -cvzf pxf_artifacts/pxf-${VERSION}.tar.gz pxf.tar.gz install_gpdb_component smoke_test_gpdb_component
