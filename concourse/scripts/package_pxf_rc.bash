#!/bin/bash

set -exo pipefail

pushd pxf_src
VERSION=`git describe --tags`
popd

echo -e "#!/bin/bash\nset -x\ntar xzf pxf.tar.gz -C \$GPHOME" > install_gpdb_component
chmod a+x install_gpdb_component
cat > smoke_test_gpdb_component <<EOF
#!/bin/bash
set -x
pwd
ls
bin/pxf start &&
bin/pxf stop || exit 1
EOF
chmod a+x smoke_test_gpdb_component
cp pxf_tarball/pxf.tar.gz .
touch sentinel-1
tar -cvzf pxf_artifacts/pxf-${VERSION}.tar.gz pxf.tar.gz install_gpdb_component smoke_test_gpdb_component sentinel-1
