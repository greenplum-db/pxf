#!/bin/bash
set -e

~/workspace/pxf/dev/install_gpdb.bash

source /usr/local/greenplum-db-devel/greenplum_path.sh
make -C ~/workspace/gpdb create-demo-cluster
source ~/workspace/gpdb/gpAux/gpdemo/gpdemo-env.sh

~/workspace/pxf/dev/configure_singlecluster.bash

pushd ~/workspace/singlecluster/bin
  echo y | ./init-gphd.sh
  ./start-hdfs.sh
  ./start-yarn.sh
  ./start-hive.sh
  ./start-zookeeper.sh
  ./start-hbase.sh
popd

make -C ~/workspace/pxf install
export PXF_BASE=$PXF_HOME
export PXF_JVM_OPTS="-Xmx512m -Xms256m"
$PXF_HOME/bin/pxf init
$PXF_HOME/bin/pxf start

cp "${PXF_BASE}"/templates/*-site.xml "${PXF_BASE}"/servers/default

if [ -d ~/workspace/gpdb/gpAux/extensions/pxf ]; then
  PXF_EXTENSIONS_DIR=gpAux/extensions/pxf
else
  PXF_EXTENSIONS_DIR=gpcontrib/pxf
fi

make -C ~/workspace/gpdb/${PXF_EXTENSIONS_DIR} installcheck
psql -d template1 -c "create extension pxf"

cd ~/workspace/pxf/automation
make GROUP=smoke
