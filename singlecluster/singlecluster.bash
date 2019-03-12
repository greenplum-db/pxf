#!/bin/bash

set -exo pipefail

_main() {
  singlecluster=$(pwd)/pxf_src/singlecluster
  HADOOP_DISTRO_LOWER=$(echo ${2} | tr A-Z a-z)
  mkdir -p ${singlecluster}/tars
  mv ${HADOOP_DISTRO_LOWER}_tars_tarball/*.tar.gz ${singlecluster}/tars
  mv tomcat/*.tar.gz ${singlecluster}/tars/
  mv jdbc/*.jar ${singlecluster}
  pushd ${singlecluster}
    make HADOOP_VERSION="${1}" HADOOP_DISTRO="${2}"
    mv singlecluster-${2}.tar.gz ../../artifacts/singlecluster-${2}.tar.gz
  popd
}

_main "$@"
