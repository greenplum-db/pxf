# How to build pxf-dev-base docker images locally?

The generated images are the base images for building and testing PXF.
This guide assumes the PXF repository lives under the `~/workspace/pxf`
directory. The `cloudbuild.yaml` file produces the following docker images:

### Docker gpdb5-centos6-test-pxf-image image

Build this image for Greenplum 5 running on CentOS 6. Run the following
command to build the image:

    pushd ~/workspace/pxf/concourse/docker/pxf-dev-base/
    docker build \
      --build-arg=BASE_IMAGE=pivotaldata/centos-gpdb-dev:6-gcc6.2-llvm3.7 \
      --tag=gpdb5-centos6-test-pxf \
      -f ~/workspace/pxf/concourse/docker/pxf-dev-base/gpdb5/centos6/Dockerfile \
      .
    popd

### Docker gpdb5-centos7-test-pxf-image image

Build this image for Greenplum 5 running on CentOS 7. Run the following
command to build the image:

    pushd ~/workspace/pxf/concourse/docker/pxf-dev-base/
    docker build \
      --build-arg=BASE_IMAGE=pivotaldata/centos-gpdb-dev:7-gcc6.2-llvm3.7 \
      --tag=gpdb5-centos7-test-pxf \
      -f ~/workspace/pxf/concourse/docker/pxf-dev-base/gpdb5/centos7/Dockerfile \
      .
    popd

### Docker gpdb6-centos7-test-pxf-image image

Build this image for for Greenplum 6 running on CentOS 7. Run the following
command to build the image:

    pushd ~/workspace/pxf/concourse/docker/pxf-dev-base/
    docker build \
      --build-arg=BASE_IMAGE=pivotaldata/gpdb6-centos7-test:latest \
      --tag=gpdb6-centos7-test-pxf \
      -f ~/workspace/pxf/concourse/docker/pxf-dev-base/gpdb6/centos7/Dockerfile \
      .
    popd

### Docker gpdb6-ubuntu18.04-test-pxf-image image

Build this image for Greenplum 6 running on Ubuntu 18.04. Run the following
command to build the image:

    pushd ~/workspace/pxf/concourse/docker/pxf-dev-base/
    docker build \
      --build-arg=BASE_IMAGE=pivotaldata/gpdb6-ubuntu18.04-test:latest \
      --tag=gpdb6-ubuntu18.04-test-pxf \
      -f ~/workspace/pxf/concourse/docker/pxf-dev-base/gpdb6/ubuntu18.04/Dockerfile \
      .
    popd

### Docker gpdb6-oel7-test-pxf-image image

Build this image for Greenplum 6 running Oracle Enterprise Linux 7. Run the
following command to build the image:

    pushd ~/workspace/pxf/concourse/docker/pxf-dev-base/
    docker build \
      --build-arg=BASE_IMAGE=pivotaldata/gpdb6-oel7-test:latest \
      --tag=gpdb6-oel7-test-pxf \
      -f ~/workspace/pxf/concourse/docker/pxf-dev-base/gpdb6/oel7/Dockerfile \
      .
    popd
