# How to build pxf-dev-base docker images locally?

The generated images are the base images for building and testing PXF.

If you do NOT want to build the images yourself, you can pull them from GCR
by running docker pull. For example, for the CentOS7 image for Greenplum 6,
use the following command:

    export PROJECT_ID=<YOUR_PROJECT_ID>
    docker pull gcr.io/$PROJECT_ID/gpdb-pxf-dev/gpdb6-centos7-test-pxf:latest

For a list of images built by `cloudbuild` take a look [here](../README.md).

This guide assumes the PXF repository lives under the `~/workspace/pxf`
directory. The `cloudbuild.yaml` file produces the following docker images:

## Greenplum 5 Images

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
      --build-arg=BASE_IMAGE=gcr.io/data-gpdb-public-images/gpdb5-centos7-build-test:latest \
      --tag=gpdb5-centos7-test-pxf \
      -f ~/workspace/pxf/concourse/docker/pxf-dev-base/gpdb5/centos7/Dockerfile \
      .
    popd

## Greenplum 6 Images

### Docker gpdb6-centos7-test-pxf-image image

Build this image for Greenplum 6 running on CentOS 7. Run the following
command to build the image:

    pushd ~/workspace/pxf/concourse/docker/pxf-dev-base/
    docker build \
      --build-arg=BASE_IMAGE=gcr.io/data-gpdb-public-images/gpdb6-centos7-test:latest \
      --tag=gpdb6-centos7-test-pxf \
      -f ~/workspace/pxf/concourse/docker/pxf-dev-base/gpdb6/centos7/Dockerfile \
      .
    popd

### Docker gpdb6-ubuntu18.04-test-pxf-image image

Build this image for Greenplum 6 running on Ubuntu 18.04. Run the following
command to build the image:

    pushd ~/workspace/pxf/concourse/docker/pxf-dev-base/
    docker build \
      --build-arg=BASE_IMAGE=gcr.io/data-gpdb-public-images/gpdb6-ubuntu18.04-test:latest \
      --tag=gpdb6-ubuntu18.04-test-pxf \
      -f ~/workspace/pxf/concourse/docker/pxf-dev-base/gpdb6/ubuntu18.04/Dockerfile \
      .
    popd

### Docker gpdb6-oel7-test-pxf-image image

Build this image for Greenplum 6 running Oracle Enterprise Linux 7. Run the
following command to build the image:

    pushd ~/workspace/pxf/concourse/docker/pxf-dev-base/
    docker build \
      --build-arg=BASE_IMAGE=gcr.io/data-gpdb-public-images/gpdb6-oel7-test:latest \
      --tag=gpdb6-oel7-test-pxf \
      -f ~/workspace/pxf/concourse/docker/pxf-dev-base/gpdb6/oel7/Dockerfile \
      .
    popd

## Greenplum 7 Images

### Docker gpdb7-centos7-test-pxf-image image

Build this image for Greenplum 7 running on CentOS 7. Run the following
command to build the image:

    pushd ~/workspace/pxf/concourse/docker/pxf-dev-base/
    docker build \
      --build-arg=BASE_IMAGE=gcr.io/data-gpdb-public-images/gpdb7-centos7-test:latest \
      --tag=gpdb7-centos7-test-pxf \
      -f ~/workspace/pxf/concourse/docker/pxf-dev-base/gpdb7/centos7/Dockerfile \
      .
    popd

### Docker gpdb7-ubuntu18.04-test-pxf-image image

Build this image for Greenplum 7 running on Ubuntu 18.04. Run the following
command to build the image:

    pushd ~/workspace/pxf/concourse/docker/pxf-dev-base/
    docker build \
      --build-arg=BASE_IMAGE=gcr.io/data-gpdb-public-images/gpdb7-ubuntu18.04-test:latest \
      --tag=gpdb7-ubuntu18.04-test-pxf \
      -f ~/workspace/pxf/concourse/docker/pxf-dev-base/gpdb7/ubuntu18.04/Dockerfile \
      .
    popd
