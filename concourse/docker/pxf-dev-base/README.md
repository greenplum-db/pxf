# How to build pxf-dev-base docker images locally?

Build the docker images on your local system. To build the following docker
images you need the `pxf-build-dependencies.tar.gz` tarball called that
contains the following:

- `.m2` cache
- `.gradle` cache
- the tomcat tarball in `.tomcat`
- the go sources in `.go-dep-cached-sources`

To copy the tarball into your local environment, use the following `gutil`
command:

```shell script
gsutil cp gs://<bucket-name>/build-dependencies/pxf-build-dependencies.tar.gz .
```

The generated images are the base images for the CDH, HDP2, HDP3, and MapR
images. This guide assumes the PXF repository lives under the `~/workspace/pxf`
directory. The `cloudbuild.yaml` file produces the following docker images:

### Docker gpdb5-centos6-test-pxf-image image

Build this image for Greenplum 5 running on CentOS 6. Run the following
command to build the image:

    pushd ~/workspace/pxf/concourse/docker/pxf-dev-base/
    docker build \
      --build-arg=BASE_IMAGE=<REGISTRY_PATH>/centos-gpdb-dev:6-gcc6.2-llvm3.7 \
      --tag=gpdb5-centos6-test-pxf \
      -f ~/workspace/pxf/concourse/docker/pxf-dev-base/gpdb5/centos6/Dockerfile \
      <PATH_TO_YOUR_DOCKER_WORKSPACE>
    popd

### Docker gpdb5-centos7-test-pxf-image image

Build this image for Greenplum 5 running on CentOS 7. Run the following
command to build the image:

    pushd ~/workspace/pxf/concourse/docker/pxf-dev-base/
    docker build \
      --build-arg=BASE_IMAGE=<REGISTRY_PATH>/centos-gpdb-dev:7-gcc6.2-llvm3.7 \
      --tag=gpdb5-centos7-test-pxf \
      -f ~/workspace/pxf/concourse/docker/pxf-dev-base/gpdb5/centos7/Dockerfile \
      <PATH_TO_YOUR_DOCKER_WORKSPACE>
    popd

### Docker gpdb6-centos6-test-pxf-image image

Build this image for Greenplum 6 running on CentOS 6. Run the following
command to build the image:

    pushd ~/workspace/pxf/concourse/docker/pxf-dev-base/
    docker build \
      --build-arg=BASE_IMAGE=<REGISTRY_PATH>/gpdb6-centos6-test \
      --tag=gpdb6-centos6-test-pxf \
      -f ~/workspace/pxf/concourse/docker/pxf-dev-base/gpdb6/centos6/Dockerfile \
      <PATH_TO_YOUR_DOCKER_WORKSPACE>
    popd

### Docker gpdb6-centos7-test-pxf-image image

Build this image for for Greenplum 6 running on CentOS 7. Run the following
command to build the image:

    pushd ~/workspace/pxf/concourse/docker/pxf-dev-base/
    docker build \
      --build-arg=BASE_IMAGE=<REGISTRY_PATH>/gpdb6-centos7-test:latest \
      --tag=gpdb6-centos7-test-pxf \
      -f ~/workspace/pxf/concourse/docker/pxf-dev-base/gpdb6/centos7/Dockerfile \
      <PATH_TO_YOUR_DOCKER_WORKSPACE>
    popd

### Docker gpdb6-ubuntu18.04-test-pxf-image image

Build this image for Greenplum 6 running on Ubuntu 18.04. Run the following
command to build the image:

    pushd ~/workspace/pxf/concourse/docker/pxf-dev-base/
    docker build \
      --build-arg=BASE_IMAGE=<REGISTRY_PATH>/gpdb6-ubuntu18.04-test:latest \
      --tag=gpdb6-ubuntu18.04-test-pxf \
      -f ~/workspace/pxf/concourse/docker/pxf-dev-base/gpdb6/ubuntu18.04/Dockerfile \
      <PATH_TO_YOUR_DOCKER_WORKSPACE>
    popd
