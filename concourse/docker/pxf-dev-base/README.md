# How to build pxf-dev-base docker images locally?

The generated images are the base images for building and testing PXF.

If you do NOT want to build the images yourself, you can pull them from GCR
by running docker pull. For example, for the CentOS7 image for Greenplum 6,
use the following command:

    export PROJECT_ID=<YOUR_PROJECT_ID>
    docker pull gcr.io/$PROJECT_ID/gpdb-pxf-dev/gpdb6-centos7-test-pxf:latest

For a list of images built by `cloudbuild` take a look [here](../README.md).

You can run the entire cloudbuild using Google Cloud Build by doing the following:
```
cd ~/workspace/pxf

gcloud builds submit . --config=concourse/docker/pxf-dev-base/cloudbuild.yaml \
  --substitutions=_BASE_IMAGE_REPOSITORY=gcr.io/data-gpdb-public-images,COMMIT_SHA=dev-build-<name>

-- or if you would like to modify the go and ginkgo versions, you can do so by doing the following --

gcloud builds submit . --config=concourse/docker/pxf-dev-base/cloudbuild.yaml \
--substitutions=_BASE_IMAGE_REPOSITORY=gcr.io/data-gpdb-public-images,_GO_VERSION=<test-version>,_MAVEN_VERSION=<mvn-version>,COMMIT_SHA=dev-build-<test-name>
```

This guide assumes the PXF repository lives under the `~/workspace/pxf`
directory. The `cloudbuild.yaml` file produces the following docker images:

You can build these images individually by first setting these local variables:
```
export GO_VERSION=1.21.3
export MAVEN_VERSION=3.9.2
```
## Greenplum 5 Images

* Note: Greenplum 5 on CentOS 6 support has been deprecated, but images are
  still available for testing.

### Docker gpdb5-centos7-test-pxf-image image

Build this image for Greenplum 5 running on CentOS 7. Run the following
command to build the image:

    pushd ~/workspace/pxf/concourse/docker/pxf-dev-base/
    docker build \
      --build-arg=BASE_IMAGE=gcr.io/data-gpdb-public-images/gpdb5-centos7-build-test:latest \
      --build-arg=GO_VERSION=${GO_VERSION} \
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
      --build-arg=GO_VERSION=${GO_VERSION} \
      --build-arg=MAVEN_VERSION=${MAVEN_VERSION} \
      --tag=gpdb6-centos7-test-pxf \
      -f ~/workspace/pxf/concourse/docker/pxf-dev-base/gpdb6/centos7/Dockerfile \
      .
    popd

### Docker gpdb6-rocky8-test-pxf-image image

Build this image for Greenplum 6 running on Rhel 8. Run the following
command to build the image:

    pushd ~/workspace/pxf/concourse/docker/pxf-dev-base/
    docker build \
      --build-arg=BASE_IMAGE=gcr.io/data-gpdb-public-images/gpdb6-rocky8-test:latest \
      --build-arg=GO_VERSION=${GO_VERSION} \
      --build-arg=MAVEN_VERSION=${MAVEN_VERSION} \
      --tag=gpdb6-rocky8-test-pxf \
      -f ~/workspace/pxf/concourse/docker/pxf-dev-base/gpdb6/rocky8/Dockerfile \
      .
    popd

### Docker gpdb6-rocky9-test-pxf-image image

Build this image for Greenplum 6 running on Rhel 9. Run the following
command to build the image:

    pushd ~/workspace/pxf/concourse/docker/pxf-dev-base/
    docker build \
      --build-arg=BASE_IMAGE=gcr.io/data-gpdb-public-images/gpdb6-rocky9-test:latest \
      --build-arg=GO_VERSION=${GO_VERSION} \
      --build-arg=MAVEN_VERSION=${MAVEN_VERSION} \
      --tag=gpdb6-rocky9-test-pxf \
      -f ~/workspace/pxf/concourse/docker/pxf-dev-base/gpdb6/rocky9/Dockerfile \
      .
    popd

### Docker gpdb6-ubuntu18.04-test-pxf-image image

Build this image for Greenplum 6 running on Ubuntu 18.04. Run the following
command to build the image:

    pushd ~/workspace/pxf/concourse/docker/pxf-dev-base/
    docker build \
      --build-arg=BASE_IMAGE=gcr.io/data-gpdb-public-images/gpdb6-ubuntu18.04-test:latest \
      --build-arg=GO_VERSION=${GO_VERSION} \
      --build-arg=MAVEN_VERSION=${MAVEN_VERSION} \
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
      --build-arg=GO_VERSION=${GO_VERSION} \
      --build-arg=MAVEN_VERSION=${MAVEN_VERSION} \
      --tag=gpdb6-oel7-test-pxf \
      -f ~/workspace/pxf/concourse/docker/pxf-dev-base/gpdb6/oel7/Dockerfile \
      .
    popd

## Greenplum 7 Images

### Docker gpdb7-rocky8-test-pxf-image image

Build this image for Greenplum 7 running on Rocky 8. Run the following
command to build the image:

    pushd ~/workspace/pxf/concourse/docker/pxf-dev-base/
    docker build \
      --build-arg=BASE_IMAGE=gcr.io/data-gpdb-public-images/gpdb7-rocky8-test:latest \
      --build-arg=GO_VERSION=${GO_VERSION} \
      --build-arg=MAVEN_VERSION=${MAVEN_VERSION} \
      --tag=gpdb7-rocky8-test-pxf \
      -f ~/workspace/pxf/concourse/docker/pxf-dev-base/gpdb7/rocky8/Dockerfile \
      .
    popd
