# How to build development docker images locally

Build the docker images on your local system. To build the following docker
images you need the `singlecluster` tarball for the Hadoop flavor you plan to
build and the `pxf-build-dependencies` tarball.

### Docker gpdb6-centos7-test-pxf-hdp2-image

Build this image for Greenplum 6 running on CentOS 7 with HDP 2 support.
Run the following command to build the image:

```
export GCR_PROJECT_ID=<YOUR_GCR_PROJECT_ID>
export BUCKET_NAME=<YOUR_BUCKET_NAME>

mkdir build
pushd build

gsutil cp gs://${BUCKET_NAME}/singlecluster/HDP2/singlecluster-HDP2.tar.gz .
gsutil cp gs://${BUCKET_NAME}/build-dependencies/pxf-build-dependencies.tar.gz .

docker build \
  --build-arg=BASE_IMAGE=gcr.io/$GCR_PROJECT_ID/gpdb-pxf-dev/gpdb6-centos7-test-pxf:latest \
  --tag=gpdb6-centos7-test-pxf-hdp2 \
  -f ~/workspace/pxf/concourse/docker/pxf-dev-server/Dockerfile .

popd
```
