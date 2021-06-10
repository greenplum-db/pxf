# How to build the pxf-dev-mapr docker image locally

Build the docker images on your local system.

### CentOS 7

#### Greenplum 6

```
docker build \
  --build-arg=BASE_IMAGE=gcr.io/$GCR_PROJECT_ID/gpdb-pxf-dev/gpdb6-centos7-test-pxf:latest \
  --tag=gpdb6-centos7-test-pxf-mapr \
  -f ~/workspace/pxf/concourse/docker/mapr/Dockerfile .
```
