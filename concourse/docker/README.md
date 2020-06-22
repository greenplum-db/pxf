# Docker container for Greenplum development/testing

## Requirements

- docker 1.13 (with 3-4 GB allocated for docker host)

Map-R 5.2 Image
```
docker run --rm --privileged -p 8443:8443 -it pivotaldata/gpdb-dev:centos6-mapr5.2
```

PXF uses [Google Cloud Build](https://cloud.google.com/cloud-build) to produce
development images that reside in
[Google Container Registry (GCR)](https://cloud.google.com/container-registry).

The `cloudbuild` pipeline provides visibility into builds using Google Cloud
Builds. The `cloudbuild` pipeline triggers on changes to `pxf-dev-base` and
changes to `pxf-build-base` and is also in charge of tagging the images as
`latest` when they are pushed to GCR.

## Available docker images

<table>
  <tr>
    <td>&nbsp;</td>
    <td colspan="2">Greenplum 5</td>
    <td colspan="3">Greenplum 6</td>
  </tr>
  <tr>
    <td>&nbsp;</td>
    <td>CentOS6</td>
    <td>CentOS7</td>
    <td>CentOS7</td>
    <td>Ubuntu 18.04</td>
    <td>OEL7</td>
  </tr>
  <tr>
    <td>Base Image</td>
    <td> <a href="https://console.cloud.google.com/gcr/images/${GCR_PROJECT_ID}/GLOBAL/gpdb-pxf-dev/gpdb5-centos6-test-pxf">gpdb5-centos6-test-pxf</a> </td>
    <td> <a href="https://console.cloud.google.com/gcr/images/${GCR_PROJECT_ID}/GLOBAL/gpdb-pxf-dev/gpdb5-centos7-test-pxf">gpdb5-centos7-test-pxf</a> </td>
    <td> <a href="https://console.cloud.google.com/gcr/images/${GCR_PROJECT_ID}/GLOBAL/gpdb-pxf-dev/gpdb6-centos7-test-pxf">gpdb6-centos7-test-pxf</a> </td>
    <td> <a href="https://console.cloud.google.com/gcr/images/${GCR_PROJECT_ID}/GLOBAL/gpdb-pxf-dev/gpdb6-ubuntu18.04-test-pxf">gpdb6-ubuntu18.04-test-pxf</a> </td>
    <td> <a href="https://console.cloud.google.com/gcr/images/${GCR_PROJECT_ID}/GLOBAL/gpdb-pxf-dev/gpdb6-oel7-test-pxf">gpdb6-oel7-test-pxf</a> </td>
  </tr>
  <tr>
    <td>MapR</td>
    <td> N/A </td>
    <td> <a href="https://console.cloud.google.com/gcr/images/${GCR_PROJECT_ID}/GLOBAL/gpdb-pxf-dev/gpdb5-centos7-test-pxf-mapr">gpdb5-centos7-test-pxf-mapr</a> </td>
    <td> <a href="https://console.cloud.google.com/gcr/images/${GCR_PROJECT_ID}/GLOBAL/gpdb-pxf-dev/gpdb6-centos7-test-pxf-mapr">gpdb6-centos7-test-pxf-mapr</a> </td>
    <td> N/A </td>
    <td> N/A </td>
  </tr>
</table>

* Note: GCR_PROJECT_ID is the name of the Google Cloud Project ID

## Development docker image

A PXF development docker image can be pulled with the following command:

```shell script
docker pull gcr.io/${GCR_PROJECT_ID}/gpdb-pxf-dev/gpdb6-centos7-test-pxf-hdp2:latest
```
