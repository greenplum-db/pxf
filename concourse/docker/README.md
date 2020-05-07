# Docker container for GPDB development/testing
## Requirements

- docker 1.13 (with 3-4 GB allocated for docker host)

Map-R 5.2 Image
```
docker run --rm --privileged -p 8443:8443 -it pivotaldata/gpdb-dev:centos6-mapr5.2
```

## Available docker images

<table>
  <tr>
    <td>&nbsp;</td>
    <td colspan="2">Greenplum 5</td>
    <td colspan="2">Greenplum 6</td>
  </tr>
  <tr>
    <td>&nbsp;</td>
    <td>CentOS6</td>
    <td>CentOS7</td>
    <td>CentOS7</td>
    <td>Ubuntu 18.04</td>
  </tr>
  <tr>
    <td>Base Image</td>
    <td> <a href="https://console.cloud.google.com/gcr/images/${PROJECT_ID}/GLOBAL/gpdb-pxf-dev/gpdb5-centos6-test-pxf">gpdb5-centos6-test-pxf</a> </td>
    <td> <a href="https://console.cloud.google.com/gcr/images/${PROJECT_ID}/GLOBAL/gpdb-pxf-dev/gpdb5-centos7-test-pxf">gpdb5-centos7-test-pxf</a> </td>
    <td> <a href="https://console.cloud.google.com/gcr/images/${PROJECT_ID}/GLOBAL/gpdb-pxf-dev/gpdb6-centos7-test-pxf">gpdb6-centos7-test-pxf</a> </td>
    <td> <a href="https://console.cloud.google.com/gcr/images/${PROJECT_ID}/GLOBAL/gpdb-pxf-dev/gpdb6-ubuntu18.04-test-pxf">gpdb6-ubuntu18.04-test-pxf</a> </td>
  </tr>
  <tr>
    <td>MapR</td>
    <td> N/A </td>
    <td> <a href="https://console.cloud.google.com/gcr/images/${PROJECT_ID}/GLOBAL/gpdb-pxf-dev/gpdb5-centos7-test-pxf-mapr">gpdb5-centos7-test-pxf-mapr</a> </td>
    <td> <a href="https://console.cloud.google.com/gcr/images/${PROJECT_ID}/GLOBAL/gpdb-pxf-dev/gpdb6-centos7-test-pxf-mapr">gpdb6-centos7-test-pxf-mapr</a> </td>
    <td> N/A </td>
  </tr>
</table>