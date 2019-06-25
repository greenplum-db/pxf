# Changelog

## 5.6.0 (06/19/2019)

#### Bug Fixes:

- [#182](https://github.com/greenplum-db/pxf/pull/182) Run a named query that ends with semicolon

#### Enhancements:

- [#188](https://github.com/greenplum-db/pxf/pull/188) Support for FDW
- [#186](https://github.com/greenplum-db/pxf/pull/186) Enable JDBC Connection Pooling
- [#183](https://github.com/greenplum-db/pxf/pull/183) Upgrade postgres driver to version 42.2.5

---

## 5.5.1 (06/03/2019)

#### Bug Fixes:

- [#180](https://github.com/greenplum-db/pxf/pull/180) Upgrade the Postgres JDBC Driver version

---

## 5.5.0 (05/31/2019)

#### Bug Fixes:

- [#176](https://github.com/greenplum-db/pxf/pull/176) Upgrade jackson 2 version from 2.9.8 -> 2.9.9

#### Enhancements:

- [#171](https://github.com/greenplum-db/pxf/pull/171) Enable JDBC connection to Hive and JDBC-specific user impersonation per server
- [#178](https://github.com/greenplum-db/pxf/pull/178) Make pxf threads configurable
- [#172](https://github.com/greenplum-db/pxf/pull/172) Support user-specific configuration in server
- [#169](https://github.com/greenplum-db/pxf/pull/169) HdfsFileFragmenter
- [#162](https://github.com/greenplum-db/pxf/pull/162) Kill JVM/Tomcat on OutOfMemoryError
- [#160](https://github.com/greenplum-db/pxf/pull/160) JDBC: Optimize resolver for INSERT queries

---

## 5.4.0 (05/13/2019)

#### Bug Fixes:

- [#161](https://github.com/greenplum-db/pxf/pull/161) Fix Oracle timestamp wrapping

#### Enhancements:

- [#157](https://github.com/greenplum-db/pxf/pull/157) JDBC profile can execute a query to read data from an external DB
- [#158](https://github.com/greenplum-db/pxf/pull/158) Add support to read multiline files as a single row

---

## 5.3.2 (05/06/2019)

#### Bug Fixes:

- [#156](https://github.com/greenplum-db/pxf/pull/156) JDBC statement properties including fetch size and timeout

---

## 5.3.1 (04/27/2019)

#### Bug Fixes:

- [#151](https://github.com/greenplum-db/pxf/pull/151) PXF cli: fix regression with version command

#### Enhancements:

- [#152](https://github.com/greenplum-db/pxf/pull/152) Ensure pxf version can be run before pxf init

---

## 5.3.0 (04/26/2019)

#### Bug Fixes:

- [#147](https://github.com/greenplum-db/pxf/pull/147) Reverse direction of rsync in pxf sync command
- [#144](https://github.com/greenplum-db/pxf/pull/144) Remove support for Logical operator NOT with Hive Partition Filtering. NOT is an unsupported logical operator
- [#138](https://github.com/greenplum-db/pxf/pull/138) Hive partition filtering with support for all Logical Operators
- [#134](https://github.com/greenplum-db/pxf/pull/134) pxf cluster: stop checking that hostname is master

#### Enhancements:

- [#150](https://github.com/greenplum-db/pxf/pull/150) Add debug statements for the JDBC connection
- [#149](https://github.com/greenplum-db/pxf/pull/149) pxf cli: Add cluster status command
- [#142](https://github.com/greenplum-db/pxf/pull/142) Allow configuration of JDBC transaction isolation. Implements [#130](https://github.com/greenplum-db/pxf/issues/130)
- [#145](https://github.com/greenplum-db/pxf/pull/145) Added integration test for JDBC session parameters
- [#135](https://github.com/greenplum-db/pxf/pull/135) Cache Fragmenter calls to improve memory consumption during the fragmenter call
- [#136](https://github.com/greenplum-db/pxf/pull/136) pxf-cli: Support sync and init on standby master
- [#141](https://github.com/greenplum-db/pxf/pull/141) pxf-api: Fix BaseConfigurationFactory logging
- [#118](https://github.com/greenplum-db/pxf/pull/118) PXF-JDBC: Enable external database configuration and connection settings modification. Implements [#129](https://github.com/greenplum-db/pxf/issues/129)
- [#133](https://github.com/greenplum-db/pxf/pull/133) Add Changelog
- pxf-cli: Use rsync on master host

---

## 5.2.1 (04/04/2019)

#### Enhancements:

- [#115](https://github.com/greenplum-db/pxf/pull/115)
  PXF no longer expects the path to contain
  transaction and segment IDs during write. PXF will
  now construct the write path for Hadoop-Compatible
  FileSystems to include transaction and segment IDs. 

---

## 5.2.0 (04/04/2019)

#### Enhancements:

- [#119](https://github.com/greenplum-db/pxf/pull/119) Remove PXF-Ignite plugin. The Ignite plugin is
  removed in favor of Ignite's JDBC driver.
- Adds more visibility to external contributors by exposing
  Pull Request pipelines. It allows external contributors
  to debug issues when submitting Pull Requests.
  
#### Bug Fixes:

- [#126](https://github.com/greenplum-db/pxf/pull/126) PXF Port Fix. Fixes issue in PXF when setting
  PXF_PORT and then starting PXF. Fixes issue [#93](https://github.com/greenplum-db/pxf/issues/93)

---

## 5.1.1 (03/26/2019)

#### Bug Fixes:

- [#116](https://github.com/greenplum-db/pxf/pull/116) Always use doAs for Kerberos with Hive, add
  request's hive-site.xml to HiveConf explicitly. Fixes
  issues with Kerberized Hive, where UGI was not being set.

---

## 5.1.0 (03/26/2019)

#### Bug Fixes:

- [#80](https://github.com/greenplum-db/pxf/pull/80) Throw IOException when fs.mkdirs() returns false

#### Enhancements:

- Improve Documentation
- [#114](https://github.com/greenplum-db/pxf/pull/114) enable file-based configuration for JDBC plugin
- [#113](https://github.com/greenplum-db/pxf/pull/113) added PARQUET_VERSION parameter and tests
- [#112](https://github.com/greenplum-db/pxf/pull/112) Support additional parquet write config options
- [#111](https://github.com/greenplum-db/pxf/pull/111) Fixed propagation of write exception from the JDBC plugin
- [#110](https://github.com/greenplum-db/pxf/pull/110) Parquet column projection
- [#108](https://github.com/greenplum-db/pxf/pull/108) Enabled column projection pushdown for JDBC profile
- [#101](https://github.com/greenplum-db/pxf/pull/101) Update logging configuration to limit Hadoop INFO logging
- Add descriptive message when JAVA_HOME is not set
- [#98](https://github.com/greenplum-db/pxf/pull/98) PXF-JDBC: quote column names
- [#95](https://github.com/greenplum-db/pxf/pull/95) Enable license generation for PXF
- [#94](https://github.com/greenplum-db/pxf/pull/94) Column projection support changes
- [#92](https://github.com/greenplum-db/pxf/pull/92) Enhanced unit test for repeated primitive Parquet types
- [#91](https://github.com/greenplum-db/pxf/pull/91) Create new groups for hive and hbase tests
- [#89](https://github.com/greenplum-db/pxf/pull/89) Implement optimized version of isTextForm
- [#88](https://github.com/greenplum-db/pxf/pull/88) Support Parquet repeated primitive types serialized into JSON
- [#87](https://github.com/greenplum-db/pxf/pull/87) Updated library versions with security issues
- [#86](https://github.com/greenplum-db/pxf/pull/86) Remove Parquet fragmenter; defer schema read to accessor
- Performance Tests
- [#81](https://github.com/greenplum-db/pxf/pull/81) Upgrade to hadoop version 2.9.2
- [#77](https://github.com/greenplum-db/pxf/pull/77) Add MapR Support for HDFS

---

## 5.0.1 (01/15/2019)
*No changelog for this release.*

---

## 5.0.0 (01/14/2019)
*Changelog needed here.*

---

## 4.1.0 (12/04/2018)
*Changelog needed here.*

---

## 4.0.3 (10/16/2018)
*Changelog needed here.*

---

## 4.0.2 (10/12/2018)
*Changelog needed here.*

---

## 4.0.1 (10/12/2018)
*Changelog needed here.*

---

## 4.0.0 (10/11/2018)
*Changelog needed here.*

---
