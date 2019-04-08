# Changelog

## 5.2.1 (04/04/2019)

#### Enhancements:

- [#115](https://github.com/greenplum-db/pxf/pull/115) PXF no longer expects the path to contain
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
