# Changelog

## 6.4.1 (09/15/2022)

### Bug Fixes:

- [#858](https://github.com/greenplum-db/pxf/pull/858) Add JsonProtocolHandler to use HdfsFileFragmenter for multi-line JSON

## 6.4.0 (08/15/2022)

### Enhancements:

- [#818](https://github.com/greenplum-db/pxf/pull/818) Add support for writing ORC primitive types
- [#836](https://github.com/greenplum-db/pxf/pull/836) Add write support for one-dimensional ORC arrays
- [#842](https://github.com/greenplum-db/pxf/pull/842) Add support for using a PreparedStatement when reading

### Bug Fixes:

- [#833](https://github.com/greenplum-db/pxf/pull/833) Bump aws-java-sdk-s3 from 1.11.472 to 1.12.261
- [#838](https://github.com/greenplum-db/pxf/pull/838) Upgrade org.xerial.snappy:snappy-java to 1.1.8.4
- [#845](https://github.com/greenplum-db/pxf/pull/845) Bump postgresql version from 42.3.3 to 42.4.1

## 6.3.2 (07/20/2022)

### Bug Fixes:

- [#781](https://github.com/greenplum-db/pxf/pull/781) Fix: In case of UnsupportedOperationException, Add an error message.
- [#789](https://github.com/greenplum-db/pxf/pull/789) Upgrade to Springboot 2.5.12
- [#799](https://github.com/greenplum-db/pxf/pull/799) Bump jackson-databind from 2.11.0 to 2.12.6.1
- [#814](https://github.com/greenplum-db/pxf/pull/814) Add data buffer boundary checks to PXF extension
- [#815](https://github.com/greenplum-db/pxf/pull/815) Upgrade ORC version to 1.6.13 to get a fix for ORC-1065
- [#819](https://github.com/greenplum-db/pxf/pull/819) Upgrade Hadoop to 2.10.2
- [#823](https://github.com/greenplum-db/pxf/pull/823) Add unsupported exception in case of hive write

## 6.3.1 (04/27/2022)

### Bug Fixes:

- [#788](https://github.com/greenplum-db/pxf/pull/788) Replace prefix macro with environment variable in scriptlets
- [#794](https://github.com/greenplum-db/pxf/pull/794) Fix NPE in Hive ORC vectorized query execution

## 6.3.0 (03/16/2022)

### Enhancements:

- [#703](https://github.com/greenplum-db/pxf/pull/703) Added Support for Avro Logical Types for Readable External Tables
- [#707](https://github.com/greenplum-db/pxf/pull/707) Enabled Kerberos Constrained Delegation impersonation for secure clusters
- [#752](https://github.com/greenplum-db/pxf/pull/752) Add support for GPDB6 on RHEL 8
- [#754](https://github.com/greenplum-db/pxf/pull/754) Add scripts for modifying PXF extension to support gpupgrade

### Bug Fixes:

- [#738](https://github.com/greenplum-db/pxf/pull/738) Fix: For reading the records correctly from a MultiLine JSON file
- [#756](https://github.com/greenplum-db/pxf/pull/756) Fixed HiveDataFragmenter not closing connections to Hive Metastore
- [#760](https://github.com/greenplum-db/pxf/pull/760) Update bundled postgresql to 42.3.3

## 6.2.3 (01/31/2022)

### Bug Fixes:

- [#720](https://github.com/greenplum-db/pxf/pull/720) Redirect PXF stdout and stderr to files in PXF_LOGDIR
- [#735](https://github.com/greenplum-db/pxf/pull/735) Bumped Log4j2 version to 2.17.1
- [#740](https://github.com/greenplum-db/pxf/pull/740) Bump go version to 1.17.6
- [#741](https://github.com/greenplum-db/pxf/pull/741) Improved performance of iterating over a list of fragments

## 6.2.2 (12/22/2021)

### Bug Fixes:

- [#732](https://github.com/greenplum-db/pxf/pull/732) Downgrade to Spring Boot 2.4.3
- [#733](https://github.com/greenplum-db/pxf/pull/733) Bump log4j2 version to 2.17.0

## 6.2.1 (12/16/2021)

### Bug Fixes:

- [#710](https://github.com/greenplum-db/pxf/pull/710) Allow skipping the header for *:text:multi profiles
- [#719](https://github.com/greenplum-db/pxf/pull/719) Add explicit UnsupportedException for Hive transactional tables
- [#721](https://github.com/greenplum-db/pxf/pull/721) Set default MySQL fetchSize to Integer.MIN_VALUE
- [#726](https://github.com/greenplum-db/pxf/pull/726) pxf-hive: Catch TTransportException when working with metastore client
- [#727](https://github.com/greenplum-db/pxf/pull/727) bump log4j2 version to 2.16.0

## 6.2.0 (09/08/2021)

### Enhancements:

- [#662](https://github.com/greenplum-db/pxf/pull/662) Remove the error-causing check
- [#670](https://github.com/greenplum-db/pxf/pull/670) Upgrade Spring Boot to 2.5.2, Gradle to 7
- [#675](https://github.com/greenplum-db/pxf/pull/675) pxf-hdfs: support for reading lists from ORC files
- [#688](https://github.com/greenplum-db/pxf/pull/688) Introduced operation retries when GSS connection failures are encountered
- [#687](https://github.com/greenplum-db/pxf/pull/687) Enhanced logging to include fragment info and minor alignment changes
- [#689](https://github.com/greenplum-db/pxf/pull/689) external-table: add simple cURL debug callback function

### Bug Fixes:

- [#680](https://github.com/greenplum-db/pxf/pull/680) fix CURLOPT_RESOLVE optimization
- [#691](https://github.com/greenplum-db/pxf/pull/691) Added dataSource to Fragmenter Cache key, made cache expiration configurable
- [#696](https://github.com/greenplum-db/pxf/pull/696) Enum and bytea types are now handled properly for external tables using FORMAT "TEXT" or FORMAT "CSV"
- [#697](https://github.com/greenplum-db/pxf/pull/697) Fixed NullPointerException in GSS failure handling retry logic

## 6.1.0 (06/08/2021)

### Enhancements:

- [#633](https://github.com/greenplum-db/pxf/pull/633) Upgrade go dependencies
- [#636](https://github.com/greenplum-db/pxf/pull/636) Add support for reading and writing arrays in AVRO
- [#638](https://github.com/greenplum-db/pxf/pull/638) Report exception class if there's no exception message
- [#640](https://github.com/greenplum-db/pxf/pull/640) Support reading JSON arrays and objects into Greenplum text columns
- [#644](https://github.com/greenplum-db/pxf/pull/644) Allow configuring connection timeout for data uploads to PXF

## 6.0.1 (05/11/2021)

### Bug Fixes:

- [#624](https://github.com/greenplum-db/pxf/pull/624) Deprecated HiveVectorizedORC profile should follow vectorized execution path
- [#626](https://github.com/greenplum-db/pxf/pull/626) Hive connector should clone SerDe properties per fragment
- [#627](https://github.com/greenplum-db/pxf/pull/627) Fix NullPointerException for ORC textMapper function when the column is repeating
- [#630](https://github.com/greenplum-db/pxf/pull/630) Fix the inconsistency between row count in external table and ORC file

## 6.0.0 (03/25/2021)

### Enhancements:

- [#404](https://github.com/greenplum-db/pxf/pull/404) Migrate PXF to Spring Boot
- [#491](https://github.com/greenplum-db/pxf/pull/491) Remove invalid GemFireXD Profile
- [#486](https://github.com/greenplum-db/pxf/pull/486) Serialize fragment metadata using kryo instead of json for better optimization
- [#457](https://github.com/greenplum-db/pxf/pull/457) Convert PXF-CLI to use go modules instead of dep
- [#498](https://github.com/greenplum-db/pxf/pull/498) Support pushing predicates of type varchar
- [#506](https://github.com/greenplum-db/pxf/pull/506) Restore FDW build
- [#470](https://github.com/greenplum-db/pxf/pull/470) Add support for reading ORC without Hive
- [#500](https://github.com/greenplum-db/pxf/pull/500) Add the InOperatorTransformer TreeVisitor (transform IN operator into chain of ORs)
- [#514](https://github.com/greenplum-db/pxf/pull/514) Improve logging of read stats(ms instead of ns)
- [#512](https://github.com/greenplum-db/pxf/pull/512) Encode header values for custom headers, add disable_ppd option for PXF FDW extension, and add pushing predicates of type varchar down for the PXF FDW extension
- [#495](https://github.com/greenplum-db/pxf/pull/495) Hive profile names now split "protocol" and "format"
- [#521](https://github.com/greenplum-db/pxf/pull/521) Bump Hadoop version to 2.10.1
- [#538](https://github.com/greenplum-db/pxf/pull/538) Add createParent option for SequenceFile during PXF write
- [#535](https://github.com/greenplum-db/pxf/pull/535) Add shortnames and "uncompressed" option for text compression codecs
- [#548](https://github.com/greenplum-db/pxf/pull/548) Update PXF CLI to support PXF on master
- [#546](https://github.com/greenplum-db/pxf/pull/546) Add Prometheus metrics endpoint
- [#555](https://github.com/greenplum-db/pxf/pull/555) Remove fragmenter call from PXF FDW extension
- [#557](https://github.com/greenplum-db/pxf/pull/557) Log OOM issues to PXF_LOGDIR
- [#542](https://github.com/greenplum-db/pxf/pull/542) Pass data encoding and database encoding from PXF client to server
- [#568](https://github.com/greenplum-db/pxf/pull/568) Support different charsets in PXF FDW extension
- [#573](https://github.com/greenplum-db/pxf/pull/573) Add trace and table headers to the request
- [#572](https://github.com/greenplum-db/pxf/pull/572) Add custom tags for MVC
- [#575](https://github.com/greenplum-db/pxf/pull/575) Add charset to Console and RollingFile appender
- [#576](https://github.com/greenplum-db/pxf/pull/576) Log empty profile message at INFO level
- [#569](https://github.com/greenplum-db/pxf/pull/569) Bump PXF external-table extension to 2.0
- [#574](https://github.com/greenplum-db/pxf/pull/574) Add application property for configuring logging level
- [#577](https://github.com/greenplum-db/pxf/pull/577) Enhance MDC with PXF context
- [#579](https://github.com/greenplum-db/pxf/pull/579) Report fragments.sent PXF metric
- [#571](https://github.com/greenplum-db/pxf/pull/571) Add PXF version header to request
- [#583](https://github.com/greenplum-db/pxf/pull/583) Report records.sent metric
- [#586](https://github.com/greenplum-db/pxf/pull/586) Report records.received metric
- [#595](https://github.com/greenplum-db/pxf/pull/595) Add bytes monitoring to PXF
- [#604](https://github.com/greenplum-db/pxf/pull/604) Log error messages with context

### Bug Fixes:

- [#519](https://github.com/greenplum-db/pxf/pull/519) Update the error message when capacity exceeded in PXF

## 5.16.2 (02/23/2021)

### Bug Fixes:

- [#554](https://github.com/greenplum-db/pxf/pull/554) Fix different encoding when using LineRecordReader
- [#553](https://github.com/greenplum-db/pxf/pull/553) Hardcode replicas in fragment
- [#549](https://github.com/greenplum-db/pxf/pull/549) pxfbridge: Return early when context->current_fragment is NULL

## 5.16.1 (01/14/2021)

### Bug Fixes:

- [#526](https://github.com/greenplum-db/pxf/pull/526) pxf-hive: properly escape strings in complex data types

## 5.16.0 (11/05/2020)

### Enhancements:

- [#480](https://github.com/greenplum-db/pxf/pull/480) Enable predicate pushdown for Hive profile when accessing Parquet backed tables
- [#477](https://github.com/greenplum-db/pxf/pull/477) CLI: Add --skip-register flag for pxf [cluster] init
- [#474](https://github.com/greenplum-db/pxf/pull/474) Optimize hive metadata
- [#467](https://github.com/greenplum-db/pxf/pull/467) clarify the pxf.fs.basePath description
- [#472](https://github.com/greenplum-db/pxf/pull/472) Enable column projection for Parquet files read via Hive profile
- [#461](https://github.com/greenplum-db/pxf/pull/461) Increase default maximumPoolSize property in jdbc-site.xml
- [#469](https://github.com/greenplum-db/pxf/pull/469) Specify Hive schema column names and types in HiveAccessor when creating RecordReaders
- [#456](https://github.com/greenplum-db/pxf/pull/456) Add support for File Storage (Attached to every segment host)
- [#453](https://github.com/greenplum-db/pxf/pull/453) Remove Configuration from SessionId
- [#453](https://github.com/greenplum-db/pxf/pull/453) Release UGI if there is an error during the filter execution
- [#451](https://github.com/greenplum-db/pxf/pull/451) Support dropping columns in PXF writable external tables
- [#451](https://github.com/greenplum-db/pxf/pull/451) Support dropping columns in PXF readable external tables
- [#445](https://github.com/greenplum-db/pxf/pull/445) pxf.service.user.name is commented out by default in pxf-site.xml

### Bug Fixes:

- [#460](https://github.com/greenplum-db/pxf/pull/460) Avro: fixing NullPointerException for writing NULL values for SMALLINT and BYTEA columns

## 5.15.1 (09/10/2020)

### Bug Fixes:

- [#418](https://github.com/greenplum-db/pxf/pull/418) Parquet performance improvements for write
- [#433](https://github.com/greenplum-db/pxf/pull/433) Parquet Write: Fix physical and logical storage for DATE types
- [#435](https://github.com/greenplum-db/pxf/pull/435) Upgrade from Tomcat 7.0.100 to 7.0.105
- [#439](https://github.com/greenplum-db/pxf/pull/439) Add lib/native directory in PXF_CONF

## 5.15.0 (08/19/2020)

### Enhancements:

- [#392](https://github.com/greenplum-db/pxf/pull/392) Add support for Avro BZip2 and XZ Compression Codecs
- [#395](https://github.com/greenplum-db/pxf/pull/395) Bump com.fasterxml.jackson.core:jackson-* version from 2.9.x to 2.11.0
- [#410](https://github.com/greenplum-db/pxf/pull/410) Allow skipping the header for *:text profiles
- [#421](https://github.com/greenplum-db/pxf/pull/421) Deprecate THREAD_SAFE custom option

### Bug Fixes:

- [#382](https://github.com/greenplum-db/pxf/pull/382) Add missing dependency for Hive profile when accessing CSV files
- [#415](https://github.com/greenplum-db/pxf/pull/415) Hive: Report the correct error message from HiveMetaStoreClientCompatibility1xx
- [#416](https://github.com/greenplum-db/pxf/pull/416) Fix performance issues when writing wide CSV/TEXT rows

---

## 5.14.0 (06/30/2020)

### Enhancements:

- [#389](https://github.com/greenplum-db/pxf/pull/389) Avro: add compression
- [#391](https://github.com/greenplum-db/pxf/pull/391) Certify Oracle Enterprise Linux 7

### Bug Fixes:

- [#383](https://github.com/greenplum-db/pxf/pull/383) Avro: support writing SMALLINT to Avro

---

## 5.13.0 (06/10/2020)

### Enhancements:

- [#341](https://github.com/greenplum-db/pxf/pull/341) Create external-table directory and PXF RPM

---

## 5.12.1 (06/03/2020)

### Enhancements:

- [#360](https://github.com/greenplum-db/pxf/pull/360) Add additional information regarding the pxf.service.user.name property

### Bug Fixes:

- [#358](https://github.com/greenplum-db/pxf/pull/358) Fix Glob Patterns for Hadoop-Compatible FileSystems

---

## 5.12.0 (05/05/2020)

### Enhancements:

- [#336](https://github.com/greenplum-db/pxf/pull/336) Add User Option to allow invalid input paths
- [#333](https://github.com/greenplum-db/pxf/pull/333) Hive: Support column projection on Hive Profiles

### Bug Fixes:

- [#324](https://github.com/greenplum-db/pxf/pull/324) Parquet: Right trim char types during insert

### Dependency updates:

- [#348](https://github.com/greenplum-db/pxf/pull/348) CVE-2020-10672: Upgrade jackson-databind to version 2.9.10.4
- [#343](https://github.com/greenplum-db/pxf/pull/343) Upgrade hive 2.3.7 and support Java 11 for Hive Profiles
- [#332](https://github.com/greenplum-db/pxf/pull/332) Hive: Add missing transitive dependency when reading parquet files
- [#331](https://github.com/greenplum-db/pxf/pull/331) pxf-cli: Update gp-common-go-libs to latest
- [#315](https://github.com/greenplum-db/pxf/pull/315) Bump greenplum-db/gp-common-go-libs to latest

---

## 5.11.2 (03/20/2020)

#### Minor enhancements:

- [#318](https://github.com/greenplum-db/pxf/pull/318) pxf-public classpath is no longer being used
- [#311](https://github.com/greenplum-db/pxf/pull/311) JDBC: Validate batchsize only on write

### Dependency updates:

- [#316](https://github.com/greenplum-db/pxf/pull/316) Tomcat: Address CVE-2020-1938
- [#313](https://github.com/greenplum-db/pxf/pull/313) Update jackson-databind

---

## 5.11.1 (03/02/2020)

### Bug Fixes:

- [#308](https://github.com/greenplum-db/pxf/pull/308) Fix ESCAPE 'OFF' is not processed correctly on PXF side error
- [#306](https://github.com/greenplum-db/pxf/pull/306) Fix JAVA_HOME from $PXF_CONF/conf/pxf-env.sh being overridden

---

## 5.11.0 (02/25/2020)

#### Enhancements:

- [#302](https://github.com/greenplum-db/pxf/pull/302) pxf cluster sync: support deletion of extraneous files
- [#304](https://github.com/greenplum-db/pxf/pull/304) Harden PXF's Tomcat configuration
- [#300](https://github.com/greenplum-db/pxf/pull/300) Implement `pxf cluster restart`
- [#295](https://github.com/greenplum-db/pxf/pull/295) (parquet-refactor) Use the first version of Guava that has a stable Cache API
- [#287](https://github.com/greenplum-db/pxf/pull/287) Update GCS connector jar in automation/prod
- [#286](https://github.com/greenplum-db/pxf/pull/286) Parquet record filter

### Bug Fixes:

- [#305](https://github.com/greenplum-db/pxf/pull/305) pxf cluster init: enforce JAVA_HOME is set
- [#299](https://github.com/greenplum-db/pxf/pull/299) Log org.apache.parquet at WARN level
- [#292](https://github.com/greenplum-db/pxf/pull/292) Fix compilation with JDK 11.
- [#290](https://github.com/greenplum-db/pxf/pull/290) Log ClientAbortException at debug level

---

## 5.10.1 (01/07/2020)

#### Enhancements:

- [#276](https://github.com/greenplum-db/pxf/pull/276) Refactor filter parser code (Non-user facing)

#### Bug Fixes:

- [#283](https://github.com/greenplum-db/pxf/pull/283) Fix parquet write decimal
- [#280](https://github.com/greenplum-db/pxf/pull/280) Introduced pxf.session.user property and JDBC conn pool qualifier

---

## 5.10.0 (12/02/2019)

#### Enhancements:

- [#248](https://github.com/greenplum-db/pxf/pull/268) Enable Avro write
- [#261](https://github.com/greenplum-db/pxf/pull/261) Add Support for impersonation per server
- [#257](https://github.com/greenplum-db/pxf/pull/257) Add Support for Kerberized Hive 3
- [#254](https://github.com/greenplum-db/pxf/pull/254) Merge pxf impersonation jdbc
- [#247](https://github.com/greenplum-db/pxf/pull/247) Add support for multiple kerberos Hadoop and Hive servers

#### Bug Fixes:

- [#268](https://github.com/greenplum-db/pxf/pull/268) Fallback on using LineRecordReader when reading encrypted files

---

## 5.9.2 (10/28/2019)

#### Bug Fixes:

- [#253](https://github.com/greenplum-db/pxf/pull/253) Improve performance of HdfsFileFragmenter

---

## 5.9.1 (10/15/2019)

#### Bug Fixes:

- [#251](https://github.com/greenplum-db/pxf/pull/251) Fix regression in *:text:multi profiles when using wildcards

---

## 5.9.0 (09/25/2019)

#### Enhancements:

- [#243](https://github.com/greenplum-db/pxf/pull/243) Upgrade jackson libraries to 2.9.10
- [#235](https://github.com/greenplum-db/pxf/pull/235) Certify support for Hive 3.1
- [#226](https://github.com/greenplum-db/pxf/pull/226) Add OR and NOT support for JDBC filter pushdown
- [#236](https://github.com/greenplum-db/pxf/pull/236) Upgrade tomcat to version 7.0.96
- [#230](https://github.com/greenplum-db/pxf/pull/230) Add support for Hive 2 (Up to Hive 2.3.6)

#### Notes:

- PXF does not support Hive when running Java 11. As a workaround run PXF on Java 8.

---

## 5.8.2 (09/08/2019)

#### Bug Fixes:

- [#228](https://github.com/greenplum-db/pxf/pull/228) Make JDBC profile not fail when MAPR JAR files override default Hadoop ones
- [#217](https://github.com/greenplum-db/pxf/pull/217) CLI: reset on standby master and don't allow cluster init without PXF_CONF set #217
- [#224](https://github.com/greenplum-db/pxf/pull/224) Fix cloud access when Kerberized Hadoop is present

---

## 5.8.1 (08/22/2019)

#### Enhancements:

- Enable multinode testing against GCP dataproc. Run automation tests against Hadoop 2.9.2 and Hive 2.3.5

---

## 5.8.0 (08/01/2019)

#### Bug Fixes:

- [#211](https://github.com/greenplum-db/pxf/pull/211) Preserve error when re-throwing IOException (#211)

#### Enhancements:

- [#187](https://github.com/greenplum-db/pxf/pull/187) Implement S3 Select
- [#189](https://github.com/greenplum-db/pxf/pull/189) Implement cluster reset command
- [#191](https://github.com/greenplum-db/pxf/pull/191) Support config option to specify the server configuration directory (#191)
- [#198](https://github.com/greenplum-db/pxf/pull/198) Support serializing a list of OneFields to CSV (#198)
- [#201](https://github.com/greenplum-db/pxf/pull/201) Add JDK11 to PXF docker base dev image (#201)
- [#202](https://github.com/greenplum-db/pxf/pull/202) Support NOT and OR operators for S3 Select
- [#203](https://github.com/greenplum-db/pxf/pull/203) Support reading and writing of timestamp with time zone for Parquet (#203)
- [#206](https://github.com/greenplum-db/pxf/pull/206) Add S3 Select support for Parquet using S3-SELECT=AUTO (#206)
- [#207](https://github.com/greenplum-db/pxf/pull/207) Enable support for PXF server to run with Java-11 (#207)
- [#212](https://github.com/greenplum-db/pxf/pull/212) Use format options for S3 Select (#212)
- [#213](https://github.com/greenplum-db/pxf/pull/213) Rename S3-SELECT option -> S3_SELECT (#213)

---

## 5.7.0 (07/08/2019)

#### Bug Fixes:

- [#193](https://github.com/greenplum-db/pxf/pull/193) Fix uncompressed write parquet (#193)
- [#170](https://github.com/greenplum-db/pxf/pull/170) JDBC: Query data from ranges outside of partition range (#170)
- [#192](https://github.com/greenplum-db/pxf/pull/192) JsonResolver: throw BadRecordException on bad JSON

#### Enhancements:

- [#196](https://github.com/greenplum-db/pxf/pull/196) Purge codehaus from codebase, and replace it with fasterXML library (#196)

---

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
