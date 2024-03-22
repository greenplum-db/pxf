# PXF

PXF is a query federation engine that provides connectors to access data 
residing in external systems such as Hadoop, Hive, HBase, relational databases,
S3, Google Cloud Storage, among other external systems.

PXF uses the [External Table Framework](https://gpdb.docs.pivotal.io/latest/ref_guide/sql_commands/CREATE_EXTERNAL_TABLE.html)
in Greenplum 5 and 6 to access external data. Greenplum 6 introduces the 
[Foreign Data Wrapper Framework](https://gpdb.docs.pivotal.io/6-0Beta/admin_guide/external/g-devel-fdw.html)
to access external data, and extensions are starting to move to the foreign
data wrapper (FDW) framework because the External Table Framework will be 
deprecated in later versions of Greenplum. 

# PXF Foreign Data Wrapper

For every PXF connector, we propose a PXF Foreign Data Wrapper. PXF will provide
the following Foreign Data Wrappers.

    CREATE FOREIGN DATA WRAPPER <connector>_pxf_fdw
        HANDLER pxf_fdw_handler
        VALIDATOR pxf_fdw_validator
        OPTIONS ( protocol '<protocol_name>' [, ... ] );

### PXF Native Foreign Data Wrappers

1. **jdbc_pxf_fdw**     Provides access to databases through JDBC
2. **hdfs_pxf_fdw**     Provides access to Hadoop filesystem
2. **hive_pxf_fdw**     Provides access to Hive
2. **hbase_pxf_fdw**    Provides access to HBase
2. **s3_pxf_fdw**       Provides access to S3
2. **gs_pxf_fdw**       Provides access to Google Cloud Storage
2. **abfss_pxf_fdw**    Provides access to Azure Datalake Storage Gen 2
2. **wasbs_pxf_fdw**    Provides access to Microsoft's Azure Blob Storage
2. **file_pxf_fdw**     Provides access to local file storage

The PXF Foreign Data Wrapper will create:

    CREATE FOREIGN DATA WRAPPER jdbc_pxf_fdw
        HANDLER pxf_fdw_handler
        VALIDATOR pxf_fdw_validator
        OPTIONS ( protocol 'jdbc' );

    CREATE FOREIGN DATA WRAPPER hdfs_pxf_fdw
        HANDLER pxf_fdw_handler
        VALIDATOR pxf_fdw_validator
        OPTIONS ( protocol 'hdfs' );

    CREATE FOREIGN DATA WRAPPER hive_pxf_fdw
        HANDLER pxf_fdw_handler
        VALIDATOR pxf_fdw_validator
        OPTIONS ( protocol 'hive' );

    CREATE FOREIGN DATA WRAPPER hbase_pxf_fdw
        HANDLER pxf_fdw_handler
        VALIDATOR pxf_fdw_validator
        OPTIONS ( protocol 'hbase' );

    CREATE FOREIGN DATA WRAPPER s3_pxf_fdw
        HANDLER pxf_fdw_handler
        VALIDATOR pxf_fdw_validator
        OPTIONS ( protocol 's3' );

    CREATE FOREIGN DATA WRAPPER gs_pxf_fdw
        HANDLER pxf_fdw_handler
        VALIDATOR pxf_fdw_validator
        OPTIONS ( protocol 'gs' );

    CREATE FOREIGN DATA WRAPPER abfss_pxf_fdw
        HANDLER pxf_fdw_handler
        VALIDATOR pxf_fdw_validator
        OPTIONS ( protocol 'abfss' );

    CREATE FOREIGN DATA WRAPPER wasbs_pxf_fdw
        HANDLER pxf_fdw_handler
        VALIDATOR pxf_fdw_validator
        OPTIONS ( protocol 'wasbs' );

    CREATE FOREIGN DATA WRAPPER file_pxf_fdw
        HANDLER pxf_fdw_handler
        VALIDATOR pxf_fdw_validator
        OPTIONS ( protocol 'localfile' );
      
### New Connectors during PXF Upgrades 
      
When a new version of PXF ships a new connector, the upgrade process will include
an upgrade SQL file called `pxf_fdw--1.0--2.0.sql` (assuming the upgrade is
from version 1.0 to 2.0). This file will contain the new foreign data wrappers
shipped in the new version. For example:
      
    CREATE FOREIGN DATA WRAPPER new_connector_pxf_fdw
      VALIDATOR pxf_fdw_validator
      HANDLER pxf_fdw_handler
      OPTIONS ( protocol 'new_connector' );
      
### PXF Community Foreign Data Wrappers

Community connectors can create it's own Foreign Data Wrapper by providing the
`protocol` option at the wrapper level. For example,
      
    CREATE FOREIGN DATA WRAPPER connector_name_fdw
      VALIDATOR pxf_fdw_validator
      HANDLER pxf_fdw_handler
      OPTIONS ( protocol 'connector_name' );
      
### Conclusions
      
In summary, we make the `protocol` option mandatory at the wrapper level,
with the following constraints:

- When you create a PXF Foreign Data Wrapper, the `protocol` option is mandatory
- Users cannot drop the `protocol` option from an existing PXF foreign data wrapper
- Users cannot alter the `protocol` option from an existing PXF foreign data wrapper

# Creating Servers for Different Connectors

This section provides an overview of the creation of different servers supported
by PXF.

    CREATE SERVER <server_name>
        FOREIGN DATA WRAPPER <connector>_pxf_fdw
        [ OPTIONS ( option 'value' [, ... ] ) ];

## External Database Access

The PXF JDBC connector provides access to external databases. 

### Oracle Server Example

     CREATE SERVER oracle_server 
         FOREIGN DATA WRAPPER jdbc_pxf_fdw
         OPTIONS ( 
             jdbc_driver 'oracle.jdbc.driver.OracleDriver', 
             db_url 'jdbc:oracle:thin:@0.0.0.0:32771:ORCLCDB',
             batch_size '10000',
             fetch_size '2000'
         );

## Cloud Object Storage Access

PXF provides out-of-the-box support for S3, Google Cloud Storage, Azure Blob File System,
and Azure Blob Storage access. Users need to create a server for each cloud access.

### S3 Server Example

     CREATE SERVER s3_server
          FOREIGN DATA WRAPPER s3_pxf_fdw
          OPTIONS ( accesskey 'MY_AWS_ACCESS_KEY', secretkey 'MY_AWS_SECRET_KEY' );

### Google Cloud Storage Example

     CREATE SERVER gs_server
          FOREIGN DATA WRAPPER gs_pxf_fdw;

### Azure Blob File System Example

     CREATE SERVER abfss_server
          FOREIGN DATA WRAPPER abfss_pxf_fdw;

### Azure Blob Storage Example

     CREATE SERVER wasbs_server
          FOREIGN DATA WRAPPER wasbs_pxf_fdw;

## Hadoop Access

Access to Hadoop clusters is nuanced, because with a single set of configurations
users are able to access HDFS, Hive, HBase and other services.

Suppose an enterprise user has a Hortonworks hadoop installation that includes
HDFS, Hive, and HBase. We would configure one server per technology we access, 
for example:

     CREATE SERVER hdfs_hdp
          FOREIGN DATA WRAPPER hdfs_pxf_fdw
          OPTIONS ( config 'hdp_1' );
     
     CREATE SERVER hive_hdp
          FOREIGN DATA WRAPPER hive_pxf_fdw
          OPTIONS ( config 'hdp_1' );
     
     CREATE SERVER hbase_hdp
          FOREIGN DATA WRAPPER hbase_pxf_fdw
          OPTIONS ( config 'hdp_1' );
          
To reduce the amount of configuration required for each server, we introduce a 
new option `config`. This new option provides the name of the server directory where
the configuration files reside. In the example above, configuration files live
in the `$PXF_BASE/servers/hdp_1` directory, and all three servers share the same
configuration directory.

The `config` option is intended for Hadoop based configurations, but we do
not intend to restrict usage by other connectors.

## Local Filesystem Access

Occasionally, access to local file system is required. PXF provides access to 
local file system using the file_pxf_fdw. The `mount_path` option needs to be
specified to access the local filesystem.

     CREATE SERVER local_file_system
          FOREIGN DATA WRAPPER file_pxf_fdw
          OPTIONS ( mount_path '/data/directory/' );

# User Mappings

User mappings allows users to access servers. We can add user specific options
in a user mapping. For example, if a user accesses the oracle server above, he
needs to provide his oracle username/password in the user mapping options.

    CREATE USER MAPPING FOR { user_name | USER | CURRENT_USER | PUBLIC }
        SERVER <server_name>
        [ OPTIONS ( option 'value' [ , ... ] ) ]

### Oracle User Mapping Example

     CREATE USER MAPPING FOR francisco
         SERVER oracle_server
         OPTIONS ( user 'francisco' , pass 'my-oracle-password' );

### S3 User Mapping Example

     CREATE USER MAPPING FOR francisco
         SERVER s3_server
         OPTIONS ( accesskey 'FRANCISCOS_AWS_ACCESS_KEY', secretkey 'FRANCISCOS_AWS_SECRET_KEY' );

**Note**: Alternatively, user-specific information can be stored in a plain text
file, that is only accessible by the gpadmin user.

# Mapping Foreign Data Wrappers to PXF Profiles

PXF uses the concept of Profiles in the [External Table Framework](https://gpdb.docs.pivotal.io/latest/ref_guide/sql_commands/CREATE_EXTERNAL_TABLE.html).
In foreign data wrappers, we map a combination of the wrapper `protocol` and the
table `format` to a profile. For example, in the case of accessing parquet files
on  S3, the `protocol` is `s3` and the format is `parquet`, which maps to the
corresponding `s3:parquet` profile.

To create a foreign table, a `resource` must be provided. Optionally, a `format`
option can be provided.

    CREATE FOREIGN TABLE [ IF NOT EXISTS ] <table_name> ( [
        column_name data_type [ OPTIONS ( option 'value' [, ... ] ) ] [ COLLATE collation ] [ column_constraint [ ... ] ]
        [, ... ]
    ] )
      SERVER server_name
      OPTIONS ( resource 'value' [ format '<file_format>' , ... ] );

Existing profiles are supported by foreign data wrappers and are mapped as
follows:

#### HBase
     
     CREATE FOREIGN TABLE hbase
          SERVER hbase_hdp
          OPTIONS ( resource 'table_name' );

#### Hive
     
     CREATE FOREIGN TABLE hive
          SERVER hive_hdp
          OPTIONS ( resource 'dbname.table_name' );

#### HiveRC
     
     CREATE FOREIGN TABLE hive_rc
          SERVER hive_hdp
          OPTIONS ( resource 'dbname.table_name', format 'rc' );

#### HiveText
     
     CREATE FOREIGN TABLE hive_text
          SERVER hive_hdp
          OPTIONS ( resource 'dbname.table_name', format 'text' );

#### HiveORC
     
     CREATE FOREIGN TABLE hive_orc
          SERVER hive_hdp
          OPTIONS ( resource 'dbname.table_name', format 'orc' );

#### HiveVectorizedORC
     
     CREATE FOREIGN TABLE hive_vectorized_orc
          SERVER hive_hdp
          OPTIONS ( resource 'dbname.table_name', format 'vectorizedorc' );

#### Avro

**Note:** Same as [hdfs:avro](#hdfsavro)

#### SequenceWritable

**Note:** Same as [hdfs:SequenceFile](#hdfssequenceFile)

#### SequenceText

**Note:** Same as [hdfs:SequenceFile](#hdfsSequenceFile)

#### Json

**Note:** Same as [hdfs:json](#hdfsjson)

#### Jdbc

     CREATE FOREIGN TABLE employees (...)
          SERVER oracle_server
          OPTIONS ( resource 'dbname.table_name' );

##### Named Query on Oracle Server

     CREATE FOREIGN TABLE employees (...)
          SERVER oracle_server
          OPTIONS ( resource 'query:my-query-file' );


#### Parquet

**Note:** Same as [hdfs:parquet](#hdfsparquet)

#### hdfs:text

     CREATE FOREIGN TABLE hdfs_text
          SERVER hdfs_hdp
          OPTIONS ( resource '/hdfs/path/to/text/files', format 'text' );

#### hdfs:text:multi

     CREATE FOREIGN TABLE hdfs_text_multi
          SERVER hdfs_hdp
          OPTIONS ( resource '/hdfs/path/to/text/files', format 'text:multi' );

##### FILE_AS_ROW each file as a single row

     CREATE FOREIGN TABLE hdfs_text_file_as_row
          SERVER hdfs_hdp
          OPTIONS ( resource '/hdfs/path/to/text/files', format 'text:multi', file_as_row 'true' );

#### s3:text

     CREATE FOREIGN TABLE s3_text
          SERVER s3_server
          OPTIONS ( resource '/bucket/file', format 'text' );

**Note:** Minio can be accessed with the S3 foreign data wrapper

#### s3:text:multi

     CREATE FOREIGN TABLE s3_text_multi
          SERVER s3_server
          OPTIONS ( resource '/bucket/multi/file', format 'text:multi' );

#### abfss:text

     CREATE FOREIGN TABLE abfss_text
          SERVER abfss_server
          OPTIONS ( resource '/YOUR_ABFSS_ACCOUNT.dfs.core.windows.net/file', format 'text' );

#### abfss:text:multi

     CREATE FOREIGN TABLE abfss_text_multi
          SERVER abfss_server
          OPTIONS ( resource '/YOUR_ABFSS_ACCOUNT.dfs.core.windows.net/file', format 'text:multi' );

#### gs:text

     CREATE FOREIGN TABLE gs_text
          SERVER gs_server
          OPTIONS ( resource '/bucket/text/file', format 'text' );

#### gs:text:multi

     CREATE FOREIGN TABLE gs_text_multi
          SERVER gs_server
          OPTIONS ( resource '/bucket/text/multi/file', format 'text:multi' );

#### hdfs:parquet

     CREATE FOREIGN TABLE hdfs_parquet
          SERVER hdfs_hdp
          OPTIONS ( resource '/hdfs/path/to/parquet/files', format 'parquet' );

     CREATE FOREIGN TABLE hdfs_parquet_gzip
          SERVER hdfs_hdp
          OPTIONS ( resource '/hdfs/path/to/parquet/gzip/files', format 'parquet', compression_codec 'gzip' );

#### s3:parquet

     CREATE FOREIGN TABLE s3_parquet
          SERVER s3_server
          OPTIONS ( resource '/bucket/parquet/file', format 'parquet' );

#### abfss:parquet

     CREATE FOREIGN TABLE abfss_parquet
          SERVER abfss_server
          OPTIONS ( resource '/YOUR_ABFSS_ACCOUNT.dfs.core.windows.net/parquet/file', format 'parquet' );

#### gs:parquet

     CREATE FOREIGN TABLE gs_parquet
          SERVER gs_server
          OPTIONS ( resource '/bucket/parquet/file', format 'parquet' );

#### hdfs:avro

     CREATE FOREIGN TABLE hdfs_avro
          SERVER hdfs_hdp
          OPTIONS ( resource '/hdfs/path/to/avro/files', format 'avro' );

#### s3:avro

     CREATE FOREIGN TABLE s3_avro
          SERVER s3_server
          OPTIONS ( resource '/bucket/avro/file', format 'avro' );

#### abfss:avro

     CREATE FOREIGN TABLE abfss_avro
          SERVER abfss_server
          OPTIONS ( resource '/YOUR_ABFSS_ACCOUNT.dfs.core.windows.net/avro/file', format 'avro' );

#### gs:avro

     CREATE FOREIGN TABLE gs_avro
          SERVER gs_server
          OPTIONS ( resource '/bucket/avro/file', format 'avro' );

#### hdfs:json

     CREATE FOREIGN TABLE hdfs_json
          SERVER hdfs_hdp
          OPTIONS ( resource '/hdfs/path/to/json/files', format 'json' );

#### s3:json

     CREATE FOREIGN TABLE s3_json
          SERVER s3_server
          OPTIONS ( resource '/bucket/json/file', format 'json' );

#### abfss:json

     CREATE FOREIGN TABLE abfss_json
          SERVER abfss_server
          OPTIONS ( resource '/YOUR_ABFSS_ACCOUNT.dfs.core.windows.net/json/file', format 'json' );

#### gs:json

     CREATE FOREIGN TABLE gs_json
          SERVER gs_server
          OPTIONS ( resource '/bucket/json/file', format 'json' );

#### hdfs:AvroSequenceFile

     CREATE FOREIGN TABLE hdfs_AvroSequenceFile
          SERVER hdfs_hdp
          OPTIONS ( resource '/hdfs/path/to/AvroSequenceFile/files', format 'AvroSequenceFile' );

#### s3:AvroSequenceFile

     CREATE FOREIGN TABLE s3_AvroSequenceFile
          SERVER s3_server
          OPTIONS ( resource '/bucket/AvroSequenceFile/file', format 'AvroSequenceFile' );

#### abfss:AvroSequenceFile

     CREATE FOREIGN TABLE abfss_AvroSequenceFile
          SERVER abfss_server
          OPTIONS ( resource '/YOUR_ABFSS_ACCOUNT.dfs.core.windows.net/AvroSequenceFile', format 'AvroSequenceFile' );

#### gs:AvroSequenceFile

     CREATE FOREIGN TABLE gs_AvroSequenceFile
          SERVER gs_server
          OPTIONS ( resource '/bucket/AvroSequenceFile/file', format 'AvroSequenceFile' );

#### hdfs:SequenceFile

     CREATE FOREIGN TABLE hdfs_SequenceFile
          SERVER hdfs_hdp
          OPTIONS ( resource '/hdfs/path/to/SequenceFile/files', format 'SequenceFile' );

#### s3:SequenceFile

     CREATE FOREIGN TABLE s3_SequenceFile
          SERVER s3_server
          OPTIONS ( resource '/bucket/SequenceFile/file', format 'SequenceFile' );

#### abfss:SequenceFile

     CREATE FOREIGN TABLE abfss_SequenceFile
          SERVER abfss_server
          OPTIONS ( resource '/YOUR_ABFSS_ACCOUNT.dfs.core.windows.net/SequenceFile', format 'SequenceFile' );

#### gs:SequenceFile

     CREATE FOREIGN TABLE gs_SequenceFile
          SERVER gs_server
          OPTIONS ( resource '/bucket/SequenceFile/file', format 'SequenceFile' );

#### wasbs:text

     CREATE FOREIGN TABLE wasbs_text
          SERVER wasbs_server
          OPTIONS ( resource '/pxf-container@YOUR_WASB_ACCOUNT_NAME.blob.core.windows.net/text/file', format 'text' );

#### wasbs:text:multi

     CREATE FOREIGN TABLE wasbs_text_multi
          SERVER wasbs_server
          OPTIONS ( resource '/pxf-container@YOUR_WASB_ACCOUNT_NAME.blob.core.windows.net/text/multi/file', format 'text:multi' );

#### wasbs:parquet

     CREATE FOREIGN TABLE wasbs_parquet
          SERVER wasbs_server
          OPTIONS ( resource '/pxf-container@YOUR_WASB_ACCOUNT_NAME.blob.core.windows.net/parquet/file', format 'parquet' );

#### wasbs:avro

     CREATE FOREIGN TABLE wasbs_avro
          SERVER wasbs_server
          OPTIONS ( resource '/pxf-container@YOUR_WASB_ACCOUNT_NAME.blob.core.windows.net/avro/file', format 'avro' );

#### wasbs:json

     CREATE FOREIGN TABLE wasbs_json
          SERVER wasbs_server
          OPTIONS ( resource '/pxf-container@YOUR_WASB_ACCOUNT_NAME.blob.core.windows.net/json/file', format 'json' );

#### wasbs:AvroSequenceFile

     CREATE FOREIGN TABLE wasbs_AvroSequenceFile
          SERVER wasbs_server
          OPTIONS ( resource '/pxf-container@YOUR_WASB_ACCOUNT_NAME.blob.core.windows.net/AvroSequenceFile/file', format 'AvroSequenceFile' );

#### wasbs:SequenceFile

     CREATE FOREIGN TABLE wasbs_SequenceFile
          SERVER wasbs_server
          OPTIONS ( resource '/pxf-container@YOUR_WASB_ACCOUNT_NAME.blob.core.windows.net/SequenceFile/file', format 'SequenceFile' );
          
The `resource` option is mandatory when you create a PXF foreign table.
The `resource` option cannot be dropped from the foreign table.
The `resource` option can be altered from the foreign table.

The `format` option is optional.
The `format` option can be added if not present.
The `format` option can be altered when present.
The `format` option can be dropped when present.
