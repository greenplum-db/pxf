# PXF JDBC plugin
The PXF JDBC plugin allows to access external databases that implement [the Java Database Connectivity API](http://www.oracle.com/technetwork/java/javase/jdbc/index.html). Both read (SELECT) and write (INSERT) operations are supported by the plugin.

PXF JDBC plugin is a JDBC client. A host running the external database does not need to deploy PXF.


## Prerequisites
Check the following before using the PXF JDBC plugin:

* The PXF JDBC plugin is installed on all PXF nodes;
* The JDBC driver for external database is installed on all PXF nodes;
* All PXF nodes are allowed to connect to the external database.


## Limitations
Both **PXF table and a table in external database must have the same definiton**. Their columns must have the same names, and the columns' types must correspond.

**Not all data types are supported** by the plugin. The following PXF data types are supported:

* `INTEGER`, `BIGINT`, `SMALLINT`
* `REAL`, `FLOAT8`
* `NUMERIC`
* `BOOLEAN`
* `VARCHAR`, `BPCHAR`, `TEXT`
* `DATE`
* `TIMESTAMP`
* `BYTEA`

The `<full_external_table_name>` (see below) **must not match** the [pattern](https://docs.oracle.com/javase/7/docs/api/java/util/regex/Pattern.html) `/.*/[0-9]*-[0-9]*_[0-9]*` (the name must not start with `/` and have an ending that consists of `/` and three groups of numbers of arbitrary length, the first two separated by `-` and the last two separated by `_`. For example, the following table name is not allowed: `/public.table/1-2_3`).

At the moment, one PXF external table cannot serve both SELECT and INSERT queries. A separate PXF external table is required for each type of queries.


## Syntax
```
CREATE [ READABLE | WRITABLE ] EXTERNAL TABLE <table_name> (
    { <column_name> <data_type> [, ...] | LIKE <other_table> }
)
LOCATION (
    'pxf://<full_external_table_name>?<pxf_parameters>[&SERVER=<server_name>]<jdbc_settings>'
)
FORMAT 'CUSTOM' (FORMATTER={'pxfwritable_import' | 'pxfwritable_export'})
```

The **`<pxf_parameters>`** are:
```
{
PROFILE=JDBC
|
FRAGMENTER=org.greenplum.pxf.plugins.jdbc.JdbcPartitionFragmenter
&ACCESSOR=org.greenplum.pxf.plugins.jdbc.JdbcAccessor
&RESOLVER=org.greenplum.pxf.plugins.jdbc.JdbcResolver
}
```

**`<jdbc_settings>`** and **`<server_name>`** are described in [Plugin settings section](#plugin-settings).


## Plugin settings

PXF JDBC plugin has a few settings, some of which are required.

Settings can be set in two sites:

* `LOCATION` clause of external table DDL. Every setting must have format `&<name>=<value>`. Hereinafter setting set in `LOCATION` clause is referred to as "option".

* Configuration file located at `$PXF_CONF/servers/<server_name>/jdbc-site.xml` (on every PXF segment), where `<server_name>` is an arbitrary name (the file is intended to include options specific for each external database server). Hereinafter setting set in configuration file is referred to as "configuration parameter".

If `SERVER` option is not set in external table DDL, PXF will assume it equal to `default` and load configuration files from `$PXF_CONF/servers/default/`. A warning is added to PXF log file if `SERVER` is set to incorrect value (PXF is unable to read the requested configuration file).

If a setting can be set by both option and configuration parameter, option value overrides configuration parameter value.

Note that if setting is provided, its value is checked for correctness.


### List of plugin settings

#### JDBC driver
*Required*

JDBC driver class to use to connect to external database.

* **Option**: `JDBC_DRIVER`
* **Configuration parameter**: `jdbc.driver`
* **Value**: String


#### External database URL
*Required*

URL of external database

* **Option**: `DB_URL`
* **Configuration parameter**: `jdbc.url`
* **Value**: String


#### JDBC user
User name (login) to use to connect to external database.

* **Option**: `USER`
* **Configuration parameter**: `jdbc.user`
* **Value**: String


#### JDBC password
Password to use to connect to external database.

* **Option**: `PASS`
* **Configuration parameter**: `jdbc.password`
* **Value**: String

Password is not required (it is possible to set [JDBC user](#jdbc-user) without setting JDBC password).


#### Quote columns
*Can be set only in `LOCATION` clause of external table DDL*

Whether PXF should quote column names when constructing SQL query to the external database.

* **Option**: `QUOTE_COLUMNS`
* **Value**:
    * not set &mdash; quote column names automatically
    * `true` (case-insensitive) &mdash; quote all column names
    * any other value &mdash; do not quote column names

When this setting is not set, PXF automatically checks whether some column name should be quoted, and if so, it quotes all column names in the query.


#### Partition by
*Can be set only in `LOCATION` clause of external table DDL*

This setting is described in section [partitioning](#Partitioning).

* **Option**: `PARTITION_BY`
* **Value**: String in format `<column>:<column_type>`


#### Partition range
*Can be set only in `LOCATION` clause of external table DDL*

This setting is described in section [partitioning](#Partitioning).

* **Option**: `RANGE`
* **Value**: String in special format


#### Partition interval
*Can be set only in `LOCATION` clause of external table DDL*

This setting is described in section [partitioning](#Partitioning).

* **Option**: `INTERVAL`
* **Value**: String in format `<value>[:<unit>]`


#### Batch size
*Can be set only in `LOCATION` clause of external table DDL*

Size of batch to be used for INSERT queries. This setting is described in section [batching](#batching).

* **Option**: `BATCH_SIZE`
* **Value**: Integer >= 0


#### Pool size
*Can be set only in `LOCATION` clause of external table DDL*

Size of thread pool to use for INSERT queries. This setting is described in section [thread pool](#thread-pool).

* **Option**: `POOL_SIZE`
* **Value**: Integer


#### External database session configuration
*Can be set only in configuration file*

Session-level variables to set in external database before SELECT or INSERT query (generated by PXF) execution. This setting is described in section [external database session configuration](#external-database-session-configuration-1).

* **Configuration parameter**: `jdbc.session.property.<name>`, where `<name>` is the name of a session-level variable
* **Value**: String


#### JDBC connection properties
*Can be set only in configuration file*

Connection properties (`java.util.Properties`) passed to JDBC driver when opening a connection to external database. See [Java documentation](https://docs.oracle.com/javase/8/docs/api/java/sql/DriverManager.html#getConnection-java.lang.String-java.util.Properties-) for the meaning of this object. See external database documentation for information on supported key-value pairs.

* **Configuration parameter**: `jdbc.connection.property.<name>`, where `<name>` is the name of a connection property (key of a key-value pair)
* **Value**: String


## SELECT queries
PXF JDBC plugin allows to perform SELECT queries to external tables.

To perform SELECT queries, create an `EXTERNAL READABLE TABLE` or just an `EXTERNAL TABLE` with `FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import')` in PXF.


### `EXTERNAL READABLE TABLE` example
The following example shows how to access a MySQL table via PXF JDBC plugin.

Suppose MySQL instance is available at `192.168.200.6:3306`. A table in MySQL is created:
```
use demodb;
create table myclass(
    id int(4) not null primary key,
    name varchar(20) not null,
    degree double(16,2)
);
```

Then some data is inserted into MySQL table:
```
insert into myclass values(1, 'tom', 90);
insert into myclass values(2, 'john', 94);
insert into myclass values(3, 'simon', 79);
```

The MySQL JDBC driver files (JAR) are copied to `/var/lib/pxf/lib` on all hosts with PXF. After this, all PXF segments are restarted.

Then a table in GPDB is created:
```
CREATE EXTERNAL TABLE myclass(
    id integer,
    name text,
    degree float8
)
LOCATION (
    'pxf://localhost:51200/demodb.myclass?PROFILE=JDBC&JDBC_DRIVER=com.mysql.jdbc.Driver&DB_URL=jdbc:mysql://192.168.200.6:3306/demodb&USER=root&PASS=root'
)
FORMAT 'CUSTOM' (
    FORMATTER='pxfwritable_import'
);
```

Finally, a query to a GPDB external table is made:
```
SELECT * FROM myclass;
SELECT id, name FROM myclass WHERE id = 2;
```


### Partitioning
PXF JDBC plugin supports simultaneous access to external database from multiple PXF segments for SELECT queries. This feature is called partitioning.


#### Syntax
Three settings control partitioning feature:

* **[Partition By](#partition-by)** enables partitioning and indicates which column to use as a partition column. Only one column can be used as the partition column. This setting must be in format `<column>:<column_type>`, where:
    * `<column>` is name of the partition column;
    * `<column_type>` is data type of the partition column (hereinafter referred to as "partition type"). Currently, **supported types** are `INT`, `DATE` and `ENUM`.

* **[Partition Range](#partition-range)** indicates the range of data to be queried. It must be in special format, depending on a type of partition:
    * If the partition type is `ENUM`, format is `<value>:<value>[:<value>[...]]`. Each `<value>` forms its own fragment;
    * If the partition type is `INT`, format is `<start_value>:<end_value>`. PXF considers values to form a finite left-closed range (`... >= start_value AND ... < end_value`);
    * If the partition type is `DATE`, format is `<start_date>:<end_date>`, and each date must be in format `yyyy-MM-dd`. PXF considers dates to form a finite left-closed range (`... >= start_date AND ... < end_value`);

* **[Partition interval](#partition-interval)** is required only for `INT` and `DATE` partitions. It is ignored if `<column_type>` is `ENUM`. This setting must be in format `<value>[:<unit>]`, where:
    * `<value>` is the size of each fragment (the last fragment may be of smaller size);
    * `<unit>` is **required** if partition type is `DATE`. `year`, `month` and `day` are supported values.

Example combinations of options to enable partitioning:
* `&PARTITION_BY=id:int&RANGE=42:142&INTERVAL=2`
* `&PARTITION_BY=createdate:date&RANGE=2008-01-01:2010-01-01&INTERVAL=1:month`
* `&PARTITION_BY=grade:enum&RANGE=excellent:good:general:bad`


#### Mechanism
If partitioning is enabled, a SELECT query is split into a set of small queries, each of which is called a *fragment*. All fragments are processed by separate PXF instances simultaneously. If there are more fragments than PXF instances, some instances will process more than one fragment; if only one PXF instance is available, it will process all fragments.

Extra query constraints (`WHERE` expressions) are automatically added to each fragment to guarantee that every tuple of data is retrieved from the external database exactly once.

Fragments are distributed randomly among PXF instances.

[Query-preceding SQL command](#query-preceding-sql-command-1) is executed once for *every* fragment. However, the command itself is taken from configuration file of the PXF instance that processes given fragment. Commands may differ (or be absent) in different configuration files. Thus, exact number of times query-preceding SQL command is executed depends on two factors:
* Number of fragments
* Structures of configuration files of PXF instances

If all configuration files set the same command, it will be executed as many times as there are fragments.


#### Example
Consider the following MySQL table:
```
CREATE TABLE sales (
    id int primary key,
    cdate date,
    amt decimal(10,2),
    grade varchar(30)
)
```
and the following GPDB table:
```
CREATE EXTERNAL TABLE sales(
    id integer,
    cdate date,
    amt float8,
    grade text
)
LOCATION ('pxf://sales?PROFILE=JDBC&JDBC_DRIVER=com.mysql.jdbc.Driver&DB_URL=jdbc:mysql://192.168.200.6:3306/demodb&PARTITION_BY=cdate:date&RANGE=2008-01-01:2010-01-01&INTERVAL=1:year')
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');
```

The PXF JDBC plugin will generate two fragments  for a query `SELECT * FROM sales`. Then GPDB will assign each of them to a separate PXF segment. Each segment will perform the SELECT query, and the first one will get tuples with `cdate` values for year `2008`, while the second will get tuples for year `2009`. Then each PXF segment will send its results back to GPDB, where they are "concatenated" and returned.


## INSERT queries
PXF JDBC plugin allows to perform INSERT queries to external tables. Note that **the plugin does not guarantee consistency for INSERT queries**. Use a staging table in external database to deal with this.

To perform INSERT queries, create an `EXTERNAL WRITABLE TABLE` with `FORMAT 'CUSTOM' (FORMATTER='pxfwritable_export')` in PXF.


### Batching
INSERT queries can be batched. This may significantly increase perfomance if batching is supported by an external database.

Batching is enabled by default, and the default batch size is `100` (this is a [recommended](https://docs.oracle.com/cd/E11882_01/java.112/e16548/oraperf.htm#JJDBC28754) value). To control this feature, create an external table with the parameter `BATCH_SIZE` set to:
* `0` or `1`. Batching is disabled;
* `integer > 1`. Use batch of given size.

Batching must be supported by the JDBC driver of an external database. If the driver does not support batching, behaviour depends on the `BATCH_SIZE` parameter:
* `BATCH_SIZE` is not present; `BATCH_SIZE` is `0` or `1`. PXF will try to execute INSERT query without batching;
* `BATCH_SIZE` is an `integer > 1`. INSERT query will fail with an appropriate error message.


### Thread pool
INSERT queries can be processed by multiple threads. This may significantly increase perfomance if the external database can work with multiple connections simultaneously.

It is recommended to use [batching](#batching) together with thread pool. Then each thread receives data from one (whole) batch and processes it. If a thread pool is used without batching, each thread in a pool will receive exactly one tuple; as a rule, this takes much more time than usual one-thread INSERT.

If any of the threads from pool fails, the user will get the error message. However, if INSERT fails, some data still may be INSERTed into the external database.

To enable thread pool, create an external table with the paramete `POOL_SIZE` set to:
* `integer < 1`. The number of threads in a pool is set equal to the number of CPUs in the system;
* `integer > 1`. Thread pool will consist of the given number of threads;
* `1`. Thread pool is disabled.

By default (`POOL_SIZE` is absent), thread pool is not used.


## External database session configuration
Before executing `SELECT` or `INSERT` query in external database, PXF JDBC plugin can prepare the environment by executing queries (hereinafter called `SET` queries) that change configuration of external database for a session.

Every external database has its own syntax to change configuration. PXF uses the following:
* MySQL, PostgreSQL: `SET <key> = <value>;`
* Microsoft SQL server: `SET <key> <value>;`
* Oracle: `ALTER SESSION SET <key> = <value>;`

For other databases, PostgreSQL syntax is used.

To use this feature, pass key-value pairs in [external database session configuration setting](#external-database-session-configuration).


### Partitioning and external database sessions
When [partitioning](#partitioning) is used, each fragment requires its own external database session.

This implies `SET` queries are executed multiple times (once for each external database session).

In addition, each PXF node can execute its own `SET` query (according to setting in its configuration file). However, it is the same for all fragments processed by one node.


#### Example
Consider the following example `EXTERNAL TABLE` definition:
```
CREATE EXTERNAL TABLE ext_table(k INT, val INT)
LOCATION ('pxf://PUBLIC.T2?PROFILE=JDBC&SERVER=EXAMPLE')
FORMAT 'CUSTOM' (formatter='pxfwritable_import');
```

and `$PXF_CONF/servers/EXAMPLE/jdbc-site.xml` on every PXF segment:
```
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
    <property>
        <name>jdbc.driver</name>
        <value>org.example.JdbcDriver</value>
    </property>
    <property>
        <name>jdbc.url</name>
        <value>jdbc:example://1.2.3.4:12345</value>
    </property>
    <property>
        <name>jdbc.session.property.key1</name>
        <value>value1</value>
    </property>
    <property>
        <name>jdbc.session.property.key2</name>
        <value>value2</value>
    </property>
</configuration>
```

When `SELECT` from this external table is called, PXF executes `SET key1 = value1`, `SET key2 = value2` and `SELECT k, val FROM PUBLIC.T2` (all in the same session). PostgreSQL syntax is used because `Example` is a database unknown to PXF. All queries are executed once as partitioning is not set (one fragment is generated).
