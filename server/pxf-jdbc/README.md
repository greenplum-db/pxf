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
    'pxf://{<full_external_table_name> | query:<query_file_without_extension>}?<pxf_parameters>[&SERVER=<server_name>]<jdbc_settings>'
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

* `LOCATION` clause of external table DDL. Every setting must have format `&<name>=<value>`. Hereinafter a setting set in `LOCATION` clause is referred to as an "option".

* Configuration file located at `$PXF_BASE/servers/<server_name>/jdbc-site.xml` (on every PXF segment), where `<server_name>` is an arbitrary name (the file is intended to include options specific for each external database server). Hereinafter setting set in configuration file is referred to as "configuration parameter".

If `SERVER` option is not set in external table DDL, PXF will assume it equal to `default` and load configuration files from `$PXF_BASE/servers/default/`. A warning is added to PXF log file if `SERVER` is set to incorrect value (PXF is unable to read the requested configuration file).

If a setting can be set by both option and configuration parameter, option value overrides configuration parameter value.


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


#### Fetch size
Size of batch to be used for SELECT queries (defaults to 1000).

* **Option**: `FETCH_SIZE`
* **Configuration parameter**: `jdbc.statement.fetchSize`
* **Value**: Integer >= 0


#### Batch size
*Can be set only in `LOCATION` clause of external table DDL*

Size of batch to be used for INSERT queries. This setting is described in section [batching](#batching).

* **Option**: `BATCH_SIZE`
* **Configuration parameter**: `jdbc.statement.batchSize`
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
PXF JDBC plugin allows to perform SELECT queries to external tables. An external table can refer to a table in a remote database or to a file that contains a pre-defined complex query to execute against a remote database.

To perform SELECT queries, create an `EXTERNAL READABLE TABLE` or just an `EXTERNAL TABLE` with `FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import')` in PXF.


### `EXTERNAL READABLE TABLE` using remote table name - example
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
    'pxf://demodb.myclass?PROFILE=JDBC&JDBC_DRIVER=com.mysql.jdbc.Driver&DB_URL=jdbc:mysql://192.168.200.6:3306/demodb&USER=root&PASS=root'
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

### `EXTERNAL READABLE TABLE` using pre-defined query - example
The following example shows how to execute an aggregation query in MySQL and return results via PXF JDBC plugin.

Suppose MySQL instance is available at `192.168.200.6:3306`. The following tables are created in MySQL:
```
use demodb;
create table dept(
    id int(4) not null primary key,
    name varchar(20) not null
);

create table emp(
    dept_id int(4) not null,
    name varchar(20) not null,
    salary int(8)
);
```

Then some data is inserted into MySQL tables:
```
insert into dept values(1, 'sales');
insert into dept values(2, 'finance');
insert into dept values(3, 'it');

insert into emp values(1, 'alice', 11000);
insert into emp values(2, 'bob', 10000);
insert into emp values(3, 'charlie', 10500);
```

Then a complex aggregation query is created and placed in a file, say `report.sql`. The file needs to be placed in the server configuration directory under `$PXF_BASE/servers/`. So, let's assume we have created a `mydb` server configuration directory, then this file will be `$PXF_BASE/servers/mydb/report.sql`. Jdbc driver name and connection parameters should be configured in `$PXF_BASE/servers/mydb/jdbc-site.xml` for this server.
```
SELECT dept.name AS name, count(*) AS count, max(emp.salary) AS max
FROM demodb.dept JOIN demodb.emp
ON dept.id = emp.dept_id
GROUP BY dept.name;
```
This query returns a name of the department, count of employees, and the maximal salary in each department.

The MySQL JDBC driver files (JAR) are copied to `$PXF_BASE/lib` on all hosts with PXF. After this, all PXF segments are restarted.

Then a table in GPDB is created with the schema corresponding to the results returned by the aggregation query. It is important that the table column names and types correspond to those returned by the aggregation query.
```
CREATE EXTERNAL TABLE dept_report (
    name text,
    count int,
    max int
)
LOCATION (
    'pxf://query:report?PROFILE=JDBC&SERVER=mydb'
)
FORMAT 'CUSTOM' (
    FORMATTER='pxfwritable_import'
);
```

Finally, a query to a GPDB external table is made:
```
SELECT * FROM dept_report;
SELECT name, count FROM dept_report WHERE max > 10000;
```


### Partitioning
PXF JDBC plugin supports simultaneous access to external database from multiple PXF segments for SELECT queries. This feature is called partitioning.

When partitioning is enabled, a SELECT query is split into a set of multiple queries according to the settings in external table DDL (see description below). Each of them and the range of data targeted is called a fragment or a partition. Every partition is processed independently.

Partitioning does not affect the integral output of a SELECT query.


#### Syntax
Three settings control the feature:

* **[Partition By](#partition-by)** enables partitioning and indicates which column to use as a partition column (the column to which constraints are applied). Only one column can be a partition column. This setting must have format `<column>:<column_type>`, where:
    * `<column>` is the name of the partition column;
    * `<column_type>` is the data type of the partition column (hereinafter referred to as "partition type"). **Supported types** are `INT`, `DATE` and `ENUM`.
        * `INT` and `DATE` partitions are for columns of the same types. These partitions are range-based (see below).
        * `ENUM` is a partition for text columns. This is a value-based partition (see below).

* **[Partition Range](#partition-range)** indicates the range of data to form partitions on. It must be in special format, depending on partition type:
    * `INT`. Format is `<start_value>:<end_value>`. PXF considers values to form a closed interval (`... >= start_value AND ... <= end_value`);
    * `DATE`. Format is `<start_value>:<end_value>` and each date must be in format `yyyy-MM-dd`. PXF considers values to form a closed interval (`... >= start_value AND ... <= end_value`);
    * `ENUM`. Format is `<value>:<value>[:<value>[...]]`. Each `<value>` forms its own partition.

* **[Partition Interval](#partition-interval)** indicates the size of each partition, except for the last one (its size may be smaller than the provided, depending on the Partition Range setting). This setting is processed differently for different partition types:
    * `INT`. Format is `<value>`;
    * `DATE`. Format is `<value>:<unit>`, where `<unit>` is `year`, `month` or `day`;
    * `ENUM`. The setting is ignored.


##### `INT` and `DATE` partitions
In `INT` and `DATE` partitions, every fragment queries data from either closed or bounded interval (intervals are formed according to [Partition interval](#partition-interval) and [Partition Range](#partition-range) settings).

For example, a `LOCATION` clause containing `PARTITION_BY=id:int&RANGE=1:5&INTERVAL=2` makes PXF produce five fragments, covering the whole range of data. Their constraints are as follows (column name is omitted for simplicity):
1. `< 1`
2. `>= 1 AND < 3`
3. `>= 3 AND < 5`
4. `>= 5`
5. `IS NULL`

When the step size goes over the upper bound, for example `PARTITION_BY=id:int&RANGE=1:5&INTERVAL=3`, PXF will generate the following fragments:
1. `< 1`
2. `>= 1 AND < 4`
3. `>= 4 AND < 5`
4. `>= 5`
5. `IS NULL`

##### `ENUM` partitions
`ENUM` partitions are value-based: every value of its [Range](#partition-range) forms its own partition.

For example, a `LOCATION` clause containing `PARTITION_BY=col:enum&RANGE=a:b:c` makes PXF produce five fragments, covering the whole range of data. Their constraints are as follows:
1. `col = 'a'`
2. `col = 'b'`
3. `col = 'c'`
4. `col <> 'a' AND col <> 'b' AND col <> 'c'`
5. `col IS NULL`


##### Example
Example combinations of options to enable partitioning:
* `&PARTITION_BY=id:int&RANGE=42:142&INTERVAL=2`
* `&PARTITION_BY=createdate:date&RANGE=2008-01-01:2010-01-01&INTERVAL=1:month`
* `&PARTITION_BY=grade:enum&RANGE=excellent:good:general:bad`
* `&PARTITION_BY=known:null`


#### Mechanism
Extra query constraints (`WHERE` expressions) are automatically added to each fragment to guarantee that every tuple of data is retrieved from the external database exactly once.

Each PXF instance processes the fragments independently from any other PXF instance.

Round-robin scheduling is used to distribute fragments among *GPDB segments*. The first segment (which acquires the first fragment) is chosen pseudo-randomly (the seed is GPDB query transaction identifier). The distribution of fragments does not change during fragment processing (i.e. if one PXF instance has finished processing of fragments assigned to it, it will not process fragments assigned to other PXF instances).

As fragments are distributed among GPDB segments (not PXF instances), in case the amount of fragments is less or equal to the number of GPDB segments on one host, all fragments may be assigned to a single PXF instance.

In addition to the fragments generated according to the partitioning settings, up to three fragments are generated implicitly:
* In case of **all partitions**, a fragment with `IS NULL` constraint is generated.
* In case of **`INT`** and **`DATE`** partitions, up to two fragments are generated, with constraints covering left-bounded interval (` < range_start_value`) and right-bounded interval (` > range_end_value`). The latter is "merged" with normal constraints (becoming ` >= some_value_in_range`) if the range of the last fragment is smaller than the requested interval.
* In case of **`ENUM`** partitions, a fragment covering all values *except* for the those that are provided in [partition range](#partition-range) setting is generated.

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
Set the value in `jdbc.statement.batchSize` in `$PXF_BASE/servers/EXAMPLE/jdbc-site.xml` on every PXF segment. Alternative, can be set in the DDL location.

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

## JDBC Connection Pooling
The JDBC connector will be using connection pooling implemented by HikariCP (https://github.com/brettwooldridge/HikariCP). To disable the connection pool, edit the value for the property in your server's `jdbc-site.xml` file:
```xml
    <property>
        <name>jdbc.pool.enabled</name>
        <value>false</value>
        <description>Use connection pool for JDBC</description>
    </property>
```
You can set other properties specific to HikariCP by specifying them in `jdbc-site.xml` file with `jdbc.pool.property.` prefix. The default `jdbc-site.xml` template comes with the connection pool enabled and pre-defined settings for `maximumPoolSize`, `connectionTimeout`, `idleTimeout`, `minimumIdle` properties.

Any property that you specify with `jdbc.connection.property.` prefix will also be used by HikariCP when requesting connections from the `DriverManager`.

Using connection pool feature will ensure you will not exceed the connection limit to the target database for a given server configuration. However, be mindful, that connection pool is established per configuration server based on combination of values for `jdbc.url`, `jdbc.user`, `jdbc.password`, set of connection properties and set of pool properties. If you use user impersonation feature you will end up using a separate connection pool per each user.

You should tune your server configuration not to exceed the maximum number of connections allowed by the target database. To come up with the maximum value for `maximumPoolSize` parameter, take the overall number of connection allowed by the external database and divide it by the number of Greenplum hosts. For example, if your Greenplum cluster has 16 nodes and your target database allows 160 concurrent connections, set `maximumPoolSize` to no more than 160 / 16 = 10. That will be the maximum value to ensure each PXF JVM can get a fair share of JDBC connections.

However, in practice, you might want to set this number to a lower value, since the number of concurrent connections per JDBC query will depend on the number of partitions for the query. If the query is not using any partitions, then only 1 JDBC connection on 1 PXF JVM will be used to run the query. If, for example, the query will be using 12 partitions (e.g. 1 per month of a year), then 12 JDBC connections will be used concurrently across all the Greenplum segment hosts and PXF JVMs. Ideally, these connections would be distributed among PXF JVMs, but it is not guaranteed by the system.

## Partitioning and external database sessions
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

and `$PXF_BASE/servers/EXAMPLE/jdbc-site.xml` on every PXF segment:
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

## Using JDBC plugin to connect to Hive
You can use the PXF JDBC connector to retrieve data from Hive. You can also use the "named query" feature to submit a custom SQL query to Hive and retrieve results using PXF JDBC connector.

While you can specify most of the properties in the EXTERNAL TABLE DDL, the instructions below assume you would use server-based configuration.
Follow these steps to enable connectivity to Hive:
1. Define a new PXF configuration server in `$PXF_BASE/servers/` directory
2. Copy template from `$PXF_BASE/templates/jdbc-site.xml` to your server directory
3. Edit the file and provide Hive JDBC driver and URL
    ```angular2html
    <property>
        <name>jdbc.driver</name>
        <value>org.apache.hive.jdbc.HiveDriver</value>
    </property>
    <property>
        <name>jdbc.url</name>
        <value>jdbc:hive2://<hiveserver2_host>:<hiveserver2_port>/<database></value>
    </property>
    ```
4. For Hive without Kerberos configure `user` and additional properties according to HiveServer2 settings in `hive-site.xml` file.

    - if `hive.server2.authentication = NOSASL`, then no authentication will be performed by HiveServer2 and you must add the following property in `jdbc-site.xml`:
        ```angular2html
        <property>
            <name>jdbc.connection.property.auth</name>
            <value>noSasl</value>
        </property>
        ```
        Alternatively, you can add `;auth=noSasl` to the JDBC URL.

    - if Hive is configured with `hive.server2.authentication = NONE` (or is not specified), you should set the value for the `jdbc.user` property.

        a. if Hive is configured with `hive.server2.enable.doAs = TRUE` (default), Hive will run Hadoop operations on behalf of the user connecting to Hive. You have an option to either:

            1) specify the user value that has read permission on all Hive data being accessed, e.g. to connect to Hive and run all request as user `gpadmin`, specify the following property:
            ```
            <property>
                <name>jdbc.user</name>
                <value>gpadmin</value>
            </property>
            ```
            2) enable PXF JDBC impersonation so that PXF will automatically use Greenplum's user name to connect to Hive:
            ```
            <property>
                <name>pxf.service.user.impersonation</name>
                <value>true</value>
            </property>
            ```
            If you enable impersonation, do not explicitly specify user name as a property or as a part of the URL.

        b. if Hive is configured with `hive.server2.enable.doAs = FALSE`, Hive will run Hadoop operations as the user who runs HiveServer2 process, usually user `hive`. Yo do not need to specify `jdbc.user` property as its value will be ignored.

5. For Hive with Kerberos (`hive.server2.authentication = KERBEROS` in `hive-site.xml`:

    - Configure SASL QoP URL parameter to match the setting in `hive-site.xml`, for example, if in `hive-site.xml` contains
        ```$xslt
        <property>
            <name>hive.server2.thrift.sasl.qop</name>
            <value>auth-conf</value>
        </property>
        ```
        then make sure the JDBC URL has `saslQop=auth-conf` fragment. This must be done in the URL and cannot be specified using connection properties.
    - Make sure there is `core-site.xml` file in `$PXF_BASE/servers/default` and it has Kerberos authentication turned on:
        ```$xslt
        <property>
            <name>hadoop.security.authentication</name>
            <value>kerberos</value>
        </property>
        ```
        This is required even if JDBC configuration server is different from `default` since PXF determines whether Kerberos is enabled by checking `core-site.xml` in the `default` server.

    - Make sure the PXF Kerberos principal is created in KDC and the keytab file named `pxf.service.keytab` is located on all PXF nodes in `$PXF_BASE/keytabs`

    - Make sure to include HiveServer2 principal name in the JDBC URL, e.g:
        ```$xslt
        jdbc:hive2://hs2server:10000/default;principal=hive/hs2server@REALM;saslQop=auth-conf
        ```

    - if Hive is configured with `hive.server2.enable.doAs = TRUE` (default), Hive will run Hadoop operations on behalf of the user connecting to Hive. You have an option to either:

        1) do not specify any additional properties, all Hadoop access will be with the identity provided by the PXF Kerberos principal (usually `gpadmin`)

        2) specify the user that has read permission on all Hive data being accessed, e.g. to connect to Hive and run all request as user `integration`, specify the following property in the URL:
            `hive.server2.proxy.user=integration` :
            ```$xslt
            jdbc:hive2://hs2server:10000/default;principal=hive/hs2server@REALM;saslQop=auth-conf;hive.server2.proxy.user=integration
            ```
        3) enable PXF JDBC impersonation in `jdbc-site.xml` so that PXF will automatically use Greenplum's user name to connect to Hive:
            ```
            <property>
                <name>pxf.service.user.impersonation</name>
                <value>true</value>
            </property>
            ```
            If you enable impersonation, do not explicitly specify `hive.server2.proxy.user` property in the URL.

    - if Hive is configured with `hive.server2.enable.doAs = FALSE`, Hive will run Hadoop operations with the identity provided by the PXF Kerberos principal (usually `gpadmin`)
