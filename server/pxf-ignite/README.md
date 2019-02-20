# Accessing Ignite database using PXF

The PXF Ignite plugin provides access to [Apache Ignite database](https://ignite.apache.org/) (both `SELECT` and `INSERT` are supported) via Ignite thin connector for Java.


## Prerequisites

Check the following before using the plugin:
* Ignite plugin is installed on all PXF nodes;
* The Apache Ignite client is installed and running.


## Syntax

```
CREATE [READABLE | WRITABLE] EXTERNAL TABLE <table_name> (
    <column_name> <data_type>[, <column_name> <data_type>, ...] | LIKE <other_table>
)
LOCATION ('pxf://<ignite_table_name>?PROFILE=Ignite[&<extra-parameter>[&<extra-parameter>[&...]]]')
FORMAT 'CUSTOM' (formatter='pxfwritable_import');
```

where each `<extra-parameter>` is one of the following:

* `IGNITE_HOST=<ignite_host_address_with_port>`. The location of Ignite client node;

* `IGNITE_HOSTS=<ignite_host_addresses_with_ports_separated_by_','>`. Locations of multiple Ignite client nodes; the one to be used is chosen randomly during query execution by Ignite thin client. If that host is unavailable, Ignite will try all other provided ones silently. If both `IGNITE_HOSTS` and `IGNITE_HOST` parameters are present, the latter is ignored. If neither of these parameters are present, `127.0.0.1:10800` is used instead;

* `USER=<string>`. Ignite user name;

* `PASSWORD=<string>`. Ignite user password;

* `BUFFER_SIZE=<unsigned_int>`. The number of tuples send to (from) Ignite per a response. The same number of tuples is stored in local (PXF) cache;

* `PARTITION_BY=<column>:<column_type>`. See below;

* `RANGE=<start_value>:<end_value>`. See below;

* `INTERVAL=<value>[:<unit>]`. See below;

* `IGNITE_CACHE=<ignite_cache_name>`. The name of Ignite cache to use. If not given, this option is not included in queries from PXF to Ignite, a default value set by Ignite is used instead;

* `QUOTE_COLUMNS=<any_value>`. If this option is present, make PXF surround all column names with double quotes. By default, all column names are passed to Ignite without double quotes;

* `IGNITE_LAZY=<any_value>`. If this option is present, perform lazy SELECTs (tell Ignite not to store data on server PXF is connected to and instead send it to PXF as soon as possible after PXF request). This may increase query execution time, but prevents crashes of Ignite server when it lacks memory to store all requested data;

* `IGNITE_TCP_NODELAY=<any_value>`. If this option is present, make Ignite send TCP packets immediately when they are emitted. Otherwise, Ignite will form large TCP packets from small ones;

* `IGNITE_REPLICATED_ONLY=<any_value>`. If this option is present, tell Ignite the given query is over "replicated" tables. This is a hint for potentially more effective execution.


## SELECT queries

The PXF Ignite plugin allows to perform SELECT queries to external tables.

To perform SELECT queries, create an `EXTERNAL READABLE TABLE` or just `EXTERNAL TABLE` with `FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import')` in PXF.


### Partitioning

PXF Ignite plugin supports simultaneous read access to Ignite database from multiple PXF segments. This feature is called partitioning. If it is not used, all data is retrieved by a single PXF segment.


#### Syntax

To use partitions, add a set of `<extra-parameter>`s:
```
&PARTITION_BY=<column>:<column_type>&RANGE=<start_value>:<end_value>[&INTERVAL=<value>[:<unit>]]
```

* The `PARTITION_BY` parameter indicates which column to use as the partition column. Only one column can be used as a partition column.
    * The `<column>` is the name of a partition column;
    * The `<column_type>` is the datatype of a partition column. At the moment, the **supported types** are `INT`, `DATE` and `ENUM`. The `DATE` format is `yyyy-MM-dd`.

* The `RANGE` parameter indicates the range of data to be queried. If the partition type is `ENUM`, the `RANGE` parameter must be a list of values, each of which forms its own fragment. In case of `INT` and `DATE` partitions, this parameter must be a finite left-closed range ("infinity" values are not supported):
    * `[ <start_value> ; <end_value> )`
    * `... >= start_value AND ... < end_value`;

* The `INTERVAL` parameter is **required** for `INT` and `DATE` partitions. This parameter is ignored if `<column_type>` is `ENUM`.
    * The `<value>` is the size of each fragment (the last one may be smaller). Note that by default PXF does not support more than 100 fragments;
    * The `<unit>` **must** be provided if `<column_type>` is `DATE`. At the moment, only `year`, `month` and `day` are supported. This parameter is ignored in case of any other `<column_type>`.

Example partitions:
* `&PARTITION_BY=id:int&RANGE=42:142&INTERVAL=2`
* `&PARTITION_BY=createdate:date&RANGE=2008-01-01:2010-01-01&INTERVAL=1:month`
* `&PARTITION_BY=grade:enum&RANGE=excellent:good:general:bad`


#### Mechanism

When partitioning is activated, SELECT query is split into a set of small queries, each of which is called a *fragment*. All fragments are processed by separate PXF instances simultaneously. If there are more fragments than PXF instances, some instances will process more than one fragment; if only one PXF instance is available, it will process all fragments.

Extra query constraints (`WHERE` expressions) are automatically added to each fragment to guarantee that every tuple of data is retrieved from Ignite exactly once.


## INSERT queries

PXF Ignite plugin allows to perform INSERT queries to external tables. Note that **the plugin does not guarantee consistency for INSERT queries**. Use a staging table in Ignite to deal with this.

To perform INSERT queries, create an `EXTERNAL WRITABLE TABLE` with `FORMAT 'CUSTOM' (FORMATTER='pxfwritable_export')` in PXF.


## Examples

### A simple `EXTERNAL TABLE`
```
DROP EXTERNAL TABLE IF EXISTS ext_ignite;

CREATE EXTERNAL TABLE ext_ignite(k INT, val INT)
LOCATION ('pxf://PUBLIC.T2?PROFILE=Ignite&IGNITE_HOST=1.2.3.4:10800')
FORMAT 'CUSTOM' (formatter='pxfwritable_import');
```
```
SELECT * FROM ext_ignite;
 k | val
---+-----
 1 |   1
(1 row)
```


### A simple `WRITABLE EXTERNAL TABLE`
```
DROP EXTERNAL TABLE IF EXISTS ext_ignite_w;

CREATE WRITABLE EXTERNAL TABLE ext_ignite_w(k INT, val INT)
LOCATION ('pxf://PUBLIC.T2?PROFILE=Ignite&IGNITE_HOSTS=1.2.3.4:10800,4.3.2.1:10800')
FORMAT 'CUSTOM' (formatter='pxfwritable_export');
```
```
INSERT INTO ext_ignite_w(k, val) VALUES (1, 100);
INSERT 0 1
```
