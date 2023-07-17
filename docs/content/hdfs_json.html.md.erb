---
title: Reading and Writing JSON Data in HDFS
---

<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

Use the PXF HDFS Connector to read and write JSON-format data. This section describes how to use PXF and external tables to access and write JSON data in HDFS.

## <a id="prereq"></a>Prerequisites

Ensure that you have met the PXF Hadoop [Prerequisites](access_hdfs.html#hadoop_prereq) before you attempt to read data from or write data to HDFS.

## <a id="hdfsjson_work"></a>Working with JSON Data

JSON is a text-based data-interchange format. A JSON data file contains one or more JSON objects. A JSON object is a collection of unordered name/value pairs. A value can be a string, a number, true, false, null, an object, or an array. You can define nested JSON objects and arrays.

JSON data is typically stored in a file with a `.json` or `.jsonl` (JSON Lines) suffix as described in the sections below.

### <a id="topic_jsonmodes"></a>About the PXF JSON Data Access Modes

PXF supports two data access modes for JSON files. The default mode expects one full JSON record per row (JSONL). PXF also supports an access mode that expects one JSON object per file where the JSON records may (but are not required to) span multiple lines.

#### <a id="jsonl"></a>Single Object Per Row

A JSON file can contain a single JSON object per row, where each row represents a database tuple. A JSON file that PXF reads that contains a single object per row may have any or no suffix. When writing, PXF creates the file with a `.jsonl` suffix.

Excerpt of sample single-object-per-row JSON data file:

``` pre
{"id":1,"color":"red"}
{"id":2,"color":"yellow"}
{"id":3,"color":"green"}
```

Refer to [JSON Lines](https://jsonlines.org/) for detailed information about this JSON syntax.

#### <a id="arrayobject"></a>Single Object Per File

A JSON file can also contain a single, named, root level JSON object whose value is an array of JSON objects. When reading, the array may contain objects with arbitrary complexity and nesting, and PXF forms database tuples from objects that have a property named the same as that specified for the `IDENTIFIER` (discussed below). When writing, each JSON object in the array represents a database tuple. JSON files of this type have the `.json` suffix.

In the following example JSON data file, the root-level `records` object is an array of three objects (tuples):

``` pre
{"records":[
{"id":1,"color":"red"}
,{"id":2,"color":"yellow"}
,{"id":3,"color":"green"}
]}
```

The records in the single JSON object may also span multiple lines:

``` pre
{
  "records":[
    {
      "id":1,
      "color":"red"
    },
    {
      "id":2,
      "color":"yellow"
    },
    {
      "id":3,
      "color":"green"
    }
  ]
}
```

Refer to [Introducing JSON](http://www.json.org/) for detailed information about this JSON syntax.

## <a id="datatypemap"></a>Data Type Mapping

To represent JSON data in Greenplum Database, map data values that use a primitive data type to Greenplum Database columns of the same type. JSON supports complex data types including projections and arrays.

### <a id="datatypemap_read"></a>Read Mapping

PXF uses the following data type mapping when reading JSON data:

| JSON Data Type    | PXF/Greenplum Data Type |
|-------------------|-------------------------------------------|
| boolean | boolean |
| number  | { bigint &#124; float8 &#124; integer &#124; numeric &#124; real &#124; smallint } |
| string | text |
| string (base64-encoded value) | bytea |
| string (date, time, timestamp, timestamptz in a text format that Greenplum understands)<sup>1</sup> | { date &#124; time &#124; timestamp &#124; timestamptz } |
| Array (one dimension) of type boolean[] | boolean[] |
| Array (one dimension) of type number[] | { bigint[] &#124; float8[] &#124; integer[] &#124; numeric[] &#124; real[] &#124; smallint[] } |
| Array (one dimension) of type string[] (base64-encoded value) | bytea[] |
| Array (one dimension) of type string[] (date, time, timestamp in a text format that Greenplum understands)<sup>1</sup> | { date[] &#124; time[] &#124; timestamp[] &#124; timestamptz[] } |
| Array (one dimension) of type string[] | text[] |
| Array of other types | text[] |
| Object                | Use dot `.` notation to specify each level of projection (nesting) to a member of a primitive or Array type.                                                                                         |
<sup>1</sup> PXF returns an error if Greenplum cannot convert the date or time string to the target type.

When reading, you can use N-level projection to map members of nested objects and arrays to primitive data types.

### <a id="datatypemap_write"></a>Write Mapping

PXF supports writing primitive types and single dimension arrays of primitive types. PXF supports writing other complex types to JSON as string.

PXF uses the following data type mapping when writing JSON data:

| PXF/Greenplum Data Type | JSON Data Type |
|-------------------|----------------------|
| bigint, float8, integer, numeric, real, smallint | number |
| boolean | boolean |
| bpchar, text, varchar | string |
| bytea | string (base64-encoded value) |
| date, time, timestamp, timestamptz | string |
| boolean[] | boolean[] |
| bigint[], float8[], int[], numeric[], real[], smallint[] | number[] |
| bytea[] | string[] (base64-encoded value)  |
| date[], time[], timestamp[], timestamptz[] | string[] |

## <a id="projection"></a>About Using Projection (Read)

In the example JSON data file excerpt below, `user` is an object composed of fields named `id` and `location`:


``` json
  {
    "created_at":"MonSep3004:04:53+00002013",
    "id_str":"384529256681725952",
    "user": {
      "id":31424214,
      "location":"COLUMBUS"
    },
    "coordinates":{
      "type":"Point",
      "values":[
         13,
         99
      ]
    }
  }
```

To specify the nested fields in the `user` object directly as Greenplum Database external table columns, use `.` projection:

``` pre
user.id
user.location
```

`coordinates` is an object composed of a text field named `type` and an array of integers named `values`. 

To read all of the elements of the `values` array in a single column, define the corresponding Greenplum Database external table column as type `int[]`.

``` pre
"coordinates.values" int[]
```

## <a id="profile_cet"></a>Creating the External Table

Use the `hdfs:json` profile to read or write JSON-format data in HDFS. The following syntax creates a Greenplum Database external table that references such a file:

``` sql
CREATE [WRITABLE] EXTERNAL TABLE <table_name>
    ( <column_name> <data_type> [, ...] | LIKE <other_table> )
LOCATION ('pxf://<path-to-hdfs-file>?PROFILE=hdfs:json[&SERVER=<server_name>][&<custom-option>=<value>[...]]')
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import'|'pxfwritable_export')
[DISTRIBUTED BY (<column_name> [, ... ] ) | DISTRIBUTED RANDOMLY];
```

The specific keywords and values used in the Greenplum Database [CREATE EXTERNAL TABLE](https://docs.vmware.com/en/VMware-Greenplum/6/greenplum-database/ref_guide-sql_commands-CREATE_EXTERNAL_TABLE.html) command are described in the table below.

| Keyword  | Value |
|-------|-------------------------------------|
| \<path&#8209;to&#8209;hdfs&#8209;file\>    | The path to the directory or file in the HDFS data store. When the `<server_name>` configuration includes a [`pxf.fs.basePath`](cfg_server.html#pxf-fs-basepath) property setting, PXF considers \<path&#8209;to&#8209;hdfs&#8209;file\> to be relative to the base path specified. Otherwise, PXF considers it to be an absolute path. \<path&#8209;to&#8209;hdfs&#8209;file\> must not specify a relative path nor include the dollar sign (`$`) character. |
| PROFILE    | The `PROFILE` keyword must specify `hdfs:json`. |
| SERVER=\<server_name\>    | The named server configuration that PXF uses to access the data. PXF uses the `default` server if not specified. |
| \<custom&#8209;option\>  | \<custom-option\>s for read and write operations are identified below.|
| FORMAT 'CUSTOM' | Use `FORMAT` '`CUSTOM`' with `(FORMATTER='pxfwritable_export')` (write) or `(FORMATTER='pxfwritable_import')` (read). |

<a id="customopts"></a>
PXF supports reading from and writing to JSON files that contain either an object per row (the default) or that contain a JSON single object. When the JSON file(s) that you want to read or write contains a single object, you must provide an `IDENTIFIER` \<custom-option\> and value. Use this option to identify the name of a field whose parent JSON object you want PXF to return or write as an individual tuple.

The `hdfs:json` profile supports the following custom **read** options:

| Option Keyword  | Description |
|-------|-----------------------|
| IDENTIFIER=\<value\> | When the JSON data that you are reading is comprised of a single JSON object, you must specify an `IDENTIFIER` to identify the name of the field whose parent JSON object you want PXF to return as an individual tuple. | 
| SPLIT_BY_FILE=\<boolean\> | Specify how PXF splits the data in \<path-to-hdfs-file\>. The default value is `false`, PXF creates multiple splits for each file that it will process in parallel. When set to `true`, PXF creates and processes a single split per file. |
| IGNORE_MISSING_PATH=\<boolean\> | Specify the action to take when \<path-to-hdfs-file\> is missing or invalid. The default value is `false`, PXF returns an error in this situation. When the value is `true`, PXF ignores missing path errors and returns an empty fragment. |

<div class="note"><b>Note:</b> When a nested object in a single object JSON file includes a field with the same name as that of a parent object field <i>and</i> the field name is also specified as the <code>IDENTIFIER</code>, there is a possibility that PXF could return incorrect results. Should you need to, you can work around this edge case by compressing the JSON file, and using PXF to read the compressed file.</div>

The `hdfs:json` profile supports the following custom **write** options:

| Option  | Value Description |
|-------|-------------------------------------|
| ROOT=\<value\>    | When writing to a single JSON object, identifies the name of the root-level object attribute. |
| COMPRESSION_CODEC    | The compression codec alias. Supported compression codecs for writing json data include: `default`, `bzip2`, `gzip`, and `uncompressed`. If this option is not provided, Greenplum Database performs no data compression. |
| DISTRIBUTED BY    | If you are loading data from an existing Greenplum Database table into the writable external table, consider specifying the same distribution policy or `<column_name>` on both tables. Doing so will avoid extra motion of data between segments on the load operation. |

When you specify compression for a JSON write operation, PXF names the files that it writes `<basename>.<json_file_type>.<compression_extension>`. For example: `jan_sales.jsonl.gz`.

## <a id="json_read"></a>Read Examples

### <a id="example_datasets"></a>Example Data Sets

In upcoming read examples, you use both JSON access modes to operate on a sample data set. The schema of the sample data set defines objects with the following member names and value data types:

- "created_at" - text
- "id_str" - text
- "user" - object

    - "id" - integer
    - "location" - text
- "coordinates" - object (optional)

    - "type" - text
    - "values" - array

        - [0] - integer
        - [1] - integer

The data set for the single-object-per-row (JSONL) access mode follows:

``` pre
{"created_at":"FriJun0722:45:03+00002013","id_str":"343136551322136576","user":{"id":395504494,"location":"NearCornwall"},"coordinates":{"type":"Point","values": [ 6, 50 ]}},
{"created_at":"FriJun0722:45:02+00002013","id_str":"343136547115253761","user":{"id":26643566,"location":"Austin,Texas"}, "coordinates": null},
{"created_at":"FriJun0722:45:02+00002013","id_str":"343136547136233472","user":{"id":287819058,"location":""}, "coordinates": null}
```

The data set for the single-object-per-file JSON access mode follows:

``` json
{
  "root":[
    {
      "record_obj":{
        "created_at":"MonSep3004:04:53+00002013",
        "id_str":"384529256681725952",
        "user":{
          "id":31424214,
          "location":"COLUMBUS"
        },
        "coordinates":null
      },
      "record_obj":{
        "created_at":"MonSep3004:04:54+00002013",
        "id_str":"384529260872228864",
        "user":{
          "id":67600981,
          "location":"KryberWorld"
        },
        "coordinates":{
          "type":"Point",
          "values":[
             8,
             52
          ]
        }
      }
    }
  ]
}
```

You will create JSON files for the sample data sets and add them to HDFS in the next section.

### <a id="jsontohdfs"></a>Loading the Sample JSON Data to HDFS

The PXF HDFS connector can read and write native JSON stored in HDFS.

Copy and paste the object-per-row JSON sample data set above to a file named `objperrow.jsonl`. Similarly, copy and paste the single object per file JSON record data set to a file named `singleobj.json`.

> **Note** Ensure that there are **no** blank lines in your JSON files.

Copy the JSON data files that you just created to your HDFS data store. Create the `/data/pxf_examples` directory if you did not do so in a previous exercise. For example:

``` shell
$ hdfs dfs -mkdir /data/pxf_examples
$ hdfs dfs -put objperrow.jsonl /data/pxf_examples/
$ hdfs dfs -put singleobj.json /data/pxf_examples/
```

Once the data is loaded to HDFS, you can use Greenplum Database and PXF to query and add to the JSON data.

### <a id="read_example1"></a>Example: Single Object Per Row (Read)

Use the following [CREATE EXTERNAL TABLE](https://docs.vmware.com/en/VMware-Greenplum/6/greenplum-database/ref_guide-sql_commands-CREATE_EXTERNAL_TABLE.html) SQL command to create a readable external table that references the single-object-per-row JSON data file and uses the PXF default server.

```sql
CREATE EXTERNAL TABLE objperrow_json_tbl(
  created_at TEXT,
  id_str TEXT,
  "user.id" INTEGER,
  "user.location" TEXT,
  "coordinates.values" INTEGER[]
)
LOCATION('pxf://data/pxf_examples/objperrow.jsonl?PROFILE=hdfs:json')
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');
```

This table reads selected fields in the JSON file. Notice the use of `.` projection to access the nested fields in the `user` and `coordinates` objects.

To view the JSON data in the file, query the external table:

``` sql
SELECT * FROM objperrow_json_tbl;
```

To access specific elements of the `coordinates.values` array, you can specify the array subscript number in square brackets:

```sql
SELECT "coordinates.values"[1], "coordinates.values"[2] FROM objperrow_json_tbl;
```

### <a id="read_example2"></a>Example: Single Object Per File (Read)

The SQL command to create a readable external table for a single object JSON file is very similar to that of the single object per row data set above. You must additionally specify the `LOCATION` clause `IDENTIFIER` keyword and an associated value. For example:

``` sql
CREATE EXTERNAL TABLE singleobj_json_tbl(
  created_at TEXT,
  id_str TEXT,
  "user.id" INTEGER,
  "user.location" TEXT,
  "coordinates.values" INTEGER[]
)
LOCATION('pxf://data/pxf_examples/singleobj.json?PROFILE=hdfs:json&IDENTIFIER=created_at')
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');
```

`created_at` identifies the member name of the first field in the JSON record `record_obj` in the sample data schema.

To view the JSON data in the file, query the external table:

``` sql
SELECT * FROM singleobj_json_tbl;
```

### <a id="array_other"></a> Other Methods to Read a JSON Array

Starting in version 6.2.0, PXF supports reading a JSON array into a `TEXT[]` column. PXF still supports the old methods of using array element projection or a single text-type column to read a JSON array. These access methods are described here.

#### <a id="arrayelproj"></a>Using Array Element Projection

PXF supports accessing specific elements of a JSON array using the syntax `[n]` in the table definition to identify the specific element.

```sql
CREATE EXTERNAL TABLE objperrow_json_tbl_aep(
  created_at TEXT,
  id_str TEXT,
  "user.id" INTEGER,
  "user.location" TEXT,
  "coordinates.values[0]" INTEGER,
  "coordinates.values[1]" INTEGER
)
LOCATION('pxf://data/pxf_examples/objperrow.jsonl?PROFILE=hdfs:json')
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');
```

**Note**: When you use this method to identify specific array elements, PXF provides _only_ those values to Greenplum Database, not the whole JSON array.

If your existing external table definition uses array element projection and you want to read the array into a `TEXT[]` column, you can use the `ALTER EXTERNAL TABLE` command to update the table definition. For example:

```sql
ALTER EXTERNAL TABLE objperrow_json_tbl_aep DROP COLUMN "coordinates.values[0]", DROP COLUMN "coordinates.values[1]", ADD COLUMN "coordinates.values" TEXT[];
```

If you choose to alter the external table definition in this manner, be sure to update any existing queries on the external table to account for the changes to column name and type.

#### <a id="singletextcol"></a> Specifying a Single Text-type Column

PXF supports accessing all of the elements within an array as a single string containing the serialized JSON array by defining the corresponding Greenplum table column with one of the following data types: `TEXT`, `VARCHAR`, or `BPCHAR`.

```sql
CREATE EXTERNAL TABLE objperrow_json_tbl_stc(
  created_at TEXT,
  id_str TEXT,
  "user.id" INTEGER,
  "user.location" TEXT,
  "coordinates.values" TEXT
)
LOCATION('pxf://data/pxf_examples/objperrow.jsonl?PROFILE=hdfs:json')
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');
```

If you retrieve the JSON array in a single text-type column and wish to convert the JSON array serialized as `TEXT` back into a native Greenplum array type, you can use the example query below:

```sql
SELECT user.id,
       ARRAY(SELECT json_array_elements_text(coordinates.values::json))::int[] AS coords
FROM objperrow_json_tbl_stc;
```

**Note**: This conversion is possible only when you are using PXF with Greenplum Database 6.x; the function `json_array_elements_text()` is not available in Greenplum 5.x.

If your external table definition uses a single text-type column for a JSON array and you want to read the array into a `TEXT[]` column, you can use the `ALTER EXTERNAL TABLE` command to update the table definition. For example:

```sql
ALTER EXTERNAL TABLE objperrow_json_tbl_stc ALTER COLUMN "coordinates.values" TYPE TEXT[];
```

If you choose to alter the external table definition in this manner, be sure to update any existing queries on the external table to account for the change in column type.

## <a id="json_write"></a>Writing JSON Data

To write JSON data, you create a writable external table that references the name of a directory on HDFS. When you insert records into the writable external table, PXF writes the block(s) of data that you insert to one or more files in the directory that you specified. In the default case (single object per row), PXF writes the data to a `.jsonl` file. When you specify a `ROOT` attribute (single object per file), PXF writes to a `.json` file.

> **Note** When writing JSON data, PXF supports only scalar or one dimensional arrays of Greenplum data types. PXF does not support column projection when writing JSON data.

Writable external tables can only be used for `INSERT` operations. If you want to query the data that you inserted, you must create a separate readable external table that references the HDFS directory and read from that table.

The write examples use a data schema similar to that of the read examples.

### <a id="write_example1"></a>Example: Single Object Per Row (Write)

In this example, we add data to a directory named `jsopr`.

Use the following [CREATE EXTERNAL TABLE](https://docs.vmware.com/en/VMware-Greenplum/6/greenplum-database/ref_guide-sql_commands-CREATE_EXTERNAL_TABLE.html) SQL command to create a writable external table that writes JSON data in single-object-per-row format and uses the PXF default server.

```sql
CREATE WRITABLE EXTERNAL TABLE add_objperrow_json_tbl(
  created_at TEXT,
  id_str TEXT,
  id INTEGER,
  location TEXT,
  coordinates INTEGER[]
)
LOCATION('pxf://data/pxf_examples/jsopr?PROFILE=hdfs:json')
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_export');
```

Write data to the table:

``` sql
INSERT INTO add_objperrow_json_tbl VALUES ( 'SunJun0912:59:07+00002013', '343136551111111111', 311111111, 'FarAway', '{ 6, 50 }' );
INSERT INTO add_objperrow_json_tbl VALUES ( 'MonJun1002:12:06+00002013', '343136557777777777', 377777777, 'NearHere', '{ 13, 93 }' );
```

Read the data that you just wrote. Recall that you must first create a readable external table:

``` sql
CREATE EXTERNAL TABLE jsopr_tbl(
  created_at TEXT,
  id_str TEXT,
  id INTEGER,
  location TEXT,
  coordinates INTEGER[]
)
LOCATION('pxf://data/pxf_examples/jsopr?PROFILE=hdfs:json')
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');
```

Query the table:

``` sql
SELECT * FROM jsopr_tbl;

        created_at         |       id_str       |    id     | location | coordinates 
---------------------------+--------------------+-----------+----------+-------------
 MonJun1002:12:06+00002013 | 343136557777777777 | 377777777 | NearHere | {13,93}
 SunJun0912:59:07+00002013 | 343136551111111111 | 311111111 | FarAway  | {6,50}
(2 rows)
```

View the files added to HDFS:

```
$ hdfs dfs -cat /data/pxf_examples/jsopr/*
{"created_at":"SunJun0912:59:07+00002013","id_str":"343136551111111111","id":311111111,"location":"FarAway","coordinates":[6,50]}
{"created_at":"MonJun1002:12:06+00002013","id_str":"343136557777777777","id":377777777,"location":"NearHere","coordinates":[13,93]}
```

Notice that PXF creates a flat JSON structure.

### <a id="write_example2"></a>Example: Single Object Per File (Write)

Use the following [CREATE EXTERNAL TABLE](https://docs.vmware.com/en/VMware-Greenplum/6/greenplum-database/ref_guide-sql_commands-CREATE_EXTERNAL_TABLE.html) SQL command to create a writable external table that writes JSON data in single object format and uses the PXF default server.

You must specify the `ROOT` keyword and associated value in the `LOCATION` clause. For example:

``` sql
CREATE WRITABLE EXTERNAL TABLE add_singleobj_json_tbl(
  created_at TEXT,
  id_str TEXT,
  id INTEGER,
  location TEXT,
  coordinates INTEGER[]
)
LOCATION('pxf://data/pxf_examples/jso?PROFILE=hdfs:json&ROOT=root')
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_export');
```

`root` identifies the name of the root attribute of the single object.

Write data to the table:

``` sql
INSERT INTO add_singleobj_json_tbl VALUES ( 'SunJun0912:59:07+00002013', '343136551111111111', 311111111, 'FarAway', '{ 6, 50 }' );
INSERT INTO add_singleobj_json_tbl VALUES ( 'WedJun1212:37:02+00002013', '333333333333333333', 333333333, 'NetherWorld', '{ 9, 63 }' );
```

Read the data that you just wrote. Recall that you must first create a new readable external table:

``` sql
CREATE EXTERNAL TABLE jso_tbl(
  created_at TEXT,
  id_str TEXT,
  id INTEGER,
  location TEXT,
  coordinates INTEGER[]
)
LOCATION('pxf://data/pxf_examples/jso?PROFILE=hdfs:json&IDENTIFIER=created_at')
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');
```

The column names that you specify in the create command must match those of the writable external table. And recall that to read a JSON file that contains a single object, you must specify the `IDENTIFIER` option.

Query the table to read the data:

``` sql
SELECT * FROM jso_tbl;

        created_at         |       id_str       |    id     |   location   | coordinates 
---------------------------+--------------------+-----------+--------------+-------------
 WedJun1212:37:02+00002013 | 333333333333333333 | 333333333 | NetherWorld  | {9,63}
 SunJun0912:59:07+00002013 | 343136551111111111 | 311111111 | FarAway      | {6,50}
(2 rows)
```

View the files added to HDFS:

```
$ hdfs dfs -cat /data/pxf_examples/jso/*
{"root":[
{"created_at":"SunJun0912:59:07+00002013","id_str":"343136551111111111","id":311111111,"location":"FarAway","coordinates":[6,50]}
]}
{"root":[
{"created_at":"WedJun1212:37:02+00002013","id_str":"333333333333333333","id":333333333,"location":"NetherWorld","coordinates":[9,63]}
]}
```

