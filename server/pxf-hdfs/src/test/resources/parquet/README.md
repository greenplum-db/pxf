# Generate Parquet files for testing

These instructions will help you generate the Parquet files required for testing.
The files are pre-generated, but if you want to generate these files again,
follow the instructions below.

## Requirements

- Hadoop CLI commands
- Hive version 2.3+
- Spark

## Generate the parquet_types.parquet file using Hive

Identify your HDFS and HIVE commands. Identify the Hive warehouse path where
table data gets stored. Identify your data filenames, for example:

```shell
export HDFS_CMD=$(which hdfs)
export HIVE_CMD=$(which hive)
export HIVE_WAREHOUSE_PATH=/hive/warehouse/parquet_types
export HQL_FILENAME=generate_parquet_types.hql
export PARQUET_FILENAME=parquet_types.parquet
```

Finally, run the script to generate the `parquet_types.parquet` file:

```shell script
./generate_parquet_types.bash
```

The `parquet_types.parquet` file will be copied to the directory where you ran the
script.

## Generate the parquet_list_types.parquet file using Hive

Identify your HDFS and HIVE commands. Identify the Hive warehouse path where
table data gets stored. Identify your data filenames, for example:

```shell script
export HDFS_CMD=$(which hdfs)
export HIVE_CMD=$(which hive)
export HIVE_WAREHOUSE_PATH=/hive/warehouse/parquet_list_types
export HQL_FILENAME=generate_parquet_list_types.hql
export PARQUET_FILENAME=parquet_list_types.parquet
```

Finally, run the script to generate the `parquet_list_types.parquet` file:

```shell script
./generate_parquet_list_types.bash
```

The `parquet_list_types.parquet` file will be copied to the directory where you ran the
script.

## Generate the parquet_list_types_without_null.parquet file using Hive

Identify your HDFS and HIVE commands. Identify the Hive warehouse path where
table data gets stored. Identify your data filenames, for example:

```shell script
export HDFS_CMD=$(which hdfs)
export HIVE_CMD=$(which hive)
export HIVE_WAREHOUSE_PATH=/hive/warehouse/parquet_list_types_without_null
export HQL_FILENAME=generate_parquet_list_types_without_null.hql
export PARQUET_FILENAME=parquet_list_types_without_null.parquet
```

Finally, run the script to generate the `parquet_list_types_without_null.parquet` file:

```shell script
./generate_parquet_list_types_without_null.bash
```

The `parquet_list_types_without_null.parquet` file will be copied to the directory where you ran the
script.

## Generate the parquet_timestamp_list_type.parquet file using Spark

According to the latest version of [Hive](https://github.com/apache/hive/blob/4e4e39c471094567dcdfd9840edbd99d7eafc230/ql/src/java/org/apache/hadoop/hive/ql/io/parquet/vector/VectorizedParquetRecordReader.java#L578),
Hive doesn't support TIMESTAMP LIST. Therefore, we use Spark to generate TIMESTAMP LIST dataset. Note that the input timestamps are in
local time zone, and Parquet will store them in UTC time zone.

```shell
import org.apache.spark.sql.types._
# prepare the timestamp array dataset in string type
scala> val df = Seq(
    (1,List("2022-10-05 11:30:00","2022-10-06 12:30:00","2022-10-07 13:30:00")),
    (2, List("2022-10-05 11:30:00","2022-10-05 11:30:00","2022-10-07 13:30:00")),
    (3, List(null, "2022-10-05 11:30:00", "2022-10-05 11:30:00")),
    (4, List(null)),
    (5, List()),
    (6, null)
).toDF("id", "tm_arr")
  
# convert from array<String> to array<Timestamp>
scala> val df2=df.withColumn("tm_arr", expr("transform(tm_arr, x -> to_timestamp(x))"))
scala> df2.printSchema()
root
 |-- id: integer (nullable = false)
 |-- tm_arr: array (nullable = true)
 |    |-- element: timestamp (containsNull = true)
 # write data into a single parquet file
scala> df2.repartition(1).write.parquet("~/workspace/pxf/server/pxf-hdfs/src/test/resources/parquet/parquet_timestamp_list_type.parquet")
```
