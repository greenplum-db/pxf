# Generate Parquet files for testing

These instructions will help you generate the Parquet files required for testing.
The files are pre-generated, but if you want to generate these files again,
follow the instructions below.

## Requirements

- Hadoop CLI commands
- Hive version 2.3+
- Spark

## Generate Parquet LIST Testing Data（Except TIMESTAMP LIST） using Hive

### Generate the parquet_types.parquet file using Hive

Run the script to generate the `parquet_types.parquet` file:

```shell script
./generate_parquet_types.bash
```

The `parquet_types.parquet` file will be copied to the directory where you ran the script.

### Generate the numeric.parquet file using Hive

Run the script to generate the `numeric.parquet` file:

```shell script
./generate_precision_numeric_parquet.bash
```

The `numeric.parquet` file will be copied to the directory where you ran the script.

### Generate the undefined_precision_numeric.parquet file using Hive

Run the script to generate the `undefined_precision_numeric.parquet` file:

```shell script
./generate_undefined_precision_numeric_parquet.bash
```

The `undefined_precision_numeric.parquet` file will be copied to the directory where you ran the script.

### Generate the parquet_list_types.parquet file using Hive

Run the script to generate the `parquet_list_types.parquet` file:

```shell script
./generate_parquet_list_types.bash
```

The `parquet_list_types.parquet` file will be copied to the directory where you ran the script.

### Generate the parquet_list_types_without_null.parquet file using Hive

Run the script to generate the `parquet_list_types_without_null.parquet` file:

```shell script
./generate_parquet_list_types_without_null.bash
```

The `parquet_list_types_without_null.parquet` file will be copied to the directory where you ran the script.

## Generate Parquet TIMESTAMP LIST Testing Data using Spark

According to the latest version of[Hive](https://github.com/apache/hive/blob/4e4e39c471094567dcdfd9840edbd99d7eafc230/ql/src/java/org/apache/hadoop/hive/ql/io/parquet/vector/VectorizedParquetRecordReader.java#L578),
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
scala> df2.repartition(1).write.parquet("~/workspace/pxf/automation/src/test/resources/data/parquet/parquet_timestamp_list_type.parquet")
```
