# Generate Parquet files for testing

These instructions will help you generate the Parquet files required for testing.
The files are pre-generated, but if you want to generate these files again,
follow the instructions below.

## Requirements

- Hadoop CLI commands
- Hive version 2.3+
- Spark 3.3.0

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

According to the latest version of [Hive](https://github.com/apache/hive/blob/4e4e39c471094567dcdfd9840edbd99d7eafc230/ql/src/java/org/apache/hadoop/hive/ql/io/parquet/vector/VectorizedParquetRecordReader.java#L578),
Hive doesn't support TIMESTAMP LIST. Therefore, we use Spark to generate TIMESTAMP LIST dataset. 

Note that the input and output timestamp data of Spark are all with zone, but Parquet stores timestamps in UTC time zone since Parquet only support `TIMESTAMP` type. 
For example, in our `generate_parquet_timestamp_list.py` and `generate_parquet_timestamp_list.bash`, we are preparing timestamp list dataset using 
`2022-10-05 11:30:00` in `America/Los Angeles` timezone. If we still use `America/Los Angeles` for reading, the output will still be `2022-10-05 11:30:00`. 
If we use another timezone like `America/New York`, the output will be `2022-10-05 14:30:00`. But if we access 
the generated parquet file `parquet_timestamp_list_type.parquet` directly, we will see that the timestamps are stored in UTC time `2022-10-05 18:30:00 +00:00`. 

```shell
export SPARK_SUBMIT_CMD=$(which spark-submit)

./generate_parquet_timestamp_list.bash
```
