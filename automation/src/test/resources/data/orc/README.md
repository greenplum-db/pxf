# Generate ORC files for testing

These instructions will help you generate the ORC files required for testing.
The files are pre-generated, but if you want to generate these files again,
follow the instructions below.

## Requirements

- Hadoop CLI commands
- Hive version 2.3+

## Generate the orc_types.orc file

Identify your hdfs and hive commands. Identify the Hive warehouse path where
table data gets stored, for example:

```shell script
export HDFS_CMD=$(which hdfs)
export HIVE_CMD=$(which hive)
export HIVE_WAREHOUSE_PATH=/warehouse/tablespace/managed/hive/hive_orc_all_types
```

Finally, run the script to generate the `orc_types.orc` file:

```shell script
./generate_orc_types.bash
```

The `orc_types.orc` file will be copied to the directory where you ran the
script.

## Generate the orc_types_unordered_subset.orc file

The `orc_types_unordered_subset.orc` file contains a subset of columns of the
`orc_types.orc` file in different order. The subset of columns in this file
are `b`, `num1`, `tm`, `vc1`, `dec1`, `t1`, and `sml`.

```shell script
export HDFS_CMD=$(which hdfs)
export HIVE_CMD=$(which hive)
export HIVE_WAREHOUSE_PATH=/warehouse/tablespace/managed/hive/hive_orc_all_types
export CSV_FILENAME=orc_types_unordered_subset.csv
export HQL_FILENAME=generate_orc_types_unordered_subset.hql
export ORC_FILENAME=orc_types_unordered_subset.orc
```

Finally, run the script to generate the `orc_types_unordered_subset.orc` file:

```shell script
./generate_orc_types.bash
```

The `orc_types_unordered_subset.orc` file will be copied to the directory
where you ran the script.

## Generate the orc_list_types.orc and orc_multidim_list_types.orc files

The `orc_list_types.orc` file contains a table with compound types.
The columns contained in this file are `id`, `bool_arr`, `smallint_arr`, `int_arr`, `bigint_arr`, `float_arr`, `double_arr`, `text_arr` and `bytea_arr`.

```shell script
export HDFS_CMD=$(which hdfs)
export HIVE_CMD=$(which hive)
export HIVE_WAREHOUSE_PATH=/hive/warehouse/hive_orc_all_types
export CSV_FILENAME=orc_list_types.csv
export HQL_FILENAME=generate_orc_list_types.hql
export ORC_FILENAME=orc_list_types.orc
```

Finally, run the script to generate the `orc_list_types.orc` file:

```shell script
./generate_orc_types.bash
```

The `orc_list_types.orc` file will be copied to the directory
where you ran the script. The `orc_list_types.orc` file is generated through insert statements in the HQL.
If desired, you can copy down the CSV file by running the following command:
```
mv "./orc_list_types/000000_0" "./${CSV_FILENAME}"
```

The `orc_multidim_list_types.orc` file contains a table similar to the `orc_list_types.orc`.
The main difference is that it contains nested arrays instead of 1-dimensional arrays.

```shell script
export HDFS_CMD=$(which hdfs)
export HIVE_CMD=$(which hive)
export HIVE_WAREHOUSE_PATH=/hive/warehouse/hive_orc_all_types
export CSV_FILENAME=orc_multidim_list_types.csv
export HQL_FILENAME=generate_orc_multidim_list_types.hql
export ORC_FILENAME=orc_multidim_list_types.orc
```

Finally, run the script to generate the `orc_multidim_list_types.orc` file:

```shell script
./generate_orc_types.bash
```
Similar to the files listed above, the `orc_multidim_list_types.orc` file is generated through insert statements in the HQL.
If desired, you can copy down the CSV file by running the following command:
```
mv "./orc_multidim_list_types/000000_0" "./${CSV_FILENAME}"
```

## Generate the orc_null_in_string.orc file

To generate this file, you will need to download a copy of the ORC Tools uber jar: <https://search.maven.org/remotecontent?filepath=org/apache/orc/orc-tools/1.8.0/orc-tools-1.8.0-uber.jar>

```shell
java -jar orc-tools-1.8.0-uber.jar convert --schema 'struct<id:int,desc:string,value:string>' --output orc_null_in_string.orc orc_null_in_string.json
```
