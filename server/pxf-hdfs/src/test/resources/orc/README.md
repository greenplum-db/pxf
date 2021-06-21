# Generate ORC files for testing

These instructions will help you generate the ORC files required for testing.
The files are pre-generated, but if you want to generate these files again,
follow the instructions below.

## Requirements

- Hadoop CLI commands
- Hive version 2.3+
- Java Version 1.8

Note: The scripts will run with a older version of Hive. However, to get the
correct schema inside the ORC files Hive 2.3+ is required. Please double check
that the Hive version is correct.

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


## Generate the orc_types_compound.orc and orc_types_compound_multi.orc files

The `orc_types_compound.orc` file contains a table with compound types.
The columns contained in this file are `id`, `bool_arr`, `smallint_arr`, `int_arr`, `bigint_arr`, `float_arr`, `double_arr`, `text_arr` and `bytea_arr`.

```shell script
export HDFS_CMD=$(which hdfs)
export HIVE_CMD=$(which hive)
export HIVE_WAREHOUSE_PATH=/hive/warehouse/hive_orc_all_types
export CSV_FILENAME=orc_types_compound.csv
export HQL_FILENAME=generate_orc_types_compound.hql
export ORC_FILENAME=orc_types_compound.orc
```

Finally, run the script to generate the `orc_types_compound.orc` file:

```shell script
./generate_orc_types.bash
```

The `orc_types_compound.orc` file will be copied to the directory
where you ran the script. The `orc_types_compound.orc` file is generated through insert statements in the HQL.
If desired, you can copy down the CSV file by running the following command:
```
mv "./orc_types_compound/000000_0" "./${CSV_FILENAME}"
```

The `orc_types_compound_multi.orc` file contains a table similar to the `orc_types_compound.orc`.
The main difference is that it contains nested arrays instead of 1-dimensional arrays.

```shell script
export HDFS_CMD=$(which hdfs)
export HIVE_CMD=$(which hive)
export HIVE_WAREHOUSE_PATH=/hive/warehouse/hive_orc_all_types
export CSV_FILENAME=orc_types_compound_multi.csv
export HQL_FILENAME=generate_orc_types_compound_multi.hql
export ORC_FILENAME=orc_types_compound_multi.orc
```

Finally, run the script to generate the `orc_types_compound_multi.orc` file:

```shell script
./generate_orc_types.bash
```
Similar to the files listed above, the `orc_types_compound_multi.orc` file is generated through insert statements in the HQL.
If desired, you can copy down the CSV file by running the following command:
```
mv "./orc_types_compound_multi/000000_0" "./${CSV_FILENAME}"
```

## Origin of orc_file_predicate_pushdown.orc file

The `orc_file_predicate_pushdown.orc` file comes from the ORC repository:
https://github.com/apache/orc/blob/master/examples/over1k_bloom.orc

```shell script
cp ~/workspace/orc/examples/over1k_bloom.orc ~/workspace/pxf/server/pxf-hdfs/src/test/resources/orc/
```

