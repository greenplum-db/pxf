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
