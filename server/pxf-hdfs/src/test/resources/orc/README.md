# Generate ORC files for testing

These instructions will help you generate the ORC files required for testing.
The files are pre-generated, but if you want to generate these files again,
follow the instructions below.

## Requirements

- Oracle Java Version 1.8
- orc-tools-1.7.5-uber.jar

Note: If you download orc-tools-1.7.5-uber.jar not into the current project path, you will need to:

```shell script
export ORC_TOOLS_JAR="your orc-tools-1.7.5-uber.jar path"
```

## Generate the orc_types.orc file

```shell script
make orc_type.orc
```

## Generate the orc_types_unordered_subset.orc file

The `orc_types_unordered_subset.orc` file contains a subset of columns of the
`orc_types.orc` file in different order. The subset of columns in this file
are `b`, `num1`, `tm`,`tmtz`, `vc1`, `dec1`, `t1`, and `sml`.

```shell script
make orc_types_unordered_subset.orc 
```

The `orc_types_unordered_subset.orc` file will be copied to the directory
where you ran the script.


## Generate the orc_types_compound.orc and orc_types_compound_multi.orc files

The `orc_types_compound.orc` file contains a table with compound types.
The columns contained in this file are `id`, `bool_arr`, `smallint_arr`, `int_arr`, `bigint_arr`, `float_arr`, `double_arr`, `text_arr` and `bytea_arr`.


```shell script
make orc_types_compound.orc
```

The `orc_types_compound_multi.orc` file contains a table similar to the `orc_types_compound.orc`.
The main difference is that it contains nested arrays instead of 1-dimensional arrays.

```shell script
make orc_types_compound_multi.orc
```

## Generate the orc_types_repeated.orc file

```shell script
make orc_types_repeated.orc
```


## Origin of orc_file_predicate_pushdown.orc file

The `orc_file_predicate_pushdown.orc` file comes from the ORC repository:
https://github.com/apache/orc/blob/master/examples/over1k_bloom.orc

```shell script
cp ~/workspace/orc/examples/over1k_bloom.orc ~/workspace/pxf/server/pxf-hdfs/src/test/resources/orc/
```

