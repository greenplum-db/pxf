#!/bin/bash

set -euxo pipefail

# Run this command to generate the parquet_list_types.parquet file
SRC_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
HDFS_CMD=${HDFS_CMD:-~/workspace/singlecluster-HDP3/bin/hdfs}
HIVE_CMD=${HIVE_CMD:-~/workspace/singlecluster-HDP3/bin/hive}

$HIVE_CMD -f "$SRC_DIR"/generate_parquet_list_types.hql

# Copy file to the directory where this script resides
rm -f "$SRC_DIR"/parquet_list_types.parquet
$HDFS_CMD dfs -copyToLocal /hive/warehouse/parquet_list_types/000000_0 "$SRC_DIR"/parquet_list_types.parquet
rm -f "/Users/yimingli/workspace/pxf/server/pxf-hdfs/src/test/resources/parquet/parquet_list_types.parquet"
$HDFS_CMD dfs -copyToLocal /hive/warehouse/parquet_list_types/000000_0 "/Users/yimingli/workspace/pxf/server/pxf-hdfs/src/test/resources/parquet/parquet_list_types.parquet"