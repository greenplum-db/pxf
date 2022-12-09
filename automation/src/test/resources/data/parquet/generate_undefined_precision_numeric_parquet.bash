#!/bin/bash

set -euxo pipefail

# Run this command to generate the undefined_precision_parquet file

SRC_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
NUMERIC_DATA_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && cd ../numeric && pwd)
HDFS_CMD=${HDFS_CMD:-~/workspace/singlecluster/bin/hdfs}
HIVE_CMD=${HIVE_CMD:-~/workspace/singlecluster/bin/hive}

$HDFS_CMD dfs -rm -r -f /tmp/csv/
$HDFS_CMD dfs -mkdir /tmp/csv/
# Copy source CSV file to HDFS
$HDFS_CMD dfs -copyFromLocal "$NUMERIC_DATA_DIR"/undefined_precision_numeric.csv /tmp/csv/
# Run the HQL file
$HIVE_CMD -f "$SRC_DIR"/generate_undefined_precision_numeric_parquet.hql
# Copy file to the directory where this script resides
$HDFS_CMD dfs -copyToLocal /hive/warehouse/undefined_precision_numeric_parquet/000000_0 "$SRC_DIR"/undefined_precision_numeric.parquet