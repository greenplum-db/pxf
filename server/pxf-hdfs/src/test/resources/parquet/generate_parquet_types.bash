#!/bin/bash

# Requires Hive 2.3+

set -euxo pipefail

# Run this command to generate the parquet_types.parquet file

SRC_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
HDFS_CMD=${HDFS_CMD:-~/workspace/singlecluster/bin/hdfs}
HIVE_CMD=${HIVE_CMD:-~/workspace/singlecluster/bin/hive}
HDFS_DIR=${HDFS_DIR:-/tmp/parquet_types/csv}

"$HDFS_CMD" dfs -rm -r -f "$HDFS_DIR"
"$HDFS_CMD" dfs -mkdir -p "$HDFS_DIR"
# Copy source CSV file to HDFS
"$HDFS_CMD" dfs -copyFromLocal "$SRC_DIR/parquet_types.csv" "$HDFS_DIR"
# Run the HQL file
"$HIVE_CMD" -f "$SRC_DIR/generate_parquet_types.hql"

rm -f "$SRC_DIR/parquet_types.parquet"
# Copy file to the directory where this script resides
"$HDFS_CMD" dfs -copyToLocal /hive/warehouse/parquet_types/000000_0 "$SRC_DIR/parquet_types.parquet"