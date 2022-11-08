#!/bin/bash

set -euxo pipefail

# Run this command to generate the numeric.parquet file

SRC_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
NUMERIC_DATA_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && cd ../numeric && pwd)
HDFS_CMD=${HDFS_CMD:-~/workspace/singlecluster/bin/hdfs}
HIVE_CMD=${HIVE_CMD:-~/workspace/singlecluster/bin/hive}
HIVE_WAREHOUSE_PATH=${HIVE_WAREHOUSE_PATH:-/hive/warehouse/precision_numeric_parquet}
HQL_FILENAME=${HQL_FILENAME:-generate_precision_numeric_parquet.hql}
CSV_FILENAME=${CSV_FILENAME:-numeric_with_precision.csv}
PARQUET_FILENAME=${PARQUET_FILENAME:-parquet_types.parquet}

$HDFS_CMD dfs -rm -r -f /tmp/csv/
$HDFS_CMD dfs -mkdir /tmp/csv/
# Copy source CSV file to HDFS
$HDFS_CMD dfs -copyFromLocal "${NUMERIC_DATA_DIR}/${CSV_FILENAME}" /tmp/csv/
# Run the HQL file
$HIVE_CMD -f "${SRC_DIR}/${HQL_FILENAME}"
# Copy file to the directory where this script resides
$HDFS_CMD dfs -copyToLocal "${HIVE_WAREHOUSE_PATH}/000000_0" "${SRC_DIR}/${PARQUET_FILENAME}"