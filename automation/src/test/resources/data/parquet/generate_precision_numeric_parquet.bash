#!/bin/bash

set -euxo pipefail

# Run this command to generate the numeric.parquet file

SRC_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
BASE_DIR=$(echo "${SRC_DIR}" | cut -d "/" -f5)
UNIT_TEST_SRC_DIR=~/workspace/"${BASE_DIR}"/server/pxf-hdfs/src/test/resources/parquet
NUMERIC_DATA_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && cd ../numeric && pwd)
HDFS_CMD=${HDFS_CMD:-~/workspace/singlecluster-HDP3/bin/hdfs}
HIVE_CMD=${HIVE_CMD:-~/workspace/singlecluster-HDP3/bin/hive}
HIVE_WAREHOUSE_PATH=${HIVE_WAREHOUSE_PATH:-/hive/warehouse/precision_numeric_parquet}
HQL_FILENAME=${HQL_FILENAME:-generate_precision_numeric_parquet.hql}
CSV_FILENAME=${CSV_FILENAME:-numeric_with_precision.csv}
PARQUET_FILENAME=${PARQUET_FILENAME:-numeric_with_precision.parquet}

$HDFS_CMD dfs -rm -r -f /tmp/csv/
$HDFS_CMD dfs -mkdir /tmp/csv/
# Copy source CSV file to HDFS
$HDFS_CMD dfs -copyFromLocal "${NUMERIC_DATA_DIR}/${CSV_FILENAME}" /tmp/csv/

# Open connection and run hql file
"$HIVE_CMD" -u jdbc:hive2://localhost:10000/ -f "${SRC_DIR}/${HQL_FILENAME}"

# Copy file to the directory where this script resides
rm -f "${SRC_DIR}/${PARQUET_FILENAME}"
$HDFS_CMD dfs -copyToLocal "${HIVE_WAREHOUSE_PATH}/000000_0" "${SRC_DIR}/${PARQUET_FILENAME}"
# Copy file to unit test resource directory
rm -f "${UNIT_TEST_SRC_DIR}/${PARQUET_FILENAME}"
$HDFS_CMD dfs -copyToLocal "${HIVE_WAREHOUSE_PATH}/000000_0" "${UNIT_TEST_SRC_DIR}/${PARQUET_FILENAME}"