#!/bin/bash

set -euxo pipefail

# Run this command to generate the undefined_precision_parquet file

SRC_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
BASE_DIR=$(echo "${SRC_DIR}" | cut -d "/" -f5)
UNIT_TEST_SRC_DIR=~/workspace/"${BASE_DIR}"/server/pxf-hdfs/src/test/resources/parquet
NUMERIC_DATA_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && cd ../numeric && pwd)
HDFS_CMD=${HDFS_CMD:-~/workspace/singlecluster-HDP3/bin/hdfs}
HIVE_CMD=${HIVE_CMD:-~/workspace/singlecluster-HDP3/bin/hive}
HIVE_WAREHOUSE_PATH=${HIVE_WAREHOUSE_PATH:-/hive/warehouse/undefined_precision_numeric_parquet}
HQL_FILENAME=${HQL_FILENAME:-generate_undefined_precision_numeric_parquet.hql}
CSV_FILENAME=${CSV_FILENAME:-undefined_precision_numeric.csv}
PARQUET_FILENAME=${PARQUET_FILENAME:-undefined_precision_numeric.parquet}

$HDFS_CMD dfs -rm -r -f /tmp/csv/
$HDFS_CMD dfs -mkdir /tmp/csv/
# Copy source CSV file to HDFS
$HDFS_CMD dfs -copyFromLocal "${NUMERIC_DATA_DIR}/${CSV_FILENAME}" /tmp/csv/
# Run the HQL file
$HIVE_CMD -f "${SRC_DIR}/${HQL_FILENAME}"
# Copy file to the directory where this script resides
rm -f "${SRC_DIR}/${PARQUET_FILENAME}"
$HDFS_CMD dfs -copyToLocal  "${HIVE_WAREHOUSE_PATH}/000000_0" "${SRC_DIR}/${PARQUET_FILENAME}"
# Copy file to unit test resource directory
rm -f "${UNIT_TEST_SRC_DIR}/${PARQUET_FILENAME}"
$HDFS_CMD dfs -copyToLocal "${HIVE_WAREHOUSE_PATH}/000000_0" "${UNIT_TEST_SRC_DIR}/${PARQUET_FILENAME}"