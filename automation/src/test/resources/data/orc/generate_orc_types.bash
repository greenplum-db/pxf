#!/bin/bash

# Requires Hive 2.3+

set -euxo pipefail

# Run this command to generate the ORC file

SRC_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
HDFS_CMD=${HDFS_CMD:-~/workspace/singlecluster/bin/hdfs}
HIVE_CMD=${HIVE_CMD:-~/workspace/singlecluster/bin/hive}
HDFS_DIR=${HDFS_DIR:-/tmp/orc_types/csv}
HIVE_WAREHOUSE_PATH=${HIVE_WAREHOUSE_PATH:-/hive/warehouse/hive_orc_all_types}
CSV_FILENAME=${CSV_FILENAME:-orc_types.csv}
HQL_FILENAME=${HQL_FILENAME:-generate_orc_types.hql}
ORC_FILENAME=${ORC_FILENAME:-orc_types.orc}

"$HDFS_CMD" dfs -rm -r -f "$HDFS_DIR"
"$HDFS_CMD" dfs -mkdir -p "$HDFS_DIR"
# Copy source CSV file to HDFS
"$HDFS_CMD" dfs -copyFromLocal "${SRC_DIR}/${CSV_FILENAME}" "$HDFS_DIR"
# Run the HQL file
"$HIVE_CMD" -f "${SRC_DIR}/${HQL_FILENAME}"

rm -f "${SRC_DIR}/${ORC_FILENAME}"
# Copy file to the directory where this script resides
"$HDFS_CMD" dfs -copyToLocal "${HIVE_WAREHOUSE_PATH}/000000_0" "${SRC_DIR}/${ORC_FILENAME}"