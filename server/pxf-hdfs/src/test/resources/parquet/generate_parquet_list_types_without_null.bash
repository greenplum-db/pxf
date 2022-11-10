#!/bin/bash

set -euxo pipefail

# Run this command to generate the parquet_list_types_without_null.parquet file
SRC_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
HDFS_CMD=${HDFS_CMD:-~/workspace/singlecluster-HDP3/bin/hdfs}
HIVE_CMD=${HIVE_CMD:-~/workspace/singlecluster-HDP3/bin/hive}
HIVE_WAREHOUSE_PATH=${HIVE_WAREHOUSE_PATH:-/hive/warehouse/parquet_list_types_without_null}
HQL_FILENAME=${HQL_FILENAME:-generate_parquet_list_types_without_null.hql}
PARQUET_FILENAME=${PARQUET_FILENAME:-parquet_list_types_without_null.parquet}

"$HIVE_CMD" -f "${SRC_DIR}/${HQL_FILENAME}"

rm -f "${SRC_DIR}/${PARQUET_FILENAME}"
$HDFS_CMD dfs -copyToLocal "${HIVE_WAREHOUSE_PATH}/000000_0" "${SRC_DIR}/${PARQUET_FILENAME}"