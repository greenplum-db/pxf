#!/bin/bash

set -euxo pipefail

# Run this command to generate the parquet_list_types_without_null.parquet file
SRC_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
BASE_DIR=$(echo "${SRC_DIR}" | cut -d "/" -f5)
UNIT_TEST_SRC_DIR=~/workspace/"${BASE_DIR}"/server/pxf-hdfs/src/test/resources/parquet
SPARK_SUBMIT_CMD=${SPARK_SUBMIT_CMD:-/usr/local/spark/bin/spark-submit}
PYTHON_FILENAME=${PYTHON_FILENAME:-generate_parquet_timestamp_list.py}
PARQUET_FILENAME=${PARQUET_FILENAME:-parquet_timestamp_list_type.parquet}


TZ="America/Los_Angeles" "$SPARK_SUBMIT_CMD" "${PYTHON_FILENAME}"

# Copy file to the directory where this script resides
rm -f "${SRC_DIR}/${PARQUET_FILENAME}"
cp "${SRC_DIR}"/tmp.parquet/part*.parquet "${SRC_DIR}/${PARQUET_FILENAME}"
# Copy file to unit test resource directory
rm -f "${UNIT_TEST_SRC_DIR}/${PARQUET_FILENAME}"
cp "${SRC_DIR}"/tmp.parquet/part*.parquet "${UNIT_TEST_SRC_DIR}/${PARQUET_FILENAME}"

rm -rf "${SRC_DIR}"/tmp.parquet