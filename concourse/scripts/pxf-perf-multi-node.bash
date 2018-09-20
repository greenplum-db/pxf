#!/usr/bin/env bash

export PGHOST=mdw
export PGUSER=gpadmin
export PGDATABASE=tpch
GPHOME="/usr/local/greenplum-db-devel"
CWDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
HADOOP_HOSTNAME="ccp-$(cat terraform_dataproc/name)-m"
VALIDATION_QUERY="COUNT(*) AS Total, COUNT(DISTINCT l_orderkey) AS ORDERKEYS, SUM(l_partkey) AS PARTKEYSUM, COUNT(DISTINCT l_suppkey) AS SUPPKEYS, SUM(l_linenumber) AS LINENUMBERSUM"
source "${CWDIR}/../pxf_common.bash"

set -eo pipefail

function create_database_and_schema {
    psql -d postgres <<-EOF
    DROP DATABASE IF EXISTS tpch;
    CREATE DATABASE tpch;
    \c tpch;
    DROP TABLE IF EXISTS lineitem;
    CREATE TABLE lineitem (
        l_orderkey    INTEGER NOT NULL,
        l_partkey     INTEGER NOT NULL,
        l_suppkey     INTEGER NOT NULL,
        l_linenumber  INTEGER NOT NULL,
        l_quantity    DECIMAL(15,2) NOT NULL,
        l_extendedprice  DECIMAL(15,2) NOT NULL,
        l_discount    DECIMAL(15,2) NOT NULL,
        l_tax         DECIMAL(15,2) NOT NULL,
        l_returnflag  CHAR(1) NOT NULL,
        l_linestatus  CHAR(1) NOT NULL,
        l_shipdate    DATE NOT NULL,
        l_commitdate  DATE NOT NULL,
        l_receiptdate DATE NOT NULL,
        l_shipinstruct CHAR(25) NOT NULL,
        l_shipmode     CHAR(10) NOT NULL,
        l_comment VARCHAR(44) NOT NULL
    ) DISTRIBUTED BY (l_partkey);
EOF
    if [ "${BENCHMARK_GPHDFS}" == "true" ]; then
        psql -c "CREATE TABLE lineitem_gphdfs (LIKE lineitem) DISTRIBUTED BY (l_partkey)"
    fi
}

function create_pxf_external_tables {
    psql -c "CREATE EXTERNAL TABLE pxf_lineitem_read (like lineitem) LOCATION ('pxf://tmp/lineitem_read/?PROFILE=HdfsTextSimple') FORMAT 'CSV' (DELIMITER '|')"
    psql -c "CREATE WRITABLE EXTERNAL TABLE pxf_lineitem_write (like lineitem) LOCATION ('pxf://tmp/lineitem_write/?PROFILE=HdfsTextSimple') FORMAT 'CSV' DISTRIBUTED BY (l_partkey)"
}

function create_gphdfs_external_tables {
    psql -c "CREATE EXTERNAL TABLE gphdfs_lineitem_read (like lineitem) LOCATION ('gphdfs://${HADOOP_HOSTNAME}:8020/tmp/lineitem_read_gphdfs/') FORMAT 'CSV' (DELIMITER '|')"
    psql -c "CREATE WRITABLE EXTERNAL TABLE gphdfs_lineitem_write (like lineitem) LOCATION ('gphdfs://${HADOOP_HOSTNAME}:8020/tmp/lineitem_write_gphdfs/') FORMAT 'CSV' DISTRIBUTED BY (l_partkey)"
}

function write_data {
    local dest
    local source
    dest=${2}
    source=${1}
    psql -c "INSERT INTO ${dest} select * from ${source}"
}

function validate_write_to_gpdb {
    local external
    local internal
    local external_values
    local gpdb_values
    external=${1}
    internal=${2}
    external_values=$(psql -t -c "SELECT ${VALIDATION_QUERY} FROM ${external}")
    gpdb_values=$(psql -t -c "SELECT ${VALIDATION_QUERY} FROM ${internal}")

    cat << EOF

Results from external query
------------------------------
EOF
    echo ${external_values}
    cat << EOF

Results from GPDB query
------------------------------
EOF
    echo ${gpdb_values}

    if [ "${external_values}" != "${gpdb_values}" ]; then
        echo ERROR! Unable to validate data written from external to GPDB
        exit 1
    fi
}

function gphdfs_validate_write_to_external {
    psql -c "CREATE EXTERNAL TABLE gphdfs_lineitem_read_after_write (like lineitem) LOCATION ('gphdfs://${HADOOP_HOSTNAME}:8020/tmp/lineitem_write_gphdfs/') FORMAT 'CSV'"
    local external_values
    local gpdb_values
    external_values=$(psql -t -c "SELECT ${VALIDATION_QUERY} FROM gphdfs_lineitem_read_after_write")
    gpdb_values=$(psql -t -c "SELECT ${VALIDATION_QUERY} FROM lineitem_gphdfs")

    cat << EOF

Results from external query
------------------------------
EOF
    echo ${external_values}
    cat << EOF

Results from GPDB query
------------------------------
EOF
    echo ${gpdb_values}

    if [ "${external_values}" != "${gpdb_values}" ]; then
        echo ERROR! Unable to validate data written from GPDB to external
        exit 1
    fi
}

function pxf_validate_write_to_external {
    psql -c "CREATE EXTERNAL TABLE pxf_lineitem_read_after_write (like lineitem) LOCATION ('pxf://tmp/lineitem_write/?PROFILE=HdfsTextSimple') FORMAT 'CSV'"
    local external_values
    local gpdb_values
    external_values=$(psql -t -c "SELECT ${VALIDATION_QUERY} FROM pxf_lineitem_read_after_write")
    gpdb_values=$(psql -t -c "SELECT ${VALIDATION_QUERY} FROM lineitem")

    cat << EOF

Results from external query
------------------------------
EOF
    echo ${external_values}
    cat << EOF

Results from GPDB query
------------------------------
EOF
    echo ${gpdb_values}

    if [ "${external_values}" != "${gpdb_values}" ]; then
        echo ERROR! Unable to validate data written from GPDB to external
        exit 1
    fi
}

function run_pxf_benchmark {
    create_pxf_external_tables

    cat << EOF


############################
#    PXF READ BENCHMARK    #
############################
EOF
    time write_data "pxf_lineitem_read" "lineitem"
    validate_write_to_gpdb "pxf_lineitem_read" "lineitem"

    cat << EOF


############################
#   PXF WRITE BENCHMARK    #
############################
EOF
    time write_data "lineitem" "pxf_lineitem_write"
    pxf_validate_write_to_external
}

function run_gphdfs_benchmark {
    create_gphdfs_external_tables

    cat << EOF


############################
#  GPHDFS READ BENCHMARK   #
############################
EOF
    time write_data "gphdfs_lineitem_read" "lineitem_gphdfs"
    validate_write_to_gpdb "gphdfs_lineitem_read" "lineitem_gphdfs"

    cat << EOF


############################
#  GPHDFS WRITE BENCHMARK  #
############################
EOF
    time write_data "lineitem_gphdfs" "gphdfs_lineitem_write"
    gphdfs_validate_write_to_external
}

function main {
    setup_gpadmin_user
    setup_sshd
    remote_access_to_gpdb
    install_gpdb

    source ${GPHOME}/greenplum_path.sh
    create_database_and_schema
    if [ "${BENCHMARK_GPHDFS}" == "true" ]; then
        run_gphdfs_benchmark
    fi
    run_pxf_benchmark
}

main
