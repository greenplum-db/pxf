#!/usr/bin/env bash

export PGHOST=mdw
export PGUSER=gpadmin
export PGDATABASE=tpch
GPHOME="/usr/local/greenplum-db-devel"
CWDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "${CWDIR}/../pxf_common.bash"

set -exo pipefail

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
    );
EOF
}

function create_external_tables {
    psql -c "CREATE EXTERNAL TABLE hdfs_lineitem_read (like lineitem) LOCATION ('pxf://tmp/lineitem.tbl?PROFILE=HdfsTextSimple') FORMAT 'CSV' (DELIMITER '|')"
    psql -c "CREATE WRITABLE EXTERNAL TABLE hdfs_lineitem_write (like lineitem) LOCATION ('pxf://tmp/lineitem_write/?PROFILE=HdfsTextSimple') FORMAT 'CSV'"
}

function write_data_from_external_to_gpdb {
    psql -c "INSERT INTO lineitem select * from hdfs_lineitem_read;"
}

function write_data_from_gpdb_to_external {
    psql -c "INSERT INTO hdfs_lineitem_write select * from lineitem"
}

function validate_write_to_gpdb {
    local external_values=
    local gpdb_values=
    external_values=$(psql -c "SELECT COUNT(*), COUNT(DISTINCT l_orderkey), SUM(l_partkey), COUNT(DISTINCT l_suppkey), SUM(l_linenumber) FROM hdfs_lineitem_read")
    gpdb_values=$(psql -c "SELECT COUNT(*), COUNT(DISTINCT l_orderkey), SUM(l_partkey), COUNT(DISTINCT l_suppkey), SUM(l_linenumber) FROM lineitem")

    echo Results from external query
    echo ${external_values}
    echo Results from GPDB query
    echo ${gpdb_values}
}

function validate_write_to_external {
#    psql -c "SELECT COUNT(*), COUNT(DISTINCT l_orderkey), SUM(l_partkey), COUNT(DISTINCT l_suppkey), SUM(l_linenumber) FROM lineitem"
}

function main {
    setup_gpadmin_user
    setup_sshd
    remote_access_to_gpdb
    install_gpdb

    source ${GPHOME}/greenplum_path.sh
    echo Write Benchmark
    create_database_and_schema
    echo Read Benchmark
    create_external_tables

    time write_data_from_external_to_gpdb
    validate_write_to_gpdb
    time write_data_from_gpdb_to_external
    validate_write_to_external
}

main
