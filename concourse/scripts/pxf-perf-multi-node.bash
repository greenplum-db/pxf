#!/usr/bin/env bash

export PGHOST=mdw

set -exo pipefail

function create_database_and_schema {
	psql <<EOF
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

function create_readable_external_table {
	psql -c "CREATE PROTOCOL pxf"
	psql -c "CREATE EXTERNAL TABLE hdfs_lineitem (like lineitem) LOCATION ('pxf://tmp/lineitem.tbl?PROFILE=HdfsTextSimple') FORMAT 'CSV'"
}

function write_data_from_external_to_gpdb {
	psql -c "CREATE EXTERNAL TABLE hdfs_lineitem (like lineitem) LOCATION ('pxf://tmp/lineitem.tbl?PROFILE=HdfsTextSimple') FORMAT 'CSV'"
}

function main {
	create_database_and_schema
	create_readable_external_table

	time write_data_from_external_to_gpdb

#	create_writable_external_table
#	write_data_from_gpdb_to_external # time
#
#	validate_data
}

exit 1
