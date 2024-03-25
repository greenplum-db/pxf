SHELL := bash

.PHONY: all test prepare-schema

all: prepare-schema test

prepare-schema:
	psql -c 'CREATE DATABASE pxfload;'
	psql -d 'pxfload' -c 'CREATE EXTENSION IF NOT EXISTS pxf'
	psql -d 'pxfload' -f 'sql/create_tables.sql'

test:
	. workload/limit.bash
