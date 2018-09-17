PXF
===

Table of Contents
=================

* Introduction
* Package Contents
* Building

Introduction
============

PXF is an extensible framework that allows a distributed database like GPDB to query external data files, whose metadata is not managed by the database.
PXF includes built-in connectors for accessing data that exists inside HDFS files, Hive tables, HBase tables and more.
Users can also create their own connectors to other data storages or processing engines.
To create these connectors using JAVA plugins, see the PXF API and Reference Guide onGPDB.

Development
===========

Build and run PXF and GPDB on Docker by following the instructions in [DEVELOPMENT.md](DEVELOPMENT.md).

Alternatively, if you want to build directly without using docker do the following:

Compile & Test PXF
```bash
make
```
  
Setup PXF (Requires Greenplum instaled with GPHOME configured)

```bash
#Install PXF
make -C pxf install

# Initialize PXF
$PXF_HOME/bin/pxf init

# Start PXF
$PXF_HOME/bin/pxf start
```

Package Contents
================

## pxf/
Contains the server side code of PXF along with the PXF Service and all the Plugins

## pxf_automation/
Contains the automation and integration tests for PXF against the various datasources

## singlecluster/
Hadoop testing environment to exercise the pxf automation tests

## concourse/
Resources for PXF's Continuous Integration pipelines