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

Building
========

    ./gradlew clean build [buildRpm] [distTar]

    For all available tasks run: ./gradlew tasks