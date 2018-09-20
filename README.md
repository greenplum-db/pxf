Introduction
============

PXF is an extensible framework that allows a distributed database like GPDB to query external data files, whose metadata is not managed by the database.
PXF includes built-in connectors for accessing data that exists inside HDFS files, Hive tables, HBase tables and more.
Users can also create their own connectors to other data storages or processing engines.
To create these connectors using JAVA plugins, see the PXF API and Reference Guide onGPDB.


Package Contents
================

## server/
Contains the server side code of PXF along with the PXF Service and all the Plugins

## automation/
Contains the automation and integration tests for PXF against the various datasources

## singlecluster/
Hadoop testing environment to exercise the pxf automation tests

## concourse/
Resources for PXF's Continuous Integration pipelines


PXF Development
=================
Below are the steps to build and install PXF along with its dependencies including GPDB and Hadoop.

To start, ensure you have a `~/workspace` directory and have cloned the `pxf` and its prerequisities(shown below) under it.
(The name `workspace` is not strictly required but will be used throughout this guide.)
Alternatively, you may create a symlink to your existing repo folder.
```bash
ln -s ~/<git_repos_root> ~/workspace
```
```bash
mkdir -p ~/workspace
cd ~/workspace

git clone https://github.com/greenplum-db/pxf.git
```

## How to Build
PXF uses gradle for build and has a wrapper makefile for abstraction
```bash
# Compile & Test PXF
make
  
# Simply Run unittest
make unittest
```

## Prerequisites
In order to demonstrate end to end functionality you will need JDK, GPDB and Hadoop installed. 

### JDK
JDK version 1.8+ is recommended.

### Hadoop
We have all the related hadoop components(hdfs,hive,hbase,zookeeper,etc) mapped into simple artifact named singlecluster. 
You can [download from here](http://storage.googleapis.com/pxf-public/singlecluster-HDP.tar.gz) and untar the `singlecluster-HDP.tar.gz` file, which contains everything needed to run Hadoop.

```bash
mv singlecluster-HDP.tar.gz ~/workspace/
cd ~/workspace
tar xzf singlecluster-HDP.tar.gz
```

### GPDB
```
git clone https://github.com/greenplum-db/gpdb.git
```

You'll end up with a directory structure like this:

```
~
└── workspace
    ├── pxf
    ├── singlecluster-HDP
    └── gpdb
```

If you already have GPDB installed and running using the instructions shown in the [GPDB README](https://github.com/greenplum-db/gpdb), 
you can ignore the ```Setup GPDB``` section below and simply follow the steps in  ```Setup Hadoop``` and ```Setup PXF```

If you don't wish to use docker, make sure you manually install JDK.  

## Development With Docker
NOTE: Since the docker container will house all Single cluster Hadoop, Greenplum and PXF, we recommend that you have atleast 4 cpus and 6GB memory allocated to Docker. These settings are available under docker preferences.

The following command runs the docker container and sets up and switches to user gpadmin.

```bash
docker run --rm -it \
  -p 5432:5432 \
  -p 5888:5888 \
  -p 8000:8000 \
  -p 8020:8020 \
  -p 9090:9090 \
  -p 50070:50070 \
  -w /home/gpadmin/workspace \
  -v ~/workspace/gpdb:/home/gpadmin/workspace/gpdb \
  -v ~/workspace/pxf:/home/gpadmin/workspace/pxf \
  -v ~/workspace/singlecluster-HDP:/home/gpadmin/workspace/singlecluster \
  pivotaldata/gpdb-dev:centos6 /bin/bash -c \
  "/home/gpadmin/workspace/pxf/dev/set_up_gpadmin_user.bash && /sbin/service sshd start && su - gpadmin"
```

### Setup GPDB

Configure, build and install GPDB. This will be needed only when you use the container for the first time with gpdb source.
```bash
~/workspace/pxf/dev/build_and_install_gpdb.bash
```

For subsequent minor changes to gpdb source you can simply do the following
```bash
pushd ~/worksapce/gpdb
make -j4 install
popd

```

Create Greenplum Cluster
```bash
source /usr/local/greenplum-db-devel/greenplum_path.sh
make -C ~/workspace/gpdb create-demo-cluster
source ~/workspace/gpdb/gpAux/gpdemo/gpdemo-env.sh
```

### Setup Hadoop
Hdfs will be needed to demonstrate functionality. You can choose to start additional hadoop components (hive/hbase) if you need them.

Setup [User Impersonation](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/Superusers.html) prior to starting the hadoop components.
(this allows the `gpadmin` user to access hadoop data).
```bash
~/workspace/pxf/dev/configure_singlecluster.bash
```

Setup and start HDFS
```bash
pushd ~/workspace/singlecluster/bin
echo y | ./init-gphd.sh
./start-hdfs.sh
popd

```

Start other optional components based on your need
```bash
pushd ~/workspace/singlecluster/bin
# Start Hive
./start-hive.sh

# Start HBase 
./start-zookeeper.sh
./start-hbase.sh
popd

```

### Setup PXF
Install PXF Server
```bash
# Install PXF
make -C ~/workspace/pxf/server install

# Initialize PXF
$PXF_HOME/bin/pxf init

# Start PXF
$PXF_HOME/bin/pxf start
```

Install PXF client (ignore if this is already done)
```bash
make -C ~/workspace/gpdb/gpAux/extensions/pxf installcheck
psql -d template1 -c "create extension pxf"
```

### Run PXF Tests
```bash
pushd ~/workspace/pxf/automation

# Run specific tests. Example: Hdfs Smoke Test
make TEST=HdfsSmokeTest

# Run all tests. This will be time consuming.
make GROUP=gpdb
popd

```

### Make Changes to PXF

To deploy your changes to PXF in the development environment.

```bash
# $PXF_HOME folder is replaced each time you make install.
# So, if you have any config changes, you may want to back those up.
$PXF_HOME/bin/pxf stop
make -C ~/workspace/pxf/server install

# Make any config changes you had backed up previously
$PXF_HOME/bin/pxf start
```

## IDE Setup (IntelliJ)

- Start IntelliJ. Click "Open" and select the directory to which you cloned the `pxf` repo.
- Select `File > Project Structure`.
- Make sure you have a JDK selected.
- In the `Project Settings > Modules` section, import two modules for the `pxf/server` and `pxf/automation` directories. The first time you'll get an error saying that there's
no JDK set for Gradle. Just cancel and retry. It goes away the second time.
- Restart IntelliJ
- Check that it worked by running a test (Cmd+O)
