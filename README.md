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

## cli/
Contains command line interface code for PXF

## automation/
Contains the automation and integration tests for PXF against the various datasources

## singlecluster/
Hadoop testing environment to exercise the pxf automation tests

## concourse/
Resources for PXF's Continuous Integration pipelines


PXF Development
=================
Below are the steps to build and install PXF along with its dependencies including GPDB and Hadoop.

To start, ensure you have a `~/workspace` directory and have cloned the `pxf` and its prerequisites(shown below) under it.
(The name `workspace` is not strictly required but will be used throughout this guide.)
```bash
mkdir -p ~/workspace
cd ~/workspace

git clone https://github.com/greenplum-db/pxf.git
```
Alternatively, you may create a symlink to your existing repo folder.
```bash
ln -s ~/<git_repos_root> ~/workspace
```

## Install Dependencies

To build PXF, you must have:

- JDK 1.8+
- Go (1.9 or later)

To install Go on CentOS, `sudo yum install go`.

For other platforms, see the [Go downloads page](https://golang.org/dl/).

Once you have installed Go, you will need the `dep` and `ginkgo` tools, which install Go dependencies and run Go tests,
respectively. Assuming `go` is on your `PATH`, you can run:

```
go get github.com/golang/dep/cmd/dep
go get github.com/onsi/ginkgo/ginkgo
```

to install them.

## How to Build
PXF uses gradle for build and has a wrapper makefile for abstraction
```bash
cd ~/workspace/pxf

# Compile & Test PXF
make
  
# Simply Run unittest
make unittest
```

## Demonstrating Hadoop Integration
In order to demonstrate end to end functionality you will need GPDB and Hadoop installed.

### Hadoop
We have all the related hadoop components (hdfs, hive, hbase, zookeeper, etc) mapped into simple artifact named singlecluster.
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
you can ignore the `Setup GPDB` section below and simply follow the steps in  `Setup Hadoop` and `Setup PXF`

If you don't wish to use docker, make sure you manually install JDK.

## Development With Docker
NOTE: Since the docker container will house all Single cluster Hadoop, Greenplum and PXF, we recommend that you have at least 4 cpus and 6GB memory allocated to Docker. These settings are available under docker preferences.

The following commands run the docker container and set up and switch to user gpadmin.

```bash
# Get the latest image
docker pull pivotaldata/gpdb-pxf-dev:centos6

# If you want to use gdb to debug gpdb you need the --privileged flag in the command below
docker run --rm -it \
  -p 5432:5432 \
  -p 5888:5888 \
  -p 8000:8000 \
  -p 5005:5005 \
  -p 8020:8020 \
  -p 9000:9000 \
  -p 9090:9090 \
  -p 50070:50070 \
  -w /home/gpadmin/workspace \
  -v ~/workspace/gpdb:/home/gpadmin/workspace/gpdb \
  -v ~/workspace/pxf:/home/gpadmin/workspace/pxf \
  -v ~/workspace/singlecluster-HDP:/home/gpadmin/workspace/singlecluster \
  pivotaldata/gpdb-pxf-dev:centos6 /bin/bash -c \
  "/home/gpadmin/workspace/pxf/dev/set_up_gpadmin_user.bash && /sbin/service sshd start && su - gpadmin"
```

### Setup GPDB

Configure, build and install GPDB. This will be needed only when you use the container for the first time with GPDB source.
```bash
~/workspace/pxf/dev/build_gpdb.bash
~/workspace/pxf/dev/install_gpdb.bash
```

For subsequent minor changes to GPDB source you can simply do the following:
```bash
~/workspace/pxf/dev/install_gpdb.bash
```

Create Greenplum Cluster
```bash
source /usr/local/greenplum-db-devel/greenplum_path.sh
make -C ~/workspace/gpdb create-demo-cluster
source ~/workspace/gpdb/gpAux/gpdemo/gpdemo-env.sh
```

### Setup Hadoop
Hdfs will be needed to demonstrate functionality. You can choose to start additional hadoop components (hive/hbase) if you need them.

Setup [User Impersonation](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/Superusers.html) prior to starting the hadoop components (this allows the `gpadmin` user to access hadoop data).
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
./start-yarn.sh
./start-hive.sh

# Start HBase 
./start-zookeeper.sh
./start-hbase.sh
popd
```
### Setup Minio (optional)
Minio is an S3-API compatible local storage solution. The development docker image comes with Minio software pre-installed. To start the Minio server, run the following script:
```bash
~/workspace/pxf/dev/start_minio.bash
```
After the server starts, you can access Minio UI at `http://localhost:9000` from the host OS. Use `admin` for the access key and `password` for the secret key when connecting to your local Minio instance.

The script also sets `PROTOCOL=minio` so that the automation framework will use the local Minio server when running S3 automation tests.

### Setup PXF

Install PXF Server
```bash
# Install PXF
make -C ~/workspace/pxf install

# Initialize PXF
export PXF_CONF=~/pxf
$PXF_HOME/bin/pxf init

# Start PXF
$PXF_HOME/bin/pxf start
```

Finally, if you don't have any servers configured, go ahead and copy the Hadoop templates to the default configuration location:

```
cp "${PXF_CONF}"/templates/*-site.xml "${PXF_CONF}/servers/default"
```

Install PXF client (ignore if this is already done)
```bash
if [ -d ~/workspace/gpdb/gpAux/extensions/pxf ]; then
	PXF_EXTENSIONS_DIR=gpAux/extensions/pxf
else
	PXF_EXTENSIONS_DIR=gpcontrib/pxf
fi
make -C ~/workspace/gpdb/${PXF_EXTENSIONS_DIR} installcheck
psql -d template1 -c "create extension pxf"
```

### Run PXF Tests
All tests use a database named `pxfautomation`.
```bash
pushd ~/workspace/pxf/automation

# Initialize default server configs using template
cp ~/pxf/templates/*.xml ~/pxf/servers/default

# Run specific tests. Example: Hdfs Smoke Test
make TEST=HdfsSmokeTest

# Run all tests. This will be time consuming.
make GROUP=gpdb

# If you wish to run test(s) against a different storage protocol set the following variable (for eg: s3) 
export PROTOCOL=s3
popd
```

If you see any HBase failures, try copying `pxf-hbase-*.jar` to the HBase classpath, and restart HBase:

```
cp ${PXF_HOME}/lib/pxf-hbase-*.jar ~/workspace/singlecluster/hbase/lib
~/workspace/singlecluster/bin/stop-hbase.sh
~/workspace/singlecluster/bin/start-hbase.sh
```

### Make Changes to PXF

To deploy your changes to PXF in the development environment.

```bash
# $PXF_HOME folder is replaced each time you make install.
# So, if you have any config changes, you may want to back those up.
$PXF_HOME/bin/pxf stop
make -C ~/workspace/pxf install

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

# To run a Kerberized Hadoop Cluster

## Requirements

- Download bin_gpdb (from any of the pipelines)
- Download pxf_tarball (from any of the pipelines)

These instructions allow you to run a Kerberized cluster

```bash
docker run --rm -it \
  --privileged \
  --hostname c6401.ambari.apache.org \
  -p 5432:5432 \
  -p 5888:5888 \
  -p 8000:8000 \
  -p 8080:8080 \
  -p 8020:8020 \
  -p 9000:9000 \
  -p 9090:9090 \
  -p 50070:50070 \
  -w /home/gpadmin/workspace \
  -v ~/workspace/gpdb:/home/gpadmin/workspace/gpdb_src \
  -v ~/workspace/pxf:/home/gpadmin/workspace/pxf_src \
  -v ~/workspace/singlecluster-HDP:/home/gpadmin/workspace/singlecluster \
  -v ~/Downloads/bin_gpdb:/home/gpadmin/workspace/bin_gpdb \
  -v ~/Downloads/pxf_tarball:/home/gpadmin/workspace/pxf_tarball \
  -e CLUSTER_NAME=hdp \
  -e NODE=c6401.ambari.apache.org \
  -e REALM=AMBARI.APACHE.ORG \
  -e TARGET_OS=centos \
  pivotaldata/gpdb-pxf-dev:centos6-hdp-secure /bin/bash

# Inside the container run the following command:
pxf_src/concourse/scripts/test_pxf_secure.bash

echo "+----------------------------------------------+"
echo "| Kerberos admin principal: admin/admin@$REALM |"
echo "| Kerberos admin password : admin              |"
echo "+----------------------------------------------+"

su - gpadmin
```
