**Master FDW** [![Concourse Build Status](http://ud.ci.gpdb.pivotal.io/api/v1/teams/main/pipelines/pg_regress/badge)](https://ud.ci.gpdb.pivotal.io/teams/main/pipelines/pg_regress) |
**PXF Build** [![Concourse Build Status](http://ud.ci.gpdb.pivotal.io/api/v1/teams/main/pipelines/pxf-build/badge)](https://ud.ci.gpdb.pivotal.io/teams/main/pipelines/pxf-build) |
**PXF Certification** [![Concourse Build Status](http://ud.ci.gpdb.pivotal.io/api/v1/teams/main/pipelines/pxf-certification/badge)](https://ud.ci.gpdb.pivotal.io/teams/main/pipelines/pxf-certification)

----------------------------------------------------------------------

Introduction
============

PXF is an extensible framework that allows a distributed database like Greenplum to query external data files, whose metadata is not managed by the database.
PXF includes built-in connectors for accessing data that exists inside HDFS files, Hive tables, HBase tables, JDBC-accessible databases and more.
Users can also create their own connectors to other data storage or processing engines.

Repository Contents
================
## external-table/
Contains the Greenplum extension implementing an External Table protocol handler

## fdw/
Contains the Greenplum extension implementing a Foreign Data Wrapper (FDW) for PXF

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

## regression/
Contains the end-to-end (integration) tests for PXF against the various datasources, utilizing the PostgreSQL testing framework `pg_regress`

## downloads/
An empty directory that serves as a staging location for Greenplum RPMs for the development Docker image

PXF Development
=================
Below are the steps to build and install PXF along with its dependencies including Greenplum and Hadoop.

To start, ensure you have a `~/workspace` directory and have cloned the `pxf` and its prerequisites (shown below) under it.
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

1. GCC compiler, `make` system, `unzip` package, `maven` for running integration tests
2. Installed Greenplum DB

    Either download and install Greenplum RPM or build Greenplum from the source by following instructions in the [GPDB README](https://github.com/greenplum-db/gpdb).

    Assuming you have installed Greenplum into `/usr/local/greenplum-db` directory, run its environment script:
    ```
    source /usr/local/greenplum-db/greenplum_path.sh
    ```

3. JDK 1.8 to compile (PXF runs on Java 8 and Java 11)

    Export your `JAVA_HOME`:
    ```
    export JAVA_HOME=<PATH_TO_YOUR_JAVA_HOME>
    ```

4. Go (1.9 or later)

    To install Go on CentOS, `sudo yum install go`. For other platforms, see the [Go downloads page](https://golang.org/dl/).

    Make sure to export your `GOPATH` and add go to your `PATH`. For example:
    ```
    export GOPATH=$HOME/go
    export PATH=$PATH:/usr/local/go/bin:$GOPATH/bin
    ```

    Once you have installed Go, you will need the `dep` and `ginkgo` tools, which install Go dependencies and run Go tests,
    respectively. Assuming `go` is on your `PATH`, you can run:
    ```
    go get github.com/golang/dep/cmd/dep
    go get github.com/onsi/ginkgo/ginkgo
    ```

5. cURL (7.29 or later):

    To install cURL devel package on CentOS 7, `sudo yum install libcurl-devel`.

    Note that CentOS 6 provides an older, unsupported version of cURL (7.19). You should install a newer version from source if you are on CentOS 6.

## How to Build PXF
PXF uses Makefiles to build its components. PXF server component uses Gradle that is wrapped into the Makefile for convenience.
```bash
cd ~/workspace/pxf

# Compile & Test PXF
make

# Only run unit tests
make test
```

## How to Install PXF

To install PXF, first make sure that the user has sufficient permissions in the `$GPHOME` and `$PXF_HOME` directories to perform the installation. It's recommended to change ownership to match the installing user. For example, when installing PXF as user `gpadmin` under `/usr/local/greenplum-db`:

```bash
export GPHOME=/usr/local/greenplum-db
export PXF_HOME=/usr/local/pxf
chown -R gpadmin:gpadmin "${GPHOME}" "${PXF_HOME}"
make -C ~/workspace/pxf install
```

## How to demonstrate Hadoop Integration
In order to demonstrate end to end functionality you will need Hadoop installed. We have all the related hadoop components (hdfs, hive, hbase, zookeeper, etc) mapped into simple artifact named singlecluster.
You can [download from here](http://storage.googleapis.com/pxf-public/singlecluster-HDP.tar.gz) and untar the `singlecluster-HDP.tar.gz` file, which contains everything needed to run Hadoop.

```bash
mv singlecluster-HDP.tar.gz ~/workspace/
cd ~/workspace
tar xzf singlecluster-HDP.tar.gz
```

# Development With Docker
NOTE: Since the docker container will house all Single cluster Hadoop, Greenplum and PXF, we recommend that you have at least 4 cpus and 6GB memory allocated to Docker. These settings are available under docker preferences.

The following commands run the docker container and set up and switch to user gpadmin.

```bash
# Get the latest image
export GCR_PROJECT=<YOUR-GOOGLE_CONTAINER_REGISTRY-PROJECT-NAME>
docker pull gcr.io/$GCR_PROJECT/gpdb-pxf-dev/gpdb6-centos7-test-pxf:latest

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
  -v ~/workspace/pxf:/home/gpadmin/workspace/pxf \
  -v ~/workspace/singlecluster-HDP:/home/gpadmin/workspace/singlecluster \
  gcr.io/$GCR_PROJECT/gpdb-pxf-dev/gpdb6-centos7-test-pxf:latest /bin/bash -c \
  "/home/gpadmin/workspace/pxf/dev/indocker_setup.bash && /sbin/service sshd start && su - gpadmin"

```


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

Run all the instructions below and run GROUP=smoke (in one script):
```bash
~/workspace/pxf/dev/smoke_shortcut.sh
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
source ~/workspace/pxf/dev/start_minio.bash
```
After the server starts, you can access Minio UI at `http://localhost:9000` from the host OS. Use `admin` for the access key and `password` for the secret key when connecting to your local Minio instance.

The script also sets `PROTOCOL=minio` so that the automation framework will use the local Minio server when running S3 automation tests. If later you would like to run Hadoop HDFS tests, unset this variable with `unset PROTOCOL` command.

### Setup PXF

Install PXF Server
```bash
# Install PXF
make -C ~/workspace/pxf install

# Start PXF
export PXF_JVM_OPTS="-Xmx512m -Xms256m"
$PXF_HOME/bin/pxf start
```

Install PXF client (ignore if this is already done)
```bash
psql -d template1 -c "create extension pxf"
```

### Run PXF Tests
All tests use a database named `pxfautomation`.
```bash
pushd ~/workspace/pxf/automation

# Initialize default server configs using template
cp ~/pxf/templates/{hdfs,mapred,yarn,core,hbase,hive}-site.xml ~/pxf/servers/default

# Run specific tests. Example: Hdfs Smoke Test
make TEST=HdfsSmokeTest

# Run all tests. This will be very time consuming.
make GROUP=gpdb

# If you wish to run test(s) against a different storage protocol set the following variable (for eg: s3)
export PROTOCOL=s3
popd
```

If you see any HBase failures, try copying `pxf-hbase-*.jar` to the HBase classpath, and restart HBase:

```
cp ${PXF_HOME}/lib/pxf-hbase-*.jar ~/workspace/singlecluster/hbase/lib/pxf-hbase.jar
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
rm -rf $PXF_HOME/pxf-service
yes | $PXF_HOME/bin/pxf init
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
