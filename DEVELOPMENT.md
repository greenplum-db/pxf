# PXF Development

This document has the steps to build and install PXF and GPDB from source along with its dependencies including Hadoop.

## IDE Setup (IntelliJ)

- Start IntelliJ. Click "Open" and select the directory to which you cloned the `pxf` repo.
- Select `File > Project Structure`.
- Make sure you have a JDK selected.
- In the `Project Settings > Modules` section, import two modules for the `pxf/pxf` and `pxf/pxf_automation` directories. The first time you'll get an error saying that there's
no JDK set for Gradle. Just cancel and retry. It goes away the second time.
- Restart IntelliJ
- Check that it worked by running a test (Cmd+O)

## Docker Setup

To start, ensure you have a `~/workspace` directory and have cloned the `gpdb` and `pxf` projects.
(The name `workspace` is not strictly required but will be used throughout this guide.)

Alternatively, you may create a symlink to your existing repo folder.
```bash
ln -s ~/<git_repos_root> ~/workspace
```

NOTE: Since the docker container all Single cluster Hadoop, Greenplum and PXF, we recommend that you have atleast 4 cpus and 6GB memory allocated to Docker. These settings are available under docker preferences.

```bash
mkdir -p ~/workspace
cd ~/workspace

git clone https://github.com/greenplum-db/gpdb.git
git clone https://github.com/greenplum-db/pxf.git
```

You must also [download from S3](https://s3-us-west-2.amazonaws.com/pivotal-public/singlecluster-HDP.tar.gz) and untar the `singlecluster-HDP.tar.gz` file, which contains everything needed to run Hadoop.

```bash
mv singlecluster-HDP.tar.gz ~/workspace/
cd ~/workspace
tar xzf singlecluster-HDP.tar.gz
```

You'll end up with a directory structure like this:

```
~
└── workspace
    ├── gpdb
    ├── pxf
    └── singlecluster-HDP
```

Run the docker container:

```bash
docker run --rm -it \
  -p 5005:5005 \
  -p 5432:5432 \
  -p 5888:5888 \
  -p 8020:8020 \
  -p 9090:9090 \
  -p 50070:50070 \
  -w /home/gpadmin \
  -v ~/workspace/gpdb:/home/gpadmin/gpdb \
  -v ~/workspace/pxf:/home/gpadmin/pxf \
  -v ~/workspace/singlecluster-HDP:/singlecluster \
  pivotaldata/gpdb-dev:centos6 /bin/bash
```

### Build GPDB

```bash
/home/gpadmin/pxf/dev/build_and_install_gpdb.bash
```


### Install GPDB

```bash
pushd /home/gpadmin/gpdb
make -j4 install
popd
```

### Start `sshd`

Greenplum uses SSH to coordinate operations like starting and stopping the cluster. Even on a single-host cluster
like our dev environment, the single host needs to be able to SSH to itself.

```bash
/sbin/service sshd start
```

### Set up the `gpadmin` User

```bash
/home/gpadmin/pxf/dev/set_up_gpadmin_user.bash
```

### Log in as `gpadmin` and Create a Greenplum Cluster

The `create-demo-cluster` make target spins up a local Greenplum cluster with three segments.
Once the cluster is up, `gpdemo-env.sh` exports the environment variables needed to operate and query the cluster.

```bash
su - gpadmin

make -C /home/gpadmin/gpdb create-demo-cluster
source /home/gpadmin/gpdb/gpAux/gpdemo/gpdemo-env.sh
```

### Configure Hadoop

Inside the container, configure the Hadoop cluster to allow
[user impersonation](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/Superusers.html)
(this allows the `gpadmin` user to access hadoop data).

Run the script below:

```bash
/home/gpadmin/pxf/dev/configure_singlecluster.bash
```

### Setup and start HDFS

```bash
pushd /singlecluster/bin
echo y | ./init-gphd.sh
./start-hdfs.sh
popd
```

### Start up Other Hadoop Services

```bash
pushd /singlecluster/bin
./start-zookeeper.sh
# Starting yarn may fail if HDFS is not up yet. Retry until it succeeds.
./start-yarn.sh
./start-hive.sh
./start-hbase.sh
popd
```

### Set up PXF

Copy-paste the `make` command separately from the others; otherwise `make` will eat the input that's
intended for the shell.

```bash
# Install PXF
make -C /home/gpadmin/pxf/pxf install
```

```bash
# Initialize PXF
$PXF_HOME/bin/pxf init

# Start PXF
$PXF_HOME/bin/pxf start

# Install PXF client
make -C /home/gpadmin/gpdb/gpAux/extensions/pxf installcheck
psql -d template1 -c "create extension pxf"
```

### Run PXF Tests

```bash
pushd /home/gpadmin/pxf/pxf_automation

# Run specific tests. Example: Hdfs Smoke Test
make TEST=HdfsSmokeTest

# Run all tests
make GROUP=gpdb
popd
```

### Make Changes to PXF

To deploy your changes to PXF in the development environment.

```bash
# $PXF_HOME folder is replaced each time you make install.
# So, if you have any config changes, you may want to back those up.
$PXF_HOME/bin/pxf stop
make -C /home/gpadmin/pxf/pxf install

# Make any config changes you had backed up previously
$PXF_HOME/bin/pxf start
```

