# Developing PXF

## Docker Setup

To start, ensure you have a `~/workspace` directory and have cloned the `gpdb` and `pxf` projects.
(The name `workspace` is not strictly required but will be used throughout this guide.)

NOTE: Since the docker container all Single cluster Hadoop, Greenplum and PXF, we recommend that you have atleast 6GB memory allocated to Docker. This memory setting is available under docker preferences.

```bash
mkdir -p ~/workspace
cd ~/workspace

git clone https://github.com/greenplum-db/gpdb.git
git clone https://github.com/greenplum-db/pxf.git
```

You must also download and untar the `singlecluster-HDP.tar.gz` file, which contains everything needed to run Hadoop.

[Download the file from S3](https://s3-us-west-2.amazonaws.com/pivotal-public/singlecluster-HDP.tar.gz).

Untar it:

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
docker run -it \
  -v ~/workspace/gpdb:/home/gpadmin/gpdb \
  -v ~/workspace/pxf:/home/gpadmin/pxf \
  -v ~/workspace/singlecluster-HDP:/singlecluster \
  pivotaldata/gpdb-dev:centos6 /bin/bash
```

### Configure Hadoop

Inside the container, configure the Hadoop cluster to allow
[user impersonation](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/Superusers.html)
(this allows the `gpadmin` user to access hadoop data).

Run the script below:

```bash 
/home/gpadmin/pxf/dev/configure_singlecluster.bash
```

### Build and Install GPDB

```bash
/home/gpadmin/pxf/dev/build_and_install_gpdb.bash
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

### Set up Hadoop Services

```bash
pushd /singlecluster/bin
echo y | ./init-gphd.sh
./start-zookeeper.sh
./start-hdfs.sh
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
make -C /home/gpadmin/pxf/pxf clean install DATABASE=gpdb
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
cd /home/gpadmin/pxf/pxf_automation
export PG_MODE=GPDB
make GROUP=gpdb

make TEST=HdfsSmokeTest # Run specific tests
```
