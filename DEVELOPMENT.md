# Developing PXF

## Docker Setup

To start, ensure you have a `~/workspace` directory and have cloned the `gpdb` and `pxf` projects.
(The name `workspace` is not strictly required but will be used throughout this guide.)

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
tar xzf singlecluster-HDP.tar.gz ~/workspace/singlecluster-HDP
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
(this allows the `gpadmin` user to access hadoop data):

Edit `/singlecluster/hadoop/etc/hadoop/core-site.xml` and `/singlecluster/hbase/conf/hbase-site.xml` and add the
following properties to both:

```
    <property>
         <name>hadoop.proxyuser.gpadmin.hosts</name>
         <value>*</value>
     </property>
     <property>
         <name>hadoop.proxyuser.gpadmin.groups</name>
         <value>*</value>
     </property>
     <property>
         <name>hadoop.security.authorization</name>
         <value>true</value>
     </property>
     <property>
         <name>hbase.security.authorization</name>
         <value>true</value>
     </property>
     <property>
         <name>hbase.rpc.protection</name>
         <value>authentication</value>
     </property>
     <property>
         <name>hbase.coprocessor.master.classes</name>
         <value>org.apache.hadoop.hbase.security.access.AccessController</value>
     </property>
     <property>
         <name>hbase.coprocessor.region.classes</name>
         <value>org.apache.hadoop.hbase.security.access.AccessController,org.apache.hadoop.hbase.security.access.SecureBulkLoadEndpoint</value>
     </property>
     <property>
         <name>hbase.coprocessor.regionserver.classes</name>
         <value>org.apache.hadoop.hbase.security.access.AccessController</value>
     </property>
```

### Build and Install GPDB

```bash
source /opt/gcc_env.sh # set the compiler to gcc6.2, for C++11 support
cd /home/gpadmin/gpdb
make clean
./configure \
  --enable-debug \
  --with-perl \
  --with-python \
  --with-libxml \
  --disable-orca \
  --prefix=/usr/local/gpdb
# TODO: Change prefix to greenplum-db-devel, as that directory already exists on the container
make -j8
make install
/sbin/service sshd start
groupadd -g 1000 gpadmin && useradd -u 1000 -g 1000 gpadmin
echo "gpadmin  ALL=(ALL)       NOPASSWD: ALL" > /etc/sudoers.d/gpadmin
groupadd supergroup && usermod -a -G supergroup gpadmin
>>/home/gpadmin/.bash_profile cat <<EOF
export PS1="[\u@\h \W]\$ "
source /opt/rh/devtoolset-6/enable
export JAVA_HOME=/etc/alternatives/java_sdk
source /usr/local/gpdb/greenplum_path.sh
EOF
chown gpadmin:gpadmin /home/gpadmin
mkdir /home/gpadmin/.ssh
ssh-keygen -t rsa -N "" -f /home/gpadmin/.ssh/id_rsa
cat /home/gpadmin/.ssh/id_rsa.pub >> /home/gpadmin/.ssh/authorized_keys
echo -e "password\npassword" | passwd gpadmin 2> /dev/null
{ ssh-keyscan localhost; ssh-keyscan 0.0.0.0; } >> /home/gpadmin/.ssh/known_hosts
chown -R gpadmin:gpadmin /home/gpadmin/.ssh
chown -R gpadmin:gpadmin /usr/local/gpdb


# TODO: check if gpadmin-limits.conf already exists and bail out if it does
>/etc/security/limits.d/gpadmin-limits.conf cat <<-EOF
gpadmin soft core unlimited
gpadmin soft nproc 131072
gpadmin soft nofile 65536
EOF

>>/home/gpadmin/.bash_profile cat <<EOF
source ~/gpdb/gpAux/gpdemo/gpdemo-env.sh
export HADOOP_ROOT=/singlecluster
export PXF_HOME=/usr/local/gpdb/pxf
export GPHD_ROOT=/singlecluster
export BUILD_PARAMS="-x test"
export LANG=en_US.UTF-8
export JAVA_HOME=/etc/alternatives/java_sdk
export SLAVES=1
EOF

su - gpadmin

# The following commands run as gpadmin

sudo pip install psi # TODO: pip reports that psi is already installed. not needed?
sudo cp -r $(find /usr/lib64 -name psi | sort -r | head -1) ${GPHOME}/lib/python
cd gpdb
make create-demo-cluster
source ./gpAux/gpdemo/gpdemo-env.sh
```

### Set up Hadoop Services
```bash
# TODO: Check if the below vars are needed to install pxf
export HADOOP_ROOT=/singlecluster
export PXF_HOME=/usr/local/gpdb/pxf
export GPHD_ROOT=/singlecluster
export BUILD_PARAMS="-x test"
export LANG=en_US.UTF-8
export JAVA_HOME=/etc/alternatives/java_sdk
export SLAVES=1
pushd /singlecluster/bin
echo "y" | ./init-gphd.sh
./start-zookeeper.sh
./start-hdfs.sh
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
make -C ~/pxf/pxf install DATABASE=gpdb
```

```bash
# Initialize PXF
$PXF_HOME/bin/pxf init

# Start PXF
$PXF_HOME/bin/pxf start

# Install PXF client
pushd /home/gpadmin/gpdb/gpAux/extensions/pxf
make installcheck
psql -d template1 -c "create extension pxf"
popd
```

### Run PXF Tests

```bash
cd /home/gpadmin/pxf/pxf_automation
make GROUP=gpdb PG_MODE=GPDB
```
