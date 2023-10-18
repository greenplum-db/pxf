# Instructions for creating PXF Sandbox for development/Testing


This PXF Sandbox (docker image) is available in docker hub (only accessible by gpdb-ud group).

```
docker pull gcr.io/$PROJECT_ID/gpdb-pxf-dev/gpdb<gp_ver>-centos7-test-pxf:latest
```

## Prerequisites
* Docker for OSX
* `~/workspace/stage` directory with
  1. `bin_gpdb.tar.gz` (centos binary for gpdb)
  2. `pxf.tar.gz` (pxf tarball)
  3. `pxf_maven_dependencies.tar.gz` (optional tarball for maven repo)
* `~/workspace/singlecluster-HDP` (single-cluster for HDP)

The above artifacts can also be downloaded from existing PXF pipelines on Concourse CI

**TODO: Automate these instructions**

Use `gcr.io/$PROJECT_ID/gpdb-pxf-dev/gpdb<gp_ver>-centos7-test-pxf:latest` as base for creating the sandbox:
```
docker run -v ~/workspace/stage:/stage -v ~/worspace/singlecluster-HDP:/singlecluster -v ~/workspace/pxf:/pxf -h pxf-dev -it gcr.io/$PROJECT_ID/gpdb-pxf-dev/gpdb<gp_ver>-centos7-test-pxf:latest /bin/bash
```

## Setup gpadmin user
```
groupadd -g 1000 gpadmin && useradd -u 1000 -g 1000 gpadmin && \
echo "gpadmin  ALL=(ALL)       NOPASSWD: ALL" > /etc/sudoers.d/gpadmin && \
groupadd supergroup && usermod -a -G supergroup gpadmin && \
# setup ssh client keys for gpadmin
mkdir /home/gpadmin/.ssh && \
ssh-keygen -t rsa -N "" -f /home/gpadmin/.ssh/id_rsa && \
cat /home/gpadmin/.ssh/id_rsa.pub >> /home/gpadmin/.ssh/authorized_keys && \
chmod 0600 /home/gpadmin/.ssh/authorized_keys && \
echo -e "password\npassword" | passwd gpadmin 2> /dev/null && \
{ ssh-keyscan localhost; ssh-keyscan 0.0.0.0; } >> /home/gpadmin/.ssh/known_hosts && \
chown -R gpadmin:gpadmin /home/gpadmin /home/gpadmin/.ssh
sed -i "s/^UsePAM yes/UsePAM no/g" /etc/ssh/sshd_config
```

## Setup Hadoop/PXF
```
export JAVA_HOME=/etc/alternatives/java_sdk
export GPHOME=/usr/local/greenplum-db-devel
export PXF_HOME=${GPHOME}/pxf
export singlecluster=/singlecluster
mkdir -p /singlecluster /automation
cp -r /singlecluster/{bin,conf,hadoop,hive,setenv.sh} /singlecluster
chown gpadmin:gpadmin /automation
[ ! -d ${GPHOME} ] && mkdir -p ${GPHOME}
tar -xzf /stage/bin_gpdb.tar.gz -C ${GPHOME}
source ${GPHOME}/greenplum_path.sh
tar -xzf /stage/pxf.tar.gz -C ${GPHOME}
chown -R gpadmin:gpadmin ${GPHOME}/pxf
sed -i -e "s|^[[:blank:]]*export HADOOP_ROOT=.*$|export HADOOP_ROOT=${singlecluster}|g" -e 's|^[[:blank:]]*export PXF_USER_IMPERSONATION=.*$|export PXF_USER_IMPERSONATION=false|g' ${PXF_HOME}/conf/pxf-env.sh
pushd ${singlecluster}/bin
  export SLAVES=1
  echo y | ./init-gphd.sh
  ./start-hdfs.sh
popd
pushd ${PXF_HOME}
  su gpadmin -c "bash ./bin/pxf init"
  su gpadmin -c "bash ./bin/pxf start"
popd
```

## Setup Greenplum
```
hostname -f > /tmp/hosts.txt
/usr/sbin/sshd
su --login --command "source /usr/local/greenplum-db-devel/greenplum_path.sh && gpseginstall -f /tmp/hosts.txt -u gpadmin -p gpadmin"

cd && rm -f run.sh
echo /usr/sbin/sshd >> run.sh
echo export JAVA_HOME=/etc/alternatives/java_sdk >> run.sh
export GPHOME=/usr/local/greenplum-db-devel >> run.sh
echo /singlecluster/bin/start-hdfs.sh >> run.sh
echo 'su gpadmin -c "export JAVA_HOME=/etc/alternatives/java_sdk && /usr/local/greenplum-db-devel/pxf/bin/pxf start"' >> run.sh
echo 'su gpadmin -c "source /usr/local/greenplum-db-devel/greenplum_path.sh && export MASTER_DATA_DIRECTORY=/home/gpadmin/data/master/gpseg-1 && gpstart -a"' >> run.sh
chmod +x run.sh
# cat run.sh
```

## Initialize Greenplum
```
su - gpadmin

source /usr/local/greenplum-db-devel/greenplum_path.sh
cp "${GPHOME}/docs/cli_help/gpconfigs/gpinitsystem_config" /home/gpadmin/gpconfigs/gpinitsystem_config
chmod +w /home/gpadmin/gpconfigs/gpinitsystem_config
sed -i "s/MASTER_HOSTNAME=mdw/MASTER_HOSTNAME=\$(hostname -f)/g" /home/gpadmin/gpconfigs/gpinitsystem_config
sed -i "s|declare -a DATA_DIRECTORY.*|declare -a DATA_DIRECTORY=(/home/gpadmin/data1/primary /home/gpadmin/data2/primary /home/gpadmin/data3/primary)|g" /home/gpadmin/gpconfigs/gpinitsystem_config
sed -i "s|MASTER_DIRECTORY=.*|MASTER_DIRECTORY=/home/gpadmin/data/master|g" /home/gpadmin/gpconfigs/gpinitsystem_config
export MASTER_DATA_DIRECTORY=/home/gpadmin/data/master/gpseg-1
gpinitsystem -a -c /home/gpadmin/gpconfigs/gpinitsystem_config -h /tmp/hosts.txt --su_password=changeme
echo 'host all all 0.0.0.0/0 password' >> /home/gpadmin/data/master/gpseg-1/pg_hba.conf
gpstop -u
psql -d template1 -c "CREATE DATABASE gpadmin;"
psql -d template1 -c "CREATE EXTENSION PXF"
gpstart -a
```

## Alternative Setup Instructions

```
# Compile and install GPDB
make clean
./configure --enable-debug --with-perl --with-python --with-libxml --disable-orca --prefix=/usr/local/greenplum-db-devel
make -j8
cd /home/build/gpdb
make install
/usr/sbin/sshd
groupadd -g 1000 gpadmin && useradd -u 1000 -g 1000 gpadmin
echo "gpadmin  ALL=(ALL)       NOPASSWD: ALL" > /etc/sudoers.d/gpadmin
groupadd supergroup && usermod -a -G supergroup gpadmin
echo 'export PS1="[\u@\h \W]\$ "' >> /home/gpadmin/.bash_profile && \
echo 'source /opt/rh/devtoolset-6/enable' >> /home/gpadmin/.bash_profile && \
echo 'export JAVA_HOME=/etc/alternatives/java_sdk' >> /home/gpadmin/.bash_profile && \
chown gpadmin:gpadmin /home/gpadmin &&
    mkdir /home/gpadmin/.ssh && \
    ssh-keygen -t rsa -N "" -f /home/gpadmin/.ssh/id_rsa && \
    cat /home/gpadmin/.ssh/id_rsa.pub >> /home/gpadmin/.ssh/authorized_keys && \
    chmod 0600 /home/gpadmin/.ssh/authorized_keys && \
    echo -e "password\npassword" | passwd gpadmin 2> /dev/null && \
    { ssh-keyscan localhost; ssh-keyscan 0.0.0.0; } >> /home/gpadmin/.ssh/known_hosts && \
    chown -R gpadmin:gpadmin /home/gpadmin/.ssh
chown -R gpadmin:gpadmin /usr/local/greenplum-db-devel
su - gpadmin

# Create demo cluster with GPDB
cd /home/build/gpdb
source /usr/local/greenplum-db-devel/greenplum_path.sh
make create-demo-cluster
source ./gpAux/gpdemo/gpdemo-env.sh

# Compile and run PXF
pushd /pxf_src
export HADOOP_ROOT=/singlecluster
export PXF_HOME=/usr/local/greenplum-db-devel/pxf
export GPHD_ROOT=/singlecluster
export BUILD_PARAMS="-x test"
export LANG=en_US.UTF-8
export JAVA_HOME=/etc/alternatives/java_sdk
make install DATABASE=gpdb
$PXF_HOME/bin/pxf init
$PXF_HOME/bin/pxf start
popd

# Start Hadoop
pushd /singlecluster/bin
echo "y" | ./init-gphd.sh
./start-zookeeper.sh
./start-hdfs.sh
./start-yarn.sh
./start-hive.sh
./start-hbase.sh
popd

# Install PXF client

if [ -d /home/build/gpdb/gpAux/extensions/pxf ]; then
	PXF_EXTENSIONS_DIR=gpAux/extensions/pxf
else
	PXF_EXTENSIONS_DIR=gpcontrib/pxf
fi
pushd /home/build/gpdb/${PXF_EXTENSIONS_DIR}
make installcheck
psql -d template1 -c "create extension pxf"
popd

# Run PXF Automation
cd /pxf/automation
make GROUP=gpdb
```

Set ulimits in `/etc/security/limits.d/gpadmin-limits.conf`

```
gpadmin soft core unlimited
gpadmin soft nproc 131072
gpadmin soft nofile 65536
```

Set `PGPORT` for the demo cluster

```
export PGPORT=15432
```

## Setup PXF Automation
```
export PGPORT=5432
export GPHD_ROOT=/singlecluster
export PXF_HOME=/usr/local/greenplum-db-devel/pxf
mkdir -p /automation ~/.m2
cp -r /pxf/automation/* /automation
# tar -xzf /stage/pxf_maven_dependencies.tar.gz -C ~/.m2

cd /automation
make TEST=HdfsSmokeTest

gpstop -a
# exit (from gpadmin shell)
/singlecluster/bin/stop-hdfs.sh
```

## Commit to a docker image
Read container-id from `docker ps -a`
```
docker commit <container-id> gcr.io/$PROJECT_ID/gpdb-pxf-dev/gpdb<gp_ver>-centos7-test-pxf-dev
```

## Using the pxf sandbox:
```
docker run --rm -p 5432:5432 -p 5888:5888 -h pxf-dev -it gcr.io/$PROJECT_ID/gpdb-pxf-dev/gpdb<gp_ver>-centos7-test-pxf-dev bin/bash -c "/root/run.sh && /bin/bash"
```
