#!/bin/bash

groupadd -g 1000 gpadmin && useradd -u 1000 -g 1000 gpadmin
echo "gpadmin  ALL=(ALL)       NOPASSWD: ALL" > /etc/sudoers.d/gpadmin
groupadd supergroup && usermod -a -G supergroup gpadmin

>>/home/gpadmin/.bash_profile cat <<EOF
export PS1="[\u@\h \W]\$ "
source /opt/rh/devtoolset-6/enable
export JAVA_HOME=/etc/alternatives/java_sdk
source /usr/local/gpdb/greenplum_path.sh
source /home/gpadmin/gpdb/gpAux/gpdemo/gpdemo-env.sh
EOF

chown gpadmin:gpadmin /home/gpadmin

# set up ssh
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