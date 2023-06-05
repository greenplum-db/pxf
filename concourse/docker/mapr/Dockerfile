ARG BASE_IMAGE

FROM ${BASE_IMAGE} as scratch

RUN rpm --import https://package.mapr.hpe.com/releases/pub/maprgpg.key && \
  groupadd -g 5000 mapr ; useradd -g 5000 -u 5000 mapr && echo -e "mapr\nmapr" | passwd mapr && \
  echo -e "[maprtech]\nname=MapR Technologies" >> /etc/yum.repos.d/maprtech.repo && \
  echo -e "baseurl=https://package.mapr.hpe.com/releases/v5.2.2/redhat/" >> /etc/yum.repos.d/maprtech.repo && \
  echo -e "enabled=1\ngpgcheck=0\nprotect=1\n" >> /etc/yum.repos.d/maprtech.repo && \
  echo -e "[maprecosystem]\nname=MapR Technologies" >> /etc/yum.repos.d/maprtech.repo && \
  echo -e "baseurl=https://package.mapr.hpe.com/releases/MEP/MEP-4.1.0/redhat/" >> /etc/yum.repos.d/maprtech.repo && \
  echo -e "enabled=1\ngpgcheck=0\nprotect=1\n" >> /etc/yum.repos.d/maprtech.repo && \
  cat /etc/yum.repos.d/maprtech.repo && echo JAVA_HOME=$JAVA_HOME

RUN yum install -y lsof mapr-nfs mapr-cldb mapr-core mapr-fileserver mapr-zookeeper mapr-webserver mapr-hive

RUN echo "mapr soft core unlimited" >> /etc/security/limits.d/mapr-limits.conf && \
  echo "mapr soft nproc 131072" >> /etc/security/limits.d/mapr-limits.conf && \
  echo "mapr soft memlock unlimited" >> /etc/security/limits.d/mapr-limits.conf && \
  echo "mapr soft nofile 65536" >> /etc/security/limits.d/mapr-limits.conf && \
  echo "mapr soft nice -10" >> /etc/security/limits.d/mapr-limits.conf && \
  echo "sed -i 's@#export JAVA_HOME=@export JAVA_HOME=/etc/alternatives/java_sdk@g' /opt/mapr/conf/env.sh" > /root/init-script && \
  echo "sed -i 's/AddUdevRules(list/#AddUdevRules(list/' /opt/mapr/server/disksetup" >> /root/init-script && \
  echo "sed -i -e 's/SetAioMaxNr/#SetAioMaxNr/' -e '254,254 {s/^/#/}' /opt/mapr/initscripts/mapr-warden" >> /root/init-script && \
  echo "sed -i -e '555,568 {s/^/#/}' -e '577,577 {s/^/#/}' /opt/mapr/server/configure-common.sh" >> /root/init-script && \
  echo "sed -i 's@/proc/meminfo@/opt/mapr/conf/meminfofake@' /opt/mapr/server/initscripts-common.sh" >> /root/init-script && \
  echo "cp /proc/meminfo /opt/mapr/conf/meminfofake" >> /root/init-script && \
  echo 'sed -i "/^MemTotal/ s/^.*$/MemTotal:     6291456 kB/" /opt/mapr/conf/meminfofake' >> /root/init-script && \
  echo 'sed -i "/^MemFree/ s/^.*$/MemFree:     6291456 kB/" /opt/mapr/conf/meminfofake' >> /root/init-script && \
  echo 'sed -i "/^MemAvailable/ s/^.*$/MemAvailable:     6291456 kB/" /opt/mapr/conf/meminfofake' >> /root/init-script && \
  echo 'echo `hostname` > /opt/mapr/hostname' >> /root/init-script && \
  echo '/opt/mapr/server/configure.sh -C `hostname` -Z `hostname` -N maprdemo.cluster' >> /root/init-script && \
  echo "mkdir -p /opt/mapr/disks && fallocate -l 10G /opt/mapr/disks/docker.disk" >> /root/init-script && \
  echo "/opt/mapr/disks/docker.disk" > /tmp/disks && cat /tmp/disks && \
  echo "/opt/mapr/server/disksetup -F /tmp/disks" >> /root/init-script && \
  echo '/opt/mapr/server/configure.sh -C `hostname -i` -Z `hostname -i` -N maprdemo.cluster' >> /root/init-script && \
  echo "sed -i '/^mapr - /d' /etc/security/limits.conf" >> /root/init-script && \
  echo "sleep 60" >> /root/init-script && \
  echo "export HADOOP_HOME=/opt/mapr/hadoop/hadoop-2.7.0" >> /root/.bashrc && \
  chmod +x /root/init-script
