ARG BASE_IMAGE=gcr.io/data-gpdb-public-images/gpdb6-centos7-test:latest

FROM ${BASE_IMAGE}

ARG GO_VERSION
ARG GO_SHA256SUM

ADD apache-maven.tar.gz /usr/share

# install Go utilities
RUN mkdir -p /tmp/pxf_src/ && cd /tmp \
    && wget -O go.tgz -q https://go.dev/dl/go${GO_VERSION}.linux-amd64.tar.gz \
    && echo "${GO_SHA256SUM} /tmp/go.tgz" | sha256sum --check \
    && rm -rf /usr/local/go && mkdir /opt/pxf && tar -C /opt/pxf -xzf go.tgz --strip-components=1 && rm go.tgz

# add Java 11
RUN wget -q https://download.java.net/openjdk/jdk11/ri/openjdk-11+28_linux-x64_bin.tar.gz \
    && mkdir -p /usr/lib/jvm \
    && tar -C /usr/lib/jvm -xzf openjdk-11+28_linux-x64_bin.tar.gz \
    && rm -f openjdk-11+28_linux-x64_bin.tar.gz

# add minio software
RUN useradd -s /sbin/nologin -d /opt/minio minio \
    && mkdir -p /opt/minio/bin \
    && chmod a+rx /opt/minio \
    && mkdir /opt/minio/data \
    && wget -q https://dl.minio.io/server/minio/release/linux-amd64/minio -O /opt/minio/bin/minio \
    && chmod +x /opt/minio/bin/minio \
    && chown -R minio:minio /opt/minio

ARG USER_HOME_DIR="/root"

# install dependencies that are missing on the base images
RUN ln -s /usr/share/apache-maven-*/bin/mvn /usr/bin/mvn \
    && yum install -y rpm-build python-devel jq sudo && yum clean all \
    && cd /tmp && /usr/bin/pip install --upgrade pip==20.3.3 \
    && /usr/bin/pip install psi paramiko --no-cache-dir

# create user gpadmin since GPDB cannot run under root
RUN ssh-keygen -t rsa -N "" -f /root/.ssh/id_rsa \
    && cat /root/.ssh/id_rsa.pub >> /root/.ssh/authorized_keys \
    && chmod 0600 /root/.ssh/authorized_keys \
    && echo -e "password\npassword" | passwd 2> /dev/null \
    && { ssh-keyscan localhost; ssh-keyscan 0.0.0.0; } >> /root/.ssh/known_hosts \
    && ssh-keygen -f /etc/ssh/ssh_host_key -N '' -t rsa1 \
    && ssh-keygen -f /etc/ssh/ssh_host_rsa_key -N '' -t rsa \
    && ssh-keygen -f /etc/ssh/ssh_host_dsa_key -N '' -t dsa \
    && sed -i -e 's|Defaults    requiretty|#Defaults    requiretty|' /etc/sudoers \
    && sed -ri 's/UsePAM yes/UsePAM no/g;s/PasswordAuthentication yes/PasswordAuthentication no/g' /etc/ssh/sshd_config \
    && sed -ri 's@^HostKey /etc/ssh/ssh_host_ecdsa_key$@#&@;s@^HostKey /etc/ssh/ssh_host_ed25519_key$@#&@' /etc/ssh/sshd_config \
    && groupadd -g 6000 gpadmin && useradd -u 6000 -g 6000 gpadmin \
    && echo "gpadmin  ALL=(ALL)       NOPASSWD: ALL" > /etc/sudoers.d/gpadmin \
    && groupadd supergroup && usermod -a -G supergroup gpadmin \
    && mkdir /home/gpadmin/.ssh \
    && ssh-keygen -t rsa -N "" -f /home/gpadmin/.ssh/id_rsa \
    && cat /home/gpadmin/.ssh/id_rsa.pub >> /home/gpadmin/.ssh/authorized_keys \
    && chmod 0600 /home/gpadmin/.ssh/authorized_keys \
    && echo -e "password\npassword" | passwd gpadmin 2> /dev/null \
    && { ssh-keyscan localhost; ssh-keyscan 0.0.0.0; } >> /home/gpadmin/.ssh/known_hosts \
    && chown -R gpadmin:gpadmin /home/gpadmin/.ssh \
    # configure gpadmin limits
    && echo >> /etc/security/limits.d/gpadmin-limits.conf 'gpadmin soft core unlimited' \
    && echo >> /etc/security/limits.d/gpadmin-limits.conf 'gpadmin soft nproc 131072' \
    && echo >> /etc/security/limits.d/gpadmin-limits.conf 'gpadmin soft nofile 65536' \
    # add locale for testing
    && localedef -c -i ru_RU -f CP1251 ru_RU.CP1251 \
    # create .pxfrc
    && echo >> ~gpadmin/.pxfrc 'export LANG=en_US.UTF-8' \
    && echo >> ~gpadmin/.pxfrc 'export PGPORT=5432' \
    && echo >> ~gpadmin/.pxfrc 'export GOPATH=/opt/go' \
    && echo >> ~gpadmin/.pxfrc 'export GPHOME=$(find /usr/local/ -name greenplum-db* -type d | head -n1)' \
    && echo >> ~gpadmin/.pxfrc 'export LD_LIBRARY_PATH=${GPHOME}/lib:${LD_LIBRARY_PATH-}' \
    && echo >> ~gpadmin/.pxfrc 'export GPHD_ROOT=/singlecluster' \
    && echo >> ~gpadmin/.pxfrc 'export PXF_HOME=/usr/local/pxf-gp6' \
    && echo >> ~gpadmin/.pxfrc 'export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk' \
    && echo >> ~gpadmin/.pxfrc 'export PATH=${GPHOME}/bin:${PXF_HOME}/bin:${GOPATH}/bin:/opt/pxf/go/bin:$PATH' \
    && ln -s ~gpadmin/.pxfrc ~root \
    && echo >> ~gpadmin/.bashrc 'source ~/.pxfrc' \
    && chown -R gpadmin:gpadmin ~gpadmin
