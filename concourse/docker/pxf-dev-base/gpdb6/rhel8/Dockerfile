ARG BASE_IMAGE=gcr.io/data-gpdb-private-images/gpdb6-rhel8-test:latest

FROM ${BASE_IMAGE}

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

ARG MAVEN_VERSION=3.6.3
ARG USER_HOME_DIR="/root"
ARG BASE_URL=https://apache.osuosl.org/maven/maven-3/${MAVEN_VERSION}/binaries

# install dependencies that are missing on the base images
RUN curl -fsSL -o /tmp/apache-maven.tar.gz ${BASE_URL}/apache-maven-${MAVEN_VERSION}-bin.tar.gz \
    && mkdir -p /usr/share/maven \
    && tar -xzf /tmp/apache-maven.tar.gz -C /usr/share/maven --strip-components=1 \
    && rm -f /tmp/apache-maven.tar.gz \
    && ln -s /usr/share/maven/bin/mvn /usr/bin/mvn \
    && /usr/bin/pip2 install paramiko --no-cache-dir

# create rsa key for the root user
RUN ssh-keygen -t rsa -N "" -f /root/.ssh/id_rsa \
    && cat /root/.ssh/id_rsa.pub >> /root/.ssh/authorized_keys \
    && chmod 0600 /root/.ssh/authorized_keys \
    && echo -e "password\npassword" | passwd 2> /dev/null \
    # create rsa key for the server
    && ssh-keygen -f /etc/ssh/ssh_host_rsa_key -N '' -t rsa \
    && sed -i -e 's|Defaults    requiretty|#Defaults    requiretty|' /etc/sudoers \
    && sed -ri 's/UsePAM yes/UsePAM no/g;s/PasswordAuthentication yes/PasswordAuthentication no/g' /etc/ssh/sshd_config \
    && sed -ri 's@^HostKey /etc/ssh/ssh_host_ecdsa_key$@#&@;s@^HostKey /etc/ssh/ssh_host_ed25519_key$@#&@' /etc/ssh/sshd_config \
    # create user gpadmin since GPDB cannot run under root
    && groupadd -g 6000 gpadmin && useradd -u 6000 -g 6000 gpadmin \
    && echo "gpadmin  ALL=(ALL)       NOPASSWD: ALL" > /etc/sudoers.d/gpadmin \
    && groupadd supergroup && usermod -a -G supergroup gpadmin \
    && mkdir /home/gpadmin/.ssh \
    # create an rsa key for the gpadmin user
    && ssh-keygen -m PEM -t rsa -f /home/gpadmin/.ssh/id_rsa \
    && cat /home/gpadmin/.ssh/id_rsa.pub >> /home/gpadmin/.ssh/authorized_keys \
    && chmod 0600 /home/gpadmin/.ssh/authorized_keys \
    && echo -e "password\npassword" | passwd gpadmin 2> /dev/null \
    && awk '{print "localhost", $1, $2; print "127.0.0.1", $1, $2}' /etc/ssh/ssh_host_rsa_key.pub >> /home/gpadmin/.ssh/known_hosts \
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
    && echo >> ~gpadmin/.pxfrc 'export PATH=${GPHOME}/bin:${PXF_HOME}/bin:${GOPATH}/bin:/usr/local/go/bin:$PATH' \
    && ln -s ~gpadmin/.pxfrc ~root \
    && echo >> ~gpadmin/.bashrc 'source ~/.pxfrc' \
    && chown -R gpadmin:gpadmin ~gpadmin
