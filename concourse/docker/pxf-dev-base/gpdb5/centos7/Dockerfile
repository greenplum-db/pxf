ARG BASE_IMAGE=pivotaldata/gpdb-dev:centos6

FROM ${BASE_IMAGE}

# install Go utilities
RUN GOPATH=/opt/go /usr/local/go/bin/go get github.com/golang/dep/cmd/dep && \
    GOPATH=/opt/go /usr/local/go/bin/go get github.com/onsi/ginkgo/ginkgo && \
    echo >> /etc/bashrc 'export GOPATH=/opt/go' && \
    echo >> /etc/bashrc 'export PATH=${GOPATH}/bin:/usr/local/go/bin:$PATH'

# add Java 8, Java 11 and rpm-build
RUN yum install -y rpm-build java-1.8.0-openjdk-devel && yum clean all && \
    wget -q https://download.java.net/openjdk/jdk11/ri/openjdk-11+28_linux-x64_bin.tar.gz && \
    mkdir -p /usr/lib/jvm && \
    tar -C /usr/lib/jvm -xzf openjdk-11+28_linux-x64_bin.tar.gz && \
    rm -f openjdk-11+28_linux-x64_bin.tar.gz

# add minio software
RUN useradd -s /sbin/nologin -d /opt/minio minio && \
    mkdir -p /opt/minio/bin && \
    chmod a+rx /opt/minio && \
    mkdir /opt/minio/data && \
    wget -q https://dl.minio.io/server/minio/release/linux-amd64/minio -O /opt/minio/bin/minio && \
    chmod +x /opt/minio/bin/minio && \
    chown -R minio:minio /opt/minio

# create user gpadmin since GPDB cannot run under root
RUN ssh-keygen -t rsa -N "" -f /root/.ssh/id_rsa && \
    cat /root/.ssh/id_rsa.pub >> /root/.ssh/authorized_keys && \
    chmod 0600 /root/.ssh/authorized_keys && \
    echo -e "password\npassword" | passwd 2> /dev/null && \
    { ssh-keyscan localhost; ssh-keyscan 0.0.0.0; } >> /root/.ssh/known_hosts && \
    ssh-keygen -f /etc/ssh/ssh_host_key -N '' -t rsa1 && \
    ssh-keygen -f /etc/ssh/ssh_host_rsa_key -N '' -t rsa && \
    ssh-keygen -f /etc/ssh/ssh_host_dsa_key -N '' -t dsa && \
    sed -i -e 's|Defaults    requiretty|#Defaults    requiretty|' /etc/sudoers && \
    sed -ri 's/UsePAM yes/UsePAM no/g;s/PasswordAuthentication yes/PasswordAuthentication no/g' /etc/ssh/sshd_config && \
    sed -ri 's@^HostKey /etc/ssh/ssh_host_ecdsa_key$@#&@;s@^HostKey /etc/ssh/ssh_host_ed25519_key$@#&@' /etc/ssh/sshd_config && \
    groupadd -g 6000 gpadmin && useradd -u 6000 -g 6000 gpadmin && \
    echo "gpadmin  ALL=(ALL)       NOPASSWD: ALL" > /etc/sudoers.d/gpadmin && \
    groupadd supergroup && usermod -a -G supergroup gpadmin && \
    mkdir /home/gpadmin/.ssh && \
    ssh-keygen -t rsa -N "" -f /home/gpadmin/.ssh/id_rsa && \
    cat /home/gpadmin/.ssh/id_rsa.pub >> /home/gpadmin/.ssh/authorized_keys && \
    chmod 0600 /home/gpadmin/.ssh/authorized_keys && \
    echo -e "password\npassword" | passwd gpadmin 2> /dev/null && \
    { ssh-keyscan localhost; ssh-keyscan 0.0.0.0; } >> /home/gpadmin/.ssh/known_hosts && \
    chown -R gpadmin:gpadmin /home/gpadmin/.ssh && \
    echo -e 'source /opt/gcc_env.sh' >> /home/gpadmin/.bashrc

COPY . /tmp/build/

# Untar the cached dependencies
RUN tar -xzf /tmp/build/pxf-build-dependencies.tar.gz -C ~gpadmin/ && \
    rm -f /tmp/build/pxf-build-dependencies.tar.gz && \
    ln -s ~gpadmin/.{tomcat,go-dep-cached-sources,m2,gradle} ~root && \
    chown -R gpadmin:gpadmin ~gpadmin
