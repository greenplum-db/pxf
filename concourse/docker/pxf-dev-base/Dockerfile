ARG BASE_IMAGE=gpdb-dev:centos6

FROM pivotaldata/${BASE_IMAGE}

# install Go utilities
RUN cd /tmp && \
    wget -q https://dl.google.com/go/go1.11.5.linux-amd64.tar.gz && \
    tar -C /usr/local -xzf go1.11.5.linux-amd64.tar.gz && \
    rm -f go1.11.5.linux-amd64.tar.gz && \
    cd - && \
    GOPATH=/opt/go /usr/local/go/bin/go get github.com/golang/dep/cmd/dep && \
    GOPATH=/opt/go /usr/local/go/bin/go get github.com/onsi/ginkgo/ginkgo && \
    echo >> /etc/bashrc 'export GOPATH=/opt/go' && \
    echo >> /etc/bashrc 'export PATH=${GOPATH}/bin:/usr/local/go/bin:$PATH'

# add Java 11
RUN wget -q https://download.java.net/openjdk/jdk11/ri/openjdk-11+28_linux-x64_bin.tar.gz && \
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

ADD pxf_src /tmp/pxf_src

# Install krb5-workstation for Kerberos tests against dataproc (only needed on CentOS 6)
RUN ["/bin/bash", "-c", "export BUILD_PARAMS=-Dorg.gradle.daemon=false && \
    GOPATH=/opt/go PATH=/opt/go/bin:/usr/local/go/bin:$PATH make -C /tmp/pxf_src tar && \
    PXF_HOME=/tmp/pxf_src/server/build/stage make -C /tmp/pxf_src/automation dev && \
    mkdir -p ~gpadmin/.tomcat && \
    mv /tmp/pxf_src/server/tomcat/build/apache-tomcat-*.tar.gz ~gpadmin/.tomcat && \
    mv /tmp/pxf_src/cli/go/pkg/dep/sources ~gpadmin/.go-dep-cached-sources && \
    mv /root/.{gradle,m2} ~gpadmin && \
    ln -s ~gpadmin/.{tomcat,go-dep-cached-sources,m2,gradle} ~root && \
    chown -R gpadmin:gpadmin ~gpadmin && \
    rm -rf /tmp/pxf_src && \
    if grep 'CentOS release 6' /etc/centos-release; then \
      yum install -y krb5-workstation python-paramiko apr-util && yum clean all; \
    else \
      pip install psi paramiko --no-cache-dir; \
    fi && \
    if grep 'CentOS Linux release 7' /etc/centos-release; then \
      yum install -y apr-util bzip2 openssl zip && yum clean all; \
    fi; \
    if grep 'VERSION=.*18.04.*LTS.*Bionic Beaver.*$' /etc/os-release; then \
      apt-get update && apt-get install -y zip libaprutil1 && rm -rf /var/lib/apt/lists/* && \
      PERL_VERSION=perl-5.22.1 && \
      wget http://www.cpan.org/src/5.0/${PERL_VERSION}.tar.gz && \
      tar -xzf ${PERL_VERSION}.tar.gz && \
      pushd $PERL_VERSION && \
        ./Configure -des -Dprefix=/usr && \
        make && make install && \
      popd && rm -rf ${PERL_VERSION}*; \
    fi"]
