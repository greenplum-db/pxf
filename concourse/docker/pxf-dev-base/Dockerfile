ARG BASE_IMAGE=gpdb-dev:centos6

FROM pivotaldata/${BASE_IMAGE}

# install Go utilities
RUN cd /tmp && \
    wget -q https://dl.google.com/go/go1.11.4.linux-amd64.tar.gz && \
    tar -C /usr/local -xzf go1.11.4.linux-amd64.tar.gz && \
    rm -f go1.11.4.linux-amd64.tar.gz && \
    cd - && \
    GOPATH=/opt/go /usr/local/go/bin/go get github.com/golang/dep/cmd/dep && \
    GOPATH=/opt/go /usr/local/go/bin/go get github.com/onsi/ginkgo/ginkgo && \
    echo >> /etc/bashrc 'export GOPATH=/opt/go' && \
    echo >> /etc/bashrc 'export PATH=${GOPATH}/bin:/usr/local/go/bin:$PATH'

# add minio software
RUN useradd -s /sbin/nologin -d /opt/minio minio && \
    mkdir -p /opt/minio/bin && \
    chmod a+rx /opt/minio && \
    mkdir /opt/minio/data && \
    wget https://dl.minio.io/server/minio/release/linux-amd64/minio -O /opt/minio/bin/minio && \
    chmod +x /opt/minio/bin/minio && \
    chown -R minio:minio /opt/minio

ADD pxf_src /tmp/pxf_src

RUN cd /tmp/pxf_src && \
    export BUILD_PARAMS='-Dorg.gradle.daemon=false' && \
    GOPATH=/opt/go PATH=/opt/go/bin:/usr/local/go/bin:$PATH make tar && \
    cd automation && PXF_HOME=/tmp/pxf_src/server/build/stage make dev && \
    mv /root/.m2 /home/gpadmin && \
    mv /root/.gradle /home/gpadmin && \
    ln -s /home/gpadmin/.gradle /root/.gradle && \
    ln -s /home/gpadmin/.m2 /root/.m2 && \
    chown -R gpadmin:gpadmin /home/gpadmin && \
    rm -rf /tmp/pxf_src
