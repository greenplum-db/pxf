ARG BASE_IMAGE=gpdb-dev:centos6

FROM pivotaldata/${BASE_IMAGE}

ADD pxf_src /tmp/pxf_src

# install Go utilities
RUN cd /tmp && \
        wget -q https://dl.google.com/go/go1.11.1.linux-amd64.tar.gz && \
        tar -C /usr/local -xzf go1.11.1.linux-amd64.tar.gz && \
        rm -f go1.11.1.linux-amd64.tar.gz && \
    cd - && \
    GOPATH=/root/go /usr/local/go/bin/go get github.com/golang/dep/cmd/dep && \
    GOPATH=/root/go /usr/local/go/bin/go get github.com/onsi/ginkgo/ginkgo && \
    echo >>/etc/bashrc 'export PATH="/root/go/bin:/usr/local/go/bin:$PATH"' && \
    echo >>/etc/bashrc 'export GOPATH=/root/go'

RUN cd /tmp/pxf_src/server && \
    GOPATH="/root/go" PATH="/root/go/bin:/usr/local/go/bin:$PATH" make tar && \
    CWD=$(pwd) && cd /tmp/pxf_src/automation && \
    PXF_HOME=/tmp/pxf_src/server/build/stage make dev && \
    mkdir -p /home/gpadmin && \
    mv /root/.m2 /home/gpadmin && \
    chown -R 1000:1000 /home/gpadmin/.m2 && \
    ln -s /home/gpadmin/.gradle /root/.gradle && \
    rm -rf /tmp/pxf_src
