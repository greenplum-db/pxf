FROM maven:3.6-openjdk-8-slim

# install build dependencies for PXF
RUN apt-get update && \
    apt-get install -y \
        curl \
        git \
        make \
        unzip \
        wget && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# install go && go utilities
RUN cd /tmp && \
    wget -O go.tgz -q https://dl.google.com/go/go1.14.2.linux-amd64.tar.gz && \
    tar -C /usr/local -xzf go.tgz && rm go.tgz && \
    GOPATH=/opt/go /usr/local/go/bin/go get github.com/golang/dep/cmd/dep && \
    GOPATH=/opt/go /usr/local/go/bin/go get github.com/onsi/ginkgo/ginkgo

ADD . /tmp/pxf_src

# build pxf server and pxf automation
RUN export BUILD_PARAMS=-Dorg.gradle.daemon=false && \
    GOPATH=/opt/go PATH=/opt/go/bin:/usr/local/go/bin:$PATH make -C /tmp/pxf_src tar && \
    mkdir -p /root/automation_tmp_lib/ &&  \
    for X in /tmp/pxf_src/server/build/stage/lib/pxf-*-[0-9]*.jar; do \
      ln -sf $X /root/automation_tmp_lib/`echo \`basename $X\` | sed -e 's/-[a-zA-Z0-9.]*.jar/.jar/'`; \
    done && touch /root/automation_tmp_lib/pxf-extras.jar && \
    make -C /tmp/pxf_src/automation dev && \
    mkdir -p /root/pxf/servers/database/ && cp /tmp/pxf_src/server/pxf-service/src/templates/user/templates/jdbc-site.xml /root/pxf/servers/database/ && \
    mkdir -p /root/pxf/servers/db-session-params/ && cp /tmp/pxf_src/server/pxf-service/src/templates/user/templates/jdbc-site.xml /root/pxf/servers/db-session-params/ && \
    mkdir -p /root/pxf/servers/db-hive/ && cp /tmp/pxf_src/server/pxf-service/src/templates/user/templates/jdbc-site.xml /root/pxf/servers/db-hive/ && \
    make -C /tmp/pxf_src/automation TEST=HdfsSmokeTest || true && \
    mkdir -p /tmp/output/.tomcat && mkdir -p /tmp/automation && \
    mv /tmp/pxf_src/pxf_commit_sha /tmp/output/pxf_commit_sha && \
    mv /tmp/pxf_src/server/tomcat/build/apache-tomcat-*.tar.gz /tmp/output/.tomcat/ && \
    mv /tmp/pxf_src/cli/go/pkg/dep/sources /tmp/output/.go-dep-cached-sources && \
    mv /root/.gradle /tmp/output && mv /root/.m2 /tmp/automation && \
    cd /tmp/output/ && tar -czf /tmp/pxf-build-dependencies.tar.gz . && \
    cd /tmp/automation && tar -czf /tmp/pxf-automation-dependencies.tar.gz . && \
    rm -rf /tmp/output /tmp/pxf_src /root/.gradle /root/.m2 /tmp/automation /root/pxf
