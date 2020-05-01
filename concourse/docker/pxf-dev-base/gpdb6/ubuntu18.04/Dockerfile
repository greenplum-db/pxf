ARG BASE_IMAGE=pivotaldata/gpdb-dev:centos6

FROM ${BASE_IMAGE}

# install Go utilities
RUN mkdir -p /tmp/pxf_src/ && cd /tmp && \
    wget -q https://dl.google.com/go/go1.14.2.linux-amd64.tar.gz && \
    tar -C /usr/local -xzf go*.linux-amd64.tar.gz && \
    rm -f go*.linux-amd64.tar.gz && \
    cd - && \
    GOPATH=/opt/go /usr/local/go/bin/go get github.com/golang/dep/cmd/dep && \
    GOPATH=/opt/go /usr/local/go/bin/go get github.com/onsi/ginkgo/ginkgo && \
    echo >> /etc/bashrc 'export GOPATH=/opt/go' && \
    echo >> /etc/bashrc 'export PATH=${GOPATH}/bin:/usr/local/go/bin:$PATH'

# install dependencies that are missing on the base images
# install a specific version of perl for tinc
RUN apt-get update -y && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y \
    maven python-dev curl sudo && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/* && \
    PERL_VERSION=perl-5.22.1 && \
    wget http://www.cpan.org/src/5.0/${PERL_VERSION}.tar.gz && \
    tar -xzf ${PERL_VERSION}.tar.gz && \
    cd $PERL_VERSION && \
      ./Configure -des -Dprefix=/usr && \
      make -j8 -s && make -j16 -s install && \
    cd - && rm -rf ${PERL_VERSION}*

# create user gpadmin since GPDB cannot run under root
RUN locale-gen en_US.UTF-8 && \
    ssh-keygen -t rsa -N "" -f /root/.ssh/id_rsa && \
    cat /root/.ssh/id_rsa.pub >> /root/.ssh/authorized_keys && \
    chmod 0600 /root/.ssh/authorized_keys && \
    echo "root:password" | chpasswd 2> /dev/null && \
    sed -i -e 's|Defaults    requiretty|#Defaults    requiretty|' /etc/sudoers && \
    sed -ri 's/UsePAM yes/UsePAM no/g;s/PasswordAuthentication yes/PasswordAuthentication no/g' /etc/ssh/sshd_config && \
    sed -ri 's@^HostKey /etc/ssh/ssh_host_ecdsa_key$@#&@;s@^HostKey /etc/ssh/ssh_host_ed25519_key$@#&@' /etc/ssh/sshd_config && \
    service ssh start && \
    { ssh-keyscan localhost; ssh-keyscan 0.0.0.0; } >> /root/.ssh/known_hosts && \
    groupadd -g 6000 gpadmin && useradd -u 6000 -g 6000 gpadmin -s /bin/bash && \
    echo "gpadmin  ALL=(ALL)       NOPASSWD: ALL" > /etc/sudoers.d/gpadmin && \
    groupadd supergroup && usermod -a -G supergroup gpadmin && \
    mkdir -p /home/gpadmin/.ssh && \
    ssh-keygen -t rsa -N "" -f /home/gpadmin/.ssh/id_rsa && \
    cat /home/gpadmin/.ssh/id_rsa.pub >> /home/gpadmin/.ssh/authorized_keys && \
    chmod 0600 /home/gpadmin/.ssh/authorized_keys && \
    echo "gpadmin:password" | chpasswd 2> /dev/null && \
    { ssh-keyscan localhost; ssh-keyscan 0.0.0.0; } >> /home/gpadmin/.ssh/known_hosts && \
    chown -R gpadmin:gpadmin /home/gpadmin && \
    mkdir /usr/local/greenplum-db-devel && \
    chown gpadmin:gpadmin /usr/local/greenplum-db-devel

ADD . /tmp/build/

# Untar the cached dependencies
# install psi + paramiko as gpadmin
RUN tar -xzf /tmp/build/pxf-build-dependencies.tar.gz -C ~gpadmin/ && \
    rm -rf /tmp/build && \
    ln -s ~gpadmin/.{tomcat,go-dep-cached-sources,m2,gradle} ~root && \
    chown -R gpadmin:gpadmin ~gpadmin && \
    su gpadmin -c "pip install psi paramiko --no-cache-dir"
