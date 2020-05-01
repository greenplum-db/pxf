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
RUN cd /tmp && wget -q http://curl.haxx.se/download/curl-7.51.0.tar.bz2 && \
    tar -xjf curl-*.tar.bz2 && rm curl-*.tar.bz2 && cd curl-* && \
    ./configure --prefix=/usr --with-gssapi > log-file 2>&1 && make -j8 >> log-file 2>&1 && \
    make install >> log-file 2>&1 && rm -rf /tmp/curl-* && \
    wget -q https://repos.fedorapeople.org/repos/dchen/apache-maven/epel-apache-maven.repo \
      -O /etc/yum.repos.d/epel-apache-maven.repo && \
    yum install -y rpm-build python-paramiko apache-maven python-devel sudo && yum clean all && \
    rm /etc/yum.repos.d/epel-apache-maven.repo && ldconfig && \
    cd /tmp && pip install psi --no-cache-dir

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

ADD . /tmp/build/

# Untar the cached dependencies
RUN tar -xzf /tmp/build/pxf-build-dependencies.tar.gz -C ~gpadmin/ && \
    rm -rf /tmp/build && \
    ln -s ~gpadmin/.{tomcat,go-dep-cached-sources,m2,gradle} ~root && \
    chown -R gpadmin:gpadmin ~gpadmin
