FROM pivotaldata/gpdb-dev:centos6

ARG TERM=xterm-256color
ARG CLUSTER_NAME=hdp
ARG REALM=AMBARI.APACHE.ORG
ARG AMBARI_HOST=c6401.ambari.apache.org
ARG KERBEROS_CLIENTS=c6401.ambari.apache.org
ARG CURL_OPTS="-u admin:admin -H X-Requested-By:ambari"
ARG AMBARI_PREFIX="http://c6401.ambari.apache.org:8080/api/v1"
ARG YUM_REPOFILE_URL=http://public-repo-1.hortonworks.com/ambari/centos6/2.x/updates/2.6.2.0/ambari.repo

# Install Ambari for creating a secure HDP cluster
RUN pushd /etc/yum.repos.d && wget -q ${YUM_REPOFILE_URL} && popd && \
    yum -y install ambari-server ambari-agent jq && yum clean all

COPY vagrant /root/vagrant

# Use ambari to create a single-node HDP cluster using blueprints
RUN sed "s/$HOSTNAME/${AMBARI_HOST} $HOSTNAME/g" /etc/hosts > /tmp/hosts && \
    echo y | cp /tmp/hosts /etc/hosts && \
    ambari-server setup --silent && \
    sed -i "s/hostname=localhost/hostname=${AMBARI_HOST}/" /etc/ambari-agent/conf/ambari-agent.ini && \
    ambari-agent start && \
    ambari-server start && \
    sleep 30 && \
    curl ${CURL_OPTS} -X POST ${AMBARI_PREFIX}/blueprints/blueprint -d@/root/vagrant/blueprint.json && \
    curl ${CURL_OPTS} -X POST ${AMBARI_PREFIX}/clusters/${CLUSTER_NAME} -d@/root/vagrant/hostmapping.json && sleep 240 && \
    echo $(curl ${CURL_OPTS} ${AMBARI_PREFIX}/clusters/${CLUSTER_NAME}/requests/1 | jq --raw-output '.Requests.request_status') && \
    curl ${CURL_OPTS} -X PUT -d '{"ServiceInfo": {"state" : "STARTED"}}' ${AMBARI_PREFIX}/clusters/${CLUSTER_NAME}/services && sleep 120 && \
    ambari-server stop && \
    ambari-agent stop && \
    service postgresql stop

# Install KDC and secure the HDP cluster using kerberos
RUN sed "s/$HOSTNAME/${AMBARI_HOST} $HOSTNAME/g" /etc/hosts > /tmp/hosts && \
	echo y | cp /tmp/hosts /etc/hosts && \
	echo "export CLUSTER_NAME=${CLUSTER_NAME}" >> ~/.bash_profile && \
	echo "export REALM=${REALM}" >> ~/.bash_profile && \
	echo "export CURL_OPTS=${CURL_OPTS}" >> ~/.bash_profile && \
	echo "export AMBARI_PREFIX=${AMBARI_PREFIX}" >> ~/.bash_profile && \
	ambari-agent start && \
    ambari-server start && \
    sudo yum -y install krb5-server krb5-workstation && yum clean all && \
    sudo sed -i "s/default_realm = EXAMPLE.COM/default_realm = ${REALM}/" /etc/krb5.conf && \
    sudo sed -i "s/EXAMPLE.COM = {/${REALM} = {/" /etc/krb5.conf && \
    sudo sed -i "s/kdc = kerberos.example.com/kdc = ${AMBARI_HOST}/" /etc/krb5.conf && \
    sudo sed -i "s/admin_server = kerberos.example.com/admin_server = ${AMBARI_HOST}/" /etc/krb5.conf && \
    sudo sed -i "/\.example.com = EXAMPLE.COM/d" /etc/krb5.conf && \
    sudo sed -i "s/example.com = EXAMPLE.COM/${AMBARI_HOST} = ${REALM}/" /etc/krb5.conf && \
    printf 'admin\nadmin\n' | sudo kdb5_util create -s -r ${REALM} && \
    sudo sed -i "s/EXAMPLE.COM/${REALM}/" /var/kerberos/krb5kdc/kadm5.acl && \
    sudo kadmin.local -q "addprinc -pw admin admin/admin@${REALM}" && \
    sudo service kadmin start && \
    sudo service krb5kdc start && \
    curl ${CURL_OPTS} -X POST ${AMBARI_PREFIX}/clusters/${CLUSTER_NAME}/services/KERBEROS && \
    curl ${CURL_OPTS} -X POST ${AMBARI_PREFIX}/clusters/${CLUSTER_NAME}/services/KERBEROS/components/KERBEROS_CLIENT && \
    curl ${CURL_OPTS} -X PUT -d @/root/vagrant/payload ${AMBARI_PREFIX}/clusters/${CLUSTER_NAME} && \
    curl ${CURL_OPTS} -X POST -d '{"host_components" : [{"HostRoles" : {"component_name":"KERBEROS_CLIENT"}}]}' ${AMBARI_PREFIX}/clusters/${CLUSTER_NAME}/hosts?Hosts/host_name=$KERBEROS_CLIENTS && \
    curl ${CURL_OPTS} -X PUT -d '{"ServiceInfo": {"state" : "INSTALLED"}}' ${AMBARI_PREFIX}/clusters/${CLUSTER_NAME}/services/KERBEROS && sleep 15 && \
    curl ${CURL_OPTS} -X PUT -d '{"ServiceInfo": {"state" : "INSTALLED"}}' ${AMBARI_PREFIX}/clusters/${CLUSTER_NAME}/services && sleep 45 && \
    curl ${CURL_OPTS} -X PUT -d @/root/vagrant/credentials ${AMBARI_PREFIX}/clusters/${CLUSTER_NAME} && sleep 30 && \
    rm -rf /root/vagrant && \
	ambari-server stop && \
	ambari-agent stop && \
	service postgresql stop && \
	service krb5kdc stop && \
	service kadmin stop

# This container must be run in privileged mode in order for the cluster to come up in secure mode
