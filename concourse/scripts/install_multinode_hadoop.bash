#!/usr/bin/env bash

set -o errexit

# Many programs (notably yum) reformat their output according to the terminal
# width. Concourse sets the width ridiculously wide by default, resulting in
# yum printing very long progress bars and other output that wraps across
# multiple lines in the browser. To prevent this ugliness, we simulate an
# 120-column terminal.
stty cols 120

if ! type jq &>/dev/null; then
	yum install -y jq
fi

cluster_name="$(cat terraform_ipa_hadoop/name)"
metadata_path="terraform_ipa_hadoop/metadata"
ansible_play_path="pxf_src/concourse/ansible/ipa-multinode-hadoop"
store_password="$(env | awk -F= '/ANSIBLE_VAR_ssl_store_password/{print $2}')"

# generate pkcs12 keystore for NameNode VMs
num_namenodes=$(jq <"${metadata_path}" -r '.namenode_tls_cert | length')
for i in $(seq 0 $((num_namenodes - 1))); do
	cert="$(jq <"${metadata_path}" -r ".namenode_tls_cert[$i].cert_pem")"
	key="$(jq <"${metadata_path}" -r ".namenode_tls_private_key[$i].private_key_pem")"
	name="$(jq <"${metadata_path}" -r ".namenode_tls_cert[$i].subject[0].common_name")"

	openssl pkcs12 -export -in <(echo "${cert}") -inkey <(echo "${key}") -out "${ansible_play_path}/${name%%.*}.p12" -name "${name}" -password "pass:${store_password}"
	echo "${cert}" | keytool -noprompt -import -trustcacerts -alias "${name}" -keystore "${ansible_play_path}/truststore.p12" -storetype pkcs12 -storepass "${store_password}"
done

# generate pkcs12 keystore for DataNode VMs
num_datanodes=$(jq <"${metadata_path}" -r '.datanode_tls_cert | length')
for i in $(seq 0 $((num_datanodes - 1))); do
	cert="$(jq <"${metadata_path}" -r ".datanode_tls_cert[$i].cert_pem")"
	key="$(jq <"${metadata_path}" -r ".datanode_tls_private_key[$i].private_key_pem")"
	name="$(jq <"${metadata_path}" -r ".datanode_tls_cert[$i].subject[0].common_name")"

	openssl pkcs12 -export -in <(echo "${cert}") -inkey <(echo "${key}") -out "${ansible_play_path}/${name%%.*}.p12" -name "${name}" -password "pass:${store_password}"
	echo "${cert}" | keytool -noprompt -import -trustcacerts -alias "${name}" -keystore "${ansible_play_path}/truststore.p12" -storetype pkcs12 -storepass "${store_password}"
done

# setup ssh_config with host alias, users, and identity files
mkdir -p ~/.ssh
jq <"${metadata_path}" -r '.ssh_config' >>~/.ssh/config
jq <"${metadata_path}" -r '.private_key' >~/.ssh/"${cluster_name}"
chmod 0600 ~/.ssh/"${cluster_name}"
ssh-keygen -y -f ~/.ssh/"${cluster_name}" >~/.ssh/"${cluster_name}".pub

# generate ansible configuration files
jq <"${metadata_path}" -r '.ansible_inventory' >"${ansible_play_path}"/inventory.ini
jq <"${metadata_path}" -r '.ansible_variables' >"${ansible_play_path}"/config.yml

# append additional (sensitive) variables from the enironment
env | sed -e '/^ANSIBLE_VAR_/!d;s/ANSIBLE_VAR_\(.*\)=\(.*\)/\1: \2/' >>"${ansible_play_path}"/config.yml

if ! type ansible-playbook &>/dev/null; then
	# ensure that we always have updated certs by installing ca-certificates
	yum install -y ca-certificates
	yum install -y ansible
fi

pushd "${ansible_play_path}" || exit 1

# https://serverfault.com/a/846232
export ANSIBLE_STDOUT_CALLBACK=debug

ansible-galaxy collection install -r requirements.yml
ansible-playbook main.yml
popd || exit 1

# Hadoop cluster has been successfully configured; now create environment files
# for downstream CI tasks to configure and run automation tests
hadoop_namenode_1="ccp-${cluster_name}-nn01"
hadoop_namenode_2="ccp-${cluster_name}-nn02"
ipa_server="ccp-${cluster_name}-ipa"
mkdir -p ipa_env_files
jq <"${metadata_path}" -r ".etc_hosts" >ipa_env_files/etc_hostfile
echo "${hadoop_namenode_1}" >ipa_env_files/nn01
echo "${hadoop_namenode_2}" >ipa_env_files/nn02
mkdir -p ipa_env_files/conf
export HADOOP_PREFIX="$(ssh ${hadoop_namenode_1} "printenv HADOOP_PREFIX")"
scp "${hadoop_namenode_1}:${HADOOP_PREFIX}/etc/hadoop/*-site.xml" ipa_env_files/conf/
export HIVE_HOME="$(ssh ${hadoop_namenode_2} "printenv HIVE_HOME")"
scp "${hadoop_namenode_2}:${HIVE_HOME}/conf/hive-site.xml" ipa_env_files/conf/

cp ~/.ssh/"${cluster_name}" ipa_env_files/google_compute_engine
cp ~/.ssh/"${cluster_name}".pub ipa_env_files/google_compute_engine.pub

gcp_project="$(jq <"${metadata_path}" -r ".project")"
domain_name="c.${gcp_project}.internal"
echo "${domain_name}" >ipa_env_files/domain
echo "${domain_name^^}" >ipa_env_files/REALM

cat <<EOF >ipa_env_files/krb5_realm
	${domain_name^^} = {
		kdc = ${ipa_server}.${domain_name}
		admin_server = ${ipa_server}.${domain_name}
	}
EOF

cat <<EOF >ipa_env_files/krb5_domain_realm
	.${domain_name} = ${domain_name^^}
EOF

scp "${ipa_server}":~/hadoop.user.keytab ipa_env_files/
scp "${ipa_server}":~/pxf.service.keytab ipa_env_files/

# list environment files
find ipa_env_files -type f
