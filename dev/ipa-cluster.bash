#!/usr/bin/env bash

set -o errexit

WORKING_DIR="$(pwd)"

# Follow symlinks to find the real script
cd "$(dirname "$0")" || exit 1
script_file=$(pwd)/$(basename "$0")
while [[ -L "$script_file" ]]; do
    script_file=$(readlink "$script_file")
    cd "$(dirname "$script_file")" || exit 1
    script_file=$(pwd)/$(basename "$script_file")
done

parent_script_dir="$( (cd "$( dirname "${script_file}" )/.." && pwd -P) )"
cd "$WORKING_DIR" || exit 1

# setup common global variables
export TF_VAR_env_name=${TF_VAR_env_name:-${USER}}
cluster_name=${TF_VAR_env_name}
terraform_dir="${parent_script_dir}/concourse/terraform/ipa-multinode-hadoop"
ansible_play_path="${parent_script_dir}/concourse/ansible/ipa-multinode-hadoop"
metadata_path="${terraform_dir}/output.json"
store_password="$(env | awk -F= '/ANSIBLE_VAR_ssl_store_password/{print $2}')"

# ANSI Colors
echoRed() { echo $'\e[0;31m'"$1"$'\e[0m'; }
echoGreen() { echo $'\e[0;32m'"$1"$'\e[0m'; }
echoYellow() { echo $'\e[0;33m'"$1"$'\e[0m'; }

function printUsage() {
    local normal bold
    normal=$(tput sgr0)
    bold=$(tput bold)
    cat <<-EOF
	${bold}NAME${normal}
	    ipa-cluster.bash - manage a multi-node HA Hadoop cluster secured by IPA in GCP

	${bold}Usage${normal}:  ipa-cluster.bash <command>
	        ipa-cluster.bash --create  - to create a new IPA cluster in GCP
	        ipa-cluster.bash --destroy - to destroy an existing IPA cluster in GCP
	EOF
}

function fail() {
    cd "$WORKING_DIR"
    echoRed "ERROR: $1"
    exit 1
}

function check_pre_requisites() {
    if ! type terraform &>/dev/null; then
      fail 'terraform is not found, did you install it (e.g. "brew install terraform") ?'
    fi

    if ! type jq &>/dev/null; then
      fail 'jq is not found, did you install it (e.g. "brew install jq") ?'
    fi

    if ! type ansible-playbook &>/dev/null; then
      fail 'ansible-playbook is not found, did you install it ?'
    fi

    if ! type xmlstarlet &>/dev/null; then
      fail 'xmlstarlet is not found, did you install it (e.g. "brew install xmlstarlet") ?'
    fi

    : "${PXF_BASE?"PXF_BASE is required. export PXF_BASE=[YOUR_PXF_CONFIG_LOCATION]"}"
    : "${TF_VAR_gcp_project?"TF_VAR_gcp_project is required. export TF_VAR_gcp_project=[YOUR_GCP_PROJECT_NAME]"}"
    : "${ANSIBLE_VAR_ipa_password?"ANSIBLE_VAR_ipa_password is required. export ANSIBLE_VAR_ipa_password=[YOUR_IPA_PASSWORD]"}"
    : "${ANSIBLE_VAR_ssl_store_password?"ANSIBLE_VAR_ssl_store_password is required. export ANSIBLE_VAR_ssl_store_password=[YOUR_SSL_STORE_PASSWORD]"}"
}

function create_firewall_resource() {
    local my_ip=$(curl ifconfig.co)
    cat << EOF > "${terraform_dir}"/local-firewall.tf
resource "google_compute_firewall" "$USER-access" {
  name    = "$USER-access"
  network = var.network
  direction = "INGRESS"
  allow {
    protocol = "all"
  }
  source_ranges = ["${my_ip}/32"]
}
EOF
}

function create_override_file() {
    # create an override file that opens up public IP for the nodes provisioned by terraform
    cat << EOF > "${terraform_dir}"/override.tf
resource "google_compute_instance" "ipa" {
  network_interface {
    subnetwork  = var.subnet
    access_config {
      // Ephemeral public IP
    }
  }
}
resource "google_compute_instance" "namenode" {
  network_interface {
    subnetwork  = var.subnet
    access_config {
      // Ephemeral public IP
    }
  }
}
resource "google_compute_instance" "datanode" {
  network_interface {
    subnetwork  = var.subnet
    access_config {
      // Ephemeral public IP
    }
  }
}
EOF
}

# apply terraform template to provision the cluster
function terraform_apply() {
    cd "${terraform_dir}" || fail "cannot go to ${terraform_dir}"
    terraform init
    create_firewall_resource
    create_override_file
    terraform apply -auto-approve -var-file="${terraform_dir}/local.tfvars"
    terraform output -json > "${terraform_dir}/output.json"
}

# destroy resources provisioned by terraform
function terraform_destroy() {
    cd "${terraform_dir}" || fail "cannot go to ${terraform_dir}"
    terraform destroy -var-file="${terraform_dir}/local.tfvars"
}

# cleanup ansible artifacts that might exist from previous runs
function cleanup_existing_artifacts() {
    rm -f "${ansible_play_path}"/*.p12
    rm -f "${ansible_play_path}"/inventory.ini
    rm -f "${ansible_play_path}"/config.yml
}

function generate_keystores() {
# generate pkcs12 keystore for NameNode VMs
    num_namenodes=$(jq <"${metadata_path}" -r '.namenode_tls_cert.value | length')
    for i in $(seq 0 $((num_namenodes - 1))); do
    	  cert="$(jq <"${metadata_path}" -r ".namenode_tls_cert.value[$i].cert_pem")"
    	  key="$(jq <"${metadata_path}" -r ".namenode_tls_private_key.value[$i].private_key_pem")"
    	  name="$(jq <"${metadata_path}" -r ".namenode_tls_cert.value[$i].subject[0].common_name")"

    	  openssl pkcs12 -export -in <(echo "${cert}") -inkey <(echo "${key}") -out "${ansible_play_path}/${name%%.*}.p12" -name "${name}" -password "pass:${store_password}"
    	  echo "${cert}" | keytool -noprompt -import -trustcacerts -alias "${name}" -keystore "${ansible_play_path}/truststore.p12" -storetype pkcs12 -storepass "${store_password}"
    done

    # generate pkcs12 keystore for DataNode VMs
    num_datanodes=$(jq <"${metadata_path}" -r '.datanode_tls_cert.value | length')
    for i in $(seq 0 $((num_datanodes - 1))); do
    	  cert="$(jq <"${metadata_path}" -r ".datanode_tls_cert.value[$i].cert_pem")"
    	  key="$(jq <"${metadata_path}" -r ".datanode_tls_private_key.value[$i].private_key_pem")"
    	  name="$(jq <"${metadata_path}" -r ".datanode_tls_cert.value[$i].subject[0].common_name")"

    	  openssl pkcs12 -export -in <(echo "${cert}") -inkey <(echo "${key}") -out "${ansible_play_path}/${name%%.*}.p12" -name "${name}" -password "pass:${store_password}"
    	  echo "${cert}" | keytool -noprompt -import -trustcacerts -alias "${name}" -keystore "${ansible_play_path}/truststore.p12" -storetype pkcs12 -storepass "${store_password}"
    done
}

# setup ssh ipa_config with host alias, users, and identity files
function setup_ssh_config() {
    mkdir -p ~/.ssh

    [ ! -f ~/.ssh/config ] && touch ~/.ssh/config

    # If necessary, update ~/.ssh/config file with Include directive
    grep -qxF 'Include "ipa_config"' ~/.ssh/config || echo 'Include "ipa_config"' >> ~/.ssh/config

    jq <"${metadata_path}" -r '.ssh_config.value'  >~/.ssh/ipa_config
    jq <"${metadata_path}" -r '.private_key.value' >~/.ssh/ipa_"${cluster_name}"_rsa

    chmod 0600 ~/.ssh/ipa_"${cluster_name}"_rsa
    ssh-keygen -y -f ~/.ssh/ipa_"${cluster_name}"_rsa >~/.ssh/ipa_"${cluster_name}"_rsa.pub
}

# run ansible playbook to configure Hadoop on the cluster
function run_ansible_playbook() {
    # generate ansible configuration files
    jq <"${metadata_path}" -r '.ansible_inventory.value' >"${ansible_play_path}"/inventory.ini
    jq <"${metadata_path}" -r '.ansible_variables.value' >"${ansible_play_path}"/config.yml

    # append additional (sensitive) variables from the enironment
    env | sed -e '/^ANSIBLE_VAR_/!d;s/ANSIBLE_VAR_\(.*\)=\(.*\)/\1: \2/' >>"${ansible_play_path}"/config.yml

    pushd "${ansible_play_path}" || fail "cannot go to ${ansible_play_path}"
    ansible-galaxy collection install -r requirements.yml
    ansible-playbook main.yml
    popd || fail "cannot come out of ${ansible_play_path}"
}

# create / gather environment files under ipa_env_files to use with PXF
function create_environment_files() {
    hadoop_namenode="ccp-${cluster_name}-nn01"
    ipa_server="ccp-${cluster_name}-ipa"
    hive_node="ccp-${cluster_name}-nn02"
    rm -rf ipa_env_files
    mkdir -p ipa_env_files
    jq <"${metadata_path}" -r ".etc_hosts.value" >ipa_env_files/etc_hostfile
    echo "${hadoop_namenode}" >ipa_env_files/nn01
    echo "${hive_node}" >ipa_env_files/nn02
    mkdir -p ipa_env_files/conf
    scp "${hadoop_namenode}:\$HADOOP_PREFIX/etc/hadoop/*-site.xml" ipa_env_files/conf/
    scp "${hive_node}:\$HIVE_HOME/conf/hive-site.xml" ipa_env_files/conf/

    cp ~/.ssh/ipa_"${cluster_name}"_rsa ipa_env_files/google_compute_engine
    cp ~/.ssh/ipa_"${cluster_name}"_rsa.pub ipa_env_files/google_compute_engine.pub

    gcp_project="$(jq <"${metadata_path}" -r ".project.value")"
    domain_name_lower="c.${gcp_project}.internal"
    domain_name_upper=$(tr '[:lower:]' '[:upper:]' <<< "$domain_name_lower")
    echo "$domain_name_upper" >ipa_env_files/REALM

    cat <<EOF >ipa_env_files/krb5_realm
	${domain_name_upper} = {
		kdc = ${ipa_server}.${domain_name_lower}
		admin_server = ${ipa_server}.${domain_name_lower}
	}
EOF

    cat <<EOF >ipa_env_files/krb5_domain_realm
	.${domain_name_lower} = ${domain_name_upper}
EOF

    scp "${ipa_server}":~/hadoop.user.keytab ipa_env_files/
    scp "${ipa_server}":~/pxf.service.keytab ipa_env_files/

    # list environment files
    find ipa_env_files -type f
}

# setup PXF servers 'hdfs-ipa' and 'hdfs-ipa-no-impersonation-no-svcuser' in PXF_BASE
function setup_pxf_server() {
    rm -rf "${PXF_BASE}"/servers/hdfs-ipa
    mkdir -p "${PXF_BASE}"/servers/hdfs-ipa
    cp ipa_env_files/conf/*.xml "${PXF_BASE}"/servers/hdfs-ipa/
    cp ipa_env_files/*.keytab "${PXF_BASE}"/servers/hdfs-ipa/
    cp "${parent_script_dir}"/server/pxf-service/src/templates/templates/pxf-site.xml "${PXF_BASE}"/servers/hdfs-ipa/
    sed -i '' -e "s|>gpadmin/_HOST@EXAMPLE.COM<|>porter@${domain_name_upper}<|g" "${PXF_BASE}"/servers/hdfs-ipa/pxf-site.xml
    sed -i '' -e 's|/keytabs/|/servers/hdfs-ipa/|g' "${PXF_BASE}"/servers/hdfs-ipa/pxf-site.xml
    # set Hadoop client to use hostnames for datanodes instead of IP addresses (which are internal in GCP network)
    xmlstarlet ed --inplace --pf --append '/configuration/property[last()]' --type elem -n property -v "" \
     --subnode '/configuration/property[last()]' --type elem -n name -v "dfs.client.use.datanode.hostname" \
     --subnode '/configuration/property[last()]' --type elem -n value -v "true" "${PXF_BASE}"/servers/hdfs-ipa/hdfs-site.xml
    # set constrained delegation property to true for the PXF server
    xmlstarlet ed --inplace --pf --update "/configuration/property[name = 'pxf.service.kerberos.constrained-delegation']/value" -v true "${PXF_BASE}"/servers/hdfs-ipa/pxf-site.xml

    rm -rf "${PXF_BASE}"/servers/hdfs-ipa-no-impersonation
    cp -R "${PXF_BASE}"/servers/hdfs-ipa "${PXF_BASE}"/servers/hdfs-ipa-no-impersonation
    sed -i '' -e 's|hdfs-ipa|hdfs-ipa-no-impersonation|g' "${PXF_BASE}"/servers/hdfs-ipa-no-impersonation/pxf-site.xml
    # set impersonation property to false for the PXF server
    xmlstarlet ed --inplace --pf --update "/configuration/property[name = 'pxf.service.user.impersonation']/value" -v false "${PXF_BASE}"/servers/hdfs-ipa-no-impersonation/pxf-site.xml
    # set service user to foobar
    xmlstarlet ed --inplace --pf --append '/configuration/property[last()]' --type elem -n property -v "" \
     --subnode '/configuration/property[last()]' --type elem -n name -v "pxf.service.user.name" \
     --subnode '/configuration/property[last()]' --type elem -n value -v "foobar" "${PXF_BASE}"/servers/hdfs-ipa-no-impersonation/pxf-site.xml

    rm -rf "${PXF_BASE}"/servers/hdfs-ipa-no-impersonation-no-svcuser
    cp -R "${PXF_BASE}"/servers/hdfs-ipa "${PXF_BASE}"/servers/hdfs-ipa-no-impersonation-no-svcuser
    sed -i '' -e 's|hdfs-ipa|hdfs-ipa-no-impersonation-no-svcuser|g' "${PXF_BASE}"/servers/hdfs-ipa-no-impersonation-no-svcuser/pxf-site.xml
    # set impersonation property to false for the PXF server
    xmlstarlet ed --inplace --pf --update "/configuration/property[name = 'pxf.service.user.impersonation']/value" -v false "${PXF_BASE}"/servers/hdfs-ipa-no-impersonation-no-svcuser/pxf-site.xml
}

# print instructions for the manual steps the user must perform
function print_user_instructions() {
    echo "Cluster $USER has been created, now do the following:"
    echo "1. --- copy the following to your /etc/hosts :"
    cat ipa_env_files/etc_hostfile
    echo "2. --- add the following to your /etc/krb5.conf :"
    cat << EOF
[libdefaults]
 default_realm = C.DATA-GPDB-UD-IPA.INTERNAL
 ticket_lifetime = 1d
 forwardable = true
 proxiable = true

[realms]
 C.DATA-GPDB-UD-IPA.INTERNAL = {
  kdc = ccp-$USER-ipa.c.data-gpdb-ud-ipa.internal
  admin_server = ccp-$USER-ipa.c.data-gpdb-ud-ipa.internal
 }

[domain_realm]
 .c.data-gpdb-ud-ipa.internal = C.DATA-GPDB-UD-IPA.INTERNAL
 c.data-gpdb-ud-ipa.internal = C.DATA-GPDB-UD-IPA.INTERNAL
EOF
    cd "$WORKING_DIR" || fail "cannot return to $WORKING_DIR"
}

# --- main script logic ---

script_command=$1
case ${script_command} in
    '--create')
        check_pre_requisites
        terraform_apply
        cleanup_existing_artifacts
        generate_keystores
        setup_ssh_config
        run_ansible_playbook
        create_environment_files
        setup_pxf_server
        print_user_instructions
        ;;
    '--destroy')
        check_pre_requisites
        terraform_destroy
        ;;
    *)
        printUsage
        exit 2
        ;;
esac

exit $?
