#!/usr/bin/env bash

set -o errexit
# uncomment the following line for debugging
# set -o xtrace

# This script uses the gcloud CLI to create and delete GCP resources in your
# default GCP project. If you want/need to create the resources in a specific
# project, export CLOUDSDK_CORE_PROJECT before running this script.
CLUSTER_USER="${CLUSTER_USER:-${USER}}"
CLUSTER_NAME="${CLUSTER_NAME:-${CLUSTER_USER}-local-cluster}"
REGION="${REGION:-us-west1}"
NUM_WORKERS="${NUM_WORKERS:-2}"
NETWORK="${NETWORK:-default}"
# valid choices for this can be found
# https://cloud.google.com/dataproc/docs/concepts/versioning/dataproc-version-clusters
IMAGE_VERSION="${IMAGE_VERSION:-2.0-debian10}"
KERBERIZED="${KERBERIZED:-false}"

function check_pre_requisites() {
    if ! type gcloud &>/dev/null; then
        >&2 echo 'gcloud is not found, did you install and configure it?'
        >&2 echo 'see https://cloud.google.com/sdk/docs/install and https://cloud.google.com/sdk/docs/initializing'
        exit 1
    fi

    if ! type xmlstarlet &>/dev/null; then
        >&2 echo 'xmlstarlet is not found, did you install it (e.g. "brew install xmlstarlet") ?'
        exit 1
    fi

    : "${PXF_HOME?"PXF_HOME is required. export PXF_HOME=[YOUR_PXF_INSTALL_LOCATION]"}"

    if [[ -d dataproc_env_files ]]; then
        >&2 echo "dataproc_env_files already exists; remove before re-running this script..."
        exit 1
    fi
}

function create_dataproc_cluster() {
    local extra_hadoop_config="${1}"

    if gcloud dataproc clusters describe "${CLUSTER_NAME}" --region="${REGION}" &>/dev/null; then
        echo "Cluster ${CLUSTER_NAME} already exists; skipping create..."
        return 0
    fi

    local -a create_cmd
    create_cmd=(gcloud dataproc clusters create "${CLUSTER_NAME}"
        --region="${REGION}"
        --master-machine-type=n1-standard-4
        --worker-machine-type=n1-standard-4
        --tags="${CLUSTER_USER}-only"
        --num-workers="${NUM_WORKERS}"
        --image-version="${IMAGE_VERSION}"
        --network="${NETWORK}")

    if [[ "${KERBERIZED}" == true ]]; then
      echo "Enabling kerberos..."
      create_cmd+=("--enable-kerberos")

      echo "Adding cluster security rules..."
      extra_hadoop_config+='core:hadoop.security.auth_to_local=RULE:[1:$1] RULE:[2:$1] DEFAULT,hdfs:dfs.client.use.datanode.hostname=true'
      echo "New config is: " ${extra_hadoop_config}
    fi

    if [[ -n $1 ]]; then
        create_cmd+=("--properties=${extra_hadoop_config}")
    fi

    "${create_cmd[@]}"

    echo "Cluster ${CLUSTER_NAME} has been created"
}

function delete_dataproc_cluster() {
    if ! gcloud dataproc clusters describe "${CLUSTER_NAME}" --region "${REGION}" &>/dev/null; then
        echo "Cluster ${CLUSTER_NAME} does not exist, skipping delete..."
        return 0
    fi

    gcloud dataproc clusters delete "${CLUSTER_NAME}" --region="${REGION}" --quiet

    echo "Cluster ${CLUSTER_NAME} has been deleted"
}

function create_firewall_rule() {
    local firewall_rule_name="${1}"

    if gcloud compute firewall-rules describe "${firewall_rule_name}" &>/dev/null; then
        echo "Firewall rule ${firewall_rule_name} already exists; skipping create..."
        return 0
    fi

    local local_external_ip
    local_external_ip="$(curl -s https://ipinfo.io/ip)"

    # Web UI                Port URL
    # YARN ResourceManager  8088 http://${CLUSTER_NAME}-m:8088
    # HDFS NameNode         9870 http://${CLUSTER_NAME}-m:9870
    #
    # https://cloud.google.com/dataproc/docs/concepts/accessing/cluster-web-interfaces
    local allow_list="tcp:8020,tcp:8088,tcp:9083,tcp:9866,tcp:9870,tcp:10000"
    if [[ "${KERBERIZED}" == true ]]; then
    # Kerberos Service  Port
    # KDC               88
    # Admin server      750
      echo "Adding KDC and admin server ports to allow list..."
      allow_list+=",udp:88,udp:750"
    fi

    gcloud compute firewall-rules create "${firewall_rule_name}" \
        --description="Allow incoming HDFS traffic from ${USER}'s home office" \
        --network="${NETWORK}" \
        --allow="${allow_list}" \
        --direction=INGRESS \
        --target-tags="${CLUSTER_USER}-only" \
        --source-ranges="${local_external_ip}/32"
}

function delete_firewall_rule() {
    local firewall_rule_name="${1}"

    if ! gcloud compute firewall-rules describe "${firewall_rule_name}" &>/dev/null; then
        echo "Firewall rule ${firewall_rule_name} does not exist; skipping delete..."
        return 0
    fi

    gcloud compute firewall-rules delete "${firewall_rule_name}" --quiet
}

function create_dataproc_env_files() {
    local zone=$(get_zone)

    mkdir -p dataproc_env_files/conf
    printf "# BEGIN LOCAL DATAPROC SECTION\n" >dataproc_env_files/etc_hostfile
    external_ip="$(gcloud compute instances describe "${CLUSTER_NAME}-m" --zone="${zone}" --format='get(networkInterfaces[0].accessConfigs[0].natIP)')"
    printf "%s\t%s %s\n" "${external_ip}" "${CLUSTER_NAME}-m" "${CLUSTER_NAME}-m.c.data-gpdb-ud.internal" >>dataproc_env_files/etc_hostfile

    for ((i = 0; i < NUM_WORKERS; i++)); do
        instance_name="${CLUSTER_NAME}-w-${i}"
        external_ip="$(gcloud compute instances describe "${instance_name}" --zone="${zone}" --format='get(networkInterfaces[0].accessConfigs[0].natIP)')"
        printf "%s\t%s %s\n" "${external_ip}" "${instance_name}" "${instance_name}.c.data-gpdb-ud.internal" >>dataproc_env_files/etc_hostfile
    done
    printf "# END LOCAL DATAPROC SECTION\n" >>dataproc_env_files/etc_hostfile

    gcloud compute scp \
        --zone="${zone}" \
        "${CLUSTER_NAME}-m:/etc/hadoop/conf/*-site.xml" \
        "${CLUSTER_NAME}-m:/etc/hive/conf/*-site.xml" \
        dataproc_env_files/conf/

    # set Hadoop client to use hostnames for datanodes instead of IP addresses (which are internal in GCP network)
    xmlstarlet ed --inplace --pf --append '/configuration/property[last()]' --type elem -n property -v "" \
        --subnode '/configuration/property[last()]' --type elem -n name -v "dfs.client.use.datanode.hostname" \
        --subnode '/configuration/property[last()]' --type elem -n value -v "true" dataproc_env_files/conf/hdfs-site.xml

    cp "${PXF_HOME}/templates/pxf-site.xml" dataproc_env_files/conf
    # set impersonation property to false for the PXF server
    xmlstarlet ed --inplace --pf --update "/configuration/property[name = 'pxf.service.user.impersonation']/value" -v false dataproc_env_files/conf/pxf-site.xml

    if [[ "${KERBERIZED}" == true ]]; then
      echo "Making changes to hive-site to update hive.metastore.uris from ${CLUSTER_NAME}-m to ${CLUSTER_NAME}-m.c.data-gpdb-ud.internal..."
      # set Hive metastore uris to full name
      xmlstarlet ed --inplace --pf --update "/configuration/property[name = 'hive.metastore.uris']/value" -x "concat(substring-before(., '${CLUSTER_NAME}-m'), '${CLUSTER_NAME}-m.c.data-gpdb-ud.internal', substring-after(., '${CLUSTER_NAME}-m'))" dataproc_env_files/conf/hive-site.xml
    fi

    echo "Cluster config for ${CLUSTER_NAME} has been written in dataproc_env_files"
}

function delete_dataproc_env_files() {
    rm -rf dataproc_env_files
}

function create_pxf_service_principal() {
    cat <<\EOF >create_service_principal.sh
#!/bin/sh

sudo kadmin.local -q "add_principal -nokey ${USER}"
sudo kadmin.local -q "ktadd -k pxf.service.keytab ${USER}"
sudo chown "${USER}:" ~/pxf.service.keytab
chmod 0600 ~/pxf.service.keytab
sudo addgroup "${USER}" hdfs
sudo addgroup "${USER}" hadoop

# verify the keytab
klist -ekt pxf.service.keytab
EOF

    local zone=$(get_zone)
    gcloud compute scp --zone="${zone}" create_service_principal.sh "${CLUSTER_NAME}-m:~/"
    echo "Creating PXF service principal for ${CLUSTER_USER}..."
    gcloud compute ssh "${CLUSTER_NAME}-m" --zone="${zone}" \
        --command 'chmod +x create_service_principal.sh && ./create_service_principal.sh'
}

function retrieve_keytab_and_krb5_conf() {
    local zone=$(get_zone)

    echo "Copying down keytab and krb5.conf files into dataproc_env_files..."
    gcloud compute scp \
        --zone="${zone}" \
        "${CLUSTER_NAME}-m:~/pxf.service.keytab" \
        "${CLUSTER_NAME}-m:/etc/krb5.conf" \
        dataproc_env_files/
}

function get_zone() {

    local zoneUri
    zoneUri="$(gcloud dataproc clusters describe "${CLUSTER_NAME}" --region="${REGION}" --format='get(config.gceClusterConfig.zoneUri)')"
    # the zoneUri field may contain a
    #
    #   * full URL    (https://www.googleapis.com/compute/v1/projects/[projectId]/zones/[zone])
    #   * partial URI (projects/[projectId]/zones/[zone])
    #   * short name  ([zone])
    #
    # <https://cloud.google.com/dataproc/docs/reference/rest/v1/ClusterConfig#gceclusterconfig>
    #
    # we need the just the short name when running
    #
    #     gcloud compute instances describe
    #
    # use bash parameter expansion to remove the longest matching prefix
    # (everything up to the last forward-slash)
    echo "${zoneUri##*/}"
}

function prompt_for_confirmation() {
    while true; do
        read -r -p "Destroy Dataproc cluster ${CLUSTER_NAME} and its related firewall rule (y/n)? " yn
        case $yn in
        [Yy]*) break ;;
        [Nn]*) exit ;;
        *) echo "Please answer yes or no." ;;
        esac
    done
}

function print_user_instructions_for_create() {
    cat <<EOF
Now do the following:

    1. Add the hostname to IP address mappings for the cluster to /etc/hosts

        sudo tee -a /etc/hosts <dataproc_env_files/etc_hostfile

    2. Create a PXF server using the clusters config files, for example

        cp -a dataproc_env_files/conf \${PXF_BASE}/servers/dataproc

    3. (Optional) Configure singlecluster-HDP3 CLI to connect to the cluster

        export HADOOP_CONF_DIR="${PWD}/dataproc_env_files/conf"
        export HIVE_CONF_DIR="${PWD}/dataproc_env_files/conf"

    4. (Optional) Export the cluster name for ease of use

        export DATAPROC_CLUSTER_NAME=${CLUSTER_NAME}
EOF
}

function print_user_instructions_for_delete() {
    cat <<EOF
Now do the following:

    1. Remove the hostname to IP address mappings for the cluster in /etc/hosts

    2. Delete the PXF server using the clusters config files, for example

        rm -rf \${PXF_BASE}/servers/dataproc
EOF
}

function print_user_instructions_for_kerberos_create() {
    cat <<EOF

This cluster has Kerberos Authentication enabled, please check the Dataproc-with-Kerberos README to finish
setting up the cluster and your local environment:

    1. Copy the krb5.conf file and keytab files into \$PXF_BASE, for example

        cp -a dataproc_env_files/krb5.conf \${PXF_BASE}/conf/krb5.conf
        cp -i dataproc_env_files/pxf.service.keytab \${PXF_BASE}/keytabs

    2. Update the \$PXF_BASE/conf/pxf-env.sh file and add the following to \`PXF_JVM_OPTS\`

        -Djava.security.krb5.conf=${PXF_BASE}/conf/krb5.conf

    3. Edit \$PXF_BASE/servers/dataproc/pxf-site.xml and set \`pxf.service.kerberos.principal\` to \`<username>@C.DATA-GPDB-UD.INTERNAL\`

        xmlstarlet ed --inplace --pf --update "/configuration/property[name = 'pxf.service.kerberos.principal']/value" -v "${USER}@C.DATA-GPDB-UD.INTERNAL" \$PXF_BASE/servers/dataproc/pxf-site.xml

    4. (Re-)Start PXF

        pxf start
EOF
}

function print_user_instructions_for_kerberos_delete() {
    cat <<EOF
PXF also needs to remove Kerberos references that were used for this cluster, please do the following:

    1. Stop PXF

        pxf stop

    2. Remove the \'-Djava.security.krb5.conf=${PXF_BASE}/conf/krb5.conf\' option from \`PXF_JVM_OPTS\` in the \$PXF_BASE/conf/pxf-env.sh file

    3. Delete the krb5.conf and keytab files in \$PXF_BASE, for example

        rm \${PXF_BASE}/conf/krb5.conf \${PXF_BASE}/keytabs/pxf.service.keytab

    4. Delete the create_service_principal.sh file in the local directory

        rm create_service_principal.sh

EOF
}

function print_usage() {
    cat <<EOF
NAME
    dataproc-cluster.bash - manage a Google Cloud Dataproc cluster

SYNOPSIS
    dataproc-cluster.bash --create [<optional-extra-hadoop-config>]
    dataproc-cluster.bash --destroy
    dataproc-cluster.bash --update_env_files

DESCRIPTION
    When creating the dataproc cluster, additional options can be passed in to
    customize the created cluster. For example:

        dataproc-cluster.bash --create hdfs:dfs.namenode.fs-limits.min-block-size=1024

    would create a cluster with a customized hdfs-site.xml that contains the
    custom value for dfs.namenode.fs-limits.min-block-size. Multiple
    properties can be specified by separating them with a comma. For a more
    detailed description of the format, See '--properties' in the man page
    for 'gcloud dataproc clusters create'.

    To create a kerberized dataproc cluster, users can set an environment variable `KERBERIZED`
    as well as some extra hadoop configurations. For example:

            KERBERIZED=true dataproc-cluster.bash --create 'core:hadoop.security.auth_to_local=RULE:[1:$1] RULE:[2:$1] DEFAULT,hdfs:dfs.client.use.datanode.hostname=true'

    would create a kerberized cluster with authentication rules and use hostnames on the datanodes.
EOF

}

# --- main script logic ---

script_command="$1"
extra_hadoop_config="$2"
case "${script_command}" in
'--create')
    check_pre_requisites
    create_dataproc_cluster "${extra_hadoop_config}"
    create_firewall_rule "${CLUSTER_NAME}-external-access"
    create_dataproc_env_files
    if [[ "${KERBERIZED}" == true ]]; then
      create_pxf_service_principal
      retrieve_keytab_and_krb5_conf
    fi
    print_user_instructions_for_create
    if [[ "${KERBERIZED}" == true ]]; then
      print_user_instructions_for_kerberos_create
    fi
    ;;
'--destroy')
    [[ -n ${NON_INTERACTIVE} ]] || prompt_for_confirmation
    delete_dataproc_env_files
    delete_firewall_rule "${CLUSTER_NAME}-external-access"
    delete_dataproc_cluster
    print_user_instructions_for_delete
    if [[ "${KERBERIZED}" == true ]]; then
      print_user_instructions_for_kerberos_delete
    fi
    ;;
'--update_env_files')
    create_dataproc_env_files
    print_user_instructions_for_create
    ;;
*)
    print_usage
    exit 2
    ;;
esac
