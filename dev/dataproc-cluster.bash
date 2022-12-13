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
        --network="${NETWORK}"
        --enable-kerberos)

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
    #
    # Kerberos Service  Port
    # KDC               88
    # Admin server      750
    gcloud compute firewall-rules create "${firewall_rule_name}" \
        --description="Allow incoming HDFS traffic from ${USER}'s home office" \
        --network="${NETWORK}" \
        --allow=tcp:8020,tcp:8088,tcp:9083,tcp:9866,tcp:9870,tcp:10000,udp:88,udp:750 \
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
    local zone="${zoneUri##*/}"

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

    echo "Cluster config for ${CLUSTER_NAME} has been written in dataproc_env_files"
}

function delete_dataproc_env_files() {
    rm -rf dataproc_env_files
}

function prompt_for_confirmation() {
    while true; do
        read -r -p "Destroy Dataproc cluster and firewall rule (y/n)? " yn
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

    1. Add the hostname to IP address mappings for the cluster to /etc/hosts:

        sudo tee -a /etc/hosts <dataproc_env_files/etc_hostfile

    2. Create a PXF server using the clusters config files, for example

        cp -a dataproc_env_files/conf \${PXF_BASE}/servers/dataproc

    3. (Optional) Configure singlecluster-HDP3 CLI to connect to the cluster

        export HADOOP_CONF_DIR="${PWD}/dataproc_env_files/conf"
        export HIVE_CONF_DIR="${PWD}/dataproc_env_files/conf"
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

        dataproc-cluster.bash create hdfs:dfs.namenode.fs-limits.min-block-size=1024

    would create a cluster with a customized hdfs-site.xml that contains the
    custom value for dfs.namenode.fs-limits.min-block-size. Multiple
    properties can be specified by separating them with a comma. For a more
    detailed description of the format, See '--properties' in the man page
    for 'gcloud dataproc clusters create'.

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
    print_user_instructions_for_create
    ;;
'--destroy')
    [[ -n ${NON_INTERACTIVE} ]] || prompt_for_confirmation
    delete_dataproc_env_files
    delete_firewall_rule "${CLUSTER_NAME}-external-access"
    delete_dataproc_cluster
    print_user_instructions_for_delete
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
