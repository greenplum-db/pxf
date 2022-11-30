#!/usr/bin/env bash

set -o errexit
# uncomment the following line for debugging
# set -o xtrace

# BEGIN USER CONFIGURATION
# This script uses the gcloud CLI to create and delete GCP resources in your
# default GCP project. If you want/need to create the resources in a specific
# project, export CLOUDSDK_CORE_PROJECT before running this script.
cluster_name="${USER}-local-cluster"
region="us-west1"
num_workers=2
network="default"

# valid choices for this can be found
# https://cloud.google.com/dataproc/docs/concepts/versioning/dataproc-version-clusters
# TODO: re-run with 1.5-debian10 to test Hadoop 2.10
image_version="2.0-debian10"
# END USER CONFIGURATION

function check_pre_requisites() {
    if ! type gcloud &>/dev/null; then
        echo 'gcloud is not found, did you install it (see https://cloud.google.com/sdk/docs/install) ?'
        exit 1
    fi

    if ! type xmlstarlet &>/dev/null; then
        echo 'xmlstarlet is not found, did you install it (e.g. "brew install xmlstarlet") ?'
        exit 1
    fi

    : "${PXF_HOME?"PXF_HOME is required. export PXF_HOME=[YOUR_PXF_INSTALL_LOCATION]"}"

    if [[ -d dataproc_env_files ]]; then
        echo "dataproc_env_files already exists; remove before re-running this script..."
        exit 1
    fi
}

function create_dataproc_cluster() {
    if gcloud dataproc clusters describe "${cluster_name}" --region="${region}" &>/dev/null; then
        echo "Cluster ${cluster_name} already exists; skipping create..."
        return 0
    fi

    local -a create_cmd
    create_cmd=(gcloud dataproc clusters create "${cluster_name}"
        --region="${region}"
        --master-machine-type=n1-standard-4
        --worker-machine-type=n1-standard-4
        --tags="${USER}-only"
        --num-workers="${num_workers}"
        --image-version="${image_version}"
        --network="${network}")

    if [[ -n $1 ]]; then
        create_cmd+=("$1")
    fi

    "${create_cmd[@]}"
}

function delete_dataproc_cluster() {
    if ! gcloud dataproc clusters describe "${cluster_name}" --region "${region}" &>/dev/null; then
        echo "Cluster ${cluster_name} does not exist, skipping delete..."
        return 0
    fi

    gcloud dataproc clusters delete "${cluster_name}" --region="${region}" --quiet
}

function create_firewall_rule() {
    local firewall_rule_name="${1}"

    if gcloud compute firewall-rules describe "${firewall_rule_name}" &>/dev/null; then
        echo "Firewall rule ${firewall_rule_name} already exists; skipping create..."
        return 0
    fi

    local local_external_ip
    local_external_ip="$(curl -s https://ipinfo.io/ip)"

    gcloud compute firewall-rules create "${firewall_rule_name}" \
        --description="Allow incoming HDFS traffic from ${USER}'s home office" \
        --network="${network}" \
        --allow=tcp:8020,tcp:9866,tcp:9870 \
        --direction=INGRESS \
        --target-tags="${USER}-only" \
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
    zoneUri="$(gcloud dataproc clusters describe "${cluster_name}" --region us-west1 --format='get(config.gceClusterConfig.zoneUri)')"
    local zone="${zoneUri##*/}"

    mkdir -p dataproc_env_files/conf
    printf "# BEGIN LOCAL DATAPROC SECTION\n" >>dataproc_env_files/etc_hostfile
    external_ip="$(gcloud compute instances describe "${cluster_name}-m" --zone="${zone}" --format='get(networkInterfaces[0].accessConfigs[0].natIP)')"
    printf "%s\t%s %s\n" "${external_ip}" "${cluster_name}-m" "${cluster_name}-m.c.data-gpdb-ud.internal" >>dataproc_env_files/etc_hostfile

    for ((i = 0; i < num_workers; i++)); do
        instance_name="${cluster_name}-w-${i}"
        external_ip="$(gcloud compute instances describe "${instance_name}" --zone="${zone}" --format='get(networkInterfaces[0].accessConfigs[0].natIP)')"
        printf "%s\t%s %s\n" "${external_ip}" "${instance_name}" "${instance_name}.c.data-gpdb-ud.internal" >>dataproc_env_files/etc_hostfile
    done
    printf "# END LOCAL DATAPROC SECTION\n" >>dataproc_env_files/etc_hostfile

    # TODO: copy Hive config and setup
    gcloud compute scp --zone="${zone}" "${cluster_name}-m:/etc/hadoop/conf/*-site.xml" dataproc_env_files/conf/
    # set Hadoop client to use hostnames for datanodes instead of IP addresses (which are internal in GCP network)
    xmlstarlet ed --inplace --pf --append '/configuration/property[last()]' --type elem -n property -v "" \
        --subnode '/configuration/property[last()]' --type elem -n name -v "dfs.client.use.datanode.hostname" \
        --subnode '/configuration/property[last()]' --type elem -n value -v "true" dataproc_env_files/conf/hdfs-site.xml

    cp "${PXF_HOME}/templates/pxf-site.xml" dataproc_env_files/conf
    # set impersonation property to false for the PXF server
    xmlstarlet ed --inplace --pf --update "/configuration/property[name = 'pxf.service.user.impersonation']/value" -v false dataproc_env_files/conf/pxf-site.xml
}

function delete_dataproc_env_files() {
    rm -rf dataproc_env_files
}

function print_user_instructions_for_create() {
    cat <<EOF
Cluster ${cluster_name} has been created, now do the following:
    1. Add the hostname to IP address mappings for the cluster to /etc/hosts:

        sudo tee -a /etc/hosts <dataproc_env_files/etc_hostfile

    2. Create a PXF server using the clusters config files, for example

        cp -a dataproc_env_files/conf \${PXF_BASE}/servers/dataproc
EOF
}

function print_user_instructions_for_delete() {
    cat <<EOF
Cluster ${cluster_name} has been deleted, now do the following:
    1. Remove the hostname to IP address mappings for the cluster in /etc/hosts

    2. Delete the PXF server using the clusters config files, for example

        rm -rf \${PXF_BASE}/servers/dataproc
EOF
}

function print_usage() {
    cat <<EOF
NAME
    dataproc-cluster.sh - manage a Google Cloud Dataproc cluster

SYNOPSIS
    dataproc-cluster.sh create [<optional-options-for-dataproc>]
    dataproc-cluster.sh delete

DESCRIPTION
    When creating the dataproc cluster, additional options can be passed in to
    customize the created cluster. For example:

        dataproc-cluster.sh create --properties=hdfs:dfs.namenode.fs-limits.min-block-size=1024

    would create a cluster with a customized hdfs-site.xml that contains the
    custom value for dfs.namenode.fs-limits.min-block-size.

EOF

}

# --- main script logic ---

script_command="$1"
custom_dataproc_properties="$2"
case "${script_command}" in
'create')
    check_pre_requisites
    create_dataproc_cluster "${custom_dataproc_properties}"
    create_firewall_rule "${cluster_name}-external-access"
    create_dataproc_env_files
    print_user_instructions_for_create
    ;;
'delete')
    delete_dataproc_env_files
    delete_firewall_rule "${cluster_name}-external-access"
    delete_dataproc_cluster
    print_user_instructions_for_delete
    ;;
*)
    print_usage
    exit 2
    ;;
esac
