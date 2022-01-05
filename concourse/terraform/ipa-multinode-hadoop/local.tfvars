####################################################################################
# --- This file is used with terraform apply when running from a local machine --- #
####################################################################################

# --- The following variables are required and the values must be set by environment variables.

# GCP project where the instances will created, use variable TF_VAR_gcp_project to specify
#gcp_project = "gcp_project"

# The environment name is used when naming created resources and should help identify the user
# who ran the deployment. A convention is to use $USER as the value set in TF_VAR_env_name
#env_name = "env_name"

# --- Type of the environment, hardcoded to "local" to override the default of "ci" in variables.tf
env_type = "local"

# --- The following variables are defined in variables.tf with the given defaults, change if needed.

#gcp_region = "us-central1-a"
#hdfs_datanode_count = 3
#gce_vm_instance_type = "n1-standard-4"
#gce_vm_boot_disk_size = 40
#gce_vm_os_family = "centos-7"
#subnet = "dynamic"

# --- The following variables are used for Concourse build metadata and are set as labels on the provisioned VMs.
#     Their values are hardcoded as they are not important when running 'terraform' locally.

build_id = "0"
build_name = "local"
build_job_name = "local"
build_pipeline_name = "local"
build_team_name = "local"
atc_external_url = "none"

# CCP reaper integration would require tfstate to be pushed to an AWS bucket, it is not used for now.
# Developers will need to destroy their GCP clusters and infrastructure manually or by using a development script,
# refer to the "Cleanup" section of the dev/IPA.md file.
#ccp_reap_mins = 480
#ccp_fail_status_behavior = "run"
