gcp_project = "gcp_project"
gcp_region = "us-central1-a"
hdfs_namenode_count = 1
hdfs_datanode_count = 3
gce_vm_instance_type = "n1-standard-4"
gce_vm_boot_disk_size = 40
gce_vm_os_family = "centos-7"
subnet = "default"

# Populated with var_files specified in the put of the terraform resource
# The environment name is used when naming create resources and should be
# something to help identify the resources as being from a local run of
# terraform.
env_name = "env_name"

# Concourse build metadata
# These variables are set as labels on the provisioned VMs and are used for
# automatically managing infrastructure that is being provisioned by CI
# pipelines. When running `terraform` locally, their values are unimportant.
build_id = "example_build_id"
build_name = "example_build_name"
build_job_name = "example_build_job_name"
build_pipeline_name = "example_build_pipeline_name"
build_team_name = "example_build_team_name"
atc_external_url = "example_atc_external_url"
ccp_reap_mins = 120
ccp_fail_status_behavior = "run"
