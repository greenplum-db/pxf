variable "gcp_project" {
  type        = string
  description = "name of Google Cloud Platform project to create resources in"
}

variable "gcp_region" {
  type        = string
  description = "name of Google Compute Engine region to create resources in"
  default     = "us-central1"
}

variable "gcp_zone" {
  type        = string
  description = "name of Google Compute Enginer zone to create resources in"
  default     = "us-central1-a"
}

variable "hdfs_datanode_count" {
  type        = number
  description = "number of HDFS DataNode VMs to create"
  default     = 3
}

variable "gce_vm_instance_type" {
  type        = string
  description = "machine type to create"
  default     = "n1-standard-4"
}

variable "gce_vm_boot_disk_size" {
  type        = number
  description = "size of the image in gigabytes"
  default     = 40
}

variable "gce_vm_os_family" {
  type        = string
  description = "family name of the image from which to initialize the boot disk"
  default     = "centos-7"
}

variable "network" {
  type        = string
  description = "name of VPC network to provision VMs in"
  default     = "bosh-network"
}

variable "subnet" {
  type        = string
  description = "name of subnet to provision VMs in"
  default     = "dynamic"
}

variable "env_type" {
  type        = string
  description = "type of execution environment, can be ci or local only, used in the path for the output templates"
  default     = "ci"
}

# Populated with var_files specified in the put of the terraform resource
variable "env_name" {
  type        = string
  description = "environment name passed from Concourse resource"
}

# Concourse build metadata
variable "build_id" {
  type        = string
  description = "internal identifier for the build"
}

variable "build_name" {
  type        = string
  description = "build number within the build's job"
}

variable "build_job_name" {
  type        = string
  description = "name of the build's job"
}

variable "build_pipeline_name" {
  type        = string
  description = "name of the pipeline that the build's job lives in"
}

variable "build_team_name" {
  type        = string
  description = "team that the build belongs to"
}

variable "atc_external_url" {
  type        = string
  description = "public URL for your ATC"
}

variable "ccp_reap_mins" {
  type        = number
  description = "number of minutes to wait before cleaning up VMs from failed CI jobs"
  default     = 480
}

variable "ccp_fail_status_behavior" {
  type        = string
  description = "default CCP cluster behavior on failure"
  default     = "run"

}
