provider "google" {
  project = var.gcp_project
  region = var.gcp_region
  zone = var.gcp_zone
}

resource "tls_private_key" "keypair_gen" {
  algorithm = "RSA"
  rsa_bits  = 4096
}

# The GCE metadata ssh key has strict format requirements of
#  <protocol> <key-blob> <username@example.com>
# This is tricky because the tls_private_key appends a newline to the key blob
# This variable gets around the trickyness in order to form a valid key
locals {
  ssh-key = "${replace(tls_private_key.keypair_gen.public_key_openssh, "\n", "")} pvtl-gp-ud@vmware.com"
}

resource "google_compute_instance" "ipa" {
  name          = "ccp-${var.env_name}-ipa"
  machine_type  = var.gce_vm_instance_type

  boot_disk {
    auto_delete = true
    initialize_params {
      size  = var.gce_vm_boot_disk_size
      image = var.gce_vm_os_family
    }
  }

  network_interface {
    subnetwork  = var.subnet
  }

  tags = ["tag-concourse-dynamic", "bosh-network", "outbound-through-nat"]

  metadata = {
    block-project-ssh-keys = true
    ssh-keys               = "ipa:${local.ssh-key}"
  }

  labels = {
    terraform                = true
    build_id                 = var.build_id
    concourse_team_name      = var.build_team_name
    host_name                = "ipa"
    ccp_fail_timestamp       = ""
    ccp_reap_mins            = var.ccp_reap_mins
    ccp_fail_status_behavior = var.ccp_fail_status_behavior
  }
}

resource "google_filestore_instance" "hadoop_shared_storage" {
  name = "ccp-${var.env_name}-shared-storage"
  zone = var.gcp_zone
  tier = "BASIC_HDD"

  file_shares {
    # minimum capacity for Basic HDD is 1 TiB
    # https://cloud.google.com/filestore/docs/service-tiers
    capacity_gb = 1024
    name        = "share1"
  }

  networks {
    network = var.network
    modes   = ["MODE_IPV4"]
  }
}

data "google_compute_default_service_account" "default" {
}

resource "google_compute_instance" "namenode" {
  # a maximum of two NameNodes may be configured per nameservice
  # https://hadoop.apache.org/docs/r2.10.1/hadoop-project-dist/hadoop-hdfs/HDFSHighAvailabilityWithNFS.html
  count         = 2
  name          = "ccp-${var.env_name}-nn${format("%02d", count.index+1)}"
  machine_type  = var.gce_vm_instance_type

  boot_disk {
    auto_delete   = true
    initialize_params {
      size  = var.gce_vm_boot_disk_size
      image = var.gce_vm_os_family
    }
  }

  network_interface {
    subnetwork  = var.subnet
  }

  tags = ["tag-concourse-dynamic", "bosh-network", "outbound-through-nat"]

  metadata = {
    block-project-ssh-keys = true
    ssh-keys               = "hdfs:${local.ssh-key}"
  }

  labels = {
    terraform                = true
    build_id                 = var.build_id
    concourse_team_name      = var.build_team_name
    host_name                = "nn${count.index+1}"
    ccp_fail_timestamp       = ""
    ccp_reap_mins            = var.ccp_reap_mins
    ccp_fail_status_behavior = var.ccp_fail_status_behavior
  }

  # this allows the VM to read objects from GCS without passing around a service account key
  service_account {
    email = data.google_compute_default_service_account.default.email
    scopes = ["storage-ro"]
  }
}

resource "tls_private_key" "namenode" {
  count       = 2
  algorithm   = "ECDSA"
  ecdsa_curve = "P256"
}

resource "tls_self_signed_cert" "namenode" {
  count           = 2
  key_algorithm   = "ECDSA"
  private_key_pem = tls_private_key.namenode[count.index].private_key_pem

  subject {
      common_name  = "${google_compute_instance.namenode[count.index].name}.c.${var.gcp_project}.internal"
      organization = "GPDB UD"
  }

  validity_period_hours = 2160

  allowed_uses = [
    "key_encipherment",
    "digital_signature",
    "server_auth",
  ]
}

resource "google_compute_instance" "datanode" {
  count         = var.hdfs_datanode_count
  name          = "ccp-${var.env_name}-dn${format("%02d", count.index+1)}"
  machine_type  = var.gce_vm_instance_type

  boot_disk {
    auto_delete   = true
    initialize_params {
      size  = var.gce_vm_boot_disk_size
      image = var.gce_vm_os_family
    }
  }

  network_interface {
    subnetwork  = var.subnet
  }

  tags = ["tag-concourse-dynamic", "bosh-network", "outbound-through-nat"]

  metadata = {
    block-project-ssh-keys = true
    ssh-keys               = "hdfs:${local.ssh-key}"
  }

  labels = {
    terraform                = true
    build_id                 = var.build_id
    concourse_team_name      = var.build_team_name
    host_name                = "nn${count.index+1}"
    ccp_fail_timestamp       = ""
    ccp_reap_mins            = var.ccp_reap_mins
    ccp_fail_status_behavior = var.ccp_fail_status_behavior
  }

  # this allows the VM to read objects from GCS without passing around a service account key
  service_account {
    email = data.google_compute_default_service_account.default.email
    scopes = ["storage-ro"]
  }
}

resource "tls_private_key" "datanode" {
  count       = var.hdfs_datanode_count
  algorithm   = "ECDSA"
  ecdsa_curve = "P256"
}

resource "tls_self_signed_cert" "datanode" {
  count           = var.hdfs_datanode_count
  key_algorithm   = "ECDSA"
  private_key_pem = tls_private_key.datanode[count.index].private_key_pem

  subject {
      common_name  = "${google_compute_instance.datanode[count.index].name}.c.${var.gcp_project}.internal"
      organization = "GPDB UD"
  }

  validity_period_hours = 2160

  allowed_uses = [
    "key_encipherment",
    "digital_signature",
    "server_auth",
  ]
}
