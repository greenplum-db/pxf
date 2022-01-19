# PXF CI Terraform

**Terraform modules for provisioning infrastructure in CI tasks**

## Multinode Hadoop with IPA provided Kerberos

The `ipa-multinode-hadoop` module provisions Google Compute Engine (GCE) VMs for running multinode Hadoop (currently only HDFS is running) cluster with Kerberos authentication backed by [FreeIPA][0].
After creating the VMs with terraform, the [`ipa-multinode-hadoop` Ansible play](../ansible/ipa-multinode-hadoop) can be used to configure and start the Hadoop cluster.
In addition to creating the VMs, the module will also generate TLS private keys and self-signed certificates for the Hadoop nodes.
There are several variables for customizing the module, see [`./variables.tf`](./ipa-multinode-hadoop/variables.tf) for a list of all variables and their description.

## Helpful Tips

The following are some tips that can be useful when debugging or iterating on Terraform modules

### Running locally

These Terraform modules are structured with running in Concourse CI in mind; that said, they can be run locally.
The first step is to run

```bash
terraform init
```

All variables without default values will need to be provided.
This can be done in one of several different ways:

1. Terraform will prompted for all undefined variables when running a plan or an apply step

2. Variables can be given on the command line

    ```bash
    terraform plan -var 'foo=bar'
    ```

3. Variables can be given in a file; if the file `terraform.tfvars` or `.auto.tfvars` are present, they will be automatically loaded

    ```bash
    terraform plan -var-file=foo
    ```

    An example variables file can be found in the module directory.

4. Variables can be specified with environment variables

    ```bash
    export TF_VAR_gcp_project="<gcp-project-id>"
    ```
There is an [`ipa-cluster.bash` script](../../dev/ipa-cluster.bash) that automates provisioning of the IPA Hadoop
cluster via `terraform` and setting it up using `ansible` when running from a local development workstation.
It is used by developers to quickly setup a testing environment.
Please refer to the [`IPA.md` document](../../dev/IPA.md) for the instructions on how to run it.
