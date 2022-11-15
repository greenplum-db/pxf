# Creating a multi-node Hadoop cluster with Kerberos provided by an IPA server

If you need to create a multinode Hadoop cluster secured by IPA in Google Cloud Platform you can run the
script [`ipa-cluster.bash` bash script](ipa-cluster.bash) that does the following:
* uses `terraform` templates from 
[concourse/terraform/ipa-multinode-hadoop/templates](../concourse/terraform/ipa-multinode-hadoop/templates) 
directory along with [local.tfvars](../concourse/terraform/ipa-multinode-hadoop/templates/local.tfvars) file to
spin up GCP instances for an IPA server, hadoop namenodes and datanodes.
* uses the [`ipa-multinode-hadoop` Ansible play](../concourse/ansible/ipa-multinode-hadoop) 
to configure and start the Hadoop cluster.
* generates TLS private keys, self-signed certificates and pkcs12 keystores for the Hadoop nodes.
* creates PXF configuration server under `$PXF_BASE` and deploys `*-site.xml` files and keytabs there

## Prerequisites

You must install the following software on your local development workstation:
* `terraform`
* `ansible-playbook`
* `jq`
* `xmlstarlet`

Setup the following environment variables:
```
export PXF_BASE=[path to your PXF_BASE] 
export TF_VAR_gcp_project=[name of your GCP project]
export ANSIBLE_VAR_ipa_password=[password for your IPA server that will be created]
export ANSIBLE_VAR_ssl_store_password=[password for your SSL store that will be created]
```

## Creating the cluster
Execute the following command to create a new cluster:
```
ipa-cluster.bash --create
```
The script will:
 * validate that the required software is present and the environment variables are set
 * generate necessary additional `terraform` artifacts in `concourse/terraform/ipa-multinode-hadoop/templates` directory
 * spin up GCP instances for the Hadoop cluster
 * apply ansible configuration steps to configure the Hadoop cluster
 * add a private key to access GCP VMs as `~/.ssh/[username]`
 * create SSH ~/.ssh/config.d config directory
 * add SSH `Include config.d/*` wild card include directive as first line in `~/.ssh/config` file
 * add SSH configurations to `~/.ssh/config.d/ipa_config` file
 * create a PXF configuration server `$PXF_BASE/servers/hdfs-ipa`
 * print out messages instructing the user to:
   * enable constrained delegation property in their `$PXF_BASE/servers/hdfs-ipa/pxf-site.xml`
   * update their `/etc/hosts` file
   * create or update their `/etc/krb5.conf` file
   
## Outcome
If the script executes successfully and you follow the post-execution steps, you will have:

1. a configured and started multi-node HA Hadoop cluster in GCP secured by an IPA server:
    * IPA server will run in `ccp-[your-user-id]-ipa` GCP VM, the realm is the same as your GCP project's DNS name suffix
    * 2 Hadoop namenodes: `ccp-[your-user-id]-nn01` (active) and `ccp-[your-user-id]-nn02` (standby)
    * 3 Hadoop datanodes: `ccp-[your-user-id]-dn01`, `ccp-[your-user-id]-dn02` and `ccp-[your-user-id]-dn03`  
    * your Hadoop nameservice set to `[your-user-id]` and `fs.defaultFS` set to `hdfs://[your-user-id]`
    * there is no Zookeeper installed, so a namenode failover must be triggered manually
    * there is no Hadoop proxy user setup, the only way to impersonate is by using Kerberos constrained delegation 
    * the user and Kerberos principal that has delegation privileges is called `porter@[REALM]`
    * users and Kerberos principals: `gpadmin`, `testuser`, `foobar`
    * Hadoop admin user `stout`
    
2. a PXF configuration server `$PXF_BASE/servers/hdfs-ipa` that you can use when creating PXF external tables
    * all config files will already be copied there pointing to the provisioned Hadoop cluster
    * the PXF principal will be set and its keytab `pxf.service.keytab` available in the server config directory

3. a GCP firewall rule allowing your IP address only to access the Hadoop services via their public IP addresses. 

You can create PXF external tables in the Greenplum database running locally on your workstation and use
the `hdfs-ipa` PXF configuration server to test against your new HA multi-node Hadoop cluster.

## Working with the cluster
To list HDFS directories of the new cluster accessible by PXF service user, run:
```
kinit -kt ${PXF_BASE}/servers/hdfs-ipa/pxf.service.keytab porter@[YOUR REALM]
hdfs --config ${PXF_BASE}/servers/hdfs-ipa dfs -ls /
```
To list HDFS directories of the new cluster accessible by Hadoop admin user, run:
```
kinit -kt ${PXF_BASE}/servers/hdfs-ipa/hadoop.user.keytab stout@[YOUR REALM]
hdfs --config ${PXF_BASE}/servers/hdfs-ipa dfs -ls /
```

## Cleanup

IMPORTANT: the cluster provisioned by the script is not yet integrated with the CPP reaper. 
This means you will need to manually issue a command that destroys all GCP artifacts created by the script.

Run the following command to destroy the cluster:
```
ipa-cluster.bash --destroy
```
This command invokes `terraform destroy` to cleanup all provisioned resources. 
Inspect the output and double check in GCP console that all the VM instances named like `ccp-[username]-*` are destroyed.

You might also want to manually delete your PXF configuration server `$PXF_BASE/servers/hdfs-ipa` 
and any modifications you have done to:
- `/etc/hosts` file
- `/etc/krb5.conf` file
- `~/.ssh/config` file
- remove `~/.ssh/ipa_[username]_rsa` and `~/.ssh/ipa_[username]_rsa.pub` files which contain keys to the GCP VM instances
that were just destroyed
