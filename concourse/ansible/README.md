# PXF CI Ansible

**Ansible plays for provisioning infrastructure in CI tasks**

## Multinode Hadoop with IPA provided Kerberos

The `ipa-multinode-hadoop` play launches a multinode Hadoop (currently only HDFS is running) cluster with Kerberos authentication backed by [FreeIPA][0].
This cluster can be used in the automation tests to provide coverage for PXF's implementation of constrained delegation.
The play assumes three (3) groups of VMs (see `example.inventory.ini`):

1. a single VM for running the IPA server
2. a single VM for running Hadoop NameNode
3. multiple VMs for running Hadoop DataNode

In addition to installing and setting up IPA and Hadoop, the play also creates two (2) users:

1. gpadmin

    This user represents the GPDB user that is running the automation test queries in GPDB and must exist in IPA in order to configure constrained delegation

2. stout

    This user is used as the PXF service Kerberos principal and is used by the automation framework for writing data files to HDFS.
    The username needs to be unique across all Hadoop clusters used by the automation framework since the principal name is used to identify to corresponding keytab to use (c.f. `BaseTestParent.trySecureLogin()`).
    The default username is `stout` but this can changed by either:

    - updating the default in `default.config.yml`
    - setting `ANSIBLE_VAR_hadoop_user` as a CI task parameter

In addition to the two users that are created, the play also creates one (1) group `hadoop` and adds `stout` to this group to make the user an HDFS super user (c.f. `dfs.permissions.supergroup`).

## Helpful Tips

The following are some tips that can be useful when debugging or iterating on Ansible plays

### Rerunning Play

If a task fails and you want to re-run after fixing the issue, you can skip ahead of the tasks that have already run successfully with `--start-at-task`.
The argument to this option is the name of the task as it appears in the `name` field of the task.

```bash
ansible-playbook --start-at-task='<name-of-task>' main.yml

# example
ansible-playbook --start-at-task='create gpadmin user' main.yml
```

### Debugging Failed Task

Most of the time when a task fails, the error message is enough to identify the issue and fix.
However, there are times when it may be necessary to examine the code that is being run by Ansible to identify the issue.
The first step is setting `ANSIBLE_KEEP_REMOTE_FILES=1` and re-running the failing task.
This will leave a copy of the code on the remote host that can be used to continue troubleshooting.
After running the failed task with `ANSIBLE_KEEP_REMOTE_FILES`, log in to the remote host via SSH and examine `~/.ansible/tmp`

```console
$ tree .ansible/tmp/
.ansible/tmp/
├── ansible-tmp-1631748671.2931817-1912435-278443088456975
│   └── AnsiballZ_setup.py
└── ansible-tmp-1631748673.641966-1912492-268641021129854
    └── AnsiballZ_gcp_storage_object.py
```

The example above contains two (2) Ansible tasks

- setup
- gcp_storage_object

Look for the one that matches the failed task.
Running the corresponding python script with the argument `explode` will unpack the code inside its corresponding directory.
The code can be inspected and edited and then run (with the changes) by running the python script with the argument `execute`.

<!-- References -->
[0]: https://www.freeipa.org/page/Main_Page
