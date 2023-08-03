# Local Dataproc Cluster with Kerberos Authentication

This developer note will guide you through the process of creating a Google Cloud Dataproc cluster with Kerberos authentication enabled.

## Environment SetUp

1. Run the modified `dataproc-cluster.bash` script with following options to create a local Dataproc cluster with Kerberos authentication

    ```sh
    IMAGE_VERSION=1.5-debian10 ./dataproc-cluster.bash --create 'core:hadoop.security.auth_to_local=RULE:[1:$1] RULE:[2:$1] DEFAULT,hdfs:dfs.client.use.datanode.hostname=true'
    ```

    **NOTE:** After running the script, but before creating the PXF server config, replace `bradford-local-cluster-m` with `bradford-local-cluster-m.c.data-gpdb-ud.internal` for `hive.metastore.uris` in `dataproc_env_files/conf/hive-site.xml`

1. SSH into cluster code (e.g., `gcloud compute ssh bradford-local-cluster-m --zone=us-west1-c`) and created a PXF service principal named `${USER}`

    ```sh
    sudo kadmin.local -q "add_principal -nokey ${USER}"
    sudo kadmin.local -q "ktadd -k pxf.service.keytab ${USER}"
    sudo chown "${USER}:" ~/pxf.service.keytab
    chmod 0600 ~/pxf.service.keytab
    sudo addgroup "${USER}" hdfs
    sudo addgroup "${USER}" hadoop

    # verify the keytab
    klist -ekt pxf.service.keytab
    ```

1. Copy Kerberos files from the cluster to the local working directory

    ```sh
    gcloud compute scp bradford-local-cluster-m:~/pxf.service.keytab bradford-local-cluster-m:/etc/krb5.conf dataproc_env_files/
    cp -i dataproc_env_files/pxf.service.keytab "${PXF_BASE}/keytabs"
    ```

1. If `kinit` and/or `klist` are not found on your path, install
    * `krb5-user` on Debian-based distros
    * `krb5-workstation` and `krb5-libs` on RHEL7-based distros

1. Verify that Kerberos is working on your local machine

    ```sh
    export KRB5_CONFIG="${PWD}/dataproc_env_files/krb5.conf"
    kinit -kt dataproc_env_files/pxf.service.keytab "${USER}"
    klist
    export HADOOP_OPTS="-Djava.security.krb5.conf=${PWD}/dataproc_env_files/krb5.conf"

    #export HADOOP_HOME=<path/to/hadoop>
    #export HIVE_HOME=<path/to/hive>
    "${HADOOP_HOME}/bin/hdfs" dfs -ls /
    "${HIVE_HOME}/bin/beeline" -u 'jdbc:hive2://bradford-local-cluster-m.c.data-gpdb-ud.internal:10000/default;principal=hive/bradford-local-cluster-m.c.data-gpdb-ud.internal@C.DATA-GPDB-UD.INTERNAL'
    ```

    **NOTE:** Java 8 does not like/support the [directives `include` or `includedir`][0]; rather than attempt to automate editing the system's `/etc/krb5.conf` or provide manual steps for editing it (which would require also removing the config when destroying the cluster), this guide takes a more conservative approach of using an alternate location for the Kerberos config (e.g., `KRB5_CONFIG` and `-Djava.security.krb5.conf` above).

## PXF Setup

1. Edit `$PXF_BASE/servers/dataproc/pxf-site.xml`
    * Set `pxf.service.kerberos.principal` to `<username>@C.DATA-GPDB-UD.INTERNAL`

1. Edit `$PXF_BASE/conf/pxf-env.sh` and add `-Djava.security.krb5.conf=${PXF_BASE}/conf/krb5.conf` to `PXF_JVM_OPTS`

1. Copy `dataproc_env_files/krb5.conf` to `$PXF_BASE/conf/krb5.conf`

    ```sh
    cp dataproc_env_files/krb5.conf $PXF_BASE/conf/krb5.conf
    ```

1. (Re-)Start PXF

    ```sh
    pxf stop
    pxf start
    ```

## Hive Setup

1. SSH into cluster node (e.g., `bradford-local-cluster-m`), run any kinit and then connect to Hive

    ```sh
    gcloud compute ssh bradford-local-cluster-m --zone=us-west1
    kinit -kt pxf.service.keytab ${USER}
    klist
    hive
    ```

1. Create a table

    ```sql
    CREATE TABLE foo (col1 INTEGER, col2 STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';
    INSERT INTO foo VALUES
        (1, 'hive row 1'),
        (2, 'hive row 2'),
        (3, 'hive row 3'),
        (4, 'hive row 4'),
        (5, 'hive row 5'),
        (6, 'hive row 6'),
        (7, 'hive row 7'),
        (8, 'hive row 8'),
        (9, 'hive row 9'),
        (10, 'hive row 10');
    ```

1. Create a copy of the data in HDFS so that we are looking at a Hive Unmanaged Table

    ```sh
    hdfs dfs -cp /user/hive/warehouse/foo /tmp/
    ```

1. Create an external Hive table

    ```sql
    CREATE EXTERNAL TABLE foo_ext (col1 INTEGER, col2 STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    LOCATION 'hdfs:///tmp/foo';

    SELECT * FROM foo_ext;
    -- OK
    -- 1    hive row 1
    -- 2    hive row 2
    -- 3    hive row 3
    -- 4    hive row 4
    -- 5    hive row 5
    -- 6    hive row 6
    -- 7    hive row 7
    -- 8    hive row 8
    -- 9    hive row 9
    -- 10   hive row 10
    -- Time taken: 8.672 seconds, Fetched: 10 row(s)
    ```

## Greenplum Setup

1. Create a readable external table using the `hdfs:text` profile; the location is set to the HDFS directory in Dataproc that contains the Hive table's data

    ```sql
    CREATE READABLE EXTERNAL TABLE pxf_hdfs_foo_k8s_r(col1 int, col2 text)
    LOCATION ('pxf://tmp/foo?PROFILE=hdfs:text&SERVER=dataproc')
    FORMAT 'TEXT';

    SELECT * FROM pxf_hdfs_foo_k8s_r ORDER BY col1;
    --  col1 |    col2
    -- ------+-------------
    --     1 | hive row 1
    --     2 | hive row 2
    --     3 | hive row 3
    --     4 | hive row 4
    --     5 | hive row 5
    --     6 | hive row 6
    --     7 | hive row 7
    --     8 | hive row 8
    --     9 | hive row 9
    --    10 | hive row 10
    -- (10 rows)
    ```

## Clean-Up

1. Stop PXF, remove `-Djava.security.krb5.conf=${PXF_BASE}/conf/krb5.conf` from `PXF_JVM_OPTS` in `$PXF_BASE/conf/pxf-env.sh`, and delete `${PXF_BASE}/conf/krb5.conf` as well as `${PXF_BASE}/keytabs/pxf.service.keytab`
2. Run `./dataproc-cluster.bash --destroy`

<!-- link ids -->
[0]: https://linux.die.net/man/5/krb5.conf
