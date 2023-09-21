# Local Dataproc Cluster with Kerberos Authentication

The is instructions on how to connect your local machine to a Google Cloud Dataproc cluster with Kerberos authentication enabled.
It runs through a simple flow with HDFS and Hive to check that you can connect locally to the kerberized cluster.

## Environment SetUp

1. Run the `dataproc-cluster.bash` script with following options to create a Dataproc cluster with Kerberos authentication in GCP

    ```sh
    KERBERIZED=true ./dataproc-cluster.bash --create
    ```

1. If `kinit` and/or `klist` are not found on your path, install
    * `krb5-user` on Debian-based distros
    * `krb5-workstation` and `krb5-libs` on RHEL7-based distros
    * `brew install krb5` for MacOS

1. Verify that Kerberos is working on your local machine

    ```sh
    export KRB5_CONFIG="${PWD}/dataproc_env_files/krb5.conf"
    kinit -kt dataproc_env_files/pxf.service.keytab "${USER}"
    klist
    export HADOOP_OPTS="-Djava.security.krb5.conf=${PWD}/dataproc_env_files/krb5.conf"

    #export HADOOP_HOME=<path/to/hadoop>
    #export HIVE_HOME=<path/to/hive>
    "${HADOOP_HOME}/bin/hdfs" dfs -ls /
    "${HIVE_HOME}/bin/beeline" -u "jdbc:hive2://${DATAPROC_CLUSTER_NAME}-m.c.data-gpdb-ud.internal:10000/default;principal=hive/${DATAPROC_CLUSTER_NAME}-m.c.data-gpdb-ud.internal@C.DATA-GPDB-UD.INTERNAL"
    ```

    **NOTE:** Java 8 does not like/support the [directives `include` or `includedir`][0]; rather than attempt to automate editing the system's `/etc/krb5.conf` or provide manual steps for editing it (which would require also removing the config when destroying the cluster), this guide takes a more conservative approach of using an alternate location for the Kerberos config (e.g., `KRB5_CONFIG` and `-Djava.security.krb5.conf` above).

## Hive Setup

1. SSH into cluster node (e.g., `${DATAPROC_CLUSTER_NAME}-m`), run any kinit and then connect to Hive

    ```sh
    gcloud compute ssh ${DATAPROC_CLUSTER_NAME}-m --zone=${ZONE}
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

<!-- link ids -->
[0]: https://linux.die.net/man/5/krb5.conf
