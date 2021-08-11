Singlecluster-HDP3
==================

Singlecluster-HDP3 is a self-contained, easy to deploy distribution of HDP3 (3.1.4.0-315)
It contains the following versions:

- Hadoop 3.1.1
- Hive 3.1.0
- Zookeeper 3.4.6
- HBase 2.0.2
- Tez 0.9.1

This version of Single cluster requires users to make some manual changes to the configuration files once the tarball has been unpacked (see Initialization steps below).

Requirements
------------

Singlecluster-HDP3 requires Java 8 since Hive 3.1 does not support Java 11 yet; see [HIVE-22415](https://issues.apache.org/jira/browse/HIVE-22415) for more details.

Initialization
--------------

1. Make sure **all** running instances of other singlecluster processes are stopped.

2. Pull down the singlecluster-HDP3 tarball from GCP and untar:

    ```sh
    mv singlecluster-HDP3.tar.gz ~/workspace
    cd ~/workspace
    mkdir singlecluster-HDP3
    tar -xf singlecluster-HDP3.tar.gz --strip-components=1 --directory=singlecluster-HDP3
    cd singlecluster-HDP3
    export GPHD_ROOT="${PWD}"
    ```

3. Adjust the configuration for Hadoop 3 (the following steps are based on the function `adjust_for_hadoop3` in `pxf_common.bash`)

      1. In `${GPHD_ROOT}/hive/conf/hive-env.sh`, remove `-hiveconf hive.log.dir=$LOGS_ROOT` from the `HIVE_OPTS` and `HIVE_SERVER_OPTS` exports: 

          ```sh
           sed -i -e 's/-hiveconf hive.log.dir=$LOGS_ROOT//' singlecluster-HDP3/hive/conf/hive-env.sh
          ```

      2. Update the `hive.execution.engine` property to `tez` in `${GPHD_ROOT}/hive/conf/hive-site.xml`:

          ```sh
           sed -e '/hive.execution.engine/{n;s/>.*</>tez</}' singlecluster-HDP3/hive/conf/hive-site.xml
           ```
      
      3. Add the following properties to `${GPHD_ROOT}/hive/conf/hive-site.xml`: 

          ```xml
           <property>
              <name>hive.tez.container.size</name>
              <value>2048</value>
           </property>
           <property>
              <name>datanucleus.schema.autoCreateAll</name>
              <value>True</value>
           </property>
           <property>
              <name>metastore.metastore.event.db.notification.api.auth</name>
              <value>false</value>
           </property>
          ```
      
      4. Add the following property to `"${GPHD_ROOT}/tez/conf/tez-site.xml`:

          ```xml
           <property>
             <name>tez.use.cluster.hadoop-libs</name>
             <value>true</value>
           </property>
          ```
      
      5. Replace `HADOOP_CONF` with `HADOOP_CONF_DIR` and `HADOOP_ROOT` with `HADOOP_HOME` in `${GPHD_ROOT}/hadoop/etc/hadoop/yarn-site.xml`:

          ```sh
          sed -i.bak -e 's|HADOOP_CONF|HADOOP_CONF_DIR|g' \
               -e 's|HADOOP_ROOT|HADOOP_HOME|g' "${GPHD_ROOT}/hadoop/etc/hadoop/yarn-site.xml"
          ```

      6. Replace `HADOOP_NAMENODE_OPTS` with `HDFS_NAMENODE_OPTS` in `${GPHD_ROOT}/hadoop/etc/hadoop/hadoop-env.sh`:

          ```sh
          sed -i.bak -e 's/HADOOP_NAMENODE_OPTS/HDFS_NAMENODE_OPTS/g' "${GPHD_ROOT}/hadoop/etc/hadoop/hadoop-env.sh"
          ```

      7. Replace `HADOOP_DATANODE_OPTS` with `HDFS_DATANODE_OPTS` in `${GPHD_ROOT}/bin/hadoop-datanode.sh`:

          ```sh
          sed -i.bak -e 's/HADOOP_DATANODE_OPTS/HDFS_DATANODE_OPTS/g' "${GPHD_ROOT}/bin/hadoop-datanode.sh"
          ```

4. Initialize an instance

    ```sh
    ${GPHD_ROOT}/bin/init-gphd.sh
    ```

5. Add the following to your environment

    ```sh
    export HADOOP_ROOT=$GPHD_ROOT/hadoop
    export HBASE_ROOT=$GPHD_ROOT/hbase
    export HIVE_ROOT=$GPHD_ROOT/hive
    export ZOOKEEPER_ROOT=$GPHD_ROOT/zookeeper
    export PATH=$PATH:$GPHD_ROOT/bin:$HADOOP_ROOT/bin:$HBASE_ROOT/bin:$HIVE_ROOT/bin:$ZOOKEEPER_ROOT/bin
    ```

Usage
-----

- Start all Hadoop services
  - `$GPHD_ROOT/bin/start-gphd.sh`
- Start HDFS only
  - `$GPHD_ROOT/bin/start-hdfs.sh`
- Start PXF only (Install pxf first to make this work. [See Install PXF session here](https://cwiki.apache.org/confluence/display/HAWQ/PXF+Build+and+Install))
  - `$GPHD_ROOT/bin/start-pxf.sh`
- Start HBase only (requires hdfs and zookeeper)
  - `$GPHD_ROOT/bin/start-hbase.sh`
- Start ZooKeeper only
  - `$GPHD_ROOT/bin/start-zookeeper.sh`
- Start YARN only
  - `$GPHD_ROOT/bin/start-yarn.sh`
- Start Hive (MetaStore)
  - `$GPHD_ROOT/bin/start-hive.sh`
- Stop all PHD services
  - `$GPHD_ROOT/bin/stop-gphd.sh`
- Stop an individual component
  - `$GPHD_ROOT/bin/stop-[hdfs|pxf|hbase|zookeeper|yarn|hive].sh`
- Start/stop HiveServer2
  - `$GPHD_ROOT/bin/hive-service.sh hiveserver2 start`
  - `$GPHD_ROOT/bin/hive-service.sh hiveserver2 stop`

Notes
-----

1. Make sure you have enough memory and space to run all services. Typically about 24GB space is needed to run pxf automation.
2. All of the data is stored under $GPHD_ROOT/storage. Cleanup this directory before running init again.


For Hive
--------

When you run `./hive`, it uses beeline. You can then run 

```shell
!connect jdbc:hive2://localhost:10000/default
```

with no username and no password. 

If you receive the following error, give the system a minute or two to finish spinning up the hiveserver before trying again. 
```shell
WARN jdbc.HiveConnection: Failed to connect to localhost:10000
Could not open connection to the HS2 server. Please check the server URI and if the URI is correct, then ask the administrator to check the server status.
Error: Could not open client transport with JDBC Uri: jdbc:hive2://localhost:10000/default: java.net.ConnectException: Connection refused (Connection refused) (state=08S01,code=0)
```
You can also check using netstat to see when the server has finished spinning up: 

```shell
netstat -vanp tcp | grep 10000
```

You can check to see if the pids for the Hive2 server and the metastore by running the following commands:

```shell
cat $GPHD_ROOT/storage/pids/hive-<username>-hiveserver2.pid
ps aux | grep <pid>
cat $GPHD_ROOT/storage/pids/hive-<username>-metastore.pid
ps aux | grep <pid>
```

If the Hive2 Server is not starting up, ensure that you are using Java 8. On a Mac, you can search for `hive.log` in the `/var` folder.

If you are trying to insert data and it is hanging for Tez, ensure that YARN is running. You can do so by checking the resourcemanager pid and see if it is running:

```shell
cat $GPHD_ROOT/storage/pids/hadoop-<username>-resourcemanager.pid
ps aux | grep <pid>
```

If it is not running, spin up YARN before starting a new Hive session.

You can view the status of your hive server as well as your YARN resources by going to the following:
- `localhost:10002` will show the status of the HiveServer2. This includes running and completed queries, and active sessions.
- `localhost:8088` willl show the status of the YARN resource manager. This includes cluster metrics and cluster node statuses.
