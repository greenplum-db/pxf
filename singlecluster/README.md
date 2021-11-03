SingleCluster
=============

Singlecluster is a self contained, easy to deploy distribution of HDP or CDH.

Singlecluster-HDP contains the following versions:

- Hadoop 2.7.3
- Hive 1.2.1000
- Zookeeper 3.4.6
- HBase 1.1.2
- Tez 0.7.0
- Tomcat 7.0.62

Singlecluster-CDH contains the following versions:

- CDH 5.12.2
- Hadoop 2.6.0-CDH5.12.2
- Hive 1.1.0-CDH5-12.2
- Zookeeper 3.4.5-CDH5.12.2
- HBase 1.2.0-CDH5.12.2

For HDP3, please use the HDP3 related README.

Prerequisites
-------------

1.	$JAVA_HOME points to a JDK7 or later install

Build
-----

-	make HADOOP_DISTRO=[CDH|HDP] HADOOP_VERSION=[CDH version|HDP version]
-	if you do "make", HDP is the default tarball to generate
-	E.g. make HADOOP_DISTRO=CDH HADOOP_VERSION=5.12.2
-   E.g. make HADOOP_DISTRO=HDP HADOOP_VERSION=2.5.3.0

Initialization
--------------

1. Untar the singlecluster tarball
	-	mv singlecluster.tar.gz ~/.
	-	cd ~/.
	-	tar -xzvf singlecluster-CDH.tar.gz
	-	cd singlecluster-CDH
2. Initialize an instance
	-	bin/init-gphd.sh
3. Add the following to your environment
	-	export GPHD_ROOT=<singlecluster location, e.g. ~/singlecluster-PHD>
	-	export HADOOP_ROOT=$GPHD_ROOT/hadoop
	-	export HBASE_ROOT=$GPHD_ROOT/hbase
	-	export HIVE_ROOT=$GPHD_ROOT/hive
	-	export ZOOKEEPER_ROOT=$GPHD_ROOT/zookeeper
	-	export PATH=$PATH:$GPHD_ROOT/bin:$HADOOP_ROOT/bin:$HBASE_ROOT/bin:$HIVE_ROOT/bin:$ZOOKEEPER_ROOT/bin

Usage
-----

-	Start all Hadoop services
	-	$GPHD_ROOT/bin/start-gphd.sh
-	Start HDFS only
	-	$GPHD_ROOT/bin/start-hdfs.sh
-	Start PXF only (Install pxf first to make this work. [See Install PXF session here](https://cwiki.apache.org/confluence/display/HAWQ/PXF+Build+and+Install))
	-	$GPHD_ROOT/bin/start-pxf.sh
-	Start HBase only (requires hdfs and zookeeper)
	-	$GPHD_ROOT/bin/start-hbase.sh
-	Start ZooKeeper only
	-	$GPHD_ROOT/bin/start-zookeeper.sh
-	Start YARN only
	-	$GPHD_ROOT/bin/start-yarn.sh
-	Start Hive (MetaStore)
	-	$GPHD_ROOT/bin/start-hive.sh
- 	Stop all PHD services
	- 	$GPHD_ROOT/bin/stop-gphd.sh
-	Stop an individual component
	-	$GPHD_ROOT/bin/stop-[hdfs|pxf|hbase|zookeeper|yarn|hive].sh
-	Start/stop HiveServer2
	-	$GPHD_ROOT/bin/hive-service.sh hiveserver2 start
	-	$GPHD_ROOT/bin/hive-service.sh hiveserver2 stop

Notes
-----

1.	Make sure you have enough memory and space to run all services. Typically about 24GB space is needed to run pxf automation.
2.	All of the data is stored under $GPHD_ROOT/storage. Cleanup this directory before running init again.

Concourse Pipeline Deployment
-----------------------------

To deploy the concourse pipeline that will build the single cluster tarballs and upload them to S3, use the following command:
```
make -C ~/workspace/pxf/concourse singlecluster
```
