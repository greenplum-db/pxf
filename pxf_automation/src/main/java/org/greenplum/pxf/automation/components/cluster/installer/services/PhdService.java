package org.greenplum.pxf.automation.components.cluster.installer.services;

import java.io.File;
import java.io.IOException;

import org.greenplum.pxf.automation.components.cluster.installer.InstallationCluster;
import org.greenplum.pxf.automation.components.cluster.installer.nodes.InstallationNode;
import org.greenplum.pxf.automation.components.cluster.installer.nodes.MasterNode;
import org.greenplum.pxf.automation.components.cluster.installer.nodes.SlaveNode;
import org.greenplum.pxf.automation.components.common.cli.ShellCommandErrorException;
import org.greenplum.pxf.automation.utils.jsystem.report.ReportUtils;

/**
 * Installation of PHD components (such HDFS,YARN,HBASE,ZOOKEEPER and HIVE) over given cluster.
 */
public class PhdService extends Service {
	private String hdfsWorkingDirectory = "/tmp";
	// indicates whether to install HDFS
	private boolean hdfs = false;
	// indicates whether to install YARN
	private boolean yarn = false;
	// indicates whether to install zookeeper
	private boolean zookeeper = false;
	// indicates whether to install HBase
	private boolean hbase = false;
	// indicates whether to install Hive
	private boolean hive = false;
	// rpm paths
	private String hadoopRpmDirectory;
	private String utilityRpmDirectory;
	private String zookeeperRpmDirectory;
	private String hbaseRpmDirectory;
	private String hiveRpmDirectory;

	@Override
	public void install(InstallationCluster cluster) throws Exception {
		ReportUtils.reportTitle(report, getClass(), "PHD Service");
		// get all nodes list and installation node
		InstallationNode installtionNode = cluster.getInstalltionNode();
		// Retrieve required PHD version to install
		String phdVersion = installtionNode.getRequiredVersion(getVersion());
		ReportUtils.reportTitle(report, getClass(), "Install " + phdVersion);
		// download and untar build on all nodes
		deployTarOnAllNodes(cluster, installtionNode.getDownloadServer(), phdVersion, "PHD");
		// rpm paths
		hadoopRpmDirectory = phdVersion + "/hadoop/rpm";
		utilityRpmDirectory = phdVersion + "/utility/rpm";
		zookeeperRpmDirectory = phdVersion + "/zookeeper/rpm";
		hbaseRpmDirectory = phdVersion + "/hbase/rpm";
		hiveRpmDirectory = phdVersion + "/hive/rpm";

		if (isHdfs()) {
			installHdfs(cluster);
		}
		if (isYarn()) {
			installYarn(cluster);
		}
		if (isZookeeper()) {
			installZookeeper(cluster);
		}
		if (isHbase()) {
			installHBase(cluster);
		}
		if (isHive()) {
			installHive(cluster);
		}
	}

	/**
	 * Recipe for HDFS installation
	 * 
	 * @param cluster to install HDFS on
	 * @throws Exception
	 */
	private void installHdfs(InstallationCluster cluster) throws Exception {
		ReportUtils.reportTitle(report, getClass(), "HDFS Installation");
		InstallationNode installtionNode = cluster.getInstalltionNode();
		MasterNode masterNode = cluster.getMasterNode();
		SlaveNode[] slaveNodes = cluster.getSlaveNodes();
		String resourcesPath = installtionNode.getResourceDirectory();
		String[] requiredRpmsDirectories = new String[] { hadoopRpmDirectory, hadoopRpmDirectory, hadoopRpmDirectory, utilityRpmDirectory, utilityRpmDirectory, zookeeperRpmDirectory };

		// install RPMs on NN
		installRpms(requiredRpmsDirectories, new String[] { "hadoop-hdfs-namenode", "hadoop-hdfs", "hadoop", "bigtop-jsvc", "bigtop-utils", "zookeeper" }, masterNode);
		// Create data directory for NN
		masterNode.runCommand("mkdir -p /data/nn/");
		masterNode.runCommand("chown hdfs:hadoop /data/nn");
		masterNode.runCommand("chmod 775 /data/nn");
		// install RPMs on DNs
		installRpmsOnNodes(requiredRpmsDirectories, new String[] { "hadoop-hdfs-datanode", "hadoop-hdfs", "hadoop", "bigtop-jsvc", "bigtop-utils", "zookeeper" }, cluster, slaveNodes);
		// create data directory for DNs
		cluster.runParallelCommand("mkdir -p /data/dn/");
		cluster.runParallelCommand("chown hdfs:hadoop /data/dn");
		cluster.runParallelCommand("chmod 775 /data/dn");
		// add gpadmin to hadoop group
		cluster.runParallelCommand("usermod -a -G hadoop gpadmin");
		// prepare /tmp/gphdtmp directory
		cluster.runParallelCommand("mkdir /tmp/gphdtmp");
		cluster.runParallelCommand("chown hdfs:hadoop /tmp/gphdtmp");
		cluster.runParallelCommand("chmod 777 /tmp/gphdtmp");
		// create required 'dfs.exclude' file
		cluster.runParallelCommand("touch /etc/gphd/hadoop/conf/dfs.exclude");
		// copy configuration files
		cluster.copyFileToNodes(new File(resourcesPath + "hdfs/*").getAbsolutePath(), "/etc/gphd/hadoop/conf/");
		// init NN and start
		masterNode.runCommand("/etc/init.d/hadoop-hdfs-namenode init");
		masterNode.runCommand("/etc/init.d/hadoop-hdfs-namenode start");
		// start all DNs
		cluster.runParallelCommand(slaveNodes, "/etc/init.d/hadoop-hdfs-datanode start");
		// create HDFS working directory
		masterNode.runCommand("sudo -u hdfs hdfs dfs -mkdir " + hdfsWorkingDirectory);
		masterNode.runCommand("sudo -u hdfs hdfs dfs -chmod 777 " + hdfsWorkingDirectory);
	}

	/**
	 * Recipe for Yarn installation
	 * 
	 * @param cluster cluster to install on
	 * @throws Exception
	 */
	private void installYarn(InstallationCluster cluster) throws Exception {
		ReportUtils.reportTitle(report, getClass(), "YARN Installation");
		InstallationNode installtionNode = cluster.getInstalltionNode();
		MasterNode masterNode = cluster.getMasterNode();
		SlaveNode[] slaveNodes = cluster.getSlaveNodes();
		String resourcesPath = installtionNode.getResourceDirectory();
		// install rpms on master node
		installRpms(hadoopRpmDirectory, new String[] { "hadoop-mapreduce-historyserver", "hadoop-yarn-resourcemanager", "hadoop-mapreduce", "hadoop-yarn" }, masterNode);
		// install rpms on all slaves
		installRpmsOnNodes(hadoopRpmDirectory, new String[] { "hadoop-yarn-nodemanager", "hadoop-yarn", "hadoop-mapreduce" }, cluster, slaveNodes);
		// create required 'yarn.exclude' file
		cluster.runParallelCommand("touch /etc/gphd/hadoop/conf/yarn.exclude");
		// copy yarn configuration files to each node
		cluster.copyFileToNodes(new File(resourcesPath + "yarn/*").getAbsolutePath(), "/etc/gphd/hadoop/conf/");
		// create required hdfs directories for yarn with 'mapred' permissions
		masterNode.runCommand("sudo -u hdfs hdfs dfs -mkdir /user");
		masterNode.runCommand("sudo -u hdfs hdfs dfs -mkdir /user/history");
		masterNode.runCommand("sudo -u hdfs hdfs dfs -chown hdfs:hadoop /user/");
		masterNode.runCommand("sudo -u hdfs hdfs dfs -chown mapred:hadoop /user/history");
		masterNode.runCommand("sudo -u hdfs hdfs dfs -chmod -R 777 /user");
		// start yarn services on all nodes
		masterNode.runCommand("/etc/init.d/hadoop-yarn-resourcemanager start");
		masterNode.runCommand("/etc/init.d/hadoop-mapreduce-historyserver start");
		cluster.runParallelCommand(slaveNodes, "/etc/init.d/hadoop-yarn-nodemanager start");
	}

	private void installZookeeper(InstallationCluster cluster) throws Exception {
		ReportUtils.reportTitle(report, getClass(), "Zookeeper Installation");
		MasterNode masterNode = cluster.getMasterNode();
		SlaveNode[] slaveNodes = cluster.getSlaveNodes();
		// install rpms
		installRpms(zookeeperRpmDirectory, new String[] { "zookeeper-server" }, masterNode);
		// HDFS is already installed zookeeper rpm
		if (!isHdfs()) {
			installRpmsOnNodes(new String[] { zookeeperRpmDirectory }, new String[] { "zookeeper" }, cluster, slaveNodes);
		}
		// init and start zookeeper
		masterNode.runCommand("/etc/init.d/zookeeper-server init --myid=1");
		masterNode.runCommand("/etc/init.d/zookeeper-server start");
	}

	private void installHBase(InstallationCluster cluster) throws Exception {
		ReportUtils.reportTitle(report, getClass(), "HBase Installation");
		InstallationNode installtionNode = cluster.getInstalltionNode();
		MasterNode masterNode = cluster.getMasterNode();
		SlaveNode[] slaveNodes = cluster.getSlaveNodes();
		String resourcesPath = installtionNode.getResourceDirectory();
		String[] rpmsDirectories = { hbaseRpmDirectory, hbaseRpmDirectory, hadoopRpmDirectory };
		// go over all nodes and install rpms
		installRpms(rpmsDirectories, new String[] { "hbase-master", "hbase", "hadoop-client" }, masterNode);
		installRpmsOnNodes(rpmsDirectories, new String[] { "hbase-regionserver", "hbase", "hadoop-client" }, cluster, slaveNodes);
		// copy configuration files
		cluster.copyFileToNodes(new File(resourcesPath + "hbase/*").getAbsolutePath(), "/etc/gphd/hbase/conf/");
		// create data directory owned by hbase
		masterNode.runCommand("sudo -u hdfs hdfs dfs -mkdir /hbase");
		masterNode.runCommand("sudo -u hdfs hdfs dfs -chown hbase /hbase");
		// assign hbase to zookeeper
		masterNode.runCommand("/usr/lib/gphd/hbase/bin/hbase-daemons.sh --config /etc/gphd/hbase/conf start zookeeper");
		// start hbase on all nodes
		masterNode.runCommand("/etc/init.d/hbase-master start");
		cluster.runParallelCommand(slaveNodes, "/etc/init.d/hbase-regionserver start");
	}

	private void installHive(InstallationCluster cluster) throws Exception {
		ReportUtils.reportTitle(report, getClass(), "Hive Installation");
		InstallationNode installtionNode = cluster.getInstalltionNode();
		MasterNode masterNode = cluster.getMasterNode();
		SlaveNode[] slaveNodes = cluster.getSlaveNodes();
		String resourcesPath = installtionNode.getResourceDirectory();
		// go over all nodes install rpms
		installRpms(hiveRpmDirectory, new String[] { "hive-metastore", "hive-server", "hive" }, masterNode);
		installRpmsOnNodes(new String[] { hiveRpmDirectory }, new String[] { "hive" }, cluster, slaveNodes);
		// add hive user to hadoop group
		cluster.runParallelCommand("usermod -a -G hadoop hive");
		// copy hive configuration files to each node
		cluster.copyFileToNodes(new File(resourcesPath + "hive/hive-site.xml").getAbsolutePath(), "/etc/gphd/hive/conf/");
		// install postgres for hive metastore
		installPostgres(cluster, 6543);
		// create hdfs dir for hive
		masterNode.runCommand("sudo -u hdfs hdfs dfs -mkdir /hive/");
		masterNode.runCommand("sudo -u hdfs hdfs dfs -mkdir /hive/gphd");
		masterNode.runCommand("sudo -u hdfs hdfs dfs -mkdir /hive/gphd/warehouse");
		masterNode.runCommand("sudo -u hdfs hdfs dfs -chown hive:hadoop /hive/gphd/warehouse");
		masterNode.runCommand("sudo -u hdfs hdfs dfs -chmod 775 /hive/gphd/warehouse");
		// run hive metastore and hive server
		masterNode.runCommand("/etc/init.d/hive-metastore start");
		masterNode.runCommand("/etc/init.d/hive-server start");
	}

	private void installPostgres(InstallationCluster cluster, int port) throws IOException, ShellCommandErrorException {
		ReportUtils.reportTitle(report, getClass(), "Postgres Installation on port " + port);
		MasterNode masterNode = cluster.getMasterNode();
		InstallationNode installationNode = cluster.getInstalltionNode();
		String resourcesPath = installationNode.getResourceDirectory();
		// install postgres server and jdbc
		masterNode.runCommand("yum -y install postgresql-server");
		masterNode.runCommand("yum -y install postgresql-jdbc");
		// init DB
		masterNode.runCommand("service postgresql initdb");
		// copy configuration files
		installationNode.copyToRemoteMachine(masterNode.getUserName(), masterNode.getPassword(), masterNode.getHost(), new File(resourcesPath + "hive/postgresql.conf").getAbsolutePath(), "/var/lib/pgsql/data/");
		installationNode.copyToRemoteMachine(masterNode.getUserName(), masterNode.getPassword(), masterNode.getHost(), new File(resourcesPath + "hive/pg_hba.conf").getAbsolutePath(), "/var/lib/pgsql/data/");
		installationNode.copyToRemoteMachine(masterNode.getUserName(), masterNode.getPassword(), masterNode.getHost(), new File(resourcesPath + "hive/grant-privs").getAbsolutePath(), "/tmp/grant-privs");
		// replace port in /etc/init.d/postgresql
		masterNode.runCommand("sed -i 's/PGPORT=5432/PGPORT=" + port + "/g' /etc/init.d/postgresql");
		// start service and build schema
		masterNode.runCommand("service postgresql start");
		masterNode.runCommand("sudo -u postgres psql -p " + port + " -c \"CREATE USER hive WITH PASSWORD 'hive';\"");
		masterNode.runCommand("sudo -u postgres psql -p " + port + " -c \"CREATE DATABASE metastore;\"");
		masterNode.runCommand("sudo -u postgres psql -p " + port + " -d metastore -f /usr/lib/gphd/hive/scripts/metastore/upgrade/postgres/hive-schema-0.13.0.postgres.sql");
		// copy grant-privs to /tmp/grant-privs and run it to postgres
		masterNode.runCommand("sudo -u postgres psql -p " + port + " -d metastore -f /tmp/grant-privs");
		// create link to postgres jar in /usr/lib/gphd/hive/lib/
		masterNode.runCommand("ln -s /usr/share/java/postgresql-jdbc.jar /usr/lib/gphd/hive/lib/");
	}

	public String getHdfsWorkingDirectory() {
		return hdfsWorkingDirectory;
	}

	public void setHdfsWorkingDirectory(String hdfsWorkingDirectory) {
		this.hdfsWorkingDirectory = hdfsWorkingDirectory;
	}

	public boolean isHdfs() {
		return hdfs;
	}

	public void setHdfs(boolean hdfs) {
		this.hdfs = hdfs;
	}

	public boolean isYarn() {
		return yarn;
	}

	public void setYarn(boolean yarn) {
		this.yarn = yarn;
	}

	public boolean isZookeeper() {
		return zookeeper;
	}

	public void setZookeeper(boolean zookeeper) {
		this.zookeeper = zookeeper;
	}

	public boolean isHbase() {
		return hbase;
	}

	public void setHbase(boolean hbase) {
		this.hbase = hbase;
	}

	public boolean isHive() {
		return hive;
	}

	public void setHive(boolean hive) {
		this.hive = hive;
	}
}
