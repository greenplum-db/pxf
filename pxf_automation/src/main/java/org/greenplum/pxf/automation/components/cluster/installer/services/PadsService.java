package org.greenplum.pxf.automation.components.cluster.installer.services;

import java.io.File;

import org.greenplum.pxf.automation.components.cluster.installer.InstallationCluster;
import org.greenplum.pxf.automation.components.cluster.installer.nodes.InstallationNode;
import org.greenplum.pxf.automation.components.cluster.installer.nodes.MasterNode;
import org.greenplum.pxf.automation.components.cluster.installer.nodes.SlaveNode;
import org.greenplum.pxf.automation.utils.jsystem.report.ReportUtils;

/**
 * Installation of PADS components (such HAWQ and PXF) over given cluster.
 */
public class PadsService extends Service {
	private boolean hawq = false;
	private boolean pxf = false;
	// PHD version to install
	private String padsVersion;

	@Override
	public void install(InstallationCluster cluster) throws Exception {
		ReportUtils.reportTitle(report, getClass(), "PADS Service");
		// get all nodes list and installation node
		InstallationNode installtionNode = cluster.getInstalltionNode();
		// Retrieve required PADS version to install
		padsVersion = installtionNode.getRequiredVersion(getVersion());
		ReportUtils.reportTitle(report, getClass(), "Install " + padsVersion);
		// download, untar and install build on all nodes
		deployTarOnAllNodes(cluster, installtionNode.getDownloadServer(), padsVersion, "PADS");

		if (isHawq()) {
			installHawq(cluster);
		}
		if (isPxf()) {
			installPxf(cluster);
		}
	}

	private void installHawq(InstallationCluster cluster) throws Exception {
		ReportUtils.reportTitle(report, getClass(), "HAWQ Installation");
		InstallationNode installtionNode = cluster.getInstalltionNode();
		MasterNode masterNode = cluster.getMasterNode();
		SlaveNode[] slaveNodes = cluster.getSlaveNodes();
		String resourcesPath = installtionNode.getResourceDirectory();
		// create /data/hawq directory
		cluster.runParallelCommand("mkdir /data/hawq");
		// install rpms on all nodes
		installRpmsOnNodes(padsVersion, new String[] { "hawq" }, cluster, cluster.getAllNodesArray());
		masterNode.runCommand("mkdir /data/hawq/master");
		// copy configuration files
		installtionNode.copyToRemoteMachine(masterNode.getUserName(), masterNode.getPassword(), masterNode.getHost(), new File(resourcesPath + "hawq/*").getAbsolutePath(), "/data/hawq/");
		// create data directory for 2 segments for each node
		cluster.runParallelCommand("mkdir /data/hawq/p1");
		cluster.runParallelCommand("mkdir /data/hawq/p2");
		// copy hdfs-client.xml only to slaves
		cluster.copyFileToNodes(slaveNodes, new File(resourcesPath + "hawq/hdfs-client.xml").getAbsolutePath(), "/usr/local/hawq/etc/hdfs-client.xml");
		// set /data/hawq directory with 'gpadmin' as owner
		cluster.runParallelCommand("chown -R gpadmin:gpadmin /data/hawq");
		// prepare HDFS directories for HAWQ with 'gpadmin' as owner
		masterNode.runCommand("sudo -u hdfs hdfs dfs -mkdir /hawq_data");
		masterNode.runCommand("sudo -u hdfs hdfs dfs -chown gpadmin:gpadmin /hawq_data");
		// init and start hawq on master node
		masterNode.runCommand("su gpadmin -");
		masterNode.runCommand("source /usr/local/hawq/greenplum_path.sh");
		masterNode.runCommand("gpinitsystem -c /data/hawq/gpinitsystem_config -h /data/hawq/slave_hosts -a", 1);
		// exit to root user
		masterNode.runCommand("exit");
	}

	private void installPxf(InstallationCluster cluster) throws Exception {
		ReportUtils.reportTitle(report, getClass(), "PXF Installation");
		MasterNode masterNode = cluster.getMasterNode();
		SlaveNode[] slaveNodes = cluster.getSlaveNodes();
		// install rpms on all nodes
		installRpmsOnNodes(padsVersion, new String[] { "pxf-service", "pxf-hdfs", "pxf-hbase", "pxf-hive", "vfabric-tc-server-standard" }, cluster, cluster.getAllNodesArray());
		// init and start pxf
		cluster.runParallelCommand("/etc/init.d/pxf-service init");
		cluster.runParallelCommand("/etc/init.d/pxf-service start");
		// need to restart hbase nodes to load pxf-hive to hbase class path
		masterNode.runCommand("/etc/init.d/hbase-master restart");
		cluster.runParallelCommand(slaveNodes, "/etc/init.d/hbase-regionserver restart");
	}

	public boolean isHawq() {
		return hawq;
	}

	public void setHawq(boolean hawq) {
		this.hawq = hawq;
	}

	public boolean isPxf() {
		return pxf;
	}

	public void setPxf(boolean pxf) {
		this.pxf = pxf;
	}
}
