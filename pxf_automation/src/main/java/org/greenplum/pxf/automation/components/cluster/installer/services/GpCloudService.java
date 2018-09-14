package org.greenplum.pxf.automation.components.cluster.installer.services;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.greenplum.pxf.automation.components.cluster.installer.InstallationCluster;
import org.greenplum.pxf.automation.components.cluster.installer.nodes.InstallationNode;
import org.greenplum.pxf.automation.components.cluster.installer.nodes.MasterNode;
import org.greenplum.pxf.automation.components.cluster.installer.nodes.Node;
import org.greenplum.pxf.automation.components.cluster.installer.nodes.SlaveNode;
import org.greenplum.pxf.automation.components.common.ShellSystemObject;
import org.greenplum.pxf.automation.utils.jsystem.report.ReportUtils;

/**
 * Installation of vcloudcli for create new cluster and collect information about nodes in the
 * cluster.
 */
public class GpCloudService extends Service {
	private boolean createCluster = false;
	// required gpCloud template name
	private String templateName;
	// required cluster name
	private String clusterName;
	// retries limit for retries mechanisms
	private final int RETRIES_LIMIT = 5;
	// gpCloud user name
	private String gpCloudUserName;
	// gpCloud password
	private String gpCloudPassword;

	@Override
	public void install(InstallationCluster cluster) throws Exception {
		ReportUtils.reportTitle(report, getClass(), "GpCloud Service");
		InstallationNode installationNode = cluster.getInstalltionNode();
		// install specific vcloudcli build
		int vcloudcliBuild = 40;
		String vcloudcliVersion = "vcloudcli-0.1." + vcloudcliBuild;
		String requiredVcloudcliFile = vcloudcliVersion + "-bin.tar.gz";
		// check if vcloud tar is already exists if so skip the download and untar
		if (!installationNode.checkFileExists(".", requiredVcloudcliFile)) {
			installationNode.runCommand("wget http://hdsh132.lss.emc.com:8080/job/jun-vcloudcli/" + vcloudcliBuild + "/artifact/target/" + requiredVcloudcliFile);
			installationNode.runCommand("tar xvzf " + requiredVcloudcliFile);
		}
		installationNode.runCommand("cd " + vcloudcliVersion);
		installationNode.runCommand("chmod 777 vcloudcli");
		// login to gpCloud
		installationNode.runCommand("./vcloudcli config -o hd");
		// the credentials embedded in the code, in the future will get from configuration file
		installationNode.runCommand("./vcloudcli login -u " + gpCloudUserName + " -p " + gpCloudPassword);
		// if createCluster is false, the cluster creation will skip
		if (createCluster) {
			createCluster(installationNode);
		}
		// show cluster details
		installationNode.runCommand("./vcloudcli show -n " + clusterName);
		// parse cluster information for build Node objects
		Matcher m = Pattern.compile("\\d+.\\d+.\\d+.\\d+.*\r\n").matcher(installationNode.getLastCmdResult());
		// prepare MasterNode Object and SlaveNode list to fill with information
		MasterNode masterNode = null;
		List<Node> slaveNodes = new ArrayList<Node>();
		boolean firstNode = true;
		// go over matches and build Node objects
		while (m.find()) {
			String nodeDetails = m.group();
			Node node;
			// first node match will always be the master
			if (firstNode) {
				node = new MasterNode();
				masterNode = (MasterNode) node;
				firstNode = false;
			} else {
				node = new SlaveNode();
				slaveNodes.add(node);
			}
			// configure and initialize node
			String[] splitedNodeDetails = nodeDetails.split(" ");
			node.setHost(splitedNodeDetails[0].trim());
			node.setHostName(splitedNodeDetails[1].replaceAll("\\r\\n", ""));
			// set default user and password for gpCloud nodes
			node.setUserName("root");
			node.setPassword("P@ssw0rd");
			// initialize connection for node using retry mechanism
			int retry = 1;
			boolean isConnected = false;
			while (!isConnected && retry <= RETRIES_LIMIT) {
				try {
					node.init();
					isConnected = true;
				} catch (Exception e) {
					ReportUtils.report(report, getClass(), "Problem connecting to node: " + node.getHost() + " Retry " + retry + "/" + RETRIES_LIMIT);
					retry++;
					Thread.sleep(ShellSystemObject._10_SECONDS);
				}
			}

			if (!isConnected) {
				throw new Exception("Node " + node.getHost() + " is down");
			}
		}
		// if firstNode stays "true" than no nodes found
		if (firstNode) {
			throw new Exception("Could not find any nodes for cluster: " + clusterName);
		}
		// set master to cluster
		cluster.setMasterNode(masterNode);
		// set slaves array to cluster
		SlaveNode[] slavesArr = new SlaveNode[slaveNodes.size()];
		slavesArr = slaveNodes.toArray(slavesArr);
		cluster.setSlaveNodes(slavesArr);
	}

	/**
	 * Create Cluster using vcloudcli. If cluster exists and running will delete it before cresting.
	 * 
	 * @param installationNode Node that perform installations
	 * @throws Exception
	 */
	private void createCluster(InstallationNode installationNode) throws Exception {
		ReportUtils.reportTitle(report, getClass(), "Create GpCloud Cluster");
		// get list of provisioned vApps
		installationNode.runCommand("./vcloudcli list");
		// if clusterName is in the list, delete it
		if (installationNode.getLastCmdResult().contains((clusterName))) {
			int retry = 1;
			boolean isDeleteSuccess = false;
			while (!isDeleteSuccess && retry <= RETRIES_LIMIT) {
				try {
					installationNode.runCommand("./vcloudcli deletevapp -n " + clusterName);
					isDeleteSuccess = true;
				} catch (Exception e) {
					// in case of delete fails
					ReportUtils.report(report, getClass(), "Problem deleting cluster: " + clusterName + " Retry " + retry + "/" + RETRIES_LIMIT);
					retry++;
					Thread.sleep(ShellSystemObject._10_SECONDS);
				}
			}
			// if after retries still cannot delete the cluster, throw exception
			if (!isDeleteSuccess) {
				throw new Exception("Cluster " + clusterName + " could not be deleted");
			}
		}
		// create cluster with given clusterName and using given templateName
		installationNode.runCommand("./vcloudcli provisionvapp -n " + clusterName + " -t " + templateName);
		// stabilization for cluster after creation
		Thread.sleep(ShellSystemObject._10_SECONDS);
	}

	public boolean isCreateCluster() {
		return createCluster;
	}

	public void setCreateCluster(boolean createCluster) {
		this.createCluster = createCluster;
	}

	public String getTemplateName() {
		return templateName;
	}

	public void setTemplateName(String templateName) {
		this.templateName = templateName;
	}

	public String getClusterName() {
		return clusterName;
	}

	public void setClusterName(String clusterName) {
		this.clusterName = clusterName;
	}

	public String getGpCloudUserName() {
		return gpCloudUserName;
	}

	public void setGpCloudUserName(String gpCloudUserName) {
		this.gpCloudUserName = gpCloudUserName;
	}

	public String getGpCloudPassword() {
		return gpCloudPassword;
	}

	public void setGpCloudPassword(String gpCloudPassword) {
		this.gpCloudPassword = gpCloudPassword;
	}
}
