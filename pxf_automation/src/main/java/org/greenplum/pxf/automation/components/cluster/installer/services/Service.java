package org.greenplum.pxf.automation.components.cluster.installer.services;

import java.io.IOException;
import java.util.Arrays;

import jsystem.framework.system.SystemObjectImpl;

import org.greenplum.pxf.automation.components.cluster.installer.InstallationCluster;
import org.greenplum.pxf.automation.components.cluster.installer.nodes.Node;
import org.greenplum.pxf.automation.components.common.cli.ShellCommandErrorException;
import org.greenplum.pxf.automation.utils.jsystem.report.ReportUtils;

/**
 * Abstract Service that can be installed on a {@link InstallationCluster} by {@link ServicesInstaller}
 */
public abstract class Service extends SystemObjectImpl {
	// version to install
	private String version;

	/**
	 * Implement for every Cluster service
	 * 
	 * @param cluster cluster where installation will take place
	 * @throws Exception
	 */
	abstract public void install(InstallationCluster cluster) throws Exception;

	/**
	 * Installs required rpmNames located in rpmPaths
	 * 
	 * @param rpmPaths for every rpmNames
	 * @param rpmNames each array member contains rpmName (without version) to look in it rpmPath
	 * @param node to look the rpm in
	 * @throws IOException
	 * @throws ShellCommandErrorException
	 */
	protected void installRpms(String[] rpmPaths, String[] rpmNames, Node node) throws IOException, ShellCommandErrorException {
		ReportUtils.reportTitle(report, getClass(), "Install RPMs");
		StringBuilder rpmStringResult = new StringBuilder();
		rpmStringResult.append("rpm -i");

		for (int i = 0; i < rpmNames.length; i++) {
			node.runCommand("ls " + rpmPaths[i] + " | grep '^" + rpmNames[i] + "-[0-9].*rpm$'");
			String requiredRpm = node.getLastCmdResult().split("\r\n")[1];
			rpmStringResult.append(" ").append(rpmPaths[i] + "/").append(requiredRpm);
		}
		node.runCommand(rpmStringResult.toString());
	}

	/**
	 * Install RPMs on array of nodes using one rpm path
	 * 
	 * @param rpmPaths path for all rpmNames array
	 * @param rpmNames rpms to install
	 * @param cluster to install on
	 * @param nodes to install on
	 * @throws Exception
	 */
	protected void installRpmsOnNodes(String rpmPaths, String[] rpmNames, InstallationCluster cluster, Node[] nodes) throws Exception {
		String[] rpmPathsArray = new String[rpmNames.length];
		Arrays.fill(rpmPathsArray, rpmPaths);
		installRpmsOnNodes(rpmPathsArray, rpmNames, cluster, nodes);
	}

	/**
	 * Installs RPMs on array on nodes using different paths for rpmNames
	 * 
	 * @param rpmPaths paths matched to rpmNames array
	 * @param rpmNames rpms to install
	 * @param cluster to install on
	 * @param nodes to install on
	 * @throws Exception
	 */
	protected void installRpmsOnNodes(String[] rpmPaths, String[] rpmNames, InstallationCluster cluster, Node[] nodes) throws Exception {
		ReportUtils.reportTitle(report, getClass(), "Install RPMs");
		StringBuilder rpmStringResult = new StringBuilder();
		rpmStringResult.append("rpm -i");

		for (int i = 0; i < rpmNames.length; i++) {
			nodes[0].runCommand("ls " + rpmPaths[i] + " | grep '^" + rpmNames[i] + "-[0-9].*rpm$'");
			String requiredRpm = nodes[0].getLastCmdResult().split("\r\n")[1];
			rpmStringResult.append(" ").append(rpmPaths[i] + "/").append(requiredRpm);
		}
		cluster.runParallelCommand(nodes, rpmStringResult.toString());
	}

	/**
	 * Installs required rpmNames locates on the same rpmsPath
	 * 
	 * @param rpmPaths
	 * @param rpmNames
	 * @param node
	 * @throws IOException
	 * @throws ShellCommandErrorException
	 */
	protected void installRpms(String rpmPaths, String[] rpmNames, Node node) throws IOException, ShellCommandErrorException {
		String[] rpmPathsArray = new String[rpmNames.length];
		Arrays.fill(rpmPathsArray, rpmPaths);
		installRpms(rpmPathsArray, rpmNames, node);
	}

	/**
	 * Gets required tar from downloadServe to directory with tarName as name and untar it
	 * 
	 * @param cluster to deploy to
	 * @param downloadServer to get tar from
	 * @param targetDirectory directory to download to
	 * @throws Exception
	 */
	protected void deployTarOnAllNodes(InstallationCluster cluster, String downloadServer, String tarFile, String targetDirectory) throws Exception {
		ReportUtils.reportTitle(report, getClass(), "Deploy tar");
		for (Node node : cluster.getAllNodesList()) {
			// make sure in 'root' user
			if (!node.getLoggedUser().equals(node.getUserName())) {
				node.runCommand("exit");
			}
		}
		// move to home directory
		cluster.runParallelCommand("cd");
		// mkdir and cd in
		cluster.runParallelCommand("rm -rf " + targetDirectory);
		cluster.runParallelCommand("mkdir " + targetDirectory);
		cluster.runParallelCommand("cd " + targetDirectory);
		// download tar and untar it
		cluster.runParallelCommand("wget " + downloadServer + tarFile + ".tar.gz");
		cluster.runParallelCommand("tar -xvzf " + tarFile + ".tar.gz");
	}

	public String getVersion() {
		return version;
	}

	public void setVersion(String version) {
		this.version = version;
	}
}
