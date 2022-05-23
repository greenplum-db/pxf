package org.greenplum.pxf.automation.components.cluster;

import org.apache.commons.lang.StringUtils;
import org.greenplum.pxf.automation.components.cluster.installer.nodes.Node;
import org.greenplum.pxf.automation.components.common.ShellSystemObject;
import org.greenplum.pxf.automation.structures.profiles.PxfProfileXml;
import org.greenplum.pxf.automation.utils.jsystem.report.ReportUtils;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

/**
 * Common functionality class between ShellSystemObject phd clusters.
 */
public abstract class PhdCluster extends ShellSystemObject {
	// PxfProfile object, holds profiles List
	private PxfProfileXml pxfProfiles;
	private String tempClusterConfDirectory = new File("tempClusterConfDirectory").getAbsolutePath();
	// the sub directory inside the tempClusterFolderDirectory that leads to pxf-profiles
	private String pathToPxfConfInGeneralConf = "";
	// pxf configuration file name
	private String pxfConfigurationFile = "pxf-env.sh";
	// path to local fetched pxf conf directory
	private String pathToLocalPxfConfDirectory = "";
	// folder which the cluster admin is installed
	private String phdRoot;
	// location where PXF is installed, for GPDB-based testing PXF is installed under GPDB, not inside single cluster
	private String pxfHome;
	// location where the PXF configuration lives
	private String pxfBase;
	// name of cluster
	private String clusterName = "test";
	// the path on hdfs directory which hive files will be stored
	private String hiveBaseHdfsDirectory = "";
	private int nodesAmount = 3;
    private String testKerberosPrincipal;
	// debug flag for the PXF server
	private String pxfServerDebug;

	public PhdCluster() {
	}

	@Override
	public void init() throws Exception {
		// set PXF_HOME if provided in the environment, this will cause framework to run different scripts
		String pxfHome = System.getenv("PXF_HOME");
		if (!org.apache.commons.lang.StringUtils.isEmpty(pxfHome)) {
			setPxfHome(pxfHome);
		}

		String pxfBase = StringUtils
				.defaultIfBlank(System.getenv("PXF_BASE"), pxfHome);
		setPxfBase(pxfBase);

		// if automation is running with debug flags, set the PXF server debug flag as well
		String pxfDebug = StringUtils
				.defaultIfBlank(System.getenv("PXF_TEST_DEBUG"), "false");
		setPxfServerDebug(pxfDebug);

		super.init();
		// some cluster commands can take a while, set max time out for 2 minutes.
		setCommandTimeout(_2_MINUTES);
		// fetch the general configuration files from cluster to tempClusterConfDirectory
		fetchConfiguration(getTempClusterConfDirectory());
		// set path to local fetched pxf conf directory
		setPathToLocalPxfConfDirectory(getTempClusterConfDirectory() + (getPathToPxfConfInGeneralConf().equals("") ? "" : "/") + getPathToPxfConfInGeneralConf());
		// initialize pxfProfiles with profiles XML
		setPxfProfiles(new PxfProfileXml(getPathToLocalPxfConfDirectory(), true));
	}

	/**
	 * Adds path to PXF classpath file and deploy to all nodes
	 *
	 * @param path to add to classpath
	 * @throws Exception when an error occurs
	 */
	public void addPathToPxfClassPath(String path) throws Exception {
		String content = "export PXF_LOADER_PATH=file:" + path;
		// path to local fetch pxf class file
		File pathToLocalClassPathFile = new File(getPxfBase() + "/conf", getPxfConfigurationFile());
		ReportUtils.report(report, getClass(), "Add " + content + " to PXF class path (" + pathToLocalClassPathFile.getAbsolutePath() + ")");
		// read file content
		String pxfClasspathContent = new String(Files.readAllBytes(Paths.get(pathToLocalClassPathFile.getAbsolutePath())));
		// check if path already in classpath, if not append
		if (!pxfClasspathContent.contains(content)) {
			pxfClasspathContent += System.lineSeparator() + content;
			ReportUtils.report(report, getClass(), pxfClasspathContent);
			// write new content to file
			Files.write(pathToLocalClassPathFile.toPath(), pxfClasspathContent.getBytes());
			// copy modified file to all nodes
			copyFileToNodes(pathToLocalClassPathFile.getAbsolutePath(), getPxfConfLocation(), false, false);
		}
	}

	/**
	 * Starts Hive server for JDBC requests
	 *
	 * @throws Exception
	 */
	abstract public void startHiveServer() throws Exception;

	/**
	 * Stops Hive server
	 *
	 * @throws Exception
	 */
	abstract public void stopHiveServer() throws Exception;

	/**
	 * Starts Cluster service
	 *
	 * @param component {@link EnumClusterServices}
	 * @throws Exception
	 */
	abstract public void start(EnumClusterServices component) throws Exception;

	/**
	 * Stops Cluster service
	 *
	 * @param service {@link EnumClusterServices}
	 * @throws Exception
	 */
	abstract public void stop(EnumClusterServices service) throws Exception;

	/**
	 * restarts Cluster service
	 *
	 * @param service {@link EnumClusterServices}
	 * @throws Exception
	 */
	abstract public void restart(EnumClusterServices service) throws Exception;

	/**
	 * Checks if given service is up
	 *
	 * @param component {@link EnumClusterServices}
	 * @return true if up
	 * @throws Exception
	 */
	abstract public boolean isUp(EnumClusterServices component) throws Exception;

	/**
	 * Gets configuration files from cluster to local directory
	 *
	 * @throws Exception
	 */
	abstract public void fetchConfiguration(String targetDirectory) throws Exception;

	/**
	 * Copies File to all Nodes in the cluster
	 *
	 * @param file file to copy
	 * @param target remote path to copy to
	 * @throws Exception
	 */
	abstract public void copyFileToNodes(String file, String target) throws Exception;

	/**
	 * Copies File to all Nodes in the cluster
	 *
	 * @param file file to copy
	 * @param target remote path to copy to
	 * @param createTargetDirectory if true create the target directory before copying
	 * @param sudo if true copy the file using sudo
	 * @throws Exception
	 */
	abstract public void copyFileToNodes(String file, String target, boolean createTargetDirectory, boolean sudo) throws Exception;

	/**
	 * Deletes target file from cluster.
	 *
	 * @param targetFile
	 * @param sudo if true delete using sudo
	 * @throws Exception
	 */
	abstract public void deleteFileFromNodes(String targetFile, boolean sudo) throws Exception;

	/**
	 * Runs shell command on all the nodes in the cluster
	 *
	 * @param command command to execute
	 * @throws Exception
	 */
	abstract public void runCommandOnAllNodes(String command) throws Exception;

	/**
	 * Runs shell command on give list of nodes in the cluster
	 *
	 * @param nodes to run command on
	 * @param command to execute
	 * @throws Exception
	 */
	abstract public void runCommandOnNodes(List<Node> nodes, String command) throws Exception;

	public PhdCluster(boolean silentReport) {
		super(silentReport);
	}

	public String getPxfConfLocation() {
		return getPxfBase() + "/conf";
	}

	public int getNodesAmount() {
		return nodesAmount;
	}

	public void setNodesAmount(int nodesAmount) {
		this.nodesAmount = nodesAmount;
	}

	public String getClusterName() {
		return clusterName;
	}

	public void setClusterName(String clusterName) {
		this.clusterName = clusterName;
	}

	public String getPhdRoot() {
		return phdRoot;
	}

	public void setPhdRoot(String phdRoot) {
		this.phdRoot = phdRoot;
	}

	public String getPxfHome() {
		return pxfHome;
	}

	public void setPxfHome(String pxfHome) {
		this.pxfHome = pxfHome;
	}

	public String getPxfBase() {
		return pxfBase;
	}

	public void setPxfBase(String pxfBase) {
		this.pxfBase = pxfBase;
	}

	public String getPxfServerDebug() {
		return pxfServerDebug;
	}

	public void setPxfServerDebug(String pxfServerDebug) {
		this.pxfServerDebug = pxfServerDebug;
	}

	public PxfProfileXml getPxfProfiles() {
		return pxfProfiles;
	}

	public void setPxfProfiles(PxfProfileXml pxfProfiles) {
		this.pxfProfiles = pxfProfiles;
	}

	// Services available for a cluster
	public enum EnumClusterServices {
		gphd(""),
		hdfs("hadoop-hdfs-namenode"),
		hbase("hbase-master"),
		hive("hive-metastore"),
		pxf("pxf-service");

		private String serviceName = "";

		private EnumClusterServices(String serviceName) {
			this.setServiceName(serviceName);
		}

		public String getServiceName() {
			return serviceName;
		}

		public void setServiceName(String serviceName) {
			this.serviceName = serviceName;
		}
	}

	public String getTempClusterConfDirectory() {
		return tempClusterConfDirectory;
	}

	public void setTempClusterConfDirectory(String tempClusterConfDirectory) {
		this.tempClusterConfDirectory = tempClusterConfDirectory;
	}

	public String getHiveBaseHdfsDirectory() {
		return hiveBaseHdfsDirectory;
	}

	public void setHiveBaseHdfsDirectory(String hiveBaseHdfsDirectory) {
		this.hiveBaseHdfsDirectory = hiveBaseHdfsDirectory;
	}

	public String getPathToPxfConfInGeneralConf() {
		return pathToPxfConfInGeneralConf;
	}

	public void setPathToPxfConfInGeneralConf(String pathToPxfConfInGeneralConf) {
		this.pathToPxfConfInGeneralConf = pathToPxfConfInGeneralConf;
	}

	public String getPathToLocalPxfConfDirectory() {
		return pathToLocalPxfConfDirectory;
	}

	public void setPathToLocalPxfConfDirectory(String pathToLocalPxfConfDirectory) {
		this.pathToLocalPxfConfDirectory = pathToLocalPxfConfDirectory;
	}

	public String getPxfConfigurationFile() {
		return pxfConfigurationFile;
	}

	public void setPxfConfigurationFile(String pxfConfigurationFile) {

        this.pxfConfigurationFile = pxfConfigurationFile;
	}

    public String getTestKerberosPrincipal() {
        return this.testKerberosPrincipal;
    }

    public void setTestKerberosPrincipal(String testKerberosPrincipal) {
        this.testKerberosPrincipal = testKerberosPrincipal;
    }
}
