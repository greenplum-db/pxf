package org.greenplum.pxf.automation.components.cluster.installer.services;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.greenplum.pxf.automation.components.cluster.installer.InstallationCluster;
import org.greenplum.pxf.automation.components.cluster.installer.nodes.InstallationNode;
import org.greenplum.pxf.automation.components.cluster.installer.nodes.MasterNode;
import org.greenplum.pxf.automation.components.cluster.installer.nodes.Node;
import org.greenplum.pxf.automation.components.common.cli.ShellCommandErrorException;
import org.greenplum.pxf.automation.utils.jsystem.report.ReportUtils;

/**
 * General required settings to be installed on each node in the cluster
 */
public class PreService extends Service {

	@Override
	public void install(InstallationCluster cluster) throws Exception {
		ReportUtils.reportTitle(report, getClass(), "Pre Service");
		InstallationNode installtionNode = cluster.getInstalltionNode();
		String resourcesPath = installtionNode.getResourceDirectory();
		// download jdk
		cluster.runParallelCommand("wget http://build-prod.dh.greenplum.com/releng/tools/jdk/jdk-7u65-linux-x64.rpm");
		// remove other java versions
		cluster.runParallelCommand("rpm -e sun-javadb-core sun-javadb-javadoc sun-javadb-common sun-javadb-client sun-javadb-docs sun-javadb-demo jdk");
		cluster.runParallelCommand("rpm -e java-1.6.0-openjdk java-1.7.0-openjdk");
		// install jdk
		cluster.runParallelCommand("rpm -i jdk-7u65-linux-x64.rpm");
		// copy memory and security settings
		cluster.copyFileToNodes(new File(resourcesPath + "general/limits.conf").getAbsolutePath(), "/etc/security/limits.conf");
		cluster.copyFileToNodes(new File(resourcesPath + "general/sysctl.conf").getAbsolutePath(), "/etc/sysctl.conf");
		// restart sysctl
		cluster.runParallelCommand("sysctl -p");
		// create 'gpadmin' user
		String user = "gpadmin";
		String password = "gpadmin";
		cluster.runParallelCommand("useradd " + user + " && echo " + password + " | passwd --stdin " + user);
		// configure ssh settings and define password less login for 'gpadmin' for all nodes
		configureSshForGpadmin(cluster);
	}

	/**
	 * Configure SSH settings: cancel StrictHostKeyChecking for new hosts, use 'sshpass' to
	 * 'ssh-copy-id' ssh key for all nodes
	 * 
	 * @param cluster cluster to configure
	 * @throws IOException
	 * @throws ShellCommandErrorException
	 */
	private void configureSshForGpadmin(InstallationCluster cluster) throws IOException, ShellCommandErrorException {
		ReportUtils.reportTitle(report, getClass(), "Configure SSH For gpadmin user");
		MasterNode masterNode = cluster.getMasterNode();
		List<Node> allNodesList = cluster.getAllNodesList();
		// cancel host key check for ssh actions
		masterNode.runCommand("echo 'StrictHostKeyChecking no' >> /etc/ssh/ssh_config");
		// install sshpass on master
		masterNode.runCommand("cd /etc/yum.repos.d/");
		masterNode.runCommand("wget http://download.opensuse.org/repositories/home:Strahlex/CentOS_CentOS-6/home:Strahlex.repo");
		masterNode.runCommand("yum -y install sshpass");
		// set user as 'gpadmin' and set password less ssh configuration for all nodes
		masterNode.runCommand("su gpadmin -");
		masterNode.runCommand("ssh-keygen -q -t rsa -N \"\" -f /home/gpadmin/.ssh/id_rsa");

		for (Node node : allNodesList) {
			masterNode.runCommand("sshpass -p 'gpadmin' ssh-copy-id -i ~/.ssh/id_rsa.pub " + node.getHostName());
		}
	}
}