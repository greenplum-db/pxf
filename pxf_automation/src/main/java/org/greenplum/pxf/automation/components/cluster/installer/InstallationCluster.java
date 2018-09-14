package org.greenplum.pxf.automation.components.cluster.installer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import jsystem.framework.system.SystemObjectImpl;

import org.greenplum.pxf.automation.components.cluster.installer.nodes.InstallationNode;
import org.greenplum.pxf.automation.components.cluster.installer.nodes.MasterNode;
import org.greenplum.pxf.automation.components.cluster.installer.nodes.Node;
import org.greenplum.pxf.automation.components.cluster.installer.nodes.SlaveNode;
import org.greenplum.pxf.automation.components.common.cli.ParallelShellActions;

/**
 * Holds the cluster Node entities used in the cluster. Provides parallel actions over cluster nodes
 * to run commands and copy a file.
 */
public class InstallationCluster extends SystemObjectImpl {
	// Node to install from
	public InstallationNode installtionNode;
	// master Node for the cluster;
	public MasterNode masterNode;
	// slaves nodes for the cluster
	public SlaveNode[] slaveNodes;
	// will hold all nodes expect Installation node
	private List<Node> allNodesList;

	/**
	 * get List of all nodes in the cluster including master node
	 * 
	 * @return {@link List} of all nodes in the cluster
	 */
	public List<Node> getAllNodesList() {
		allNodesList = new ArrayList<Node>();
		allNodesList.add(masterNode);
		for (int i = 0; i < slaveNodes.length; i++) {
			allNodesList.add(slaveNodes[i]);
		}

		return allNodesList;
	}

	/**
	 * @return {@link Node} array of all nodes in the cluster
	 */
	public Node[] getAllNodesArray() {
		List<Node> nodesList = getAllNodesList();
		Node[] nodes = new Node[nodesList.size()];
		nodesList.toArray(nodes);
		return nodes;
	}

	/**
	 * Run parallel command over all nodes in the cluster.
	 * 
	 * @param command command to execute parallel
	 * @throws Exception
	 */
	public void runParallelCommand(String command) throws Exception {
		runParallelCommand(getAllNodesArray(), command);
	}

	/**
	 * Run parallel command over a given {@link Node} array.
	 * 
	 * @param command command to execute parallel
	 * @throws Exception
	 */
	public void runParallelCommand(Node[] nodes, String command) throws Exception {
		ParallelShellActions.runParallelCommand(Arrays.asList(nodes), command);
	}

	/**
	 * Copy file to a given {@link Node} array.
	 * 
	 * @param fromPath local path to copy from
	 * @param toPath remote path to copy to
	 * @throws Exception
	 */
	public void copyFileToNodes(Node[] nodes, String fromPath, String toPath) throws Exception {
		ParallelShellActions.copyFile(installtionNode, Arrays.asList(nodes), fromPath, toPath);
	}

	/**
	 * Copy file to all nodes in cluster.
	 * 
	 * @param fromPath local path to copy from
	 * @param toPath remote path to copy to
	 * @throws Exception
	 */
	public void copyFileToNodes(String fromPath, String toPath) throws Exception {
		copyFileToNodes(getAllNodesArray(), fromPath, toPath);
	}

	public MasterNode getMasterNode() {
		return masterNode;
	}

	public void setMasterNode(MasterNode masterNode) {
		this.masterNode = masterNode;
	}

	public SlaveNode[] getSlaveNodes() {
		return slaveNodes;
	}

	public void setSlaveNodes(SlaveNode[] slaveNodes) {
		this.slaveNodes = slaveNodes;
	}

	public InstallationNode getInstalltionNode() {
		return installtionNode;
	}

	public void setInstalltionNode(InstallationNode installtionNode) {
		this.installtionNode = installtionNode;
	}
}