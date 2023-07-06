package org.greenplum.pxf.automation.components.cluster;

import java.io.File;
import java.net.Inet4Address;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import jsystem.framework.report.Reporter;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;

import org.greenplum.pxf.automation.components.cluster.installer.nodes.CoordinatorNode;
import org.greenplum.pxf.automation.components.cluster.installer.nodes.Node;
import org.greenplum.pxf.automation.components.cluster.installer.nodes.StandByNode;
import org.greenplum.pxf.automation.components.common.cli.ParallelShellActions;
import org.greenplum.pxf.automation.utils.jsystem.report.ReportUtils;

/**
 * Describes multiple nodes cluster. {@link Node} array holds Nodes with ssh connection to every
 * Node in the cluster. Allows to perform parallel operations over a cluster (start/stop/restart
 * services)
 */
public class MultiNodeCluster extends PhdCluster {
    // nodes in cluster
    public Node[] nodes;
    // root credentials for cluster
    private String rootUserName = "root";
    private String rootPaswword = "P@ssw0rd";

    public MultiNodeCluster() {

    }

    public MultiNodeCluster(boolean silentReport) {
        super(silentReport);
    }

    @Override
    public void init() throws Exception {
        if (StringUtils.isEmpty(getPhdRoot())) {
            setPhdRoot("/Users/pivotal/workspace/singlecluster");
        }
        // path to pxf conf in general conf
        setPathToPxfConfInGeneralConf("pxf/conf");
        // set hive base hdfs directory for PHD cluster
        if (StringUtils.isEmpty(getHiveBaseHdfsDirectory())) {
            setHiveBaseHdfsDirectory("/hive/warehouse/");
        }

        super.init();

        // check if nodes exist
        if (nodes == null || nodes.length == 0) {
            ReportUtils.report(report, getClass(), "No nodes in cluster", Reporter.FAIL);
        }
    }

    @Override
    public void startHiveServer() throws Exception {
        throw new UnsupportedOperationException("Start Hive Server is not supported yet");
    }

    @Override
    public void stopHiveServer() throws Exception {
        throw new UnsupportedOperationException("Stop Hive Server is not supported yet");
    }

    @Override
    public void start(EnumClusterServices service) throws Exception {
        handleOperation("start", service);
    }

    @Override
    public void stop(EnumClusterServices service) throws Exception {
        handleOperation("stop", service);
    }

    @Override
    public void restart(EnumClusterServices service) throws Exception {
        handleOperation("restart", service);
    }

    /**
     * Inside method to handle similar cluster operations
     *
     * @param operation currently "stop", "start"
     * @param service required cluster service
     * @throws Exception
     */
    private void handleOperation(String operation, EnumClusterServices service) throws Exception {
        // not supporting null service or all services
        if (service == null || service.equals(EnumClusterServices.gphd)) {
            // get the unsupported case for exception message
            String serviceName = "null";
            if (service != null) {
                serviceName = service.toString();
            }
            throw new UnsupportedOperationException("Trying to perform operation: " + operation + " on unsupported service: " + serviceName);
        }
        // build command
        String command;
        if (service == EnumClusterServices.pxf && getPxfHome() != null) {
            command = "PXF_BASE=" + getPxfBase() + " " + getPxfHome() + "/bin/pxf " + operation;
        } else {
            command = "sudo -s /etc/init.d/" + service.getServiceName() + " " + operation;
        }
        // run on relevant nodes
        ReportUtils.startLevel(report, getClass(), operation + " " + service.toString());
        // get List of Nodes to run operation on according to service
        List<Node> nodesListByService;
        switch (service) {
        case hive:
            nodesListByService = getNode(CoordinatorNode.class, service);
            break;
        case pxf:
            nodesListByService = getNode(service).stream()
                    .filter(n -> n instanceof StandByNode)
                    .collect(Collectors.toList());
            break;
        default:
            nodesListByService = getNode(service);
            break;
        }
        // run operation on Node List
        runCommandOnNodes(nodesListByService, command);
        ReportUtils.stopLevel(report);
    }

    @Override
    public boolean isUp(EnumClusterServices service) throws Exception {
        // TODO: need to implement, for now return true;
        return true;
    }

    /**
     * Fetches configuration files from remote admin node to local target directory (where code is
     * running)
     *
     * @throws Exception if fetching configuration had failed.
     */
    @Override
    public void fetchConfiguration(String targetDirectory) throws Exception {
        ReportUtils.startLevel(report, getClass(), "Fetch Configuration from Cluster to " + targetDirectory);
        // clean confDirectory in remote admin node before fetch into it
        String tempClusterConfDirectory = getTempClusterConfDirectory();
        deleteDirectory(tempClusterConfDirectory);
        // currently copy only the pxf-conf to tempClusterConfDirectory
        FileUtils.copyDirectory(new File(getPxfConfLocation()), new File(tempClusterConfDirectory + "/" + getPathToPxfConfInGeneralConf()));
        // if current node is not pxf node, it requires copying pxf/conf directory from the pxf node
        Node pxfNode = getNode(CoordinatorNode.class, EnumClusterServices.pxf).get(0);
        // if pxf node is same as local node, then pxf conf is already there, skip pxf conf copying
        String localHostName = Inet4Address.getLocalHost().getHostName();
        if (!localHostName.equals(pxfNode.getHost())) {
            File pathToLocalPxfConfigDir = new File(tempClusterConfDirectory + "/" + getPathToPxfConfInGeneralConf());
            if (pathToLocalPxfConfigDir.exists()) {
                FileUtils.deleteDirectory(pathToLocalPxfConfigDir);
            }
            pathToLocalPxfConfigDir.getParentFile().mkdirs();
            copyFromRemoteMachine(pxfNode.getUserName(), pxfNode.getPassword(), pxfNode.getHost(), getPxfConfLocation(), pathToLocalPxfConfigDir.getParentFile().getAbsolutePath());
        }
        ReportUtils.stopLevel(report);
    }

    /**
     * Remotely copy the file to target in all machines
     */
    @Override
    public void copyFileToNodes(String file, String target, boolean createTargetDirectory, boolean sudo) throws Exception {
        ReportUtils.startLevel(report, getClass(), "Copy File: " + file + " to nodes");
        String escapedFile = escapeSpaces(file);
        String escapedTarget = escapeSpaces(target);
        if (createTargetDirectory) {
            String cmd = "mkdir -p " + escapedTarget;
            if (sudo) {
                cmd = "sudo -s " + cmd;
            }
            runCommandOnAllNodes(cmd);
        }
        if (sudo) {
            // first copy to /tmp, then with sudo to target
            ParallelShellActions.copyFile(this, Arrays.asList(nodes), escapedFile, "/tmp");
            String fileName = file.substring(file.lastIndexOf("/") + 1);
            runCommandOnAllNodes("sudo -s mv /tmp/" + fileName + " " + escapedTarget);
        } else {
            ParallelShellActions.copyFile(this, Arrays.asList(nodes), escapedFile, escapedTarget);
        }
        report.stopLevel();
    }

    @Override
    public void copyFileToNodes(String file, String target) throws Exception {
        copyFileToNodes(file, target, false, false);
    }

    /**
     * Deletes targetFile from cluster
     */
    @Override
    public void deleteFileFromNodes(String targetFile, boolean sudo) throws Exception {
        ReportUtils.startLevel(report, getClass(), "Delete File: " + targetFile + " from nodes");
        String escapedTargetFile = escapeSpaces(targetFile);
        for (Node pxfNode : nodes) {
            deleteFileFromRemoteMachine(pxfNode.getUserName(), pxfNode.getPassword(), pxfNode.getHost(), escapedTargetFile, sudo);
        }
        ReportUtils.stopLevel(report);
    }

    @Override
    public void runCommandOnAllNodes(String command) throws Exception {
        runCommandOnNodes(Arrays.asList(nodes), command);
    }

    @Override
    public void runCommandOnNodes(List<Node> nodes, String command) throws Exception {
        ReportUtils.startLevel(report, getClass(), "Run Command: " + command + " on " + nodes.toString());
        ParallelShellActions.runParallelCommand(nodes, command);
        ReportUtils.stopLevel(report);
    }

    /**
     * Gets node List from nodes array according to {@link Node} type and serviceType
     *
     * @param nodeType {@link CoordinatorNode} or {@link StandByNode}
     * @param serviceType required service type to locate in nodes
     * @return list of nodes of given nodeType and serviceType
     */
    public List<Node> getNode(Class<? extends Node> nodeType, EnumClusterServices serviceType) {
        ReportUtils.report(report, getClass(), "Get nodes for " + serviceType.toString() + " service");
        List<Node> resultList = new ArrayList<>();
        for (Node node : nodes) {
            ReportUtils.report(report, getClass(), node.toString());
            if (serviceType == null || node.getServicesList().contains(serviceType.toString())) {
                if (nodeType == null || node.getClass().isAssignableFrom(nodeType)) {
                    resultList.add(node);
                }
            }
        }
        return resultList;
    }

    /**
     * Gets node List from nodes array according to serviceType
     *
     * @param serviceType required service type to locate in nodes
     * @return list of nodes with given serviceType
     */
    public List<Node> getNode(EnumClusterServices serviceType) {
        return getNode(null, serviceType);
    }

    public String getRootPaswword() {
        return rootPaswword;
    }

    public void setRootPaswword(String rootPaswword) {
        this.rootPaswword = rootPaswword;
    }

    public String getRootUserName() {
        return rootUserName;
    }

    public void setRootUserName(String rootUserName) {
        this.rootUserName = rootUserName;
    }

    public Node[] getNodes() {
        return nodes;
    }

    public void setNodes(Node[] nodes) {
        this.nodes = nodes;
    }

    /**
     * escape spaces in file name, so command line commands will work.
     *
     * @param file
     * @return escaped file name
     */
    private String escapeSpaces(String file) {
        if (StringUtils.isEmpty(file))
            return file;
        return file.replace(" ", "\\ ");
    }
}
