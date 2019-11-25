package org.greenplum.pxf.automation.components.cluster;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;

import org.greenplum.pxf.automation.components.cluster.installer.nodes.Node;
import org.greenplum.pxf.automation.components.common.cli.ShellCommandErrorException;
import org.greenplum.pxf.automation.utils.jsystem.report.ReportUtils;

/**
 * SingleCluster system object using SingleCluster scripts for cluster functionality.
 */
public class SingleCluster extends PhdCluster {

    public SingleCluster() {
    }

    public SingleCluster(boolean silentReport) {
        super(silentReport);
    }

    @Override
    public void init() throws Exception {
        // if user not injected, get from "GPHD_ROOT" env var
        if (StringUtils.isEmpty(getPhdRoot())) {
            setPhdRoot(System.getenv("GPHD_ROOT"));
        }
        // if after setting var env, still no value, throw exception
        if (StringUtils.isEmpty(getPhdRoot()) || getPhdRoot().equals("null")) {
            throw new Exception(getClass().getSimpleName() + ": Illegal Cluster Folder: please define GPHD_ROOT");
        }

        super.init();

        runCommand("cd " + getPhdRoot());

        // set hive base hdfs directory for SC cluster
        if (StringUtils.isEmpty(getHiveBaseHdfsDirectory())) {
            setHiveBaseHdfsDirectory("/hive/warehouse/");
        }
    }

    @Override
    public void startHiveServer() throws Exception {
        ReportUtils.startLevel(report, getClass(), "Start Hive Server2");
        runCommand(getPhdRoot() + "/bin/hive-service.sh hiveserver2 start");
        ReportUtils.stopLevel(report);
    }

    @Override
    public void start(EnumClusterServices service) throws Exception {
        ReportUtils.startLevel(report, getClass(), "Start " + service);

        // treat standalone PXF case separately
        if (service == EnumClusterServices.pxf && getPxfHome() != null) {
            runCommand(getPxfHome() + "/bin/pxf start");
        } else {
            String serviceName = service.toString();
            runCommand(getPhdRoot() + "/bin/start-" + serviceName + ".sh");
        }
        // stabilization
        Thread.sleep(_2_SECONDS);
        ReportUtils.stopLevel(report);
    }

    @Override
    public void stop(EnumClusterServices service) throws Exception {
        ReportUtils.startLevel(report, getClass(), "Stop " + service);

        // treat standalone PXF case separately
        if (service == EnumClusterServices.pxf && getPxfHome() != null) {
            runCommand(getPxfHome() + "/bin/pxf stop");
        } else {
            String serviceName = service.toString();
            runCommand(getPhdRoot() + "/bin/stop-" + serviceName + ".sh");
        }
        // stabilization
        Thread.sleep(_2_SECONDS);
        ReportUtils.stopLevel(report);
    }

    /**
     * Restart a SingleCluster service, afterwards check if up
     */
    @Override
    public void restart(EnumClusterServices service) throws Exception {
        // currently singlecluster restart scripts supports only PXF and GPHD
        if ((!service.equals(EnumClusterServices.pxf)) && (!service.equals(EnumClusterServices.gphd))) {
            throw new UnsupportedOperationException("SingleCluster -> restart is not supported for " + service.toString() + " service");
        }
        ReportUtils.startLevel(report, getClass(), "Restart " + service);

        // treat standalone PXF case separately
        if (service == EnumClusterServices.pxf && getPxfHome() != null) {
            runCommand(getPxfHome() + "/bin/pxf restart");
        } else {
            String serviceName = service.toString();
            runCommand(getPhdRoot() + "/bin/restart-" + serviceName + ".sh");
        }
        // stabilization
        Thread.sleep(_2_SECONDS);
        ReportUtils.stopLevel(report);
    }

    /**
     * check if Cluster service is up<br>
     * TODO: use singlecluster script when available
     */
    @Override
    public boolean isUp(EnumClusterServices component) throws Exception {
        ReportUtils.startLevel(report, getClass(), "Check " + component.toString() + " is Up");
        boolean result = false;

        // check service is up according to its required processes
        switch (component) {
        case hdfs:
            result = isComponentUp(new EnumScProcesses[] { EnumScProcesses.DataNode, EnumScProcesses.NameNode });
            break;
        case hive:
            result = isComponentUp(new EnumScProcesses[] { EnumScProcesses.DataNode, EnumScProcesses.NameNode, EnumScProcesses.RunJar });
            break;
        case pxf:
            result = isComponentUp(new EnumScProcesses[] { EnumScProcesses.Bootstrap });
            break;
        default:
            result = isComponentUp(EnumScProcesses.values());
            break;
        }

        ReportUtils.report(report, getClass(), "Required Processes are " + ((result) ? " Up" : " Not all up"));
        ReportUtils.stopLevel(report);

        return result;
    }

    /**
     * check if required process are up and appear as defined in the EnumScProcesses enum<br>
     * TODO: use singlecluster script when available
     *
     * @param proccessesToCheck
     * @return true if component is up
     * @throws IOException
     * @throws ShellCommandErrorException
     */
    private boolean isComponentUp(EnumScProcesses[] proccessesToCheck) throws IOException, ShellCommandErrorException {
        // use process map to map all process return from jps command:
        // <process-name, amount-of-instances>
        HashMap<String, Integer> processesMap = getProcessMap();

        // run over given required process array and check if in map includes required amount
        for (int i = 0; i < proccessesToCheck.length; i++) {
            ReportUtils.report(report, getClass(), "Check: " + proccessesToCheck[i].toString());
            // try to get the process key from map if not in there return false
            Integer value = processesMap.get(proccessesToCheck[i].toString());
            if (value == null) {
                report.stopLevel();
                return false;
            }
            // if the process is in the map check amount, if not according to enum, return false
            int amount = value.intValue();

            if (amount != proccessesToCheck[i].getInstances()) {
                report.stopLevel();
                return false;
            }
        }
        // everything is cool. return true.
        return true;
    }

    /**
     * @return Map of running cluster processes <process-name, amount-of-instances>
     * @throws IOException
     * @throws ShellCommandErrorException
     */
    // TODO: remove method when using singlecluster script
    private HashMap<String, Integer> getProcessMap() throws IOException, ShellCommandErrorException {
        // run jps command
        runCommand("jps");
        // get result from command
        String cmdResult = getLastCmdResult();
        // split according to line separator into String array
        String[] splitResults = cmdResult.split(System.getProperty("line.separator"));
        // create map to store results
        HashMap<String, Integer> map = new HashMap<>();
        // go over split results from jps command
        for (int i = 0; i < splitResults.length; i++) {
            String currentSplitResult = null;
            try {
                // get the process name
                currentSplitResult = splitResults[i].split(" ")[1].trim();
            } catch (Exception e) {
                continue;
            }
            // if process already in map just increment the count
            int value = 1;
            if (map.get(currentSplitResult) != null) {
                value = map.get(currentSplitResult).intValue();
                value++;
            }
            // put in the results map
            map.put(currentSplitResult, Integer.valueOf(value));
        }
        return map;
    }

    // TODO: remove enum when using singlecluster script
    public enum EnumScProcesses {
        DataNode(3),
        HMaster(1),
        HRegionServer(3),
        NameNode(1),
        NodeManager(3),
        ResourceManager(1),
        RunJar(1),
        Bootstrap(3);

        private int instances;

        private EnumScProcesses(int instances) {
            this.instances = instances;
        }

        public int getInstances() {
            return instances;
        }
    }

    @Override
    public void stopHiveServer() throws Exception {
        ReportUtils.startLevel(report, getClass(), "Stop Hive Server2");
        runCommand(getPhdRoot() + "/bin/hive-service.sh hiveserver2 stop");
        ReportUtils.stopLevel(report);
    }

    /**
     * Fetch PXF configuration to targetDirectory
     */
    @Override
    public void fetchConfiguration(String targetDirectory) throws Exception {
        ReportUtils.startLevel(report, getClass(), "Fetch Configuration from Cluster to " + targetDirectory);
        // currently copy only the pxf-conf content to the temp directory
        FileUtils.copyDirectory(new File(getPxfConfLocation()), new File(getTempClusterConfDirectory()));
        ReportUtils.stopLevel(report);
    }

    /**
     * copy given file to target folder in cluster
     */
    @Override
    public void copyFileToNodes(String file, String target) throws Exception {
        copyFileToNodes(file, target, false, false);
    }

    @Override
    public void copyFileToNodes(String file, String target, boolean createTargetDirectory, boolean sudo) throws Exception {
        ReportUtils.startLevel(report, getClass(), "Copy from " + file + " to Nodes: " + target);
        if (createTargetDirectory) {
            new File(target).mkdirs();
        }
        File fileToCopy = new File(file);
        FileUtils.copyFile(new File(file), new File(target + "/" + fileToCopy.getName()), true);
        ReportUtils.stopLevel(report);
    }

    /**
     * Delete file from single cluster
     */
    @Override
    public void deleteFileFromNodes(String targetFile, boolean sudo) throws Exception {
        ReportUtils.startLevel(report, getClass(), "Delete " + targetFile + " from Nodes");
        File file = new File(targetFile);
        file.delete();
        ReportUtils.stopLevel(report);
    }

    @Override
    public void runCommandOnAllNodes(String command) throws Exception {
        throw new UnsupportedOperationException("runCommandOnAllNodes is not supported over SingleCluster");
    }

    @Override
    public void runCommandOnNodes(List<Node> nodes, String command) throws Exception {
        throw new UnsupportedOperationException("runCommandOnNodes is not supported over SingleCluster");
    }
}
