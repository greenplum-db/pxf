package org.greenplum.pxf.automation.features.general;

import annotations.FailsWithFDW;
import org.greenplum.pxf.automation.components.cluster.PhdCluster;
import org.greenplum.pxf.automation.features.BaseFeature;
import org.greenplum.pxf.automation.structures.tables.pxf.ReadableExternalTable;
import org.testng.annotations.Test;

import java.io.File;

/** Tests how failures are handled **/
@FailsWithFDW
public class FailOverTest extends BaseFeature {

    String testPackageLocation = "/org/greenplum/pxf/automation/testplugin/";
    String testPackage = "org.greenplum.pxf.automation.testplugin.";

    @Override
    protected void beforeClass() throws Exception {
        String newPath = "/tmp/publicstage/pxf";
        // copy additional plugins classes to cluster nodes, used for filter pushdown cases
        cluster.copyFileToNodes(new File("target/classes/" + testPackageLocation + "OutOfMemoryFragmenter.class").getAbsolutePath(), newPath + testPackageLocation, true, false);
        // add new path to classpath file and restart PXF service
        cluster.addPathToPxfClassPath(newPath);
        cluster.restart(PhdCluster.EnumClusterServices.pxf);
    }

    @Override
    protected void afterClass() throws Exception {
        super.afterClass();
        // We need to restore the service after it has been stopped
        if (cluster != null) {
            cluster.start(PhdCluster.EnumClusterServices.pxf);
        }
    }

    /**
     * Should stop the JVM by invoking OutOfMemoryFragmenter
     *
     * @throws Exception
     */
    @Test(groups = {"features", "gpdb", "security"})
    public void stopTomcatOnOutOfMemory() throws Exception {

        // Create PXF external table for out of memory testing
        ReadableExternalTable pxfExternalTable = new ReadableExternalTable("test_out_of_memory", new String[] {
                "t0    text",
                "a1    integer",
                "b2    boolean",
                "colprojValue  text"
        }, "dummy_path","TEXT");

        pxfExternalTable.setFragmenter(testPackage + "OutOfMemoryFragmenter");
        pxfExternalTable.setAccessor("org.greenplum.pxf.plugins.hdfs.LineBreakAccessor");
        pxfExternalTable.setResolver("org.greenplum.pxf.plugins.hdfs.StringPassResolver");
        pxfExternalTable.setDelimiter(",");
        pxfExternalTable.setHost(pxfHost);
        pxfExternalTable.setPort(pxfPort);

        gpdb.createTableAndVerify(pxfExternalTable);

        runSqlTest("features/general/outOfMemory");
    }
}
