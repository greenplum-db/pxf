package org.greenplum.pxf.automation.features.filterpushdown;

import java.io.File;

import org.greenplum.pxf.automation.components.cluster.PhdCluster;
import org.greenplum.pxf.automation.structures.tables.pxf.ReadableExternalTable;
import org.testng.annotations.Test;

import org.greenplum.pxf.automation.features.BaseFeature;

/** Functional PXF filter pushdown cases */
public class FilterPushDownTest extends BaseFeature {

    String testPackageLocation = "/com/pxf/automation/testplugin/";
    String testPackage = "org.greenplum.pxf.automation.testplugin.";

    @Override
    protected void beforeClass() throws Exception {
        String newPath = "/tmp/publicstage/pxf";
        // copy additional plugins classes to cluster nodes, used for filter pushdown cases
        cluster.copyFileToNodes(new File("target/classes/" + testPackageLocation + "FilterVerifyFragmenter.class").getAbsolutePath(), newPath + testPackageLocation, true, false);
        cluster.copyFileToNodes(new File("target/classes/" + testPackageLocation + "FilterVerifyFragmenter$TestFilterBuilder.class").getAbsolutePath(), newPath + testPackageLocation, true, false);
        cluster.copyFileToNodes(new File("target/classes/" + testPackageLocation + "UserDataVerifyAccessor.class").getAbsolutePath(), newPath + testPackageLocation, true, false);
        // add new path to classpath file and restart PXF service
        cluster.addPathToPxfClassPath(newPath);
        cluster.restart(PhdCluster.EnumClusterServices.pxf);
    }

    @Override
    protected void afterClass() throws Exception {
        super.afterClass();
    }

    /**
     * Check PXF receive the expected filter string from hawq/gpdb.
     * Column delimiter is ",".
     *
     * @throws Exception
     */
    @Test(groups = {"features", "gpdb"})
    public void checkFilterPushDown() throws Exception {

        // Create PXF external table for filter testing
        ReadableExternalTable pxfExternalTable = new ReadableExternalTable("test_filter", new String[] {
                "t0    text",
                "a1    integer",
                "b2    boolean",
                "filterValue  text"
        }, "dummy_path","TEXT");

        pxfExternalTable.setFragmenter(testPackage + "FilterVerifyFragmenter");
        pxfExternalTable.setAccessor(testPackage + "UserDataVerifyAccessor");
        pxfExternalTable.setResolver("org.greenplum.pxf.plugins.hdfs.StringPassResolver");
        pxfExternalTable.setDelimiter(",");
        pxfExternalTable.setUserParameters(new String[] { "delimiter=," });
        pxfExternalTable.setHost(pxfHost);
        pxfExternalTable.setPort(pxfPort);

        hawq.createTableAndVerify(pxfExternalTable);

        runTincTest("pxf.features.filterpushdown.checkFilterPushDown.runTest");
    }

    /**
     * Check PXF receive the expected filter string from hawq/gpdb.
     * Column delimiter is ",".
     *
     * @throws Exception
     */
    @Test(groups = {"features", "gpdb"})
    public void checkFilterPushDownDisabled() throws Exception {

        // Create PXF external table for filter testing
        ReadableExternalTable pxfExternalTable = new ReadableExternalTable("test_filter", new String[] {
                "t0    text",
                "a1    integer",
                "b2    boolean",
                "filterValue  text"
        }, "dummy_path","TEXT");

        pxfExternalTable.setFragmenter(testPackage + "FilterVerifyFragmenter");
        pxfExternalTable.setAccessor(testPackage + "UserDataVerifyAccessor");
        pxfExternalTable.setResolver("org.greenplum.pxf.plugins.hdfs.StringPassResolver");
        pxfExternalTable.setDelimiter(",");
        pxfExternalTable.setUserParameters(new String[] { "delimiter=," });
        pxfExternalTable.setHost(pxfHost);
        pxfExternalTable.setPort(pxfPort);

        hawq.createTableAndVerify(pxfExternalTable);

        runTincTest("pxf.features.filterpushdown.checkFilterPushDownDisabled.runTest");
    }

    /**
     * Check PXF receive the expected filter string
     * Column delimiter is hexadecimal
     *
     * @throws Exception
     */
    @Test(groups = {"features", "gpdb"})
    public void checkFilterStringHexDelimiter() throws Exception {

        // Create PXF external table for filter testing
        ReadableExternalTable pxfExternalTable = new ReadableExternalTable("test_filter", new String[] {
                "t0    text",
                "a1    integer",
                "b2    boolean",
                "filterValue  text"
        }, "dummy_path","TEXT");

        pxfExternalTable.setFragmenter(testPackage + "FilterVerifyFragmenter");
        pxfExternalTable.setAccessor(testPackage + "UserDataVerifyAccessor");
        pxfExternalTable.setResolver("org.greenplum.pxf.plugins.hdfs.StringPassResolver");
        pxfExternalTable.setDelimiter("E'\\x01'");
        pxfExternalTable.setUserParameters(new String[] { "delimiter=\\x01" });
        pxfExternalTable.setHost(pxfHost);
        pxfExternalTable.setPort(pxfPort);

        hawq.createTableAndVerify(pxfExternalTable);

        runTincTest("pxf.features.filterpushdown.checkFilterPushDownHexDelimiter.runTest");
    }
}
