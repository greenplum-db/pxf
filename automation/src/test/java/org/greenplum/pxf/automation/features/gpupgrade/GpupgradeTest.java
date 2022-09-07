package org.greenplum.pxf.automation.features.gpupgrade;

import org.greenplum.pxf.automation.BaseFunctionality;
import org.greenplum.pxf.automation.structures.tables.basic.Table;
import org.greenplum.pxf.automation.structures.tables.pxf.ReadableExternalTable;
import org.greenplum.pxf.automation.structures.tables.utils.TableFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

public class GpupgradeTest extends BaseFunctionality {

    public static final String[] FIELDS = {
            "name    text",
            "num     integer",
            "dub     double precision",
            "longNum bigint",
            "bool    boolean"
    };

    private ReadableExternalTable externalTable;

    @Override
    protected void beforeMethod() throws Exception {
        super.beforeMethod();
        String location = prepareData();
        createReadablePxfTable("default", location);
    }

    @Override
    protected void afterMethod() throws Exception {
        if (gpdb != null) {
            gpdb.dropTable(externalTable, true);
        }
        super.afterMethod();
    }

    @Test(groups = {"features", "gpdb"})
    public void testGpdbUpgradeScenario() throws Exception {
        runTincTest("pxf.features.gpupgrade.step_1_before_running_pxf_pre_gpupgrade.runTest");

        cluster.runCommand("pxf-pre-gpupgrade");
        runTincTest("pxf.features.gpupgrade.step_2_after_running_pxf_pre_gpupgrade.runTest");

        cluster.runCommand("pxf-post-gpupgrade");
        runTincTest("pxf.features.gpupgrade.step_3_after_running_pxf_post_gpupgrade.runTest");
    }

    private String prepareData() throws Exception {
        Table smallData = getSmallData("", 10);
        String location = hdfs.getWorkingDirectory() + "/gpupgrade-test-data.txt";
        hdfs.writeTableToFile(location, smallData, ",");

        return location;
    }

    private void createReadablePxfTable(String serverName, String location) throws Exception {
        externalTable = TableFactory.getPxfReadableTextTable("pxf_gpupgrade_test", FIELDS, location, ",");
        externalTable.setHost(pxfHost);
        externalTable.setPort(pxfPort);
        externalTable.setServer("SERVER=" + serverName);
        gpdb.createTableAndVerify(externalTable);
    }

}
