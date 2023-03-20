package org.greenplum.pxf.automation.features.pxfupgrade;

import org.greenplum.pxf.automation.BaseFunctionality;
import org.greenplum.pxf.automation.structures.tables.basic.Table;
import org.greenplum.pxf.automation.structures.tables.pxf.ReadableExternalTable;
import org.greenplum.pxf.automation.structures.tables.utils.TableFactory;
import org.testng.SkipException;
import org.testng.annotations.Test;

public class PxfupgradeTest extends BaseFunctionality {

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
        gpdb.createDataBase("pxfupgradetest", false);
    }

    @Override
    protected void afterMethod() throws Exception {
        if (gpdb != null) {
            gpdb.dropDataBase("pxfupgradetest", true, false);
        }
        super.afterMethod();
    }

    @Test(groups = {"features", "gpdb"})
    public void testPXFInstallScenario() throws Exception {
        gpdb.setDb("pxfupgradetest");
        // drop the existing extension
        gpdb.runQueryWithExpectedWarning("DROP EXTENSION pxf CASCADE", "drop cascades to external table pxf_upgrade_test", false, true);
        runTincTest("pxf.features.pxfupgrade.install.step_1_no_pxf.runTest");

        gpdb.runQuery("CREATE EXTENSION pxf");
        String location = prepareData();
        createReadablePxfTable("default", location);
        runTincTest("pxf.features.pxfupgrade.install.step_2_create_extension.runTest");

    }

    @Test(groups = {"features", "gpdb"})
    public void testPXFUpgradeScenario() throws Exception {
        gpdb.setDb("pxfupgradetest");
        // drop the existing extension
        gpdb.runQueryWithExpectedWarning("DROP EXTENSION pxf CASCADE", "drop cascades to external table pxf_upgrade_test", false, true);
        runTincTest("pxf.features.pxfupgrade.upgrade.step_1_no_pxf.runTest");

        gpdb.runQuery("CREATE EXTENSION pxf VERSION \"2.0\"");
        String location = prepareData();
        createReadablePxfTable("default", location);
        runTincTest("pxf.features.pxfupgrade.upgrade.step_2_create_extension_with_older_pxf_version.runTest");

        gpdb.runQuery("ALTER EXTENSION pxf UPDATE");
        runTincTest("pxf.features.pxfupgrade.upgrade.step_3_after_alter_extension.runTest");

    }

    @Test(groups = {"features", "gpdb"})
    public void testPXFUpgradeScenarioExplicitVersion() throws Exception {
        gpdb.setDb("pxfupgradetest");
        // drop the existing extension
        gpdb.runQueryWithExpectedWarning("DROP EXTENSION pxf CASCADE", "drop cascades to external table pxf_upgrade_test", false, true);
        runTincTest("pxf.features.pxfupgrade.explicit_upgrade.step_1_no_pxf.runTest");

        gpdb.runQuery("CREATE EXTENSION pxf VERSION \"2.0\"");
        String location = prepareData();
        createReadablePxfTable("default", location);
        runTincTest("pxf.features.pxfupgrade.explicit_upgrade.step_2_create_extension_with_older_pxf_version.runTest");

        gpdb.runQuery("ALTER EXTENSION pxf UPDATE TO \"2.1\"");
        runTincTest("pxf.features.pxfupgrade.explicit_upgrade.step_3_after_alter_extension.runTest");

    }
    private String prepareData() throws Exception {
        Table smallData = getSmallData("", 10);
        String location = hdfs.getWorkingDirectory() + "/gpupgrade-test-data.txt";
        hdfs.writeTableToFile(location, smallData, ",");

        return location;
    }

    private void createReadablePxfTable(String serverName, String location) throws Exception {
        externalTable = TableFactory.getPxfReadableTextTable("pxf_upgrade_test", FIELDS, location, ",");
        externalTable.setHost(pxfHost);
        externalTable.setPort(pxfPort);
        externalTable.setServer("SERVER=" + serverName);
        gpdb.createTableAndVerify(externalTable);
    }

}
