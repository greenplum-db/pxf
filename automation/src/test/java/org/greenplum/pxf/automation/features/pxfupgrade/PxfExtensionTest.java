package org.greenplum.pxf.automation.features.pxfupgrade;

import org.greenplum.pxf.automation.BaseFunctionality;
import org.greenplum.pxf.automation.structures.tables.basic.Table;
import org.greenplum.pxf.automation.structures.tables.pxf.ReadableExternalTable;
import org.greenplum.pxf.automation.structures.tables.utils.TableFactory;
import org.testng.annotations.Test;

public class PxfExtensionTest extends BaseFunctionality {

    public static final String[] FIELDS = {
            "name    text",
            "num     integer",
            "dub     double precision",
            "longNum bigint",
            "bool    boolean"
    };

    private ReadableExternalTable externalTable;
    private String lastDb;

    @Override
    public void beforeClass() throws Exception {
        super.beforeClass();
        lastDb = gpdb.getDb();
        gpdb.dropDataBase("extension_tests", true, true);
        gpdb.createDataBase("extension_tests", false);
    }

    @Override
    protected void afterClass() throws Exception {
        if (gpdb != null) {
            gpdb.dropDataBase("extension_tests", true, false);
            gpdb.setDb(lastDb);
        }
        super.afterClass();
    }
    @Test(groups = {"features", "gpdb"})
    public void testPxfInstallScenario() throws Exception {
        // drop the existing extension
        gpdb.runQueryWithExpectedWarning("DROP EXTENSION pxf CASCADE", "drop cascades to *", true, true);
        runTincTest("pxf.features.extension_tests.install.step_1_no_pxf.runTest");

        gpdb.runQuery("CREATE EXTENSION pxf");
        // create a regular external table
        String location = prepareData(false);
        createReadablePxfTable("default", location, false);
        // create an external table with the multibyte formatter
        String location_multi = prepareData(true);
        createReadablePxfTable("default", location_multi, true);
        runTincTest("pxf.features.extension_tests.install.step_2_create_extension.runTest");
    }

    @Test(groups = {"features", "gpdb"})
    public void testPxfUpgradeScenario() throws Exception {
        // drop the existing extension
        gpdb.runQueryWithExpectedWarning("DROP EXTENSION pxf CASCADE", "drop cascades to *", true, true);
        runTincTest("pxf.features.extension_tests.upgrade.step_1_no_pxf.runTest");

        gpdb.runQuery("CREATE EXTENSION pxf VERSION \'2.0\'");
        String location = prepareData(false);
        createReadablePxfTable("default", location, false);
        runTincTest("pxf.features.extension_tests.upgrade.step_2_create_extension_with_older_pxf_version.runTest");

        // create an external table with the multibyte formatter
        String location_multi = prepareData(true);
        createReadablePxfTable("default", location_multi, true);
        gpdb.runQuery("ALTER EXTENSION pxf UPDATE");
        runTincTest("pxf.features.extension_tests.upgrade.step_3_after_alter_extension.runTest");
    }

    @Test(groups = {"features", "gpdb"})
    public void testPxfUpgradeScenarioExplicitVersion() throws Exception {
        // drop the existing extension
        gpdb.runQueryWithExpectedWarning("DROP EXTENSION pxf CASCADE", "drop cascades to *", true, true);
        runTincTest("pxf.features.extension_tests.explicit_upgrade.step_1_no_pxf.runTest");

        gpdb.runQuery("CREATE EXTENSION pxf VERSION \'2.0\'");
        String location = prepareData(false);
        createReadablePxfTable("default", location, false);
        runTincTest("pxf.features.extension_tests.explicit_upgrade.step_2_create_extension_with_older_pxf_version.runTest");

        // create an external table with the multibyte formatter
        String location_multi = prepareData(true);
        createReadablePxfTable("default", location_multi, true);
        gpdb.runQuery("ALTER EXTENSION pxf UPDATE TO \'2.1\'");
        runTincTest("pxf.features.extension_tests.explicit_upgrade.step_3_after_alter_extension.runTest");
    }

    @Test(groups = {"features", "gpdb"})
    public void testPxfDowngradeScenario() throws Exception {
        // drop the existing extension
        gpdb.runQueryWithExpectedWarning("DROP EXTENSION pxf CASCADE", "drop cascades to *", true, true);
        runTincTest("pxf.features.extension_tests.downgrade.step_1_no_pxf.runTest");

        gpdb.runQuery("CREATE EXTENSION pxf");
        String location = prepareData(false);
        createReadablePxfTable("default", location, false);
        // create an external table with the multibyte formatter
        String location_multi = prepareData(true);
        createReadablePxfTable("default", location_multi, true);
        runTincTest("pxf.features.extension_tests.downgrade.step_2_create_extension.runTest");

        gpdb.runQuery("ALTER EXTENSION pxf UPDATE TO \'2.0\'");
        runTincTest("pxf.features.extension_tests.downgrade.step_3_after_alter_extension_downgrade.runTest");
    }

    @Test(groups = {"features", "gpdb"})
    public void testPxfDowngradeThenUpgradeAgain() throws Exception {
        String location = prepareData(false);
        createReadablePxfTable("default", location, false);
        // create an external table with the multibyte formatter
        String location_multi = prepareData(true);
        createReadablePxfTable("default", location_multi, true);
        runTincTest("pxf.features.extension_tests.downgrade_then_upgrade.step_1_check_extension.runTest");

        gpdb.runQuery("ALTER EXTENSION pxf UPDATE TO \'2.0\'");
        runTincTest("pxf.features.extension_tests.downgrade_then_upgrade.step_2_after_alter_extension_downgrade.runTest");

        gpdb.runQuery("ALTER EXTENSION pxf UPDATE TO \'2.1\'");
        runTincTest("pxf.features.extension_tests.downgrade_then_upgrade.step_3_after_alter_extension_upgrade.runTest");
    }

    private String prepareData(boolean multi) throws Exception {
        Table smallData = getSmallData("", 10);
        String location;
        if (multi) {
            location = hdfs.getWorkingDirectory() + "/gpupgrade-test-data_multibyte.txt";
            hdfs.writeTableToFile(location, smallData, "停");
        } else {
            location = hdfs.getWorkingDirectory() + "/gpupgrade-test-data.txt";
            hdfs.writeTableToFile(location, smallData, ",");
        }

        return location;
    }

    private void createReadablePxfTable(String serverName, String location, boolean multi) throws Exception {
        if (multi) {
            externalTable = TableFactory.getPxfReadableTextTable("pxf_upgrade_test_multibyte", FIELDS, location, "停");
            externalTable.setFormat("CUSTOM");
            externalTable.setFormatter("pxfdelimited_import");
        } else {
            externalTable = TableFactory.getPxfReadableTextTable("pxf_upgrade_test", FIELDS, location, ",");
        }
        externalTable.setHost(pxfHost);
        externalTable.setPort(pxfPort);
        externalTable.setServer("SERVER=" + serverName);
        gpdb.createTableAndVerify(externalTable);
    }

}
