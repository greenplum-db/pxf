package org.greenplum.pxf.automation.features.extension;

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
    private String location;
    private String location_multi;

    @Override
    public void beforeClass() throws Exception {
        super.beforeClass();
        gpdb.dropDataBase("pxfautomation_extension", true, true);
        gpdb.createDataBase("pxfautomation_extension", false);
        gpdb.setDb("pxfautomation_extension");
        gpdb.connectToDataBase("pxfautomation_extension");

        Table smallData = getSmallData("", 10);

        location = hdfs.getWorkingDirectory() + "/upgrade-test-data.txt";
        hdfs.writeTableToFile(location, smallData, ",");

        location_multi = hdfs.getWorkingDirectory() + "/upgrade-test-data_multibyte.txt";
        hdfs.writeTableToFile(location_multi, smallData, "停");
    }

    @Override
    public void afterClass() throws Exception {
        gpdb.connectToDataBase("pxfautomation");
        gpdb.dropDataBase("pxfautomation_extension", true, true);
        super.afterClass();
    }

    @Override
    public void beforeMethod() throws Exception {
        gpdb.setDb("pxfautomation_extension");
        gpdb.connectToDataBase("pxfautomation_extension");
        gpdb.runQuery("DROP EXTENSION IF EXISTS pxf CASCADE", true, false);
    }

    @Test(groups = {"gpdb", "pxfExtensionVersion2_1"})
    public void testPxfCreateExtension() throws Exception {
        gpdb.runQuery("CREATE EXTENSION pxf");
        // create a regular external table
        createReadablePxfTable("default", location, false);
        // create an external table with the multibyte formatter
        createReadablePxfTable("default", location_multi, true);
        runSqlTest("features/extension_tests/create_extension");
    }

    @Test(groups = {"pxfExtensionVersion2"})
    public void testPxfCreateExtensionOldRPM() throws Exception {
        gpdb.runQuery("CREATE EXTENSION pxf");
        // create a regular external table
        createReadablePxfTable("default", location, false);
        // create an external table with the multibyte formatter
        createReadablePxfTable("default", location_multi, true);
        runSqlTest("features/extension_tests/create_extension_rpm");
    }

    @Test(groups = {"gpdb", "pxfExtensionVersion2_1"})
    public void testPxfUpgrade() throws Exception {
        gpdb.runQuery("CREATE EXTENSION pxf VERSION '2.0'");
        createReadablePxfTable("default", location, false);
        runSqlTest("features/extension_tests/upgrade/step_1_create_extension_with_older_pxf_version");

        // create an external table with the multibyte formatter
        createReadablePxfTable("default", location_multi, true);
        gpdb.runQuery("ALTER EXTENSION pxf UPDATE");
        runSqlTest("features/extension_tests/upgrade/step_2_after_alter_extension");
    }

    @Test(groups = {"gpdb", "pxfExtensionVersion2_1"})
    public void testPxfExplicitUpgrade() throws Exception {
        gpdb.runQuery("CREATE EXTENSION pxf VERSION '2.0'");
        createReadablePxfTable("default", location, false);
        runSqlTest("features/extension_tests/explicit_upgrade/step_1_create_extension_with_older_pxf_version");

        // create an external table with the multibyte formatter
        createReadablePxfTable("default", location_multi, true);
        gpdb.runQuery("ALTER EXTENSION pxf UPDATE TO '2.1'");
        runSqlTest("features/extension_tests/explicit_upgrade/step_2_after_alter_extension");
    }

    @Test(groups = {"gpdb", "pxfExtensionVersion2_1"})
    public void testPxfDowngrade() throws Exception {
        gpdb.runQuery("CREATE EXTENSION pxf");

        createReadablePxfTable("default", location, false);
        // create an external table with the multibyte formatter
        createReadablePxfTable("default", location_multi, true);
        runSqlTest("features/extension_tests/downgrade/step_1_create_extension");

        gpdb.runQuery("ALTER EXTENSION pxf UPDATE TO '2.0'");
        runSqlTest("features/extension_tests/downgrade/step_2_after_alter_extension_downgrade");
    }

    @Test(groups = {"gpdb", "pxfExtensionVersion2_1"})
    public void testPxfDowngradeThenUpgradeAgain() throws Exception {
        gpdb.runQuery("CREATE EXTENSION pxf");

        createReadablePxfTable("default", location, false);
        // create an external table with the multibyte formatter
        createReadablePxfTable("default", location_multi, true);
        runSqlTest("features/extension_tests/downgrade_then_upgrade/step_1_check_extension");

        gpdb.runQuery("ALTER EXTENSION pxf UPDATE TO '2.0'");
        runSqlTest("features/extension_tests/downgrade_then_upgrade/step_2_after_alter_extension_downgrade");

        gpdb.runQuery("ALTER EXTENSION pxf UPDATE TO '2.1'");
        runSqlTest("features/extension_tests/downgrade_then_upgrade/step_3_after_alter_extension_upgrade");
    }

    private void createReadablePxfTable(String serverName, String location, boolean multi) throws Exception {
        if (multi) {
            externalTable = TableFactory.getPxfReadableTextTable("pxf_upgrade_test_multibyte", FIELDS, location, null);
            externalTable.setFormat("CUSTOM");
            externalTable.setFormatter("pxfdelimited_import");
            externalTable.addFormatterOption("delimiter='停'");
        } else {
            externalTable = TableFactory.getPxfReadableTextTable("pxf_upgrade_test", FIELDS, location, ",");
        }
        externalTable.setHost(pxfHost);
        externalTable.setPort(pxfPort);
        externalTable.setServer("SERVER=" + serverName);
        gpdb.createTableAndVerify(externalTable);
    }

}
