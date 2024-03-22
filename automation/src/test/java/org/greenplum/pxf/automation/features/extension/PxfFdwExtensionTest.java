package org.greenplum.pxf.automation.features.extension;

import org.greenplum.pxf.automation.BaseFunctionality;
import org.testng.annotations.Test;

public class PxfFdwExtensionTest extends BaseFunctionality {

    @Override
    public void beforeClass() throws Exception {
        super.beforeClass();
        gpdb.dropDataBase("pxfautomation_extension", true, true);
        gpdb.createDataBase("pxfautomation_extension", false);
        gpdb.setDb("pxfautomation_extension");
        gpdb.connectToDataBase("pxfautomation_extension");
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
        gpdb.runQuery("DROP EXTENSION IF EXISTS pxf_fdw CASCADE", true, false);
    }

    @Test(groups = {"pxfFdwExtensionVersion2"})
    public void testPxfCreateExtension() throws Exception {
        gpdb.runQuery("CREATE EXTENSION pxf_fdw");
        runSqlTest("features/fdw_extension_tests/create_extension");
    }

    @Test(groups = {"pxfFdwExtensionVersion1"})
    public void testPxfCreateExtensionOldRPM() throws Exception {
        gpdb.runQuery("CREATE EXTENSION pxf_fdw");
        runSqlTest("features/fdw_extension_tests/create_extension_rpm");
    }

    @Test(groups = {"pxfFdwExtensionVersion2"})
    public void testPxfUpgrade() throws Exception {
        gpdb.runQuery("CREATE EXTENSION pxf_fdw VERSION '1.0'");
        runSqlTest("features/fdw_extension_tests/upgrade/step_1_create_extension_with_older_pxf_version");

        gpdb.runQuery("ALTER EXTENSION pxf_fdw UPDATE");
        runSqlTest("features/fdw_extension_tests/upgrade/step_2_after_alter_extension");
    }

    @Test(groups = {"pxfFdwExtensionVersion2"})
    public void testPxfDowngradeThenUpgradeAgain() throws Exception {
        gpdb.runQuery("CREATE EXTENSION pxf_fdw");
        runSqlTest("features/fdw_extension_tests/downgrade_then_upgrade/step_1_check_extension");

        gpdb.runQuery("ALTER EXTENSION pxf_fdw UPDATE TO '1.0'");
        runSqlTest("features/fdw_extension_tests/downgrade_then_upgrade/step_2_after_alter_extension_downgrade");

        gpdb.runQuery("ALTER EXTENSION pxf_fdw UPDATE TO '2.0'");
        runSqlTest("features/fdw_extension_tests/downgrade_then_upgrade/step_3_after_alter_extension_upgrade");
    }

}
