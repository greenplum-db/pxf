package org.greenplum.pxf.automation.features.hive;

import jsystem.framework.sut.SutFactory;
import jsystem.framework.system.SystemManagerImpl;
import org.greenplum.pxf.automation.components.hdfs.Hdfs;
import org.greenplum.pxf.automation.components.hive.Hive;
import org.greenplum.pxf.automation.structures.tables.hive.HiveTable;

import org.testng.annotations.Test;

/**
 * This test runs as part of Longevity suite where restart of PXF is not allowed.
 * Make sure this class does not include predicate pushdown tests for which custom classes
 * and PXF restart is required.
 */
 public class HiveMultiClusterSecurityTest extends HiveBaseTest {

    private static final String HIVE_DATA_FILE_NAME_2 = "hive_small_data_second.txt";
    private static final String PXF_HIVE_SMALL_DATA_TABLE_SECURE = "pxf_hive_small_data_hive_secure";

    private Hive hive2;

    @Override
    protected boolean isRestartAllowed() {
        return false;
    }

    // TODO: this test is being ignored because the reverse DNS lookup seems
    // TODO: to cause issues when accessing the metastore on the second dataproc
    // TODO: environment
    /**
     * query for small data hive table against two kerberized hive servers
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"features", "multiClusterSecurity"}, enabled = false)
    public void testTwoSecuredServers() throws Exception {

        createExternalTable(PXF_HIVE_SMALL_DATA_TABLE, PXF_HIVE_SMALLDATA_COLS, hiveSmallDataTable);

        Hdfs hdfs2 = (Hdfs) systemManager.
                getSystemObject("/sut", "hdfs2", -1, null, false, null, SutFactory.getInstance().getSutInstance());

        if (hdfs2 == null) return;

        trySecureLogin(hdfs2, hdfs2.getTestKerberosPrincipal());
        initializeWorkingDirectory(hdfs2, gpdb.getUserName());
        hive2 = (Hive) SystemManagerImpl.getInstance().getSystemObject("hive2");

        HiveTable hiveSmallDataTable2 =
                prepareTableData(hdfs2, hive2, null, HIVE_SMALL_DATA_TABLE, HIVE_SMALLDATA_COLS, HIVE_DATA_FILE_NAME_2);
        createExternalTable(PXF_HIVE_SMALL_DATA_TABLE_SECURE, PXF_HIVE_SMALLDATA_COLS, hiveSmallDataTable2, true, "SERVER=hdfs-secure");

        runSqlTest("features/hive/two_secured_hive");
    }

}
