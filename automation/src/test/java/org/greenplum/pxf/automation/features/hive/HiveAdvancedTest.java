package org.greenplum.pxf.automation.features.hive;

import org.greenplum.pxf.automation.AdvancedFunctionality;
import org.greenplum.pxf.automation.components.cluster.PhdCluster;
import org.greenplum.pxf.automation.structures.tables.hive.HiveTable;
import org.greenplum.pxf.automation.structures.tables.utils.TableFactory;
import org.testng.annotations.Test;

/**
 * Tests Hive plugin in PXF
 */
public class HiveAdvancedTest extends AdvancedFunctionality {

    @Override
    protected void afterClass() throws Exception {
        cluster.stop(PhdCluster.EnumClusterServices.hive);
        cluster.start(PhdCluster.EnumClusterServices.hive);
    }

    /**
     * Stop Hive metastore, run query and expect error message, and restart Hive
     * metastore.
     *
     * @throws Exception if test fails to run
     */
     @Test(groups = {"hive", "features", "gpdb", "security"})
    public void hiveMetastoreDown() throws Exception {
        HiveTable hiveTable = new HiveTable("hive_table", null);
        exTable = TableFactory.getPxfHiveReadableTable(
                "pxf_hive_metastore_down", new String[]{
                        "t1 text",
                        "num1 integer"}, hiveTable, true);
        exTable.setHost(pxfHost);
        exTable.setPort(pxfPort);
        gpdb.createTableAndVerify(exTable);
        cluster.stop(PhdCluster.EnumClusterServices.hive);
        runSqlTest("features/hive/errors/hiveMetastoreDown");
    }
}
