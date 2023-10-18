package org.greenplum.pxf.automation.features.hive;

import annotations.ExpectedFailure;

import org.greenplum.pxf.automation.AdvancedFunctionality;
import org.greenplum.pxf.automation.components.cluster.PhdCluster;
import org.greenplum.pxf.automation.structures.tables.hive.HiveTable;
import org.greenplum.pxf.automation.structures.tables.utils.TableFactory;

/**
 * Tests Hive plugin in PXF
 */
public class HiveAdvancedTest extends AdvancedFunctionality {

    @Override
    protected void afterClass() throws Exception {
        cluster.start(PhdCluster.EnumClusterServices.hive);
    }

    /**
     * Stop Hive metastore, run query and expect error message, and restart Hive
     * metastore.
     * <p>
     * TODO: enable test when issue is resolved.
     *
     * @throws Exception if test fails to run
     */
    // @Test(groups = {"hive", "features", "gpdb", "security"})
    @ExpectedFailure(reason = "[#91052648] - Can't start/stop Hive MetaStore in Ambari setup.")
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
