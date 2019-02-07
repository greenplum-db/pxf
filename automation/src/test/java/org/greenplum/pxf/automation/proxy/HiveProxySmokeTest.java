package org.greenplum.pxf.automation.proxy;

import org.greenplum.pxf.automation.components.hive.Hive;
import org.greenplum.pxf.automation.smoke.BaseSmoke;
import org.greenplum.pxf.automation.structures.tables.basic.Table;
import org.greenplum.pxf.automation.structures.tables.hive.HiveTable;
import org.greenplum.pxf.automation.structures.tables.pxf.ReadableExternalTable;
import org.greenplum.pxf.automation.structures.tables.utils.TableFactory;
import jsystem.framework.system.SystemManagerImpl;
import org.testng.annotations.Test;

/** Basic PXF on small Hive table using non-gpadmin user */
public class HiveProxySmokeTest extends BaseSmoke {
    Hive hive;
    Table dataTable;
    HiveTable hiveTableAllowed, hiveTableProhibited;
    String fileName = "hiveSmallData.txt";

    @Override
    public void beforeClass() throws Exception {
        // Initialize Hive system object
        hive = (Hive) SystemManagerImpl.getInstance().getSystemObject("hive");
    }

    @Override
    public void afterClass() throws Exception {
        // close hive connection
        if (hive != null)
            hive.close();
    }

    @Override
    protected void prepareData() throws Exception {
        // Create Hive tables
        hiveTableAllowed = TableFactory.getHiveByRowCommaTable("hive_table_allowed", new String[] {
                "s1 string",
                "n1 int",
                "d1 double",
                "bg bigint",
                "b boolean"
        });
        hiveTableProhibited = TableFactory.getHiveByRowCommaTable("hive_table_prohibited", new String[] {
                "s1 string",
                "n1 int",
                "d1 double",
                "bg bigint",
                "b boolean"
        });

        // hive.dropTable(hiveTable, false);
        hive.createTableAndVerify(hiveTableAllowed);
        hive.createTableAndVerify(hiveTableProhibited);

        // Generate Small data, write to HDFS and load to Hive
        Table dataTable = getSmallData();
        hdfs.writeTableToFile((hdfs.getWorkingDirectory() + "/" + fileName), dataTable, ",");
        hive.loadData(hiveTableAllowed, (hdfs.getWorkingDirectory() + "/" + fileName), false);
        hdfs.writeTableToFile((hdfs.getWorkingDirectory() + "/" + fileName), dataTable, ",");
        hive.loadData(hiveTableProhibited, (hdfs.getWorkingDirectory() + "/" + fileName), false);

        // update permissions on HDFS directory for prohibited table to be only readable by gpadmin user
        hdfs.setMode("/hive/warehouse/hive_table_prohibited/hiveSmallData.txt", "700");
    }

    @Override
    protected void createTables() throws Exception {
        // Create GPDB external tables
        ReadableExternalTable exTableAllowed = TableFactory.getPxfHiveReadableTable("pxf_proxy_hive_small_data_allowed", new String[] {
                "name text",
                "num integer",
                "dub double precision",
                "longNum bigint",
                "bool boolean"
        }, hiveTableAllowed, true);
        exTableAllowed.setHost(pxfHost);
        exTableAllowed.setPort(pxfPort);
        gpdb.createTableAndVerify(exTableAllowed);

        ReadableExternalTable exTableProhibited = TableFactory.getPxfHiveReadableTable("pxf_proxy_hive_small_data_prohibited", new String[] {
                "name text",
                "num integer",
                "dub double precision",
                "longNum bigint",
                "bool boolean"
        }, hiveTableProhibited, true);
        exTableProhibited.setHost(pxfHost);
        exTableProhibited.setPort(pxfPort);
        gpdb.createTableAndVerify(exTableProhibited);

    }

    @Override
    protected void queryResults() throws Exception {
        runTincTest("pxf.proxy.hive_small_data.runTest");
    }

    @Test(groups = { "proxy", "hive" })
    public void test() throws Exception {
        runTest();
    }
}
