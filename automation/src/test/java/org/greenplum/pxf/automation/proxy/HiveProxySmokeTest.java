// TODO: spring_boot_todo restore when pxf-hive is introduced
//package org.greenplum.pxf.automation.proxy;
//
//import org.greenplum.pxf.automation.components.hive.Hive;
//import org.greenplum.pxf.automation.smoke.BaseSmoke;
//import org.greenplum.pxf.automation.structures.tables.basic.Table;
//import org.greenplum.pxf.automation.structures.tables.hive.HiveTable;
//import org.greenplum.pxf.automation.structures.tables.pxf.ReadableExternalTable;
//import org.greenplum.pxf.automation.structures.tables.utils.TableFactory;
//import jsystem.framework.system.SystemManagerImpl;
//import org.testng.annotations.Test;
//
///**
// * Basic PXF on small Hive table using non-gpadmin user
// */
//public class HiveProxySmokeTest extends BaseSmoke {
//    private static final String[] FIELDS = {
//            "name text",
//            "num integer",
//            "dub double precision",
//            "longNum bigint",
//            "bool boolean"
//    };
//    public static final String[] HIVE_FIELDS = {
//            "name string",
//            "num int",
//            "dub double",
//            "longNum bigint",
//            "bool boolean"
//    };
//    private static final String TEST_USER = "testuser";
//    private Hive hive;
//    private HiveTable hiveTableAllowed;
//    private HiveTable hiveTableProhibited;
//
//    @Override
//    public void beforeClass() throws Exception {
//        // Initialize Hive system object
//        hive = (Hive) SystemManagerImpl.getInstance().getSystemObject("hive");
//    }
//
//    @Override
//    public void afterClass() throws Exception {
//        // close hive connection
//        if (hive != null)
//            hive.close();
//    }
//
//    @Override
//    protected void prepareData() throws Exception {
//        // Create Hive tables
//        hiveTableAllowed = TableFactory.getHiveByRowCommaTable("hive_table_allowed", HIVE_FIELDS);
//        hiveTableProhibited = TableFactory.getHiveByRowCommaTable("hive_table_prohibited", HIVE_FIELDS);
//
//        // hive.dropTable(hiveTable, false);
//        hive.createTableAndVerify(hiveTableAllowed);
//        hive.createTableAndVerify(hiveTableProhibited);
//
//        // Generate Small data, write to HDFS and load to Hive
//        Table dataTable = getSmallData();
//        String fileName = "hiveSmallData.txt";
//        hdfs.writeTableToFile((hdfs.getWorkingDirectory() + "/" + fileName), dataTable, ",");
//        hive.loadData(hiveTableAllowed, (hdfs.getWorkingDirectory() + "/" + fileName), false);
//        hdfs.writeTableToFile((hdfs.getWorkingDirectory() + "/" + fileName), dataTable, ",");
//        hive.loadData(hiveTableProhibited, (hdfs.getWorkingDirectory() + "/" + fileName), false);
//
//        // update permissions on HDFS directory for prohibited table to be only readable by gpadmin user
//        hdfs.setMode(cluster.getHiveBaseHdfsDirectory() + "/hive_table_prohibited/hiveSmallData.txt", "700");
//
//        // update permissions on HDFS directory for allowed table to only be readable by testuser
//        hdfs.setOwner(cluster.getHiveBaseHdfsDirectory() + "/hive_table_allowed/hiveSmallData.txt", TEST_USER, TEST_USER);
//        hdfs.setMode(cluster.getHiveBaseHdfsDirectory() + "/hive_table_allowed/hiveSmallData.txt", "700");
//    }
//
//    @Override
//    protected void createTables() throws Exception {
//        // Create GPDB external tables
//        ReadableExternalTable exTableAllowed =
//                TableFactory.getPxfHiveReadableTable("pxf_proxy_hive_small_data_allowed",
//                        FIELDS, hiveTableAllowed, true);
//        exTableAllowed.setHost(pxfHost);
//        exTableAllowed.setPort(pxfPort);
//        gpdb.createTableAndVerify(exTableAllowed);
//
//        ReadableExternalTable exTableAllowedNoImpersonation =
//                TableFactory.getPxfHiveReadableTable("pxf_proxy_hive_small_data_allowed_no_impersonation",
//                        FIELDS, hiveTableAllowed, true);
//        exTableAllowedNoImpersonation.setHost(pxfHost);
//        exTableAllowedNoImpersonation.setPort(pxfPort);
//        exTableAllowedNoImpersonation.setServer("SERVER=default-no-impersonation");
//        gpdb.createTableAndVerify(exTableAllowedNoImpersonation);
//
//        ReadableExternalTable exTableProhibited =
//                TableFactory.getPxfHiveReadableTable("pxf_proxy_hive_small_data_prohibited",
//                        FIELDS, hiveTableProhibited, true);
//        exTableProhibited.setHost(pxfHost);
//        exTableProhibited.setPort(pxfPort);
//        gpdb.createTableAndVerify(exTableProhibited);
//
//        ReadableExternalTable exTableProhibitedNoImpersonation =
//                TableFactory.getPxfHiveReadableTable("pxf_proxy_hive_small_data_prohibited_no_impersonation",
//                        FIELDS, hiveTableProhibited, true);
//        exTableProhibitedNoImpersonation.setHost(pxfHost);
//        exTableProhibitedNoImpersonation.setPort(pxfPort);
//        exTableProhibitedNoImpersonation.setServer("SERVER=default-no-impersonation");
//        gpdb.createTableAndVerify(exTableProhibitedNoImpersonation);
//
//    }
//
//    @Override
//    protected void queryResults() throws Exception {
//        runTincTest("pxf.proxy.hive_small_data.runTest");
//    }
//
//    @Test(groups = {"proxy", "hive", "proxySecurity"})
//    public void test() throws Exception {
//        runTest();
//    }
//}
