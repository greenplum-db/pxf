package org.greenplum.pxf.automation.proxy;

import org.greenplum.pxf.automation.components.common.ShellSystemObject;
import org.greenplum.pxf.automation.components.hbase.HBase;
import org.greenplum.pxf.automation.datapreparer.hbase.HBaseSmokeDataPreparer;
import org.greenplum.pxf.automation.smoke.BaseSmoke;
import org.greenplum.pxf.automation.structures.tables.basic.Table;
import org.greenplum.pxf.automation.structures.tables.hbase.HBaseTable;
import org.greenplum.pxf.automation.structures.tables.hbase.LookupTable;
import org.greenplum.pxf.automation.structures.tables.pxf.ReadableExternalTable;
import org.greenplum.pxf.automation.structures.tables.utils.TableFactory;
import jsystem.framework.system.SystemManagerImpl;
import org.apache.commons.lang.StringUtils;
import org.testng.annotations.Test;

/** Basic PXF on HBase table filled with small data using non-gpadmin user. */
public class HBaseProxySmokeTest extends BaseSmoke {

    public static final String TEST_USER = "testuser";

    HBase hbase;
    Table dataTable;
    HBaseTable hbaseTableAllowed, hbaseTableProhibited;
    LookupTable lookupTable;

    @Override
    public void beforeClass() throws Exception {
        // Initialize HBase System Object
        hbase = (HBase) SystemManagerImpl.getInstance().getSystemObject("hbase");

        // this test requires authorization to be turned on for HBase
        // specify explicitly if hbase-site.xml is not available (for default.xml SUT file)
        hbase.setAuthorization(true);

        // for Kerberos, use principal identity, not username
        String kerberosPrincipal = cluster.getTestKerberosPrincipal();
        String adminUser = StringUtils.isEmpty(kerberosPrincipal) ? System.getProperty("user.name") : kerberosPrincipal;

        hbase.grantGlobalForUser(adminUser);
    }

    @Override
    protected void prepareData() throws Exception {
        // create and load 2 HBase tables owned by admin user

        hbaseTableAllowed = loadTable("hbase_table_allowed");
        hbaseTableProhibited = loadTable("hbase_table_prohibited");

    }

    private HBaseTable loadTable(String tableName) throws Exception {
        // Create HBase Table
        HBaseTable hbaseTable = new HBaseTable(tableName, new String[] { "col" });
        hbaseTable.setNumberOfSplits(0);
        hbaseTable.setRowsPerSplit(100);
        hbaseTable.setRowKeyPrefix("row");
        hbaseTable.setQualifiers(new String[] {
                "name",
                "number",
                "doub",
                "longnum",
                "bool" });

        hbase.createTableAndVerify(hbaseTable);

        // Prepare data for HBase table
        HBaseSmokeDataPreparer dataPreparer = new HBaseSmokeDataPreparer();
        dataPreparer.setColumnFamilyName(hbaseTable.getFields()[0]);
        dataPreparer.prepareData(hbaseTable.getRowsPerSplit(), hbaseTable);

        hbase.put(hbaseTable);

        Thread.sleep(ShellSystemObject._5_SECONDS);

        return hbaseTable;
    }

    @Override
    protected void createTables() throws Exception {
        // Create GPDB external tables
        ReadableExternalTable exTableAllowed = TableFactory.getPxfHBaseReadableTable("pxf_proxy_hbase_small_data_allowed",
                new String[] {
                        "name text",
                        "num integer",
                        "dub double precision",
                        "longnum bigint",
                        "bool boolean" }, hbaseTableAllowed);
        exTableAllowed.setHost(pxfHost);
        exTableAllowed.setPort(pxfPort);
        gpdb.createTableAndVerify(exTableAllowed);

        ReadableExternalTable exTableProhibited = TableFactory.getPxfHBaseReadableTable("pxf_proxy_hbase_small_data_prohibited",
                new String[] {
                        "name text",
                        "num integer",
                        "dub double precision",
                        "longnum bigint",
                        "bool boolean" }, hbaseTableProhibited);
        exTableProhibited.setHost(pxfHost);
        exTableProhibited.setPort(pxfPort);
        gpdb.createTableAndVerify(exTableProhibited);

        // Create PXF Lookup HBase table
        lookupTable = new LookupTable();

        lookupTable.addMapping(hbaseTableAllowed.getName(), "name", "col:name");
        lookupTable.addMapping(hbaseTableAllowed.getName(), "num", "col:number");
        lookupTable.addMapping(hbaseTableAllowed.getName(), "dub", "col:doub");
        lookupTable.addMapping(hbaseTableAllowed.getName(), "longnum", "col:longnum");
        lookupTable.addMapping(hbaseTableAllowed.getName(), "bool", "col:bool");

        lookupTable.addMapping(hbaseTableProhibited.getName(), "name", "col:name");
        lookupTable.addMapping(hbaseTableProhibited.getName(), "num", "col:number");
        lookupTable.addMapping(hbaseTableProhibited.getName(), "dub", "col:doub");
        lookupTable.addMapping(hbaseTableProhibited.getName(), "longnum", "col:longnum");
        lookupTable.addMapping(hbaseTableProhibited.getName(), "bool", "col:bool");

        hbase.createTableAndVerify(lookupTable);
        hbase.put(lookupTable);

        // grant read permission on allowed table, do not grant permissions to test user on prohibited one
        hbase.grantReadOnTable(hbaseTableAllowed, TEST_USER);
        // lookup table requires both CREATE (for getting table description) and READ permissions
        hbase.grantCreateReadOnTable(lookupTable, TEST_USER);

        Thread.sleep(ShellSystemObject._5_SECONDS);
    }

    @Override
    protected void queryResults() throws Exception {
        runSqlTest("proxy/hbase_small_data");
    }

    @Test(groups = { "proxy", "hbase" })
    public void test() throws Exception {
        runTest();
    }
}
