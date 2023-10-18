package org.greenplum.pxf.automation.smoke;

import annotations.WorksWithFDW;
import jsystem.framework.system.SystemManagerImpl;

import org.greenplum.pxf.automation.components.common.ShellSystemObject;
import org.greenplum.pxf.automation.components.hbase.HBase;
import org.greenplum.pxf.automation.structures.tables.basic.Table;
import org.greenplum.pxf.automation.structures.tables.hbase.HBaseTable;
import org.greenplum.pxf.automation.structures.tables.hbase.LookupTable;
import org.greenplum.pxf.automation.structures.tables.utils.TableFactory;
import org.testng.annotations.Test;

import org.greenplum.pxf.automation.datapreparer.hbase.HBaseSmokeDataPreparer;

/** Basic PXF on HBase table filled with small data. */
public class HBaseSmokeTest extends BaseSmoke {
    HBase hbase;
    Table dataTable;
    HBaseTable hbaseTable;
    LookupTable lookupTable;

    @Override
    public void beforeClass() throws Exception {
        // Initialize HBase System Object
        hbase = (HBase) SystemManagerImpl.getInstance().getSystemObject("hbase");

        // if hbase authorization is not enabled then grant will not be
        // performed
        hbase.grantGlobalForUser("pxf");
    }

    @Override
    protected void prepareData() throws Exception {
        // Create HBase Table
        hbaseTable = new HBaseTable("hbase_table", new String[] { "col" });
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
    }

    @Override
    protected void createTables() throws Exception {
        // Create GPDB external table
        exTable = TableFactory.getPxfHBaseReadableTable("pxf_smoke_small_data",
                new String[] {
                        "name text",
                        "num integer",
                        "dub double precision",
                        "longnum bigint",
                        "bool boolean" }, hbaseTable);
        exTable.setHost(pxfHost);
        exTable.setPort(pxfPort);
        gpdb.createTableAndVerify(exTable);

        // Create PXF Lookup HBase table
        lookupTable = new LookupTable();
        lookupTable.addMapping(hbaseTable.getName(), "name", "col:name");
        lookupTable.addMapping(hbaseTable.getName(), "num", "col:number");
        lookupTable.addMapping(hbaseTable.getName(), "dub", "col:doub");
        lookupTable.addMapping(hbaseTable.getName(), "longnum", "col:longnum");
        lookupTable.addMapping(hbaseTable.getName(), "bool", "col:bool");

        hbase.createTableAndVerify(lookupTable);
        hbase.put(lookupTable);

        Thread.sleep(ShellSystemObject._5_SECONDS);
    }

    @Override
    protected void queryResults() throws Exception {
        runSqlTest("smoke/small_data");
    }

    @Test(groups = { "smoke", "gpdb", "hbase" })
    @WorksWithFDW
    public void test() throws Exception {
        runTest();
    }
}
