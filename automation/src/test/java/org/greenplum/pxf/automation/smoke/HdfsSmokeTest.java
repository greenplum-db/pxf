package org.greenplum.pxf.automation.smoke;

import annotations.WorksWithFDW;
import org.greenplum.pxf.automation.structures.tables.basic.Table;
import org.greenplum.pxf.automation.structures.tables.utils.TableFactory;
import org.testng.annotations.Test;

/** Basic PXF on HDFS small text file */
public class HdfsSmokeTest extends BaseSmoke {

    @Override
    protected void prepareData() throws Exception {
        // Create Data and write it to HDFS
        Table dataTable = getSmallData();
        hdfs.writeTableToFile(hdfs.getWorkingDirectory() + "/" + fileName, dataTable, ",");
    }

    @Override
    protected void createTables() throws Exception {
        // Create GPDB external table directed to the HDFS file
        exTable =
                TableFactory.getPxfReadableTextTable("pxf_smoke_small_data", new String[] {
                        "name text",
                        "num integer",
                        "dub double precision",
                        "longNum bigint",
                        "bool boolean"
                }, hdfs.getWorkingDirectory() + "/" + fileName, ",");
        exTable.setHost(pxfHost);
        exTable.setPort(pxfPort);
        gpdb.createTableAndVerify(exTable);
    }

    @Override
    protected void queryResults() throws Exception {
        runTincTest("pxf.smoke.small_data.runTest");
    }

    @Test(groups = { "smoke", "gpdb", "security" })
    @WorksWithFDW
    public void test() throws Exception {
        runTest();
    }
}
