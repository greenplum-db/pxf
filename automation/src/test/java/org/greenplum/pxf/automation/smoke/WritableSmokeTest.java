package org.greenplum.pxf.automation.smoke;

import java.io.File;

import annotations.WorksWithFDW;
import org.greenplum.pxf.automation.structures.tables.basic.Table;
import org.greenplum.pxf.automation.structures.tables.pxf.WritableExternalTable;
import org.greenplum.pxf.automation.structures.tables.utils.TableFactory;
import org.greenplum.pxf.automation.utils.files.FileUtils;
import org.testng.annotations.Test;

/** Write data to HDFS using Writable External table. Read it using PXF. */
@WorksWithFDW
public class WritableSmokeTest extends BaseSmoke {
    WritableExternalTable writableExTable;

    @Override
    protected void prepareData() throws Exception {
        // Generate Small data, write to File and copy to external table
        Table dataTable = getSmallData();
        File file = new File(dataTempFolder + "/" + fileName);
        FileUtils.writeTableDataToFile(dataTable, file.getAbsolutePath(), "|");
    }

    @Override
    protected void createTables() throws Exception {
        // Create Writable external table
        writableExTable = TableFactory.getPxfWritableTextTable("hdfs_writable_table", new String[]{
                "name text",
                "num integer",
                "dub double precision",
                "longNum bigint",
                "bool boolean"
        }, hdfs.getWorkingDirectory() + "/bzip", "|");

        writableExTable.setCompressionCodec("org.apache.hadoop.io.compress.BZip2Codec");
        writableExTable.setHost(pxfHost);
        writableExTable.setPort(pxfPort);
        gpdb.createTableAndVerify(writableExTable);
        gpdb.copyFromFile(writableExTable, new File(dataTempFolder + "/" + fileName), "|", false);
        // Create Readable External Table
        exTable = TableFactory.getPxfReadableTextTable("pxf_smoke_small_data", new String[] {
                "name text",
                "num integer",
                "dub double precision",
                "longNum bigint",
                "bool boolean"
        }, hdfs.getWorkingDirectory() + "/bzip", "|");
        exTable.setHost(pxfHost);
        exTable.setPort(pxfPort);
        gpdb.createTableAndVerify(exTable);
    }

    @Override
    protected void queryResults() throws Exception {
        runTincTest("pxf.smoke.small_data.runTest");
    }

    @Test(groups = "smoke")
    public void test() throws Exception {
        runTest();
    }
}
