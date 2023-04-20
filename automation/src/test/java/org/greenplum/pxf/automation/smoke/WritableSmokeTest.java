package org.greenplum.pxf.automation.smoke;

import java.io.File;

import annotations.WorksWithFDW;
import org.greenplum.pxf.automation.structures.tables.basic.Table;
import org.greenplum.pxf.automation.structures.tables.pxf.WritableExternalTable;
import org.greenplum.pxf.automation.structures.tables.utils.TableFactory;
import org.greenplum.pxf.automation.utils.files.FileUtils;
import org.greenplum.pxf.automation.utils.system.FDWUtils;
import org.testng.annotations.Test;

/** Write data to HDFS using Writable External table. Read it using PXF. */
@WorksWithFDW
public class WritableSmokeTest extends BaseSmoke {
    WritableExternalTable writableExTable;
    private final static String[] FIELDS = new String[]{
            "name text",
            "num integer",
            "dub double precision",
            "longNum bigint",
            "bool boolean"
    };

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
        writableExTable = TableFactory.getPxfWritableTextTable("hdfs_writable_table", FIELDS,
                hdfs.getWorkingDirectory() + "/bzip", "|");

        writableExTable.setCompressionCodec("org.apache.hadoop.io.compress.BZip2Codec");
        writableExTable.setHost(pxfHost);
        writableExTable.setPort(pxfPort);
        gpdb.createTableAndVerify(writableExTable);

        // this test case exercises [COPY TO <external | foreign table> FROM <file>] data flow
        // however with FDW when foreign tables are used this is not supported for GP6 and only works with GP7
        // so for GP6 with FDW we create a native table, copy data from the file into it and then perform a CTAS
        // into the foreign table
        if (FDWUtils.useFDW && gpdb.getVersion() < 7) {
            Table nativeTable = new Table("writablesmoke_table", FIELDS);
            nativeTable.setDistributionFields(new String[]{"name"});
            gpdb.createTableAndVerify(nativeTable);
            gpdb.copyFromFile(nativeTable, new File(dataTempFolder + "/" + fileName), "|", false);
            // need to ignore WARNING:  skipping "hdfs_writable_table" --- cannot analyze this foreign table
            gpdb.copyData(nativeTable, writableExTable,true);
        } else {
            gpdb.copyFromFile(writableExTable, new File(dataTempFolder + "/" + fileName), "|", false);
        }

        // Create Readable External Table
        exTable = TableFactory.getPxfReadableTextTable("pxf_smoke_small_data", FIELDS,
                hdfs.getWorkingDirectory() + "/bzip", "|");
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
