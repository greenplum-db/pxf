package org.greenplum.pxf.automation.features.hdfs;

import org.greenplum.pxf.automation.features.BaseFeature;

import org.greenplum.pxf.automation.structures.tables.pxf.ReadableExternalTable;
import org.testng.annotations.Test;

public class ParquetReadableTest extends BaseFeature {

    private String hdfsPath;
    private String resourcePath;

    private final String parquetPrimitiveTypes = "parquet_primitive_types";

    @Override
    public void beforeClass() throws Exception {
        // path for storing data on HDFS (for processing by PXF)
        hdfsPath = hdfs.getWorkingDirectory() + "/parquet/";

        resourcePath = localDataResourcesFolder + "/parquet/";
        hdfs.copyFromLocal(resourcePath + parquetPrimitiveTypes, hdfsPath + parquetPrimitiveTypes);
    }

    @Override
    public void afterClass() throws Exception {
        hdfs.removeDirectory(hdfsPath);
    }


    @Test(groups = {"features", "gpdb", "hcfs"})
    public void parquetSupportedPrimitives() throws Exception {

        exTable = new ReadableExternalTable("pxf_parquet_primitive_types",
                new String[]{
                        "t1    TEXT",
                        "t2    TEXT",
                        "num1  INTEGER",
                        "dub1  DOUBLE PRECISION",
                        "dec1  NUMERIC",
                        "tm    TIMESTAMP",
                        "r     REAL",
                        "bg    BIGINT",
                        "b     BOOLEAN",
                        "tn    SMALLINT",
                        "sml   SMALLINT",
                        "vc1   VARCHAR(5)",
                        "c1    CHAR(3)",
                        "bin   BYTEA"},
                hdfsPath + parquetPrimitiveTypes,
                "custom");
        exTable.setHost(pxfHost);
        exTable.setPort(pxfPort);
        exTable.setFormatter("pxfwritable_import");
        exTable.setProfile("Parquet");

        gpdb.createTableAndVerify(exTable);
        runTincTest("pxf.features.hdfs.readable.parquet.primitive_types.runTest");
    }
}
