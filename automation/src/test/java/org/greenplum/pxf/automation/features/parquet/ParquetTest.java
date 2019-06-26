package org.greenplum.pxf.automation.features.parquet;

import org.greenplum.pxf.automation.features.BaseFeature;
import org.greenplum.pxf.automation.structures.tables.pxf.ReadableExternalTable;
import org.greenplum.pxf.automation.structures.tables.pxf.WritableExternalTable;
import org.greenplum.pxf.automation.utils.system.ProtocolUtils;
import org.testng.annotations.Test;

public class ParquetTest extends BaseFeature {

    private String hdfsPath;
    private String resourcePath;

    private final String pxfParquetTable = "pxf_parquet_primitive_types";
    private final String parquetWritePrimitives = "parquet_write_primitives";
    private final String parquetWritePrimitivesGzip = "parquet_write_primitives_gzip";
    private final String parquetWritePrimitivesGzipClassName = "parquet_write_primitives_gzip_classname";
    private final String parquetWritePrimitivesV2 = "parquet_write_primitives_v2";
    private final String parquetPrimitiveTypes = "parquet_primitive_types";
    private final String[] parquet_table_columns = new String[]{
            "s1    TEXT",
            "s2    TEXT",
            "n1    INTEGER",
            "d1    DOUBLE PRECISION",
            "dc1   NUMERIC",
            "tm    TIMESTAMP",
            "f     REAL",
            "bg    BIGINT",
            "b     BOOLEAN",
            "tn    SMALLINT",
            "vc1   VARCHAR(5)",
            "sml   SMALLINT",
            "c1    CHAR(3)",
            "bin   BYTEA"
    };

    private final String pxfParquetSubsetTable = "pxf_parquet_subset";
    private final String[] parquet_table_columns_subset = new String[]{
            "s1    TEXT",
            "n1    INTEGER",
            "d1    DOUBLE PRECISION",
            "f     REAL",
            "b     BOOLEAN",
            "vc1   VARCHAR(5)",
            "bin   BYTEA"
    };

    @Override
    public void beforeClass() throws Exception {
        // path for storing data on HDFS (for processing by PXF)
        hdfsPath = hdfs.getWorkingDirectory() + "/parquet/";

        resourcePath = localDataResourcesFolder + "/parquet/";
        hdfs.copyFromLocal(resourcePath + parquetPrimitiveTypes, hdfsPath + parquetPrimitiveTypes);
    }

    @Test(groups = {"features", "gpdb", "hcfs"})
    public void parquetReadPrimitives() throws Exception {

        exTable = new ReadableExternalTable(pxfParquetTable,
                parquet_table_columns, hdfsPath + parquetPrimitiveTypes, "custom");
        exTable.setHost(pxfHost);
        exTable.setPort(pxfPort);
        exTable.setFormatter("pxfwritable_import");
        exTable.setProfile(ProtocolUtils.getProtocol().value() + ":parquet");

        gpdb.createTableAndVerify(exTable);

        gpdb.runQuery("CREATE OR REPLACE VIEW parquet_view AS SELECT s1, s2, n1, d1, dc1, " +
                "CAST(tm AS TIMESTAMP WITH TIME ZONE) AT TIME ZONE 'PDT' as tm, " +
                "f, bg, b, tn, sml, vc1, c1, bin FROM " + pxfParquetTable);

        runTincTest("pxf.features.parquet.primitive_types.runTest");
    }

    @Test(groups = {"features", "gpdb", "hcfs"})
    public void parquetReadSubset() throws Exception {
        exTable = new ReadableExternalTable(pxfParquetSubsetTable,
                parquet_table_columns_subset, hdfsPath + parquetPrimitiveTypes, "custom");
        exTable.setHost(pxfHost);
        exTable.setPort(pxfPort);
        exTable.setFormatter("pxfwritable_import");
        exTable.setProfile(ProtocolUtils.getProtocol().value() + ":parquet");

        gpdb.createTableAndVerify(exTable);

        runTincTest("pxf.features.parquet.read_subset.runTest");
    }

    @Test(groups = {"features", "gpdb", "hcfs"})
    public void parquetWritePrimitives() throws Exception {
        runWriteScenario("pxf_parquet_write_primitives", "pxf_parquet_read_primitives", parquetWritePrimitives, null);
    }

    @Test(groups = {"features", "gpdb", "hcfs"})
    public void parquetWritePrimitivesV2() throws Exception {
        runWriteScenario("pxf_parquet_write_primitives_v2", "pxf_parquet_read_primitives_v2", parquetWritePrimitivesV2, new String[]{"PARQUET_VERSION=v2"});
    }

    @Test(groups = {"features", "gpdb", "hcfs"})
    public void parquetWritePrimitivesGZip() throws Exception {
        runWriteScenario("pxf_parquet_write_primitives_gzip", "pxf_parquet_read_primitives_gzip", parquetWritePrimitivesGzip, new String[]{"COMPRESSION_CODEC=gzip"});
    }

    @Test(groups = {"features", "gpdb", "hcfs"})
    public void parquetWritePrimitivesGZipClassName() throws Exception {
        runWriteScenario("pxf_parquet_write_primitives_gzip_classname", "pxf_parquet_read_primitives_gzip_classname", parquetWritePrimitivesGzipClassName, new String[]{"COMPRESSION_CODEC=org.apache.hadoop.io.compress.GzipCodec"});
    }

    private void runWriteScenario(String writeTableName, String readTableName,
                                  String filename, String[] userParameters) throws Exception {
        exTable = new WritableExternalTable(writeTableName,
                parquet_table_columns, hdfsPath + filename, "custom");
        exTable.setHost(pxfHost);
        exTable.setPort(pxfPort);
        exTable.setFormatter("pxfwritable_export");
        exTable.setProfile(ProtocolUtils.getProtocol().value() + ":parquet");

        if (userParameters != null) {
            exTable.setUserParameters(userParameters);
        }

        gpdb.createTableAndVerify(exTable);
        gpdb.runQuery("INSERT INTO " + exTable.getName() + " SELECT s1, s2, n1, d1, dc1, tm, " +
                "f, bg, b, tn, vc1, sml, c1, bin FROM " + pxfParquetTable);

        exTable = new ReadableExternalTable(readTableName,
                parquet_table_columns, hdfsPath + filename, "custom");
        exTable.setHost(pxfHost);
        exTable.setPort(pxfPort);
        exTable.setFormatter("pxfwritable_import");
        exTable.setProfile(ProtocolUtils.getProtocol().value() + ":parquet");
        gpdb.createTableAndVerify(exTable);
        gpdb.runQuery("CREATE OR REPLACE VIEW parquet_view AS SELECT s1, s2, n1, d1, dc1, " +
                "CAST(tm AS TIMESTAMP WITH TIME ZONE) AT TIME ZONE 'PDT' as tm, " +
                "f, bg, b, tn, sml, vc1, c1, bin FROM " + readTableName);

        runTincTest("pxf.features.parquet.primitive_types.runTest");
    }
}
