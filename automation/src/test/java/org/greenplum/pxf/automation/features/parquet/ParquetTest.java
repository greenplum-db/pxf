package org.greenplum.pxf.automation.features.parquet;

import org.greenplum.pxf.automation.features.BaseFeature;
import org.greenplum.pxf.automation.structures.tables.basic.Table;
import org.greenplum.pxf.automation.structures.tables.pxf.ReadableExternalTable;
import org.greenplum.pxf.automation.structures.tables.pxf.WritableExternalTable;
import org.greenplum.pxf.automation.utils.system.ProtocolUtils;
import org.testng.annotations.Test;

import java.io.File;

public class ParquetTest extends BaseFeature {

    private String hdfsPath;
    private String resourcePath;

    private static final String NUMERIC_UNDEFINED_PRECISION_TABLE = "numeric_undefined_precision";
    private final String pxfParquetTable = "pxf_parquet_primitive_types";
    private final String parquetWritePrimitives = "parquet_write_primitives";
    private final String parquetWritePrimitivesGzip = "parquet_write_primitives_gzip";
    private final String parquetWritePrimitivesGzipClassName = "parquet_write_primitives_gzip_classname";
    private final String parquetWritePrimitivesV2 = "parquet_write_primitives_v2";
    private final String parquetUndefinedPrecisionNumericFile = "undefined_precision_numeric.parquet";
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

    private final String[] parquet_table_decimal_columns = new String[]{
            "description   TEXT",
            "a             DECIMAL(5,  2)",
            "b             DECIMAL(12, 2)",
            "c             DECIMAL(18, 18)",
            "d             DECIMAL(24, 16)",
            "e             DECIMAL(30, 5)",
            "f             DECIMAL(34, 30)",
            "g             DECIMAL(38, 10)",
            "h             DECIMAL(38, 38)"
    };

    private static final String[] UNDEFINED_PRECISION_NUMERIC = new String[]{
            "description   text",
            "value         numeric"};

    private final String[] parquet_table_columns_subset = new String[]{
            "s1    TEXT",
            "n1    INTEGER",
            "d1    DOUBLE PRECISION",
            "f     REAL",
            "b     BOOLEAN",
            "vc1   VARCHAR(5)",
            "bin   BYTEA"
    };

    private static final String undefinedPrecisionNumericFileName = "undefined_precision_numeric.csv";

    @Override
    public void beforeClass() throws Exception {
        // path for storing data on HDFS (for processing by PXF)
        hdfsPath = hdfs.getWorkingDirectory() + "/parquet/";

        resourcePath = localDataResourcesFolder + "/parquet/";
        hdfs.copyFromLocal(resourcePath + parquetPrimitiveTypes, hdfsPath + parquetPrimitiveTypes);
        hdfs.copyFromLocal(resourcePath + parquetUndefinedPrecisionNumericFile, hdfsPath + parquetUndefinedPrecisionNumericFile);

        Table gpdbUndefinedPrecisionNumericTable = new Table(NUMERIC_UNDEFINED_PRECISION_TABLE, UNDEFINED_PRECISION_NUMERIC);
        gpdbUndefinedPrecisionNumericTable.setDistributionFields(new String[]{"description"});
        gpdb.createTableAndVerify(gpdbUndefinedPrecisionNumericTable);
        gpdb.copyFromFile(gpdbUndefinedPrecisionNumericTable, new File(localDataResourcesFolder
                + "/numeric/" + undefinedPrecisionNumericFileName), "E','", true);
    }

    @Test(groups = {"features", "gpdb", "security", "hcfs"})
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

    @Test(groups = {"features", "gpdb", "security", "hcfs"})
    public void parquetReadSubset() throws Exception {
        String pxfParquetSubsetTable = "pxf_parquet_subset";
        exTable = new ReadableExternalTable(pxfParquetSubsetTable,
                parquet_table_columns_subset, hdfsPath + parquetPrimitiveTypes, "custom");
        exTable.setHost(pxfHost);
        exTable.setPort(pxfPort);
        exTable.setFormatter("pxfwritable_import");
        exTable.setProfile(ProtocolUtils.getProtocol().value() + ":parquet");

        gpdb.createTableAndVerify(exTable);

        runTincTest("pxf.features.parquet.read_subset.runTest");
    }

    @Test(groups = {"features", "gpdb", "security", "hcfs"})
    public void parquetWritePrimitives() throws Exception {
        runWriteScenario("pxf_parquet_write_primitives", "pxf_parquet_read_primitives", parquetWritePrimitives, null);
    }

    @Test(groups = {"features", "gpdb", "security", "hcfs"})
    public void parquetWritePrimitivesV2() throws Exception {
        runWriteScenario("pxf_parquet_write_primitives_v2", "pxf_parquet_read_primitives_v2", parquetWritePrimitivesV2, new String[]{"PARQUET_VERSION=v2"});
    }

    @Test(groups = {"features", "gpdb", "security", "hcfs"})
    public void parquetWritePrimitivesGZip() throws Exception {
        runWriteScenario("pxf_parquet_write_primitives_gzip", "pxf_parquet_read_primitives_gzip", parquetWritePrimitivesGzip, new String[]{"COMPRESSION_CODEC=gzip"});
    }

    @Test(groups = {"features", "gpdb", "security", "hcfs"})
    public void parquetWritePrimitivesGZipClassName() throws Exception {
        runWriteScenario("pxf_parquet_write_primitives_gzip_classname", "pxf_parquet_read_primitives_gzip_classname", parquetWritePrimitivesGzipClassName, new String[]{"COMPRESSION_CODEC=org.apache.hadoop.io.compress.GzipCodec"});
    }

    @Test(groups = {"features", "gpdb", "security", "hcfs"})
    public void parquetReadUndefinedPrecisionNumericFromAParquetFileGeneratedByHive() throws Exception {
        exTable = new ReadableExternalTable("pxf_parquet_read_undefined_precision_numeric",
                UNDEFINED_PRECISION_NUMERIC, hdfsPath + parquetUndefinedPrecisionNumericFile, "custom");
        exTable.setHost(pxfHost);
        exTable.setPort(pxfPort);
        exTable.setFormatter("pxfwritable_import");
        exTable.setProfile(ProtocolUtils.getProtocol().value() + ":parquet");
        gpdb.createTableAndVerify(exTable);

        runTincTest("pxf.features.parquet.decimal.numeric_undefined_precision.runTest");
    }

    @Test(groups = {"features", "gpdb", "security", "hcfs"})
    public void parquetWriteUndefinedPrecisionNumeric() throws Exception {

        String filename = "parquet_write_undefined_precision_numeric";
        exTable = new WritableExternalTable("pxf_parquet_write_undefined_precision_numeric",
                UNDEFINED_PRECISION_NUMERIC, hdfsPath + filename, "custom");
        exTable.setHost(pxfHost);
        exTable.setPort(pxfPort);
        exTable.setFormatter("pxfwritable_export");
        exTable.setProfile(ProtocolUtils.getProtocol().value() + ":parquet");

        gpdb.createTableAndVerify(exTable);
        gpdb.runQuery("INSERT INTO " + exTable.getName() + " SELECT * FROM " + NUMERIC_UNDEFINED_PRECISION_TABLE);

        exTable = new ReadableExternalTable("pxf_parquet_read_undefined_precision_numeric",
                UNDEFINED_PRECISION_NUMERIC, hdfsPath + filename, "custom");
        exTable.setHost(pxfHost);
        exTable.setPort(pxfPort);
        exTable.setFormatter("pxfwritable_import");
        exTable.setProfile(ProtocolUtils.getProtocol().value() + ":parquet");
        gpdb.createTableAndVerify(exTable);

        runTincTest("pxf.features.parquet.decimal.numeric_undefined_precision.runTest");
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
