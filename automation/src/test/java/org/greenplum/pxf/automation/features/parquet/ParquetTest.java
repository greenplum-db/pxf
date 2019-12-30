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

    private static final String NUMERIC_TABLE = "numeric_precision";
    private static final String NUMERIC_UNDEFINED_PRECISION_TABLE = "numeric_undefined_precision";
    private static final String PXF_PARQUET_TABLE = "pxf_parquet_primitive_types";
    private static final String PARQUET_WRITE_PRIMITIVES = "parquet_write_primitives";
    private static final String PARQUET_WRITE_PRIMITIVES_GZIP = "parquet_write_primitives_gzip";
    private static final String PARQUET_WRITE_PRIMITIVES_GZIP_CLASSNAME = "parquet_write_primitives_gzip_classname";
    private static final String PARQUET_WRITE_PRIMITIVES_V2 = "parquet_write_primitives_v2";
    private static final String PARQUET_PRIMITIVE_TYPES = "parquet_primitive_types";
    private static final String PARQUET_UNDEFINED_PRECISION_NUMERIC_FILE = "undefined_precision_numeric.parquet";
    private static final String PARQUET_NUMERIC_FILE = "numeric.parquet";
    private static final String UNDEFINED_PRECISION_NUMERIC_FILENAME = "undefined_precision_numeric.csv";
    private static final String NUMERIC_FILENAME = "numeric_with_precision.csv";

    private static final String[] PARQUET_TABLE_COLUMNS = new String[]{
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

    private static final String[] PARQUET_TABLE_DECIMAL_COLUMNS = new String[]{
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

    private static final String[] PARQUET_TABLE_COLUMNS_SUBSET = new String[]{
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
        hdfs.copyFromLocal(resourcePath + PARQUET_PRIMITIVE_TYPES, hdfsPath + PARQUET_PRIMITIVE_TYPES);
        hdfs.copyFromLocal(resourcePath + PARQUET_UNDEFINED_PRECISION_NUMERIC_FILE, hdfsPath + PARQUET_UNDEFINED_PRECISION_NUMERIC_FILE);
        hdfs.copyFromLocal(resourcePath + PARQUET_NUMERIC_FILE, hdfsPath + PARQUET_NUMERIC_FILE);

        Table gpdbUndefinedPrecisionNumericTable = new Table(NUMERIC_UNDEFINED_PRECISION_TABLE, UNDEFINED_PRECISION_NUMERIC);
        gpdbUndefinedPrecisionNumericTable.setDistributionFields(new String[]{"description"});
        gpdb.createTableAndVerify(gpdbUndefinedPrecisionNumericTable);
        gpdb.copyFromFile(gpdbUndefinedPrecisionNumericTable, new File(localDataResourcesFolder
                + "/numeric/" + UNDEFINED_PRECISION_NUMERIC_FILENAME), "E','", true);

        Table gpdbNumericWithPrecisionScaleTable = new Table(NUMERIC_TABLE, PARQUET_TABLE_DECIMAL_COLUMNS);
        gpdbNumericWithPrecisionScaleTable.setDistributionFields(new String[]{"description"});
        gpdb.createTableAndVerify(gpdbNumericWithPrecisionScaleTable);
        gpdb.copyFromFile(gpdbNumericWithPrecisionScaleTable, new File(localDataResourcesFolder
                + "/numeric/" + NUMERIC_FILENAME), "E','", true);
    }

    @Test(groups = {"features", "gpdb", "security", "hcfs"})
    public void parquetReadPrimitives() throws Exception {

        exTable = new ReadableExternalTable(PXF_PARQUET_TABLE,
                PARQUET_TABLE_COLUMNS, hdfsPath + PARQUET_PRIMITIVE_TYPES, "custom");
        exTable.setHost(pxfHost);
        exTable.setPort(pxfPort);
        exTable.setFormatter("pxfwritable_import");
        exTable.setProfile(ProtocolUtils.getProtocol().value() + ":parquet");

        gpdb.createTableAndVerify(exTable);

        gpdb.runQuery("CREATE OR REPLACE VIEW parquet_view AS SELECT s1, s2, n1, d1, dc1, " +
                "CAST(tm AS TIMESTAMP WITH TIME ZONE) AT TIME ZONE 'PDT' as tm, " +
                "f, bg, b, tn, sml, vc1, c1, bin FROM " + PXF_PARQUET_TABLE);

        runTincTest("pxf.features.parquet.primitive_types.runTest");
    }

    @Test(groups = {"features", "gpdb", "security", "hcfs"})
    public void parquetReadSubset() throws Exception {
        String pxfParquetSubsetTable = "pxf_parquet_subset";
        exTable = new ReadableExternalTable(pxfParquetSubsetTable,
                PARQUET_TABLE_COLUMNS_SUBSET, hdfsPath + PARQUET_PRIMITIVE_TYPES, "custom");
        exTable.setHost(pxfHost);
        exTable.setPort(pxfPort);
        exTable.setFormatter("pxfwritable_import");
        exTable.setProfile(ProtocolUtils.getProtocol().value() + ":parquet");

        gpdb.createTableAndVerify(exTable);

        runTincTest("pxf.features.parquet.read_subset.runTest");
    }

    @Test(groups = {"features", "gpdb", "security", "hcfs"})
    public void parquetWritePrimitives() throws Exception {
        runWriteScenario("pxf_parquet_write_primitives", "pxf_parquet_read_primitives", PARQUET_WRITE_PRIMITIVES, null);
    }

    @Test(groups = {"features", "gpdb", "security", "hcfs"})
    public void parquetWritePrimitivesV2() throws Exception {
        runWriteScenario("pxf_parquet_write_primitives_v2", "pxf_parquet_read_primitives_v2", PARQUET_WRITE_PRIMITIVES_V2, new String[]{"PARQUET_VERSION=v2"});
    }

    @Test(groups = {"features", "gpdb", "security", "hcfs"})
    public void parquetWritePrimitivesGZip() throws Exception {
        runWriteScenario("pxf_parquet_write_primitives_gzip", "pxf_parquet_read_primitives_gzip", PARQUET_WRITE_PRIMITIVES_GZIP, new String[]{"COMPRESSION_CODEC=gzip"});
    }

    @Test(groups = {"features", "gpdb", "security", "hcfs"})
    public void parquetWritePrimitivesGZipClassName() throws Exception {
        runWriteScenario("pxf_parquet_write_primitives_gzip_classname", "pxf_parquet_read_primitives_gzip_classname", PARQUET_WRITE_PRIMITIVES_GZIP_CLASSNAME, new String[]{"COMPRESSION_CODEC=org.apache.hadoop.io.compress.GzipCodec"});
    }

    @Test(groups = {"features", "gpdb", "security", "hcfs"})
    public void parquetReadUndefinedPrecisionNumericFromAParquetFileGeneratedByHive() throws Exception {

        // Make sure that data generated by Hive is the same as data
        // written by PXF to Parquet for the same dataset
        exTable = new ReadableExternalTable("pxf_parquet_read_undefined_precision_numeric",
                UNDEFINED_PRECISION_NUMERIC, hdfsPath + PARQUET_UNDEFINED_PRECISION_NUMERIC_FILE, "custom");
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

    @Test(groups = {"features", "gpdb", "security", "hcfs"})
    public void parquetReadNumericWithPrecisionAndScaleFromAParquetFileGeneratedByHive() throws Exception {

        exTable = new ReadableExternalTable("pxf_parquet_read_numeric",
                PARQUET_TABLE_DECIMAL_COLUMNS, hdfsPath + PARQUET_NUMERIC_FILE, "custom");
        exTable.setHost(pxfHost);
        exTable.setPort(pxfPort);
        exTable.setFormatter("pxfwritable_import");
        exTable.setProfile(ProtocolUtils.getProtocol().value() + ":parquet");
        gpdb.createTableAndVerify(exTable);

        runTincTest("pxf.features.parquet.decimal.numeric.runTest");
    }

    @Test(groups = {"features", "gpdb", "security", "hcfs"})
    public void parquetWriteNumericWithPrecisionAndScale() throws Exception {

        String filename = "parquet_write_numeric";
        exTable = new WritableExternalTable("pxf_parquet_write_numeric",
                PARQUET_TABLE_DECIMAL_COLUMNS, hdfsPath + filename, "custom");
        exTable.setHost(pxfHost);
        exTable.setPort(pxfPort);
        exTable.setFormatter("pxfwritable_export");
        exTable.setProfile(ProtocolUtils.getProtocol().value() + ":parquet");

        gpdb.createTableAndVerify(exTable);
        gpdb.runQuery("INSERT INTO " + exTable.getName() + " SELECT * FROM " + NUMERIC_TABLE);

        exTable = new ReadableExternalTable("pxf_parquet_read_numeric",
                PARQUET_TABLE_DECIMAL_COLUMNS, hdfsPath + filename, "custom");
        exTable.setHost(pxfHost);
        exTable.setPort(pxfPort);
        exTable.setFormatter("pxfwritable_import");
        exTable.setProfile(ProtocolUtils.getProtocol().value() + ":parquet");
        gpdb.createTableAndVerify(exTable);

        runTincTest("pxf.features.parquet.decimal.numeric.runTest");
    }

    private void runWriteScenario(String writeTableName, String readTableName,
                                  String filename, String[] userParameters) throws Exception {
        exTable = new WritableExternalTable(writeTableName,
                PARQUET_TABLE_COLUMNS, hdfsPath + filename, "custom");
        exTable.setHost(pxfHost);
        exTable.setPort(pxfPort);
        exTable.setFormatter("pxfwritable_export");
        exTable.setProfile(ProtocolUtils.getProtocol().value() + ":parquet");

        if (userParameters != null) {
            exTable.setUserParameters(userParameters);
        }

        gpdb.createTableAndVerify(exTable);
        gpdb.runQuery("INSERT INTO " + exTable.getName() + " SELECT s1, s2, n1, d1, dc1, tm, " +
                "f, bg, b, tn, vc1, sml, c1, bin FROM " + PXF_PARQUET_TABLE);

        exTable = new ReadableExternalTable(readTableName,
                PARQUET_TABLE_COLUMNS, hdfsPath + filename, "custom");
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
