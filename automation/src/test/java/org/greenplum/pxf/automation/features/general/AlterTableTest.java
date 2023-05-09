package org.greenplum.pxf.automation.features.general;

import annotations.FailsWithFDW;
import annotations.WorksWithFDW;
import org.greenplum.pxf.automation.features.BaseFeature;
import org.greenplum.pxf.automation.structures.tables.basic.Table;
import org.greenplum.pxf.automation.structures.tables.utils.TableFactory;
import org.greenplum.pxf.automation.utils.system.ProtocolUtils;
import org.testng.annotations.Test;

@WorksWithFDW
public class AlterTableTest extends BaseFeature {

    private static final String AVRO_TYPES_FILE_NAME = "supported_primitive_types";
    private static final String FILE_SCHEME = "file://";
    private static final String PXF_ALTER_AVRO_TABLE = "pxf_alter_avro_table";
    private static final String PXF_ALTER_CSV_TABLE = "pxf_alter_csv_table";
    private static final String PXF_PARQUET_TABLE_SOURCE = "pxf_alter_parquet_primitive_types";
    private static final String PXF_ALTER_PARQUET_TABLE = "pxf_alter_parquet_table";
    private static final String PXF_ALTER_WRITE_PARQUET_TABLE = "pxf_alter_write_parquet_table";
    private static final String PARQUET_PRIMITIVE_TYPES = "parquet_primitive_types";
    private static final String PARQUET_WRITE_PRIMITIVES = "parquet_write_primitives";
    private static final String SUFFIX_JSON = ".json";
    private static final String SUFFIX_AVRO = ".avro";
    private static final String SUFFIX_AVSC = ".avsc";

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

    private static final String[] PARQUET_TABLE_SUBSET_COLUMNS = new String[]{
            "s1    TEXT",
            "s2    TEXT",
            "n1    INTEGER",
            "d1    DOUBLE PRECISION",
            "dc1   NUMERIC",
            "f     REAL",
            "bg    BIGINT",
            "b     BOOLEAN",
            "tn    SMALLINT",
            "vc1   VARCHAR(5)",
            "sml   SMALLINT",
            "c1    CHAR(3)"
    };

    private String hdfsPath;

    @Override
    public void beforeClass() throws Exception {
        // path for storing data on HDFS (for processing by PXF)
        hdfsPath = hdfs.getWorkingDirectory() + "/alter-tests";

        String resourcePath = localDataResourcesFolder + "/parquet/";
        hdfs.copyFromLocal(resourcePath + PARQUET_PRIMITIVE_TYPES, hdfsPath + "/parquet/" + PARQUET_PRIMITIVE_TYPES);

        // Create Data and write it to HDFS
        Table dataTable = getSmallData();
        hdfs.writeTableToFile(hdfsPath + "/csv/" + fileName, dataTable, ",");

        // Avro
        // location of schema and data files
        String absolutePath = getClass().getClassLoader().getResource("data").getPath();
        resourcePath = absolutePath + "/avro/";
        hdfs.writeAvroFileFromJson(hdfsPath + "/avro/" + AVRO_TYPES_FILE_NAME + SUFFIX_AVRO,
                FILE_SCHEME + resourcePath + AVRO_TYPES_FILE_NAME + SUFFIX_AVSC,
                FILE_SCHEME + resourcePath + AVRO_TYPES_FILE_NAME + SUFFIX_JSON, null);
    }

    /**
     * Query data on a table, then drops column(s), then queries again.
     * Finally, the test adds back one of the dropped columns to the table,
     * and a new query is performed. The query uses parquet which supports
     * column projection, and uses the pxfwritable_import formatter.
     *
     * @throws Exception when the test execution fails
     */
    @Test(groups = {"features", "gpdb", "security"})
    public void dropAndAddColumnsPxfWritableImportWithColumnProjectionSupport() throws Exception {

        exTable = TableFactory.getPxfReadableCustomTable(PXF_ALTER_PARQUET_TABLE,
                PARQUET_TABLE_COLUMNS, hdfsPath + "/parquet/" + PARQUET_PRIMITIVE_TYPES, "parquet");
        setParamsAndVerifyTable();

        runTincTest("pxf.features.general.alter.pxfwritable_import.with_column_projection.runTest");
    }

    // TODO: Determine the reason why FDW is failing with the below class cast exception:
    //
    // ERROR:  PXF server error : class java.io.DataInputStream cannot be cast to class java.lang.String
    // (java.io.DataInputStream and java.lang.String are in module java.base of loader 'bootstrap')
    @FailsWithFDW
    @Test(groups = {"features", "gpdb", "security"})
    public void dropColumnsPxfWritableExport() throws Exception {

        // Create source table
        exTable = TableFactory.getPxfReadableCustomTable(PXF_PARQUET_TABLE_SOURCE,
                PARQUET_TABLE_COLUMNS, hdfsPath + "/parquet/" + PARQUET_PRIMITIVE_TYPES, "parquet");
        setParamsAndVerifyTable();

        // Create writable table
        exTable = TableFactory.getPxfWritableTextTable(PXF_ALTER_WRITE_PARQUET_TABLE,
                PARQUET_TABLE_COLUMNS, hdfsPath + "/parquet-write/" + PARQUET_WRITE_PRIMITIVES, null);
        exTable.setFormatter("pxfwritable_export");
        exTable.setProfile(ProtocolUtils.getProtocol().value() + ":parquet");
        exTable.setFormat("CUSTOM");
        setParamsAndVerifyTable();

        // Create validation table
        exTable = TableFactory.getPxfReadableCustomTable(PXF_ALTER_WRITE_PARQUET_TABLE + "_r",
                PARQUET_TABLE_SUBSET_COLUMNS, hdfsPath + "/parquet-write/" + PARQUET_WRITE_PRIMITIVES, "parquet");
        setParamsAndVerifyTable();

        runTincTest("pxf.features.general.alter.pxfwritable_export.parquet.runTest");
    }

    @Test(groups = {"features", "gpdb", "security"})
    public void dropAndAddColumnsPxfWritableImportWithoutColumnProjectionSupport() throws Exception {
        // default external table with common settings
        exTable = TableFactory.getPxfReadableCustomTable(PXF_ALTER_AVRO_TABLE, new String[]{
                "type_int int",
                "type_double float8",
                "type_string text",
                "type_float real",
                "col_does_not_exist text",
                "type_long bigint",
                "type_bytes bytea",
                "type_boolean bool"}, hdfsPath + "/avro/" + AVRO_TYPES_FILE_NAME + SUFFIX_AVRO, "avro");
        setParamsAndVerifyTable();

        // Verify results
        runTincTest("pxf.features.general.alter.pxfwritable_import.without_column_projection.runTest");
    }

    /**
     * Attempt to query data on a table with column that does not exist on the
     * CSV file. We then drop the non-existent column from the table definition
     * such that the CSV schema and table schema now match, and the query
     * executes successfully.
     *
     * @throws Exception when the test execution fails
     */
    @Test(groups = {"features", "gpdb", "security"})
    public void dropAndAddColumsCsv() throws Exception {

        // Create GPDB external table directed to the HDFS file
        exTable =
                TableFactory.getPxfReadableTextTable(PXF_ALTER_CSV_TABLE, new String[]{
                        "col_does_not_exist text",
                        "name text",
                        "num integer",
                        "dub double precision",
                        "longNum bigint",
                        "bool boolean"
                }, hdfsPath + "/csv/" + fileName, ",");
        setParamsAndVerifyTable();

        runTincTest("pxf.features.general.alter.csv.runTest");
    }
    private void setParamsAndVerifyTable() throws Exception
    {
        exTable.setHost(pxfHost);
        exTable.setPort(pxfPort);
        gpdb.createTableAndVerify(exTable);
    }
}
