package org.greenplum.pxf.automation.features.parquet;

import jsystem.framework.system.SystemManagerImpl;
import org.greenplum.pxf.automation.components.hive.Hive;
import org.greenplum.pxf.automation.features.BaseFeature;
import org.greenplum.pxf.automation.structures.tables.basic.Table;
import org.greenplum.pxf.automation.structures.tables.hive.HiveExternalTable;
import org.greenplum.pxf.automation.structures.tables.hive.HiveTable;
import org.greenplum.pxf.automation.structures.tables.pxf.ExternalTable;
import org.greenplum.pxf.automation.structures.tables.utils.TableFactory;
import org.greenplum.pxf.automation.utils.system.ProtocolEnum;
import org.greenplum.pxf.automation.utils.system.ProtocolUtils;
import org.greenplum.pxf.plugins.hdfs.utilities.PgUtilities;
import org.testng.annotations.Test;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.StringJoiner;

import static java.lang.Thread.sleep;
import static org.junit.Assert.assertEquals;

public class ParquetWriteTest extends BaseFeature {
    private static final String NUMERIC_TABLE = "numeric_precision";
    private static final String NUMERIC_UNDEFINED_PRECISION_TABLE = "numeric_undefined_precision";
    private static final String PXF_PARQUET_PRIMITIVE_TABLE = "pxf_parquet_primitive_types";
    private static final String PXF_PARQUET_LIST_TYPES = "pxf_parquet_list_types";
    private static final String PXF_PARQUET_TIMESTAMP_LIST_TYPES = "pxf_parquet_timestamp_list_types";
    private static final String PARQUET_PRIMITIVE_TYPES = "parquet_primitive_types";
    private static final String PARQUET_LIST_TYPES = "parquet_list_types.parquet";
    private static final String PARQUET_TIMESTAMP_LIST_TYPES = "parquet_timestamp_list_type.parquet";
    private static final String PARQUET_UNDEFINED_PRECISION_NUMERIC_FILE = "undefined_precision_numeric.parquet";
    private static final String PARQUET_NUMERIC_FILE = "numeric.parquet";
    private static final String HIVE_JDBC_DRIVER_CLASS = "org.apache.hive.jdbc.HiveDriver";
    private static final String HIVE_JDBC_URL_PREFIX = "jdbc:hive2://";
    private static final String[] PARQUET_PRIMITIVE_TABLE_COLUMNS = new String[]{
            "s1    TEXT"            ,
            "s2    TEXT"            ,
            "n1    INTEGER"         ,
            "d1    DOUBLE PRECISION",
            "dc1   NUMERIC"         ,
            "tm    TIMESTAMP"       ,
            "f     REAL"            ,
            "bg    BIGINT"          ,
            "b     BOOLEAN"         ,
            "tn    SMALLINT"        ,
            "vc1   VARCHAR(5)"      ,
            "sml   SMALLINT"        ,
            "c1    CHAR(3)"         ,
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
            "value         numeric"
    };

    // HIVE Parquet array vectorization read currently doesn't support TIMESTAMP and INTERVAL_DAY_TIME
    //https://github.com/apache/hive/blob/master/ql/src/java/org/apache/hadoop/hive/ql/io/parquet/vector/VectorizedParquetRecordReader.java
    private static final String[] PARQUET_LIST_TABLE_COLUMNS = {
            "id                   INTEGER"      ,
            "bool_arr             BOOLEAN[]"    ,         // DataType.BOOLARRAY
            "smallint_arr         SMALLINT[]"   ,         // DataType.INT2ARRAY
            "int_arr              INTEGER[]"    ,         // DataType.INT4ARRAY
            "bigint_arr           BIGINT[]"     ,         // DataType.INT8ARRAY
            "real_arr             REAL[]"       ,         // DataType.FLOAT4ARRAY
            "double_arr           FLOAT[]"      ,         // DataType.FLOAT8ARRAY
            "text_arr             TEXT[]"       ,         // DataType.TEXTARRAY
            "bytea_arr            BYTEA[]"      ,         // DataType.BYTEAARRAY
            "char_arr             CHAR(15)[]"   ,         // DataType.BPCHARARRAY
            "varchar_arr          VARCHAR(15)[]",         // DataType.VARCHARARRAY
            "date_arr             DATE[]"       ,         // DataType.DATEARRAY
            "numeric_arr          NUMERIC[]"              // DataType.NUMERICARRAY
    };

    private static final String[] PARQUET_PRIMITIVE_ARRAYS_TABLE_COLUMNS_HIVE = {
            "id                   int"                  ,
            "bool_arr             array<boolean>"       ,           // DataType.BOOLARRAY
            "smallint_arr         array<smallint>"      ,           // DataType.INT2ARRAY
            "int_arr              array<int>"           ,           // DataType.INT4ARRAY
            "bigint_arr           array<bigint>"        ,           // DataType.INT8ARRAY
            "real_arr             array<float>"         ,           // DataType.FLOAT4ARRAY
            "double_arr           array<double>"        ,           // DataType.FLOAT8ARRAY
            "text_arr             array<string>"        ,           // DataType.TEXTARRAY
            "bytea_arr            array<binary>"        ,           // DataType.BYTEAARRAY
            "char_arr             array<char(15)>"      ,           // DataType.BPCHARARRAY
            "varchar_arr          array<varchar(15)>"   ,           // DataType.VARCHARARRAY
            "date_arr             array<date>"          ,           // DataType.DATEARRAY
            "numeric_arr          array<decimal(38,18)>"            // DataType.NUMERICARRAY
    };

    // JDBC dosen't support array, so convert array into text type for comparison
    private static final String[] PARQUET_PRIMITIVE_ARRAYS_TABLE_COLUMNS_READ_FROM_HIVE = {
            "id                   int",
            "bool_arr             text",           // DataType.BOOLARRAY
            "smallint_arr         text",           // DataType.INT2ARRAY
            "int_arr              text",           // DataType.INT4ARRAY
            "bigint_arr           text",           // DataType.INT8ARRAY
            "real_arr             text",           // DataType.FLOAT4ARRAY
            "double_arr           text",           // DataType.FLOAT8ARRAY
            "text_arr             text",           // DataType.TEXTARRAY
            "bytea_arr            text",           // DataType.BYTEAARRAY
            "char_arr             text",           // DataType.BPCHARARRAY
            "varchar_arr          text",           // DataType.VARCHARARRAY
            "date_arr             text",           // DataType.DATEARRAY
            "numeric_arr          text"            // DataType.NUMERICARRAY
    };
    private static final String[] PARQUET_TIMESTAMP_LIST_TABLE_COLUMNS = {
            "id            INTEGER",
            "tm_arr        TIMESTAMP[]"
    };
    private String hdfsPath;
    private ProtocolEnum protocol;
    private Hive hive;
    private HiveTable hiveTable;
    private String resourcePath;

    @Override
    public void beforeClass() throws Exception {
        // path for storing data on HDFS (for processing by PXF)
        hdfsPath = hdfs.getWorkingDirectory() + "/parquet/";
        protocol = ProtocolUtils.getProtocol();
        resourcePath = localDataResourcesFolder + "/parquet/";

        hdfs.copyFromLocal(resourcePath + PARQUET_PRIMITIVE_TYPES, hdfsPath + PARQUET_PRIMITIVE_TYPES);
        prepareReadableExternalTable(PXF_PARQUET_PRIMITIVE_TABLE, PARQUET_PRIMITIVE_TABLE_COLUMNS, hdfsPath + PARQUET_PRIMITIVE_TYPES);

        hdfs.copyFromLocal(resourcePath + PARQUET_LIST_TYPES, hdfsPath + PARQUET_LIST_TYPES);
        prepareReadableExternalTable(PXF_PARQUET_LIST_TYPES, PARQUET_LIST_TABLE_COLUMNS, hdfsPath + PARQUET_LIST_TYPES);
    }

    @Test(groups = {"features", "gpdb", "security", "hcfs"})
    public void parquetWritePaddedChar() throws Exception {
        /* 1. run the regular test */
        runWritePrimitivesScenario("pxf_parquet_write_padded_char", "pxf_parquet_read_padded_char", "parquet_write_padded_char", null);

        /* 2. Insert data with chars that need padding */
        gpdb.runQuery("INSERT INTO pxf_parquet_write_padded_char VALUES ('row25_char_needs_padding', 's_17', 11, 37, 0.123456789012345679, " +
                "'2013-07-23 21:00:05', 7.7, 23456789, false, 11, 'abcde', 1100, 'a  ', '1')");
        gpdb.runQuery("INSERT INTO pxf_parquet_write_padded_char VALUES ('row26_char_with_tab', 's_17', 11, 37, 0.123456789012345679, " +
                "'2013-07-23 21:00:05', 7.7, 23456789, false, 11, 'abcde', 1100, e'b\\t ', '1')");
        gpdb.runQuery("INSERT INTO pxf_parquet_write_padded_char VALUES ('row27_char_with_newline', 's_17', 11, 37, 0.123456789012345679, " +
                "'2013-07-23 21:00:05', 7.7, 23456789, false, 11, 'abcde', 1100, e'c\\n ', '1')");

        if (protocol != ProtocolEnum.HDFS && protocol != ProtocolEnum.FILE) {
            // for HCFS on Cloud, wait a bit for async write in previous steps to finish
            sleep(10000);
        }

        runTincTest("pxf.features.parquet.padded_char_pushdown.runTest");
    }

    @Test(groups = {"features", "gpdb", "security", "hcfs"})
    public void parquetWritePrimitives() throws Exception {
        runWritePrimitivesScenario("pxf_parquet_write_primitives", "pxf_parquet_read_primitives", "parquet_write_primitives", null);
    }

    @Test(groups = {"features", "gpdb", "security", "hcfs"})
    public void parquetWritePrimitivesV2() throws Exception {
        runWritePrimitivesScenario("pxf_parquet_write_primitives_v2", "pxf_parquet_read_primitives_v2", "parquet_write_primitives_v2", new String[]{"PARQUET_VERSION=v2"});
    }

    @Test(groups = {"features", "gpdb", "security", "hcfs"})
    public void parquetWritePrimitivesGZip() throws Exception {
        runWritePrimitivesScenario("pxf_parquet_write_primitives_gzip", "pxf_parquet_read_primitives_gzip", "parquet_write_primitives_gzip", new String[]{"COMPRESSION_CODEC=gzip"});
    }

    @Test(groups = {"features", "gpdb", "security", "hcfs"})
    public void parquetWritePrimitivesGZipClassName() throws Exception {
        runWritePrimitivesScenario("pxf_parquet_write_primitives_gzip_classname", "pxf_parquet_read_primitives_gzip_classname", "parquet_write_primitives_gzip_classname", new String[]{"COMPRESSION_CODEC=org.apache.hadoop.io.compress.GzipCodec"});
    }

    @Test(groups = {"features", "gpdb", "security", "hcfs"})
    public void parquetWriteUndefinedPrecisionNumeric() throws Exception {
        hdfs.copyFromLocal(resourcePath + PARQUET_UNDEFINED_PRECISION_NUMERIC_FILE, hdfsPath + PARQUET_UNDEFINED_PRECISION_NUMERIC_FILE);

        Table gpdbUndefinedPrecisionNumericTable = new Table(NUMERIC_UNDEFINED_PRECISION_TABLE, UNDEFINED_PRECISION_NUMERIC);
        gpdbUndefinedPrecisionNumericTable.setDistributionFields(new String[]{"description"});
        gpdb.createTableAndVerify(gpdbUndefinedPrecisionNumericTable);
        gpdb.copyFromFile(gpdbUndefinedPrecisionNumericTable, new File(localDataResourcesFolder
                + "/numeric/undefined_precision_numeric.csv"), "E','", true);

        String filename = "parquet_write_undefined_precision_numeric";
        prepareWritableExternalTable("pxf_parquet_write_undefined_precision_numeric",
                UNDEFINED_PRECISION_NUMERIC, hdfsPath + filename, null);
        exTable.setHost(pxfHost);
        exTable.setPort(pxfPort);
        exTable.setFormatter("pxfwritable_export");
        exTable.setProfile(ProtocolUtils.getProtocol().value() + ":parquet");

        gpdb.createTableAndVerify(exTable);
        gpdb.runQuery("INSERT INTO " + exTable.getName() + " SELECT * FROM " + NUMERIC_UNDEFINED_PRECISION_TABLE);

        prepareReadableExternalTable("pxf_parquet_read_undefined_precision_numeric",
                UNDEFINED_PRECISION_NUMERIC, hdfsPath + filename);
        exTable.setHost(pxfHost);
        exTable.setPort(pxfPort);
        exTable.setFormatter("pxfwritable_import");
        exTable.setProfile(ProtocolUtils.getProtocol().value() + ":parquet");
        gpdb.createTableAndVerify(exTable);

        runTincTest("pxf.features.parquet.decimal.numeric_undefined_precision.runTest");
    }

    @Test(groups = {"features", "gpdb", "security", "hcfs"})
    public void parquetWriteNumericWithPrecisionAndScale() throws Exception {
        hdfs.copyFromLocal(resourcePath + PARQUET_NUMERIC_FILE, hdfsPath + PARQUET_NUMERIC_FILE);

        Table gpdbNumericWithPrecisionScaleTable = new Table(NUMERIC_TABLE, PARQUET_TABLE_DECIMAL_COLUMNS);
        gpdbNumericWithPrecisionScaleTable.setDistributionFields(new String[]{"description"});
        gpdb.createTableAndVerify(gpdbNumericWithPrecisionScaleTable);
        gpdb.copyFromFile(gpdbNumericWithPrecisionScaleTable, new File(localDataResourcesFolder
                + "/numeric/numeric_with_precision.csv"), "E','", true);

        String filename = "parquet_write_numeric";
        prepareWritableExternalTable("pxf_parquet_write_numeric",
                PARQUET_TABLE_DECIMAL_COLUMNS, hdfsPath + filename, null);
        gpdb.runQuery("INSERT INTO " + exTable.getName() + " SELECT * FROM " + NUMERIC_TABLE);

        prepareReadableExternalTable("pxf_parquet_read_numeric",
                PARQUET_TABLE_DECIMAL_COLUMNS, hdfsPath + filename);
        runTincTest("pxf.features.parquet.decimal.numeric.runTest");
    }

    @Test(groups = {"features", "gpdb", "security", "hcfs"})
    public void parquetWriteLists() throws Exception {
        String writeTableName = "pxf_parquet_write_list";
        String readTableName = "pxf_parquet_read_list";
        String fullTestPath = hdfsPath + "parquet_write_list";

        prepareWritableExternalTable(writeTableName, PARQUET_LIST_TABLE_COLUMNS, fullTestPath, null);
        gpdb.runQuery("INSERT INTO " + exTable.getName() + " SELECT id, bool_arr, smallint_arr, int_arr, bigint_arr, real_arr, " +
                "double_arr, text_arr, bytea_arr, char_arr, varchar_arr, date_arr, numeric_arr FROM " + PXF_PARQUET_LIST_TYPES);

        waitForAsyncWriteToSucceedOnHCFS("parquet_write_list");

        prepareReadableExternalTable(readTableName, PARQUET_LIST_TABLE_COLUMNS, fullTestPath);
        runTincTest("pxf.features.parquet.write_list.list.runTest");
    }

    @Test(groups = {"features", "gpdb", "security", "hcfs"})
    public void parquetWriteTimestampLists() throws Exception {
        hdfs.copyFromLocal(resourcePath + PARQUET_TIMESTAMP_LIST_TYPES, hdfsPath + PARQUET_TIMESTAMP_LIST_TYPES);
        prepareReadableExternalTable(PXF_PARQUET_TIMESTAMP_LIST_TYPES, PARQUET_TIMESTAMP_LIST_TABLE_COLUMNS, hdfsPath + PARQUET_TIMESTAMP_LIST_TYPES);

        String writeTableName = "pxf_parquet_write_timestamp_list";
        String readTableName = "pxf_parquet_read_timestamp_list";
        String fullTestPath = hdfsPath + "parquet_write_timestamp_list";

        prepareWritableExternalTable(writeTableName, PARQUET_TIMESTAMP_LIST_TABLE_COLUMNS, fullTestPath, null);
        gpdb.runQuery("INSERT INTO " + exTable.getName() + " SELECT id, tm_arr FROM " + PXF_PARQUET_TIMESTAMP_LIST_TYPES);

        waitForAsyncWriteToSucceedOnHCFS("parquet_write_timestamp_list");

        prepareReadableExternalTable(readTableName, PARQUET_TIMESTAMP_LIST_TABLE_COLUMNS, fullTestPath);

        runTincTest("pxf.features.parquet.write_list.timestamp_list.runTest");
    }

    @Test(groups = {"features", "gpdb", "security"})
    public void parquetWriteListsReadWithHive() throws Exception {
        // init only here, not in beforeClass() method as other tests run in environments without Hive
        hive = (Hive) SystemManagerImpl.getInstance().getSystemObject("hive");

        String writeTableName = "pxf_parquet_write_list_read_with_hive_writable";
        String readTableName = "pxf_parquet_write_list_read_with_hive_readable";
        String fullTestPath = hdfsPath + "parquet_write_list_read_with_hive";

        prepareWritableExternalTable(writeTableName, PARQUET_LIST_TABLE_COLUMNS, fullTestPath, null);
        insertArrayDataWithoutNulls(writeTableName, 33);

        // load the data into hive to check that PXF-written Parquet files can be read by other data
        String hiveExternalTableName = writeTableName + "_external";
        hiveTable = new HiveExternalTable(hiveExternalTableName, PARQUET_PRIMITIVE_ARRAYS_TABLE_COLUMNS_HIVE, "hdfs:/" + fullTestPath);
        hiveTable.setStoredAs("PARQUET");
        hive.createTableAndVerify(hiveTable);

        String ctasHiveQuery = new StringJoiner(",",
                "CREATE TABLE " + hiveTable.getFullName() + "_ctas AS SELECT ", " FROM " + hiveTable.getFullName() + " ORDER BY id")
                .add("id")
                .add("bool_arr")
                .add("smallint_arr")
                .add("int_arr")
                .add("bigint_arr")
                .add("real_arr")
                .add("double_arr")
                .add("text_arr")
                .add("bytea_arr")
                .add("char_arr")
                .add("varchar_arr")
                .add("date_arr")
                .add("numeric_arr")
                .toString();

        hive.runQuery("DROP TABLE IF EXISTS " + hiveTable.getFullName() + "_ctas");
        hive.runQuery(ctasHiveQuery);

        // Check the bytea_array using the following way since the JDBC profile cannot handle binary
        Table hiveResultTable = new Table(hiveTable.getFullName() + "_ctas", PARQUET_PRIMITIVE_ARRAYS_TABLE_COLUMNS_HIVE);
        hive.queryResults(hiveResultTable, "SELECT id,bytea_arr FROM " + hiveTable.getFullName() + "_ctas ORDER BY id");
        assertHiveByteaArrayData(hiveResultTable.getData());

        // use the Hive JDBC profile to avoid using the PXF Parquet reader implementation
        String jdbcUrl = HIVE_JDBC_URL_PREFIX + hive.getHost() + ":10000/default";

        ExternalTable exHiveJdbcTable = TableFactory.getPxfJdbcReadableTable(
                readTableName, PARQUET_PRIMITIVE_ARRAYS_TABLE_COLUMNS_READ_FROM_HIVE,
                hiveTable.getName() + "_ctas", HIVE_JDBC_DRIVER_CLASS, jdbcUrl, null);
        exHiveJdbcTable.setHost(pxfHost);
        exHiveJdbcTable.setPort(pxfPort);
        gpdb.createTableAndVerify(exHiveJdbcTable);

        runTincTest("pxf.features.parquet.write_list.write_list_read_with_hive.runTest");
    }

    @Test(groups = {"features", "gpdb", "security", "hcfs"})
    public void parquetWriteListsUserProvidedSchemaFile_ValidSchema() throws Exception {

        String writeTableName = "parquet_list_user_provided_schema_on_hcfs_write";
        String readTableName = "parquet_list_user_provided_schema_on_hcfs_read";

        String fullTestPath = hdfsPath + "parquet_write_lists_with_user_provided_schema_file_on_hcfs";
        prepareWritableExternalTable(writeTableName, PARQUET_LIST_TABLE_COLUMNS, fullTestPath, null);

        String schemaPath;
        ProtocolEnum protocol = ProtocolUtils.getProtocol();
        String absoluteSchemaPath = hdfs.getWorkingDirectory() + "/parquet_schema/parquet_list.schema";
        if (protocol == ProtocolEnum.FILE) {
            // we expect user to provide relative path for the schema file
            schemaPath = hdfs.getRelativeWorkingDirectory() + "/parquet_schema/parquet_list.schema";
        } else {
            schemaPath = "/" + absoluteSchemaPath;
        }

        hdfs.copyFromLocal(resourcePath + "parquet_list.schema", absoluteSchemaPath);
        exTable.setExternalDataSchema(schemaPath);
        // update the exTable with schema file provided
        gpdb.createTableAndVerify(exTable);

        gpdb.runQuery("INSERT INTO " + exTable.getName() + " SELECT id, bool_arr, smallint_arr, int_arr, bigint_arr, real_arr, " +
                "double_arr, text_arr, bytea_arr, char_arr, varchar_arr, date_arr, numeric_arr FROM " + PXF_PARQUET_LIST_TYPES);

        waitForAsyncWriteToSucceedOnHCFS("parquet_write_lists_with_user_provided_schema_file_on_hcfs");

        prepareReadableExternalTable(readTableName, PARQUET_LIST_TABLE_COLUMNS, fullTestPath);
        runTincTest("pxf.features.parquet.write_list.write_with_valid_schema_hcfs.runTest");
    }

    @Test(groups = {"features", "gpdb", "security", "hcfs"})
    public void parquetWriteListsUserProvidedSchemaFile_InvalidSchema() throws Exception {
        String writeTableName = "parquet_list_user_provided_invalid_schema_write";

        String fullTestPath = hdfsPath + "parquet_write_list_with_user_provided_invalid_schema_file";
        prepareWritableExternalTable(writeTableName, PARQUET_LIST_TABLE_COLUMNS, fullTestPath, null);

        String schemaPath;
        ProtocolEnum protocol = ProtocolUtils.getProtocol();
        String absoluteSchemaPath = hdfs.getWorkingDirectory() + "/parquet_schema/invalid_parquet_list.schema";
        if (protocol == ProtocolEnum.FILE) {
            // we expect user to provide relative path for the schema file
            schemaPath = hdfs.getRelativeWorkingDirectory() + "/parquet_schema/invalid_parquet_list.schema";
        } else {
            schemaPath = "/" + absoluteSchemaPath;
        }

        hdfs.copyFromLocal(resourcePath + "invalid_parquet_list.schema", absoluteSchemaPath);
        exTable.setExternalDataSchema(schemaPath);
        // update the exTable with schema file provided
        gpdb.createTableAndVerify(exTable);

        runTincTest("pxf.features.parquet.write_list.write_with_invalid_schema_hcfs.runTest");
    }

    private void runWritePrimitivesScenario(String writeTableName, String readTableName,
                                            String filename, String[] userParameters) throws Exception {
        prepareWritableExternalTable(writeTableName,
                PARQUET_PRIMITIVE_TABLE_COLUMNS, hdfsPath + filename, userParameters);
        gpdb.runQuery("INSERT INTO " + exTable.getName() + " SELECT s1, s2, n1, d1, dc1, tm, " +
                "f, bg, b, tn, vc1, sml, c1, bin FROM " + PXF_PARQUET_PRIMITIVE_TABLE);

        waitForAsyncWriteToSucceedOnHCFS(filename);

        prepareReadableExternalTable(readTableName,
                PARQUET_PRIMITIVE_TABLE_COLUMNS, hdfsPath + filename);
        gpdb.runQuery("CREATE OR REPLACE VIEW parquet_view AS SELECT s1, s2, n1, d1, dc1, " +
                "CAST(tm AS TIMESTAMP WITH TIME ZONE) AT TIME ZONE 'PDT' as tm, " +
                "f, bg, b, tn, sml, vc1, c1, bin FROM " + readTableName);

        runTincTest("pxf.features.parquet.primitive_types.runTest");
    }

    private void prepareReadableExternalTable(String name, String[] fields, String path) throws Exception {
        exTable = TableFactory.getPxfHcfsReadableTable(name, fields, path, hdfs.getBasePath(), "parquet");
        createTable(exTable);
    }

    private void prepareWritableExternalTable(String name, String[] fields, String path, String[] userParameters) throws Exception {
        exTable = TableFactory.getPxfHcfsWritableTable(name, fields, path, hdfs.getBasePath(), "parquet");
        if (userParameters != null) {
            exTable.setUserParameters(userParameters);
        }
        createTable(exTable);
    }

    private void waitForAsyncWriteToSucceedOnHCFS(String filename) throws Exception {
        if (protocol != ProtocolEnum.HDFS && protocol != ProtocolEnum.FILE) {
            // for HCFS on Cloud, wait a bit for async write in previous steps to finish
            sleep(10000);
            List<String> files = hdfs.list(hdfsPath + filename);
            for (String file : files) {
                // make sure the file is available, saw flakes on Cloud that listed files were not available
                int attempts = 0;
                while (!hdfs.doesFileExist(file) && attempts++ < 20) {
                    sleep(1000);
                }
            }
        }
    }

    private void insertArrayDataWithoutNulls(String exTable, int numRows) throws Exception {
        StringBuilder insertStatement = new StringBuilder();
        insertStatement.append("INSERT INTO " + exTable + " VALUES ");
        for (int i = 0; i < numRows; i++) {
            StringJoiner statementBuilder = new StringJoiner(",", "(", ")")
                    .add(String.valueOf(i))    // always not-null row index, column index starts with 0 after it
                    .add(String.format("'{\"%b\"}'", i % 2 != 0))                                    // DataType.BOOLEANARRAY
                    .add(String.format("'{%d}'", 10L + i % 32000))                                   // DataType.INT2ARRAY
                    .add(String.format("'{%d}'", 100L + i))                                          // DataType.INT4ARRAY
                    .add(String.format("'{%d}'", 123456789000000000L + i))                           // DataType.INT8ARRAY
                    .add(String.format("'{%.4f}'", Float.valueOf(i + 0.00001f * i)))              // DataType.FLOAT4ARRAY
                    .add(String.format("'{%f}'", i + Math.PI))                                       // DataType.FLOAT8ARRAY
                    .add(String.format("'{\"row-%02d\"}'", i))                                       // DataType.TEXTARRAY
                    .add(String.format("'{\\\\x%02d%02d}'::bytea[]", i % 100, (i + 1) % 100))        // DataType.BYTEAARRAY
                    .add(String.format("'{\"%s\"}'", i))                                             // DataType.BPCHARARRAY
                    .add(String.format("'{\"var%02d\"}'", i))                                        // DataType.VARCHARARRAY
                    .add(String.format("'{\"2010-01-%02d\"}'", (i % 30) + 1))                        // DataType.DATEARRAY
                    .add(String.format("'{12345678900000.00000%s}'", i))                             // DataType.NUMERICARRAY
                    ;
            insertStatement.append(statementBuilder.toString().concat((i < (numRows - 1)) ? "," : ";"));
        }
        gpdb.runQuery(insertStatement.toString());
    }

    private void assertHiveByteaArrayData(List<List<String>> queryResultData) {
        PgUtilities pgUtilities = new PgUtilities();

        for (int i = 0; i < queryResultData.size(); i++) {
            StringJoiner rowBuilder = new StringJoiner(", ", "[", "]")
                    .add(String.valueOf(i))    // always not-null row index, column index starts with 0 after it                    // DataType.TEXTARRAY
                    .add(String.format("[\\\\x%02d%02d]", i % 100, (i + 1) % 100))                      // DataType.BYTEAARRAY
                    ;

            // Only 1 bytea element in bytea_array. Need to convert the bytea result in the array into a hex string
            String byteaArrayString = queryResultData.get(i).get(1);
            byteaArrayString = byteaArrayString.substring(1, byteaArrayString.length() - 1);
            ByteBuffer byteBuffer = ByteBuffer.wrap(byteaArrayString.getBytes());
            String hexString = pgUtilities.encodeByteaHex(byteBuffer); // \x0001, need another \ when added into string
            queryResultData.get(i).set(1, "[\\" + hexString + "]");
            assertEquals(rowBuilder.toString(), queryResultData.get(i).toString());
        }
    }
}