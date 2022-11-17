package org.greenplum.pxf.automation.features.parquet;

import jsystem.framework.system.SystemManagerImpl;
import org.greenplum.pxf.automation.components.hive.Hive;
import org.greenplum.pxf.automation.features.BaseFeature;
import org.greenplum.pxf.automation.structures.tables.basic.Table;
import org.greenplum.pxf.automation.structures.tables.hive.HiveTable;
import org.greenplum.pxf.automation.structures.tables.pxf.ReadableExternalTable;
import org.greenplum.pxf.automation.structures.tables.pxf.WritableExternalTable;
import org.greenplum.pxf.automation.structures.tables.utils.TableFactory;
import org.greenplum.pxf.automation.utils.system.ProtocolEnum;
import org.greenplum.pxf.automation.utils.system.ProtocolUtils;
import org.testng.annotations.Test;

import java.io.File;
import java.util.List;
import java.util.StringJoiner;

import static java.lang.Thread.sleep;

public class ParquetWriteTest extends BaseFeature {
    private static final String NUMERIC_TABLE = "numeric_precision";
    private static final String NUMERIC_UNDEFINED_PRECISION_TABLE = "numeric_undefined_precision";
    private static final String PXF_PARQUET_PRIMITIVE_TABLE = "pxf_parquet_primitive_types";
    private static final String PXF_PARQUET_LIST_TYPES = "pxf_parquet_list_types";
    private static final String PARQUET_WRITE_PRIMITIVES = "parquet_write_primitives";
    private static final String PARQUET_WRITE_PADDED_CHAR = "parquet_write_padded_char";
    private static final String PARQUET_WRITE_PRIMITIVES_GZIP = "parquet_write_primitives_gzip";
    private static final String PARQUET_WRITE_PRIMITIVES_GZIP_CLASSNAME = "parquet_write_primitives_gzip_classname";
    private static final String PARQUET_WRITE_PRIMITIVES_V2 = "parquet_write_primitives_v2";
    private static final String PARQUET_WRITE_LIST = "parquet_write_list";
    private static final String PARQUET_PRIMITIVE_TYPES = "parquet_primitive_types";
    private static final String PARQUET_LIST_TYPES = "parquet_list_types.parquet";
    private static final String PARQUET_TYPES = "parquet_types.parquet";
    private static final String PARQUET_UNDEFINED_PRECISION_NUMERIC_FILE = "undefined_precision_numeric.parquet";
    private static final String PARQUET_NUMERIC_FILE = "numeric.parquet";
    private static final String UNDEFINED_PRECISION_NUMERIC_FILENAME = "undefined_precision_numeric.csv";
    private static final String NUMERIC_FILENAME = "numeric_with_precision.csv";
    private static final String[] PARQUET_PRIMITIVE_TABLE_COLUMNS = new String[]{
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
            "value         numeric"
    };

    // HIVE Parquet array vectorization read currently doesn't support TIMESTAMP and INTERVAL_DAY_TIME
    //https://github.com/apache/hive/blob/master/ql/src/java/org/apache/hadoop/hive/ql/io/parquet/vector/VectorizedParquetRecordReader.java
    private static final String[] PARQUET_LIST_TABLE_COLUMNS = {
            "id                   INTEGER",
            "bool_arr             BOOLEAN[]", // DataType.BOOLARRAY
            "smallint_arr         SMALLINT[]", // DataType.INT2ARRAY
            "int_arr              INTEGER[]", // DataType.INT4ARRAY
            "bigint_arr           BIGINT[]", // DataType.INT8ARRAY
            "real_arr             REAL[]", // DataType.FLOAT4ARRAY
            "double_arr           FLOAT[]", // DataType.FLOAT8ARRAY
            "text_arr             TEXT[]", // DataType.TEXTARRAY
            "bytea_arr            BYTEA[]", // DataType.BYTEAARRAY
            "char_arr             CHAR(15)[]", // DataType.BPCHARARRAY
            "varchar_arr          VARCHAR(15)[]", // DataType.VARCHARARRAY
            "date_arr             DATE[]", // DataType.DATEARRAY
            "numeric_arr          NUMERIC[]", // DataType.NUMERICARRAY
    };

    private static final String[] PARQUET_PRIMITIVE_ARRAYS_TABLE_COLUMNS_HIVE = {
            "id                   integer",
            "bool_arr             array<boolean>", // DataType.BOOLARRAY
//            "bytea_arr            array<binary>"      , // DataType.BYTEAARRAY  // not correct
            "bigint_arr           array<bigint>", // DataType.INT8ARRAY
            "smallint_arr         array<smallint>", // DataType.INT2ARRAY
            "int_arr              array<int>", // DataType.INT4ARRAY
            "text_arr             array<string>", // DataType.TEXTARRAY
            "real_arr             array<float>", // DataType.FLOAT4ARRAY
            "double_arr            array<double>", // DataType.FLOAT8ARRAY
            "char_arr             array<char(7)>", // DataType.BPCHARARRAY
            "varchar_arr          array<varchar(8)>", // DataType.VARCHARARRAY
            "varchar_arr_nolimit  array<varchar(65535)>", // DataType.VARCHARARRAY with no length limit, varchar length must be in the range [1, 65535]
            "date_arr             array<date>", // DataType.DATEARRAY
//            "timestamp_arr        array<timestamp>"  , // DataType.TIMESTAMPARRAY
//           "timestamptz_arr      timestamptz[]", // DataType.TIMESTAMP_WITH_TIME_ZONE_ARRAY
            "numeric_arr          array<decimal(38,18)>", // DataType.NUMERICARRAY
    };

    private static final String[] PARQUET_PRIMITIVE_ARRAYS_TABLE_COLUMNS_READ_FROM_HIVE = {
            "id                   INTEGER",
            "bool_arr             BOOLEAN[]", // DataType.BOOLARRAY
            "bytea_arr            TEXT[]", // DataType.BYTEAARRAY
            "bigint_arr           BIGINT[]", // DataType.INT8ARRAY
            "smallint_arr         SMALLINT[]", // DataType.INT2ARRAY
            "int_arr              INTEGER[]", // DataType.INT4ARRAY
            "text_arr             TEXT[]", // DataType.TEXTARRAY
            "real_arr             REAL[]", // DataType.FLOAT4ARRAY
            "double_arr           FLOAT[]", // DataType.FLOAT8ARRAY
            "char_arr             CHAR(7)[]", // DataType.BPCHARARRAY
            "varchar_arr          VARCHAR(8)[]", // DataType.VARCHARARRAY
            "varchar_arr_nolimit  VARCHAR[]", // DataType.VARCHARARRAY with no length limit
            "date_arr             DATE[]", // DataType.DATEARRAY
//            "timestamp_arr        TIMESTAMP[]"  , // DataType.TIMESTAMPARRAY
//            "timestamptz_arr      TIMESTAMPTZ[]", // DataType.TIMESTAMP_WITH_TIME_ZONE_ARRAY
            "numeric_arr          NUMERIC[]", // DataType.NUMERICARRAY
    };

    // parquet TIMESTAMP LIST data generated using Spark
    private static final String[] PARQUET_TIMESTAMP_LIST_TABLE_COLUMNS = {
            "id            INTEGER",
            "tm_arr        TIMESTAMP[]",
//            "tmtz_zrr      TIMESTAMPTZ[]"
    };
    private static final String HIVE_JDBC_DRIVER_CLASS = "org.apache.hive.jdbc.HiveDriver";
    private static final String HIVE_JDBC_URL_PREFIX = "jdbc:hive2://";
    private String hdfsPath;
    private ProtocolEnum protocol;
    private Hive hive;
    private HiveTable hiveTable;

    @Override
    public void beforeClass() throws Exception {
        // path for storing data on HDFS (for processing by PXF)
        hdfsPath = hdfs.getWorkingDirectory() + "/parquet/";
        protocol = ProtocolUtils.getProtocol();

        String resourcePath = localDataResourcesFolder + "/parquet/";
        hdfs.copyFromLocal(resourcePath + PARQUET_PRIMITIVE_TYPES, hdfsPath + PARQUET_PRIMITIVE_TYPES);
        hdfs.copyFromLocal(resourcePath + PARQUET_UNDEFINED_PRECISION_NUMERIC_FILE, hdfsPath + PARQUET_UNDEFINED_PRECISION_NUMERIC_FILE);
        hdfs.copyFromLocal(resourcePath + PARQUET_NUMERIC_FILE, hdfsPath + PARQUET_NUMERIC_FILE);
        hdfs.copyFromLocal(resourcePath + PARQUET_TYPES, hdfsPath + PARQUET_TYPES);

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

        prepareReadableExternalTable(PXF_PARQUET_PRIMITIVE_TABLE, PARQUET_PRIMITIVE_TABLE_COLUMNS, hdfsPath + PARQUET_PRIMITIVE_TYPES);
        hdfs.copyFromLocal(resourcePath + PARQUET_LIST_TYPES, hdfsPath + PARQUET_LIST_TYPES);
        prepareReadableExternalTable(PXF_PARQUET_LIST_TYPES, PARQUET_LIST_TABLE_COLUMNS, hdfsPath + PARQUET_LIST_TYPES);
    }

    @Test(groups = {"features", "gpdb", "security", "hcfs"})
    public void parquetWritePaddedChar() throws Exception {

        /* 1. run the regular test */
        runWritePrimitiveScenario("pxf_parquet_write_padded_char", "pxf_parquet_read_padded_char", PARQUET_WRITE_PADDED_CHAR, null);

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
        runWritePrimitiveScenario("pxf_parquet_write_primitives", "pxf_parquet_read_primitives", PARQUET_WRITE_PRIMITIVES, null);
    }

    @Test(groups = {"features", "gpdb", "security", "hcfs"})
    public void parquetWritePrimitivesV2() throws Exception {
        runWritePrimitiveScenario("pxf_parquet_write_primitives_v2", "pxf_parquet_read_primitives_v2", PARQUET_WRITE_PRIMITIVES_V2, new String[]{"PARQUET_VERSION=v2"});
    }

    @Test(groups = {"features", "gpdb", "security", "hcfs"})
    public void parquetWritePrimitivesGZip() throws Exception {
        runWritePrimitiveScenario("pxf_parquet_write_primitives_gzip", "pxf_parquet_read_primitives_gzip", PARQUET_WRITE_PRIMITIVES_GZIP, new String[]{"COMPRESSION_CODEC=gzip"});
    }

    @Test(groups = {"features", "gpdb", "security", "hcfs"})
    public void parquetWritePrimitivesGZipClassName() throws Exception {
        runWritePrimitiveScenario("pxf_parquet_write_primitives_gzip_classname", "pxf_parquet_read_primitives_gzip_classname", PARQUET_WRITE_PRIMITIVES_GZIP_CLASSNAME, new String[]{"COMPRESSION_CODEC=org.apache.hadoop.io.compress.GzipCodec"});
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
    public void parquetWriteNumericWithPrecisionAndScale() throws Exception {

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
        String fullTestPath = hdfsPath + PARQUET_WRITE_LIST;

        prepareWritableExternalTable(writeTableName, PARQUET_LIST_TABLE_COLUMNS, fullTestPath, null);
        gpdb.runQuery("INSERT INTO " + exTable.getName() + " SELECT id, bool_arr, smallint_arr, int_arr, bigint_arr, real_arr, " +
                "double_arr, text_arr, bytea_arr, char_arr, varchar_arr, date_arr, numeric_arr FROM " + PXF_PARQUET_LIST_TYPES);

        if (protocol != ProtocolEnum.HDFS && protocol != ProtocolEnum.FILE) {
            // for HCFS on Cloud, wait a bit for async write in previous steps to finish
            sleep(10000);
            List<String> files = hdfs.list(fullTestPath);
            for (String file : files) {
                // make sure the file is available, saw flakes on Cloud that listed files were not available
                int attempts = 0;
                while (!hdfs.doesFileExist(file) && attempts++ < 20) {
                    sleep(1000);
                }
            }
        }

        prepareReadableExternalTable(readTableName, PARQUET_LIST_TABLE_COLUMNS, fullTestPath);
        runTincTest("pxf.features.parquet.write_list.list.runTest");
    }

    @Test(groups = {"features", "gpdb", "security", "hcfs"})
    public void parquetWriteTimestampLists() throws Exception {
        String writeTableName = "pxf_parquet_write_timestamp_list";
        String readTableName = "pxf_parquet_read_timestamp_list";
        String fullTestPath = hdfsPath + "parquet_write_timestamp_list";

        prepareWritableExternalTable(writeTableName, PARQUET_TIMESTAMP_LIST_TABLE_COLUMNS, fullTestPath, null);
        prepareReadableExternalTable(readTableName, PARQUET_TIMESTAMP_LIST_TABLE_COLUMNS, fullTestPath);
        insertTimestampListData(writeTableName, 6);

        runTincTest("pxf.features.parquet.write_list.timestamp_list.runTest");
    }


    //    @Test(groups = {"features", "gpdb", "security", "hcfs"})
    public void parquetWriteListsReadWithHive() throws Exception {
        String writeTableName = "pxf_parquet_write_list";
        String readTableName = "pxf_parquet_read_list";
        String fullTestPath = hdfsPath + PARQUET_WRITE_LIST;

        prepareWritableExternalTable(writeTableName, PARQUET_LIST_TABLE_COLUMNS, fullTestPath, null);
        insertArrayDataWithoutNulls(writeTableName, 33);

        //
//        // load the data into hive to check that PXF-written Parquet files can be read by other data
//        String hiveExternalTableName=writeTableName+"_external";
//        hiveTable = new HiveExternalTable(hiveExternalTableName, PARQUET_PRIMITIVE_ARRAYS_TABLE_COLUMNS_HIVE, "hdfs:/" + fullTestPath);
//        hiveTable.setStoredAs("PARQUET");
//        hive.createTableAndVerify(hiveTable);
//
//        // the JDBC profile cannot handle binary, time and uuid types
//        String ctasHiveQuery = new StringJoiner(",",
//                "CREATE TABLE " + hiveTable.getFullName() + "_ctas AS SELECT ", " FROM " + hiveTable.getFullName())
//                .add("id")
//                .add("bool_arr")
////                .add("bytea_arr") // binary cast as string
//                .add("bigint_arr")
//                .add("smallint_arr")
//                .add("int_arr")
//                .add("text_arr")
//                .add("real_arr")
//                .add("double_arr")
//                .add("char_arr")
//                .add("varchar_arr")
//                .add("varchar_arr_nolimit")
//                .add("date_arr")
////                .add("timestamp_arr")
////                .add("numeric_arr")
//                .toString();
//
//        hive.runQuery("DROP TABLE IF EXISTS " + hiveTable.getFullName() + "_ctas");
//        hive.runQuery(ctasHiveQuery);
//
//        // use the Hive JDBC profile to avoid using the PXF Parquet reader implementation
//        String jdbcUrl = HIVE_JDBC_URL_PREFIX + hive.getHost() + ":10000/default";
//        ExternalTable exHiveJdbcTable = TableFactory.getPxfJdbcReadableTable(
//                writeTableName + "_readable", PARQUET_PRIMITIVE_ARRAYS_TABLE_COLUMNS_READ_FROM_HIVE,
//                hiveTable.getName() + "_ctas", HIVE_JDBC_DRIVER_CLASS, jdbcUrl, null);
//        exHiveJdbcTable.setHost(pxfHost);
//        exHiveJdbcTable.setPort(pxfPort);
//        gpdb.createTableAndVerify(exHiveJdbcTable);

        // use PXF hive:jdbc profile to read the data

    }

    private void runWritePrimitiveScenario(String writeTableName, String readTableName,
                                           String filename, String[] userParameters) throws Exception {
        prepareWritableExternalTable(writeTableName,
                PARQUET_PRIMITIVE_TABLE_COLUMNS, hdfsPath + filename, userParameters);
        gpdb.runQuery("INSERT INTO " + exTable.getName() + " SELECT s1, s2, n1, d1, dc1, tm, " +
                "f, bg, b, tn, vc1, sml, c1, bin FROM " + PXF_PARQUET_PRIMITIVE_TABLE);

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
        prepareReadableExternalTable(readTableName,
                PARQUET_PRIMITIVE_TABLE_COLUMNS, hdfsPath + filename);
        gpdb.runQuery("CREATE OR REPLACE VIEW parquet_view AS SELECT s1, s2, n1, d1, dc1, " +
                "CAST(tm AS TIMESTAMP WITH TIME ZONE) AT TIME ZONE 'PDT' as tm, " +
                "f, bg, b, tn, sml, vc1, c1, bin FROM " + readTableName);

        runTincTest("pxf.features.parquet.primitive_types.runTest");
    }

    private void prepareReadableExternalTable(String name, String[] fields, String path) throws Exception {
        exTable = new ReadableExternalTable(name, fields,
                protocol.getExternalTablePath(hdfs.getBasePath(), path), "custom");
        exTable.setFormatter("pxfwritable_import");
        exTable.setProfile(protocol.value() + ":parquet");
        createTable(exTable);
    }

    private void prepareWritableExternalTable(String name, String[] fields, String path, String[] userParameters) throws Exception {
        exTable = new WritableExternalTable(name, fields,
                protocol.getExternalTablePath(hdfs.getBasePath(), path), "custom");
        exTable.setFormatter("pxfwritable_export");
        exTable.setProfile(protocol.value() + ":parquet");
        if (userParameters != null) {
            exTable.setUserParameters(userParameters);
        }
        createTable(exTable);
    }

    private void insertTimestampListData(String exTable, int numRows) throws Exception {
        StringBuilder insertStatement = new StringBuilder();
        insertStatement.append("INSERT INTO " + exTable + " VALUES ");
        for (int i = 0; i < numRows; i++) {
            StringJoiner statementBuilder = new StringJoiner(",", "(", ")")
                    .add(String.valueOf(i))
                    .add(String.format("'{\"2013-07-13 21:00:05.%03d456\",null,\"2013-07-13 21:00:05.%03d456\"}'", i % 1000, (i + 20) % 1000));
            insertStatement.append(statementBuilder.toString().concat((i < (numRows - 1)) ? "," : ";"));
        }
        gpdb.runQuery(insertStatement.toString());
    }

    private void insertArrayDataWithoutNulls(String exTable, int numRows) throws Exception {
        StringBuilder insertStatement = new StringBuilder();
        insertStatement.append("INSERT INTO " + exTable + " VALUES ");
        for (int i = 0; i < numRows; i++) {
            StringJoiner statementBuilder = new StringJoiner(",", "(", ")")
                    .add(String.valueOf(i))    // always not-null row index, column index starts with 0 after it
                    .add(String.format("'{\"%b\"}'", i % 2 != 0))                                   // DataType.BOOLEANARRAY
//                    .add(String.format("'{\\\\x%02d%02d}'::bytea[]", i % 100, (i + 1) % 100))       // DataType.BYTEAARRAY
                    .add(String.format("'{%d}'", 123456789000000000L + i))                          // DataType.INT8ARRAY
                    .add(String.format("'{%d}'", 10L + i % 32000))                                   // DataType.INT2ARRAY
                    .add(String.format("'{%d}'", 100L + i))                                         // DataType.INT4ARRAY
                    .add(String.format("'{\"row-%02d\"}'", i))                                      // DataType.TEXTARRAY
                    .add(String.format("'{%f}'", Float.valueOf(i + 0.00001f * i)))    // DataType.FLOAT4ARRAY
                    .add(String.format("'{%f}'", i + Math.PI))                           // DataType.FLOAT8ARRAY
                    .add(String.format("'{\"%s\"}'", i))                                            // DataType.BPCHARARRAY
                    .add(String.format("'{\"var%02d\"}'", i))                                       // DataType.VARCHARARRAY
                    .add(String.format("'{\"longer string var%02d\"}'", i))                        // DataType.VARCHARARRAY no limit
                    .add(String.format("'{\"2010-01-%02d\"}'", (i % 30) + 1))                      // DataType.DATEARRAY
//                    .add(String.format("'{\"2013-07-13 21:00:05.%03d456\"}'", i % 1000))           // DataType.TIMESTAMPARRAY
                    .add(String.format("'{12345678900000.00000%s}'", i))                           // DataType.NUMERICARRAY
                    ;
            insertStatement.append(statementBuilder.toString().concat((i < (numRows - 1)) ? "," : ";"));
        }
        gpdb.runQuery(insertStatement.toString());
    }
}