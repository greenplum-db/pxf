package org.greenplum.pxf.automation.features.orc;

import jsystem.framework.system.SystemManagerImpl;
import org.greenplum.pxf.automation.components.hive.Hive;
import org.greenplum.pxf.automation.features.BaseFeature;
import org.greenplum.pxf.automation.structures.tables.hive.HiveExternalTable;
import org.greenplum.pxf.automation.structures.tables.hive.HiveTable;
import org.greenplum.pxf.automation.structures.tables.pxf.ExternalTable;
import org.greenplum.pxf.automation.structures.tables.pxf.ReadableExternalTable;
import org.greenplum.pxf.automation.structures.tables.pxf.WritableExternalTable;
import org.greenplum.pxf.automation.structures.tables.utils.TableFactory;
import org.greenplum.pxf.automation.utils.system.ProtocolEnum;
import org.greenplum.pxf.automation.utils.system.ProtocolUtils;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.StringJoiner;

public class OrcWriteTest extends BaseFeature {

    private static final String[] ORC_PRIMITIVE_TABLE_COLUMNS = {
            "id integer"               ,
            "c_bool boolean"           , // DataType.BOOLEAN
            "c_bytea bytea"            , // DataType.BYTEA
            "c_bigint bigint"          , // DataType.BIGINT
            "c_small smallint"         , // DataType.SMALLINT
            "c_int integer"            , // DataType.INTEGER
            "c_text text"              , // DataType.TEXT
            "c_real real"              , // DataType.REAL
            "c_float float"            , // DataType.FLOAT8
            "c_char char(4)"           , // DataType.BPCHAR
            "c_varchar varchar(7)"     , // DataType.VARCHAR
            "c_varchar_nolimit varchar", // DataType.VARCHAR with no length
            "c_date date"              , // DataType.DATE
            "c_time time"              , // DataType.TIME
            "c_timestamp timestamp"    , // DataType.TIMESTAMP
            "c_numeric numeric"        , // DataType.NUMERIC
            "c_uuid uuid"                // DataType.UUID
    };

    private static final String[] ORC_TIMESTAMP_TABLE_COLUMNS = {
            "id integer"               ,
            "c_date date"              , // DataType.DATE
            "c_time time"              , // DataType.TIME
            "c_timestamp timestamp"    , // DataType.TIMESTAMP
            "c_timestamptz timestamptz", // DataType.TIMESTAMP_WITH_TIME_ZONE
    };

    private static final String[] ORC_PRIMITIVE_TABLE_COLUMNS_HIVE = {
            "id INT"                  ,
            "c_bool BOOLEAN"          , // DataType.BOOLEAN
            "c_bytea BINARY"          , // DataType.BYTEA
            "c_bigint BIGINT"         , // DataType.BIGINT
            "c_small SMALLINT"        , // DataType.SMALLINT
            "c_int INT"               , // DataType.INTEGER
            "c_text STRING"           , // DataType.TEXT
            "c_real FLOAT"            , // DataType.REAL
            "c_float DOUBLE"          , // DataType.FLOAT8
            "c_char CHAR(4)"          , // DataType.BPCHAR
            "c_varchar VARCHAR(7)"    , // DataType.VARCHAR
            "c_varchar_nolimit STRING", // DataType.VARCHAR with no length
            "c_date DATE"             , // DataType.DATE
            "c_time STRING"           , // DataType.TIME (ORC stores TIME as string type)
            "c_timestamp TIMESTAMP"   , // DataType.TIMESTAMP
            "c_numeric DECIMAL(38,18)", // DataType.NUMERIC
            "c_uuid STRING"             // DataType.UUID
    };

    private static final String[] ORC_PRIMITIVE_TABLE_COLUMNS_READ_FROM_HIVE = {
            "id integer"               ,
            "c_bool boolean"           , // DataType.BOOLEAN
            "c_bytea text"             , // DataType.BYTEA
            "c_bigint bigint"          , // DataType.BIGINT
            "c_small smallint"         , // DataType.SMALLINT
            "c_int integer"            , // DataType.INTEGER
            "c_text text"              , // DataType.TEXT
            "c_real real"              , // DataType.REAL
            "c_float float"            , // DataType.FLOAT8
            "c_char char(4)"           , // DataType.BPCHAR
            "c_varchar varchar(7)"     , // DataType.VARCHAR
            "c_varchar_nolimit varchar", // DataType.VARCHAR with no length
            "c_date date"              , // DataType.DATE
            "c_time text"              , // DataType.TIME
            "c_timestamp timestamp"    , // DataType.TIMESTAMP
            "c_numeric numeric"        , // DataType.NUMERIC
            "c_uuid text"                // DataType.UUID
    };

    private static final String[] ORC_PRIMITIVE_ARRAYS_TABLE_COLUMNS = {
            "id     integer"        ,
            "col00  boolean[]"      , // DataType.BOOLARRAY
            "col01  bytea[]"        , // DataType.BYTEAARRAY
            "col02  bigint[]"       , // DataType.INT8ARRAY
            "col03  smallint[]"     , // DataType.INT2ARRAY
            "col04  integer[]"      , // DataType.INT4ARRAY
            "col05  text[]"         , // DataType.TEXTARRAY
            "col06  real[]"         , // DataType.FLOAT4ARRAY
            "col07  float[]"        , // DataType.FLOAT8ARRAY
            "col08  char(4)[]"      , // DataType.BPCHARARRAY
            "col09  varchar(7)[]"   , // DataType.VARCHARARRAY
            "col10  date[]"         , // DataType.DATEARRAY
            "col11  time[]"         , // DataType.TIMEARRAY
            "col12  timestamp[]"    , // DataType.TIMESTAMPARRAY
            "col13  timestamptz[]"  , // DataType.TIMESTAMP_WITH_TIME_ZONE
            "col14  numeric[]"      , // DataType.NUMERICARRAY
            "col15  uuid[]"           // DataType.UUIDARRAY
    };

    private static final String[] ORC_PRIMITIVE_ARRAYS_TABLE_COLUMNS_READ = {
            "id     integer"      ,
            "col00  boolean[]"      , // DataType.BOOLARRAY
            "col01  bytea[]"        , // DataType.BYTEAARRAY
            "col02  bigint[]"       , // DataType.INT8ARRAY
            "col03  smallint[]"     , // DataType.INT2ARRAY
            "col04  integer[]"      , // DataType.INT4ARRAY
            "col05  text[]"         , // DataType.TEXTARRAY
            "col06  real[]"         , // DataType.FLOAT4ARRAY
            "col07  float[]"        , // DataType.FLOAT8ARRAY
            "col08  char(4)[]"      , // DataType.BPCHARARRAY
            "col09  varchar(7)[]"   , // DataType.VARCHARARRAY
            "col10  date[]"         , // DataType.DATEARRAY
            "col11  text[]"         , // DataType.TIMEARRAY --> time is not a separate type in orc (see OrcSchemaBuilder.java)
            "col12  timestamp[]"    , // DataType.TIMESTAMPARRAY
            "col13  timestamptz[]"  , // DataType.TIMESTAMP_WITH_TIME_ZONE
            "col14  numeric[]"      , // DataType.NUMERICARRAY
            "col15  text[]"           // DataType.UUIDARRAY --> uuid is stored as string (see OrcSchemaBuilder.java)
    };


    private static final boolean[] NO_NULLS = new boolean[ORC_PRIMITIVE_TABLE_COLUMNS.length - 1]; // 16 main columns
    private static final boolean[] ALL_NULLS = new boolean[ORC_PRIMITIVE_TABLE_COLUMNS.length - 1]; // 16 main columns
    static {
        Arrays.fill(ALL_NULLS, true);
    }
    private static final String HIVE_JDBC_DRIVER_CLASS = "org.apache.hive.jdbc.HiveDriver";
    private static final String HIVE_JDBC_URL_PREFIX = "jdbc:hive2://";

    private String gpdbTableNamePrefix;
    private String hdfsPath;
    private String fullTestPath;
    private ProtocolEnum protocol;
    private Hive hive;
    private HiveTable hiveTable;


    @Override
    public void beforeClass() throws Exception {
        // path for storing data on HDFS (for processing by PXF)
        hdfsPath = hdfs.getWorkingDirectory() + "/writableOrc/";
        protocol = ProtocolUtils.getProtocol();
    }

    @Override
    protected void afterClass() throws Exception {
        super.afterClass();

        // close hive connection
        if (hive != null)
            hive.close();
    }

    @Test(groups = {"features", "gpdb", "security", "hcfs"})
    public void orcWritePrimitives() throws Exception {
        gpdbTableNamePrefix = "pxf_orc_primitive_types";
        fullTestPath = hdfsPath + "orc_primitive_types";

        prepareWritableExternalTable(gpdbTableNamePrefix, ORC_PRIMITIVE_TABLE_COLUMNS, fullTestPath);
        prepareReadableExternalTable(gpdbTableNamePrefix, ORC_PRIMITIVE_TABLE_COLUMNS, fullTestPath, false /*mapByPosition*/);

        insertDataWithoutNulls(gpdbTableNamePrefix, 33); // > 30 to let the DATE field to repeat the value

        // use PXF *:orc profile to read the data
        runTincTest("pxf.features.orc.write.primitive_types.runTest");
    }

    /*
     * Do not run this test with "hcfs" group as Hive is not available in the environments prepared for that group
     * Also do not run with "security" group that would require kerberos principal to be included in Hive JDBC URL
     */
    @Test(groups = {"features", "gpdb"})
    public void orcWritePrimitivesReadWithHive() throws Exception {
        // init only here, not in beforeClass() method as other tests run in environments without Hive
        hive = (Hive) SystemManagerImpl.getInstance().getSystemObject("hive");

        gpdbTableNamePrefix = "pxf_orc_primitive_types_with_hive";
        fullTestPath = hdfsPath + "orc_primitive_types_with_hive";

        prepareWritableExternalTable(gpdbTableNamePrefix, ORC_PRIMITIVE_TABLE_COLUMNS, fullTestPath);
        insertDataWithoutNulls(gpdbTableNamePrefix, 33); // > 30 to let the DATE field to repeat the value

        // load the data into hive to check that PXF-written ORC files can be read by other data
        hiveTable = new HiveExternalTable(gpdbTableNamePrefix, ORC_PRIMITIVE_TABLE_COLUMNS_HIVE, "hdfs:/" + fullTestPath);
        hiveTable.setStoredAs("ORC");
        hive.createTableAndVerify(hiveTable);

        // the JDBC profile cannot handle binary, time and uuid types
        String ctasHiveQuery = new StringJoiner(",",
                "CREATE TABLE " + hiveTable.getFullName() + "_ctas AS SELECT ", " FROM " + hiveTable.getFullName())
                .add("id")
                .add("c_bool")
                .add("hex(c_bytea) as c_bytea") // binary cast as string
                .add("c_bigint")
                .add("c_small")
                .add("c_int")
                .add("c_text")
                .add("c_real")
                .add("c_float")
                .add("c_char")
                .add("c_varchar")
                .add("c_varchar_nolimit")
                .add("c_date")
                .add("cast(c_time as string) as c_time") // time cast as string
                .add("c_timestamp")
                .add("c_numeric")
                .add("cast(c_uuid as string) as c_uuid") // uuid cast as string
                .toString();

        hive.runQuery("DROP TABLE IF EXISTS " + hiveTable.getFullName() + "_ctas");
        hive.runQuery(ctasHiveQuery);

        // use the Hive JDBC profile to avoid using the PXF ORC reader implementation
        String jdbcUrl = HIVE_JDBC_URL_PREFIX + hive.getHost() + ":10000/default";
        ExternalTable exHiveJdbcTable = TableFactory.getPxfJdbcReadableTable(
                gpdbTableNamePrefix + "_readable", ORC_PRIMITIVE_TABLE_COLUMNS_READ_FROM_HIVE,
                hiveTable.getName() + "_ctas", HIVE_JDBC_DRIVER_CLASS, jdbcUrl, null);
        exHiveJdbcTable.setHost(pxfHost);
        exHiveJdbcTable.setPort(pxfPort);
        gpdb.createTableAndVerify(exHiveJdbcTable);

        // use PXF hive:jdbc profile to read the data
        runTincTest("pxf.features.orc.write.primitive_types_with_hive.runTest");
    }

    @Test(groups = {"features", "gpdb", "security", "hcfs"})
    public void orcWritePrimitivesWithNulls() throws Exception {
        gpdbTableNamePrefix = "pxf_orc_primitive_types_nulls";
        fullTestPath = hdfsPath + "orc_primitive_types_nulls";
        prepareWritableExternalTable(gpdbTableNamePrefix, ORC_PRIMITIVE_TABLE_COLUMNS, fullTestPath);
        prepareReadableExternalTable(gpdbTableNamePrefix, ORC_PRIMITIVE_TABLE_COLUMNS  , fullTestPath, false /*mapByPosition*/);

        insertDataWithNulls(gpdbTableNamePrefix, 33);

        runTincTest("pxf.features.orc.write.primitive_types_nulls.runTest");
    }

    @Test(groups = {"features", "gpdb", "security", "hcfs"})
    public void orcWritePrimitivesLargeDataset() throws Exception {
        gpdbTableNamePrefix = "pxf_orc_primitive_types_large";
        fullTestPath = hdfsPath + "orc_primitive_types_large";
        prepareWritableExternalTable(gpdbTableNamePrefix, ORC_PRIMITIVE_TABLE_COLUMNS, fullTestPath);
        prepareReadableExternalTable(gpdbTableNamePrefix, ORC_PRIMITIVE_TABLE_COLUMNS  , fullTestPath, false /*mapByPosition*/);

        // write 3 batches and 1 row of data (1024*3+1=3073) to make sure batch is properly reset when reused
        insertDataWithoutNulls(gpdbTableNamePrefix, 3073);

        runTincTest("pxf.features.orc.write.primitive_types_large.runTest");
    }

    @Test(groups = {"features", "gpdb", "security", "hcfs"})
    public void orcWriteTimestampWithTimezone() throws Exception {
        gpdbTableNamePrefix = "pxf_orc_timestamp_with_timezone";
        fullTestPath = hdfsPath + gpdbTableNamePrefix;
        prepareWritableExternalTable(gpdbTableNamePrefix, ORC_TIMESTAMP_TABLE_COLUMNS, fullTestPath);
        prepareReadableExternalTable(gpdbTableNamePrefix, ORC_TIMESTAMP_TABLE_COLUMNS, fullTestPath, false /*mapByPosition*/);

        insertDataWithTimestamps(gpdbTableNamePrefix, 10, 5);

        runTincTest("pxf.features.orc.write.timestamp_with_timezone_types.runTest");
    }

    @Test(groups = {"features", "gpdb", "security", "hcfs"})
    public void orcWritePrimitiveArraysWithNulls() throws Exception {
        gpdbTable = "orc_primitive_arrays";
        fullTestPath = hdfsPath + gpdbTable;

        prepareWritableExternalTable(gpdbTable, ORC_PRIMITIVE_ARRAYS_TABLE_COLUMNS, fullTestPath);
        prepareReadableExternalTable(gpdbTable, ORC_PRIMITIVE_ARRAYS_TABLE_COLUMNS_READ  , fullTestPath, false /*mapByPosition*/);

        insertArrayDataWithNulls(gpdbTable, 33, 17); // > 30 to let the DATE field to repeat the value

        // use PXF *:orc profile to read the data
        runTincTest("pxf.features.orc.write.primitive_types_array.runTest");
    }

    @Test(groups = {"features", "gpdb", "security", "hcfs"})
    public void orcWritePrimitiveArraysWithNullElements() throws Exception {
        gpdbTable = "orc_primitive_arrays_null_elements";
        fullTestPath = hdfsPath + gpdbTable;

        prepareWritableExternalTable(gpdbTable, ORC_PRIMITIVE_ARRAYS_TABLE_COLUMNS, fullTestPath);
        prepareReadableExternalTable(gpdbTable, ORC_PRIMITIVE_ARRAYS_TABLE_COLUMNS_READ  , fullTestPath, false /*mapByPosition*/);

        insertArrayDataWithNullElements(gpdbTable, 33, 17); // > 30 to let the DATE field to repeat the value

        // use PXF *:orc profile to read the data
        runTincTest("pxf.features.orc.write.primitive_types_array_null_elements.runTest");
    }
    private void insertDataWithoutNulls(String exTable, int numRows) throws Exception {
        StringBuilder statementBuilder = new StringBuilder("INSERT INTO " + exTable + "_writable VALUES ");
        for (int i = 0; i < numRows; i++) {
            statementBuilder.append(getRecordCSV(i, NO_NULLS));
            statementBuilder.append((i < (numRows - 1)) ? "," : ";");
        }
        gpdb.runQuery(statementBuilder.toString());
    }

    private void insertDataWithNulls(String exTable, int numRows) throws Exception {
        StringBuilder statementBuilder = new StringBuilder("INSERT INTO " + exTable + "_writable VALUES ");
        int nullableColumnCount = ORC_PRIMITIVE_TABLE_COLUMNS.length - 1;
        boolean[] isNull = new boolean[nullableColumnCount]; // id column does not count
        for (int i = 0; i < numRows; i++) {
            Arrays.fill(isNull, false); // reset the isNull array
            if (i > 0) {
                int indexOfNullColumn = (i - 1) % nullableColumnCount;
                isNull[indexOfNullColumn] = true;
            }
            statementBuilder.append(getRecordCSV(i, (i == 0) ? ALL_NULLS : isNull)); // zero row is all nulls
            statementBuilder.append((i < (numRows - 1)) ? "," : ";");
        }
        gpdb.runQuery(statementBuilder.toString());
    }

    private void insertDataWithTimestamps(String exTable, int numRows, int nullModulo) throws Exception {
        StringBuilder statementBuilder = new StringBuilder("INSERT INTO " + exTable + "_writable VALUES ");
        for (int i = 0; i < numRows; i++) {
            statementBuilder.append("(").append(i).append(",")
                .append((i % nullModulo == 0) ? "NULL" : String.format("'2010-01-%02d'", (i % 30) + 1)).append(",")                // DataType.DATE
                .append((i % nullModulo == 1) ? "NULL" : String.format("'10:11:%02d'", i % 60)).append(",")                        // DataType.TIME
                .append((i % nullModulo == 2) ? "NULL" : String.format("'2013-07-13 21:00:05.%03d456'", i % 1000)).append(",")     // DataType.TIMESTAMP
                .append((i % nullModulo == 3) ? "NULL" : String.format("'2013-07-13 21:00:05.987%03d-07'", i % 1000)).append(")"); // DataType.TIMESTAMP_WITH_TIME_ZONE
            statementBuilder.append((i < (numRows - 1)) ? "," : ";");
        }
        gpdb.runQuery(statementBuilder.toString());
    }

    private void insertArrayDataWithNulls(String exTable, int numRows, int nullModulo) throws Exception {
        StringBuilder statementBuilder = new StringBuilder("INSERT INTO " + exTable + "_writable VALUES ");
        for (int i = 0; i < numRows; i++) {
            statementBuilder.append("(")
                .append(i).append(",")    // always not-null row index, column index starts with 0 after it
                .append((i % nullModulo == 0) ? "NULL" : String.format("'{\"%b\"}'", i % 2 != 0)).append(",")                                // DataType.BOOLEAN
                .append((i % nullModulo == 1) ? "NULL" : String.format("'{\\\\x%02d%02d}'::bytea[]", i % 100, (i + 1) % 100)).append(",")      // DataType.BYTEA
                .append((i % nullModulo == 2) ? "NULL" : String.format("'{%d}'", 123456789000000000L + i)).append(",")                       // DataType.BIGINT
                .append((i % nullModulo == 3) ? "NULL" : String.format("'{%d}'",10L + i % 32000)).append(",")                                // DataType.SMALLINT
                .append((i % nullModulo == 4) ? "NULL" : String.format("'{%d}'", 100L + i)).append(",")                                      // DataType.INTEGER
                .append((i % nullModulo == 5) ? "NULL" : String.format("'{\"row-%02d\"}'", i)).append(",")                                   // DataType.TEXT
                .append((i % nullModulo == 6) ? "NULL" : String.format("'{%f}'", Float.valueOf(i + 0.00001f * i).doubleValue())).append(",") // DataType.REAL
                .append((i % nullModulo == 7) ? "NULL" : String.format("'{%f}'", i + Math.PI)).append(",")                                   // DataType.FLOAT8
                .append((i % nullModulo == 8) ? "NULL" : String.format("'{\"%s\"}'", i)).append(",")                                         // DataType.BPCHAR
                .append((i % nullModulo == 9) ? "NULL" : String.format("'{\"var%02d\"}'", i)).append(",")                                    // DataType.VARCHAR
                .append((i % nullModulo == 10) ? "NULL" : String.format("'{\"2010-01-%02d\"}'", (i % 30) + 1)).append(",")                   // DataType.DATE
                .append((i % nullModulo == 11) ? "NULL" : String.format("'{\"10:11:%02d\"}'", i % 60)).append(",")                           // DataType.TIME
                .append((i % nullModulo == 12) ? "NULL" : String.format("'{\"2013-07-13 21:00:05.%03d456\"}'", i % 1000)).append(",")        // DataType.TIMESTAMP
                .append((i % nullModulo == 13) ? "NULL" : String.format("'{\"2013-07-13 21:00:05.987%03d-07\"}'", i % 1000)).append(",")     // DataType.TIMESTAMP_WITH_TIME_ZONE
                .append((i % nullModulo == 14) ? "NULL" : String.format("'{12345678900000.00000%s}'", i)).append(",")                        // DataType.NUMERIC
                .append((i % nullModulo == 15) ? "NULL" : String.format("'{\"476f35e4-da1a-43cf-8f7c-950a%08d\"}'", i % 100000000))          // DataType.UUID
                .append(")");
            statementBuilder.append((i < (numRows - 1)) ? "," : ";");
        }
        gpdb.runQuery(statementBuilder.toString());
    }

    private void insertArrayDataWithNullElements(String exTable, int numRows, int nullModulo) throws Exception {
        StringBuilder statementBuilder = new StringBuilder("INSERT INTO " + exTable + "_writable VALUES ");
        for (int i = 0; i < numRows; i++) {
            statementBuilder.append("(")
                    .append(i).append(",")    // always not-null row index, column index starts with 0 after it
                    .append((i % nullModulo == 0) ? "'{NULL}'" : (i % nullModulo == 8) ? "'{}'" : String.format("'{\"%b\", \"%b\", NULL}'", i % 2 != 0, i % 3 != 0)).append(",")                                // DataType.BOOLEAN
                    .append((i % nullModulo == 1) ? "'{NULL}'" : (i % nullModulo == 9) ? "'{}'" : String.format("'{\\\\x%02d%02d, NULL, \\\\x%02d%02d}'::bytea[]", i % 100, (i + 1) % 100,  (i + 2) % 100, (i + 3) % 100)).append(",")      // DataType.BYTEA
                    .append((i % nullModulo == 2) ? "'{NULL}'" : (i % nullModulo == 10) ? "'{}'" : String.format("'{NULL, %d}'", 123456789000000000L + i)).append(",")                       // DataType.BIGINT
                    .append((i % nullModulo == 3) ? "'{NULL}'" : (i % nullModulo == 11) ? "'{}'" : String.format("'{%d, NULL}'", 10L + i % 32000)).append(",")                                             // DataType.SMALLINT
                    .append((i % nullModulo == 4) ? "'{NULL}'" : (i % nullModulo == 12) ? "'{}'" : String.format("'{%d, %d, NULL}'", 100L + i, 200L + i)).append(",")                                      // DataType.INTEGER
                    .append((i % nullModulo == 5) ? "'{NULL}'" : (i % nullModulo == 13) ? "'{}'" : String.format("'{\"row-%02d\", \"row-%02d\", NULL, \"\"}'", i, i * 2)).append(",")                                   // DataType.TEXT
                    .append((i % nullModulo == 6) ? "'{NULL}'" : (i % nullModulo == 14) ? "'{}'" : String.format("'{NULL, %f}'", Float.valueOf(i + 0.00001f * i).doubleValue())).append(",") // DataType.REAL
                    .append((i % nullModulo == 7) ? "'{NULL}'" : (i % nullModulo == 15) ? "'{}'" : String.format("'{%f, NULL}'", i + Math.PI)).append(",")                                   // DataType.FLOAT8
                    .append((i % nullModulo == 8) ? "'{NULL}'" : (i % nullModulo == 0) ? "'{}'" : String.format("'{\"%s\", \"%s\", \"%s\", NULL}'", i, i + 1, i + 2)).append(",")                                         // DataType.BPCHAR
                    .append((i % nullModulo == 9) ? "'{NULL}'" : (i % nullModulo == 1) ? "'{}'" : String.format("'{NULL, \"var%02d\", \"var%02d\"}'", i, i + 2)).append(",")                                    // DataType.VARCHAR
                    .append((i % nullModulo == 10) ? "'{NULL}'" : (i % nullModulo == 2) ? "'{}'" : String.format("'{\"2010-01-%02d\", NULL, \"2010-01-%02d\"}'", (i % 30) + 1, ((i * 2) % 30) + 1)).append(",")                   // DataType.DATE
                    .append((i % nullModulo == 11) ? "'{NULL}'" : (i % nullModulo == 3) ? "'{}'" : String.format("'{NULL, \"10:11:%02d\", \"10:11:%02d\"}'", i % 60, (i * 3) % 60)).append(",")                           // DataType.TIME
                    .append((i % nullModulo == 12) ? "'{NULL}'" : (i % nullModulo == 4) ? "'{}'" : String.format("'{\"2013-07-13 21:00:05.%03d456\", NULL}'", i % 1000)).append(",")        // DataType.TIMESTAMP
                    .append((i % nullModulo == 13) ? "'{NULL}'" : (i % nullModulo == 5) ? "'{}'" : String.format("'{\"2013-07-13 21:00:05.987%03d-07\", \"2013-07-13 21:00:05.987%03d-07\", NULL}'", i % 1000, i % 999)).append(",") // DataType.TIMESTAMP_WITH_TIME_ZONE
                    .append((i % nullModulo == 14) ? "'{NULL}'" : (i % nullModulo == 6) ? "'{}'" : String.format("'{NULL, 12345678900000.00000%s, 12345678900000.00000%s}'", i, i + 1)).append(",")                        // DataType.NUMERIC
                    .append((i % nullModulo == 15) ? "'{NULL}'" : (i % nullModulo == 7) ? "'{}'" : String.format("'{\"476f35e4-da1a-43cf-8f7c-950a%08d\", NULL, \"476f35e4-da1a-43cf-8f7c-950a%08d\"}'", i % 100000000,  (i+2) % 100000000))          // DataType.UUID
                    .append(")");
            statementBuilder.append((i < (numRows - 1)) ? "," : ";");
        }
        gpdb.runQuery(statementBuilder.toString());
    }

    private String getRecordCSV(int row, boolean[] isNull) {
        // refer to ORCVectorizedResolverWriteTest unit test where this data is used
        StringBuilder rowBuilder = new StringBuilder("(")
        .append(row).append(",")    // always not-null row index, column index starts with 0 after it
        .append(isNull [0] ? "NULL" : row % 2 != 0).append(",")                                                  // DataType.BOOLEAN
        .append(isNull [1] ? "NULL" : String.format("'\\x%02d%02d'::bytea", row%100, (row + 1) % 100)).append(",")   // DataType.BYTEA
        .append(isNull [2] ? "NULL" : 123456789000000000L + row).append(",")                                     // DataType.BIGINT
        .append(isNull [3] ? "NULL" : 10L + row % 32000).append(",")                                             // DataType.SMALLINT
        .append(isNull [4] ? "NULL" : 100L + row).append(",")                                                    // DataType.INTEGER
        .append(isNull [5] ? "NULL" : String.format("'row-%02d'", row)).append(",")                              // DataType.TEXT
        .append(isNull [6] ? "NULL" : Float.valueOf(row + 0.00001f * row).doubleValue()).append(",")          // DataType.REAL
        .append(isNull [7] ? "NULL" : row + Math.PI).append(",")                                                 // DataType.FLOAT8
        .append(isNull [8] ? "NULL" : String.format("'%s'", row)).append(",")                                    // DataType.BPCHAR
        .append(isNull [9] ? "NULL" : String.format("'var%02d'", row)).append(",")                               // DataType.VARCHAR
        .append(isNull[10] ? "NULL" : String.format("'var-no-length-%02d'", row)).append(",")                    // DataType.VARCHAR no length
        .append(isNull[11] ? "NULL" : String.format("'2010-01-%02d'", (row % 30) + 1)).append(",")               // DataType.DATE
        .append(isNull[12] ? "NULL" : String.format("'10:11:%02d'", row % 60)).append(",")                       // DataType.TIME
        .append(isNull[13] ? "NULL" : String.format("'2013-07-13 21:00:05.%03d456'", row % 1000)).append(",")    // DataType.TIMESTAMP
        .append(isNull[14] ? "NULL" : String.format("'12345678900000.00000%s'", row)).append(",")                // DataType.NUMERIC
        .append(isNull[15] ? "NULL" : String.format("'476f35e4-da1a-43cf-8f7c-950a%08d'", row % 100000000))      // DataType.UUID
        .append(")");
        return rowBuilder.toString();
    }

    private void prepareWritableExternalTable(String name, String[] fields, String path) throws Exception {
        exTable = new WritableExternalTable(name + "_writable", fields,
                protocol.getExternalTablePath(hdfs.getBasePath(), path), "custom");
        exTable.setFormatter("pxfwritable_export");
        exTable.setProfile(protocol.value() + ":orc");

        createTable(exTable);
    }

    private void prepareReadableExternalTable(String name, String[] fields, String path, boolean mapByPosition) throws Exception {
        exTable = new ReadableExternalTable(name+ "_readable", fields,
                protocol.getExternalTablePath(hdfs.getBasePath(), path), "custom");
        exTable.setFormatter("pxfwritable_import");
        exTable.setProfile(protocol.value() + ":orc");

        if (mapByPosition) {
            exTable.setUserParameters(new String[]{"MAP_BY_POSITION=true"});
        }

        createTable(exTable);
    }

}
