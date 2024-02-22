package org.greenplum.pxf.automation.features.orc;

import jsystem.framework.system.SystemManagerImpl;
import org.greenplum.pxf.automation.components.hive.Hive;
import org.greenplum.pxf.automation.features.BaseFeature;
import org.greenplum.pxf.automation.structures.tables.basic.Table;
import org.greenplum.pxf.automation.structures.tables.hive.HiveExternalTable;
import org.greenplum.pxf.automation.structures.tables.hive.HiveTable;
import org.greenplum.pxf.automation.structures.tables.pxf.ExternalTable;
import org.greenplum.pxf.automation.structures.tables.utils.TableFactory;
import org.greenplum.pxf.automation.utils.jsystem.report.ReportUtils;
import org.greenplum.pxf.automation.utils.system.ProtocolEnum;
import org.greenplum.pxf.automation.utils.system.ProtocolUtils;
import org.postgresql.util.PSQLException;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.StringJoiner;

public class OrcWriteTest extends BaseFeature {

    private static final String[] ORC_PRIMITIVE_TABLE_COLUMNS = {
            "id                 integer"    ,
            "c_bool             boolean"    , // DataType.BOOLEAN
            "c_bytea            bytea"      , // DataType.BYTEA
            "c_bigint           bigint"     , // DataType.BIGINT
            "c_small            smallint"   , // DataType.SMALLINT
            "c_int              integer"    , // DataType.INTEGER
            "c_text             text"       , // DataType.TEXT
            "c_real             real"       , // DataType.REAL
            "c_float            float"      , // DataType.FLOAT8
            "c_char             char(4)"    , // DataType.BPCHAR
            "c_varchar          varchar(7)" , // DataType.VARCHAR
            "c_varchar_nolimit  varchar"    , // DataType.VARCHAR with no length
            "c_date             date"       , // DataType.DATE
            "c_time             time"       , // DataType.TIME
            "c_timestamp        timestamp"  , // DataType.TIMESTAMP
            "c_numeric          numeric"    , // DataType.NUMERIC
            "c_uuid             uuid"         // DataType.UUID
    };

    private static final String[] ORC_TIMESTAMP_TABLE_COLUMNS = {
            "id             integer"    ,
            "c_date         date"       , // DataType.DATE
            "c_time         time"       , // DataType.TIME
            "c_timestamp    timestamp"  , // DataType.TIMESTAMP
            "c_timestamptz  timestamptz", // DataType.TIMESTAMP_WITH_TIME_ZONE
    };

    private static final String[] ORC_DECIMAL_WITH_LARGE_PRECISION_TABLE_COLUMNS = {
            "id             integer"        ,
            "c_numeric      numeric(90)"     // DataType.NUMERIC
    };

    private static final String[] ORC_PRIMITIVE_TABLE_COLUMNS_HIVE = {
            "id                 INT"           ,
            "c_bool             BOOLEAN"       , // DataType.BOOLEAN
            "c_bytea            BINARY"        , // DataType.BYTEA
            "c_bigint           BIGINT"        , // DataType.BIGINT
            "c_small            SMALLINT"      , // DataType.SMALLINT
            "c_int              INT"           , // DataType.INTEGER
            "c_text             STRING"        , // DataType.TEXT
            "c_real             FLOAT"         , // DataType.REAL
            "c_float            DOUBLE"        , // DataType.FLOAT8
            "c_char             CHAR(4)"       , // DataType.BPCHAR
            "c_varchar          VARCHAR(7)"    , // DataType.VARCHAR
            "c_varchar_nolimit  STRING"        , // DataType.VARCHAR with no length
            "c_date             DATE"          , // DataType.DATE
            "c_time             STRING"        , // DataType.TIME (ORC stores TIME as string type)
            "c_timestamp        TIMESTAMP"     , // DataType.TIMESTAMP
            "c_numeric          DECIMAL(38,18)", // DataType.NUMERIC
            "c_uuid             STRING"          // DataType.UUID
    };

    private static final String[] ORC_PRIMITIVE_TABLE_COLUMNS_READ_FROM_HIVE = {
            "id                 integer"    ,
            "c_bool             boolean"    , // DataType.BOOLEAN
            "c_bytea            text"       , // DataType.BYTEA
            "c_bigint           bigint"     , // DataType.BIGINT
            "c_small            smallint"   , // DataType.SMALLINT
            "c_int              integer"    , // DataType.INTEGER
            "c_text             text"       , // DataType.TEXT
            "c_real             real"       , // DataType.REAL
            "c_float            float"      , // DataType.FLOAT8
            "c_char             char(4)"    , // DataType.BPCHAR
            "c_varchar          varchar(7)" , // DataType.VARCHAR
            "c_varchar_nolimit  varchar"    , // DataType.VARCHAR with no length
            "c_date             date"       , // DataType.DATE
            "c_time             text"       , // DataType.TIME
            "c_timestamp        timestamp"  , // DataType.TIMESTAMP
            "c_numeric          numeric"    , // DataType.NUMERIC
            "c_uuid             text"         // DataType.UUID
    };

    private static final String[] ORC_PRIMITIVE_ARRAYS_TABLE_COLUMNS = {
            "id                   integer"      ,
            "bool_arr             boolean[]"    , // DataType.BOOLARRAY
            "bytea_arr            bytea[]"      , // DataType.BYTEAARRAY
            "bigint_arr           bigint[]"     , // DataType.INT8ARRAY
            "smallint_arr         smallint[]"   , // DataType.INT2ARRAY
            "int_arr              integer[]"    , // DataType.INT4ARRAY
            "text_arr             text[]"       , // DataType.TEXTARRAY
            "real_arr             real[]"       , // DataType.FLOAT4ARRAY
            "float_arr            float[]"      , // DataType.FLOAT8ARRAY
            "char_arr             char(4)[]"    , // DataType.BPCHARARRAY
            "varchar_arr          varchar(7)[]" , // DataType.VARCHARARRAY
            "varchar_arr_nolimit  varchar[]"    , // DataType.VARCHARARRAY with no length
            "date_arr             date[]"       , // DataType.DATEARRAY
            "time_arr             time[]"       , // DataType.TIMEARRAY
            "timestamp_arr        timestamp[]"  , // DataType.TIMESTAMPARRAY
            "timestamptz_arr      timestamptz[]", // DataType.TIMESTAMP_WITH_TIME_ZONE_ARRAY
            "numeric_arr          numeric[]"    , // DataType.NUMERICARRAY
            "uuid_arr             uuid[]"         // DataType.UUIDARRAY
    };

    private static final boolean[] NO_NULLS = new boolean[ORC_PRIMITIVE_TABLE_COLUMNS.length - 1]; // 16 main columns
    private static final boolean[] ALL_NULLS = new boolean[ORC_PRIMITIVE_TABLE_COLUMNS.length - 1]; // 16 main columns
    static {
        Arrays.fill(ALL_NULLS, true);
    }
    private static final String HIVE_JDBC_DRIVER_CLASS = "org.apache.hive.jdbc.HiveDriver";
    private static final String HIVE_JDBC_URL_PREFIX = "jdbc:hive2://";

    private static final Integer NUM_RETRIES = 5;

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

        attemptInsert(() -> insertDataWithoutNulls(gpdbTableNamePrefix, 33), // > 30 to let the DATE field to repeat the value
                33,
                gpdbTableNamePrefix + "_readable",
                NUM_RETRIES);
        // use PXF *:orc profile to read the data
        runSqlTest("features/orc/write/primitive_types");
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

        Integer retryCount = NUM_RETRIES;
        boolean success = false;
        for (int i = retryCount; i > 1; i--) {
            if (success) {
                break;
            }
            try {
                insertDataWithoutNulls(gpdbTableNamePrefix, 33); // > 30 to let the DATE field to repeat the value
                success = true;
            } catch (PSQLException e) {
                if (e.getMessage().contains("Operation could not be completed within the specified time")) {
                    ReportUtils.startLevel(null, getClass(), String.format("Operation timed out, trying again. Retries left: %d : '%s'", retryCount, e.getMessage()));

                    // operation timed out in the middle and not all the data was added, delete all the files and try again
                    hdfs.removeDirectory(fullTestPath);
                    success = false;
                }
            }
        }

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
        runSqlTest("features/orc/write/primitive_types_with_hive");
    }

    @Test(groups = {"features", "gpdb", "security", "hcfs"})
    public void orcWritePrimitivesWithNulls() throws Exception {
        gpdbTableNamePrefix = "pxf_orc_primitive_types_nulls";
        fullTestPath = hdfsPath + "orc_primitive_types_nulls";
        prepareWritableExternalTable(gpdbTableNamePrefix, ORC_PRIMITIVE_TABLE_COLUMNS, fullTestPath);
        prepareReadableExternalTable(gpdbTableNamePrefix, ORC_PRIMITIVE_TABLE_COLUMNS  , fullTestPath, false /*mapByPosition*/);

        attemptInsert(() -> insertDataWithNulls(gpdbTableNamePrefix, 33),
                33,
                gpdbTableNamePrefix + "_readable",
                NUM_RETRIES);

        runSqlTest("features/orc/write/primitive_types_nulls");
    }

    @Test(groups = {"features", "gpdb", "security", "hcfs"})
    public void orcWritePrimitivesLargeDataset() throws Exception {
        gpdbTableNamePrefix = "pxf_orc_primitive_types_large";
        fullTestPath = hdfsPath + "orc_primitive_types_large";
        prepareWritableExternalTable(gpdbTableNamePrefix, ORC_PRIMITIVE_TABLE_COLUMNS, fullTestPath);
        prepareReadableExternalTable(gpdbTableNamePrefix, ORC_PRIMITIVE_TABLE_COLUMNS  , fullTestPath, false /*mapByPosition*/);

        // write 3 batches and 1 row of data (1024*3+1=3073) to make sure batch is properly reset when reused
        attemptInsert(() -> insertDataWithoutNulls(gpdbTableNamePrefix, 3073),
                3073,
                gpdbTableNamePrefix + "_readable",
                NUM_RETRIES);

        runSqlTest("features/orc/write/primitive_types_large");
    }

    @Test(groups = {"features", "gpdb", "security", "hcfs"})
    public void orcWriteTimestampWithTimezone() throws Exception {
        gpdbTableNamePrefix = "pxf_orc_timestamp_with_timezone";
        fullTestPath = hdfsPath + gpdbTableNamePrefix;
        prepareWritableExternalTable(gpdbTableNamePrefix, ORC_TIMESTAMP_TABLE_COLUMNS, fullTestPath);
        prepareReadableExternalTable(gpdbTableNamePrefix, ORC_TIMESTAMP_TABLE_COLUMNS, fullTestPath, false /*mapByPosition*/);

        attemptInsert(() -> insertDataWithTimestamps(gpdbTableNamePrefix, 10, 5),
                10,
                gpdbTableNamePrefix + "_readable",
                NUM_RETRIES);

        runSqlTest("features/orc/write/timestamp_with_timezone_types");
    }

    @Test(groups = {"features", "gpdb", "security", "hcfs"})
    public void orcWritePrimitiveArraysWithNulls() throws Exception {
        gpdbTableNamePrefix = "orc_primitive_arrays";
        fullTestPath = hdfsPath + gpdbTableNamePrefix;

        prepareWritableExternalTable(gpdbTableNamePrefix, ORC_PRIMITIVE_ARRAYS_TABLE_COLUMNS, fullTestPath);
        prepareReadableExternalTable(gpdbTableNamePrefix, ORC_PRIMITIVE_ARRAYS_TABLE_COLUMNS, fullTestPath, false /*mapByPosition*/);

        attemptInsert(() -> insertArrayDataWithNulls(gpdbTableNamePrefix, 33, 17), // > 30 to let the DATE field to repeat the value
                33,
                gpdbTableNamePrefix + "_readable",
                NUM_RETRIES);

        // use PXF *:orc profile to read the data
        runSqlTest("features/orc/write/primitive_types_array_with_nulls");
    }

    @Test(groups = {"features", "gpdb", "security", "hcfs"})
    public void orcWritePrimitiveArraysWithNullElements() throws Exception {
        gpdbTableNamePrefix = "orc_primitive_arrays_null_elements";
        fullTestPath = hdfsPath + gpdbTableNamePrefix;

        prepareWritableExternalTable(gpdbTableNamePrefix, ORC_PRIMITIVE_ARRAYS_TABLE_COLUMNS, fullTestPath);
        prepareReadableExternalTable(gpdbTableNamePrefix, ORC_PRIMITIVE_ARRAYS_TABLE_COLUMNS, fullTestPath, false /*mapByPosition*/);

        attemptInsert(() -> insertArrayDataWithNullElements(gpdbTableNamePrefix, 33, 17), // > 30 to let the DATE field to repeat the value
                33,
                gpdbTableNamePrefix + "_readable",
                NUM_RETRIES);

        // use PXF *:orc profile to read the data
        runSqlTest("features/orc/write/primitive_types_array_null_elements");
    }

    @Test(groups = {"features", "gpdb", "security", "hcfs"})
    public void orcWritePrimitiveArraysMultidimensional() throws Exception {
        gpdbTableNamePrefix = "orc_primitive_arrays_multi";
        fullTestPath = hdfsPath + gpdbTableNamePrefix;
        // create heap table containing all the data
        Table heapTable = new Table(gpdbTableNamePrefix + "_heap", ORC_PRIMITIVE_ARRAYS_TABLE_COLUMNS);
        heapTable.setDistributionFields(new String[]{"id"});
        gpdb.createTableAndVerify(heapTable);
        insertMultidimensionalArrayData(gpdbTableNamePrefix + "_heap", 33, 17); // > 30 to let the DATE field to repeat the value

        prepareWritableExternalTable(gpdbTableNamePrefix + "_bool", new String[]{"id integer", "bool_arr bool[]"}, fullTestPath + "_bool");
        prepareWritableExternalTable(gpdbTableNamePrefix + "_bytea", new String[]{"id integer", "bytea_arr bytea[]"}, fullTestPath + "_bytea");
        prepareWritableExternalTable(gpdbTableNamePrefix + "_int", new String[]{"id integer", "int_arr int[]"}, fullTestPath + "_int");
        prepareWritableExternalTable(gpdbTableNamePrefix + "_float", new String[]{"id integer", "float_arr float[]"}, fullTestPath + "_float");
        prepareWritableExternalTable(gpdbTableNamePrefix + "_date", new String[]{"id integer", "date_arr date[]"}, fullTestPath + "_date");
        prepareWritableExternalTable(gpdbTableNamePrefix + "_text", new String[]{"id integer", "text_arr text[]"}, fullTestPath + "_text");

        // expected failures: bool, byte, int, floats, dates
        prepareReadableExternalTable(gpdbTableNamePrefix + "_bool", new String[]{"id integer", "bool_arr bool[]"}, fullTestPath + "_bool", false);
        prepareReadableExternalTable(gpdbTableNamePrefix + "_bytea", new String[]{"id integer", "bytea_arr bytea[]"}, fullTestPath + "_bytea", false);
        prepareReadableExternalTable(gpdbTableNamePrefix + "_int", new String[]{"id integer", "int_arr int[]"}, fullTestPath + "_int", false);
        prepareReadableExternalTable(gpdbTableNamePrefix + "_float", new String[]{"id integer", "float_arr float[]"}, fullTestPath + "_float", false);
        prepareReadableExternalTable(gpdbTableNamePrefix + "_date", new String[]{"id integer", "date_arr date[]"}, fullTestPath + "_date", false);
        // expected success with string values as the internal arrays
        prepareReadableExternalTable(gpdbTableNamePrefix + "_text", new String[]{"id integer", "text_arr text[]"}, fullTestPath + "_text", false);

        // use the heap table to insert data into the external tables
        runSqlTest("features/orc/write/primitive_types_array_multi");
    }

    @Test(groups = {"features", "gpdb", "security", "hcfs"})
    public void orcWriteDecimalWithLargePrecisionDefined() throws Exception {
        gpdbTableNamePrefix = "orc_decimals_with_large_precision";
        fullTestPath = hdfsPath + gpdbTableNamePrefix;

        prepareWritableExternalTable(gpdbTableNamePrefix, ORC_DECIMAL_WITH_LARGE_PRECISION_TABLE_COLUMNS, fullTestPath);

        runSqlTest("features/orc/write/decimal_with_large_precision_defined");
    }

    @Test(groups = {"features", "gpdb", "security", "hcfs"})
    public void orcWriteDecimalWithLargePrecisionNotDefined() throws Exception {
        gpdbTableNamePrefix = "orc_decimals_with_large_precision_not_defined";
        fullTestPath = hdfsPath + gpdbTableNamePrefix;

        prepareWritableExternalTable(gpdbTableNamePrefix, ORC_PRIMITIVE_TABLE_COLUMNS, fullTestPath);

        runSqlTest("features/orc/write/decimal_with_large_precision_not_defined");
    }

    @Test(groups = {"features", "gpdb", "security", "hcfs"})
    public void orcWriteDecimalIntegerDigitCountOverflow() throws Exception {
        gpdbTableNamePrefix = "orc_decimals_with_large_integer_digit_count";
        fullTestPath = hdfsPath + gpdbTableNamePrefix;

        prepareWritableExternalTable(gpdbTableNamePrefix, ORC_PRIMITIVE_TABLE_COLUMNS, fullTestPath);

        runSqlTest("features/orc/write/decimal_with_large_integer_digit");
    }

    @Test(groups = {"features", "gpdb", "security", "hcfs"})
    public void orcWriteDecimalScaleOverflow() throws Exception {
        gpdbTableNamePrefix = "orc_decimals_with_large_scale";
        fullTestPath = hdfsPath + gpdbTableNamePrefix;

        prepareWritableExternalTable(gpdbTableNamePrefix, ORC_PRIMITIVE_TABLE_COLUMNS, fullTestPath);
        prepareReadableExternalTable(gpdbTableNamePrefix, ORC_PRIMITIVE_TABLE_COLUMNS, fullTestPath, false /*mapByPosition*/);

        runSqlTest("features/orc/write/decimal_with_large_scale");
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
        String insertStatement = "INSERT INTO " + exTable + "_writable VALUES ";
        for (int i = 0; i < numRows; i++) {
            StringJoiner statementBuilder = new StringJoiner(",", "(", ")")
                .add(String.valueOf(i))
                .add((i % nullModulo == 0) ? "NULL" : String.format("'2010-01-%02d'", (i % 30) + 1))                // DataType.DATE
                .add((i % nullModulo == 1) ? "NULL" : String.format("'10:11:%02d'", i % 60))                        // DataType.TIME
                .add((i % nullModulo == 2) ? "NULL" : String.format("'2013-07-13 21:00:05.%03d456'", i % 1000))     // DataType.TIMESTAMP
                .add((i % nullModulo == 3) ? "NULL" : String.format("'2013-07-13 21:00:05.987%03d-07'", i % 1000))  // DataType.TIMESTAMP_WITH_TIME_ZONE
            ;
            insertStatement += statementBuilder.toString().concat((i < (numRows - 1)) ? "," : ";");
        }
        gpdb.runQuery(insertStatement);
    }

    private void insertArrayDataWithNulls(String exTable, int numRows, int nullModulo) throws Exception {
        String insertStatement = "INSERT INTO " + exTable + "_writable VALUES ";
        for (int i = 0; i < numRows; i++) {
            StringJoiner statementBuilder = new StringJoiner(",", "(", ")")
                .add(String.valueOf(i))    // always not-null row index, column index starts with 0 after it
                .add((i % nullModulo == 0) ? "NULL" : String.format("'{\"%b\"}'", i % 2 != 0))                                   // DataType.BOOLEANARRAY
                .add((i % nullModulo == 1) ? "NULL" : String.format("'{\\\\x%02d%02d}'::bytea[]", i % 100, (i + 1) % 100))       // DataType.BYTEAARRAY
                .add((i % nullModulo == 2) ? "NULL" : String.format("'{%d}'", 123456789000000000L + i))                          // DataType.INT8ARRAY
                .add((i % nullModulo == 3) ? "NULL" : String.format("'{%d}'",10L + i % 32000))                                   // DataType.INT2ARRAY
                .add((i % nullModulo == 4) ? "NULL" : String.format("'{%d}'", 100L + i))                                         // DataType.INT4ARRAY
                .add((i % nullModulo == 5) ? "NULL" : String.format("'{\"row-%02d\"}'", i))                                      // DataType.TEXTARRAY
                .add((i % nullModulo == 6) ? "NULL" : String.format("'{%f}'", Float.valueOf(i + 0.00001f * i).doubleValue()))    // DataType.FLOAT4ARRAY
                .add((i % nullModulo == 7) ? "NULL" : String.format("'{%f}'", i + Math.PI))                                      // DataType.FLOAT8ARRAY
                .add((i % nullModulo == 8) ? "NULL" : String.format("'{\"%s\"}'", i))                                            // DataType.BPCHARARRAY
                .add((i % nullModulo == 9) ? "NULL" : String.format("'{\"var%02d\"}'", i))                                       // DataType.VARCHARARRAY
                .add((i % nullModulo == 10) ? "NULL" : String.format("'{\"longer string var%02d\"}'", i))                        // DataType.VARCHARARRAY no limit
                .add((i % nullModulo == 11) ? "NULL" : String.format("'{\"2010-01-%02d\"}'", (i % 30) + 1))                      // DataType.DATEARRAY
                .add((i % nullModulo == 12) ? "NULL" : String.format("'{\"10:11:%02d\"}'", i % 60))                              // DataType.TIMEARRAY
                .add((i % nullModulo == 13) ? "NULL" : String.format("'{\"2013-07-13 21:00:05.%03d456\"}'", i % 1000))           // DataType.TIMESTAMPARRAY
                .add((i % nullModulo == 14) ? "NULL" : String.format("'{\"2013-07-13 21:00:05.987%03d-07\"}'", i % 1000))        // DataType.TIMESTAMP_WITH_TIME_ZONE_ARRAY
                .add((i % nullModulo == 15) ? "NULL" : String.format("'{12345678900000.00000%s}'", i))                           // DataType.NUMERICARRAY
                .add((i % nullModulo == 16) ? "NULL" : String.format("'{\"476f35e4-da1a-43cf-8f7c-950a%08d\"}'", i % 100000000)) // DataType.UUIDARRAY
                ;
            insertStatement += statementBuilder.toString().concat((i < (numRows - 1)) ? "," : ";");
        }
        gpdb.runQuery(insertStatement);
    }

    private void insertArrayDataWithNullElements(String exTable, int numRows, int nullModulo) throws Exception {
        String insertStatement = "INSERT INTO " + exTable + "_writable VALUES ";
        for (int i = 0; i < numRows; i++) {
            StringJoiner statementBuilder = new StringJoiner(",", "(", ")")
                .add(String.valueOf(i))    // always not-null row index, column index starts with 0 after it
                .add((i % nullModulo == 0) ? "'{NULL}'" : (i % nullModulo == 8) ? "'{}'" : String.format("'{\"%b\", \"%b\", NULL}'", i % 2 != 0, i % 3 != 0))                                                                          // DataType.BOOLEANARRAY
                .add((i % nullModulo == 1) ? "'{NULL}'" : (i % nullModulo == 9) ? "'{}'" : String.format("'{\\\\x%02d%02d, NULL, \\\\x%02d%02d}'::bytea[]", i % 100, (i + 1) % 100,  (i + 2) % 100, (i + 3) % 100))                    // DataType.BYTEAARRAY
                .add((i % nullModulo == 2) ? "'{NULL}'" : (i % nullModulo == 10) ? "'{}'" : String.format("'{NULL, %d}'", 123456789000000000L + i))                                                                                    // DataType.INT8ARRAY
                .add((i % nullModulo == 3) ? "'{NULL}'" : (i % nullModulo == 11) ? "'{}'" : String.format("'{%d, NULL}'", 10L + i % 32000))                                                                                            // DataType.INT2ARRAY
                .add((i % nullModulo == 4) ? "'{NULL}'" : (i % nullModulo == 12) ? "'{}'" : String.format("'{%d, %d, NULL}'", 100L + i, 200L + i))                                                                                     // DataType.INT4ARRAY
                .add((i % nullModulo == 5) ? "'{NULL}'" : (i % nullModulo == 13) ? "'{}'" : String.format("'{\"row-%02d\", \"row-%02d\", NULL, \"\"}'", i, i * 2))                                                                     // DataType.TEXTARRAY
                .add((i % nullModulo == 6) ? "'{NULL}'" : (i % nullModulo == 14) ? "'{}'" : String.format("'{NULL, %f}'", Float.valueOf(i + 0.00001f * i).doubleValue()))                                                              // DataType.FLOAT4ARRAY
                .add((i % nullModulo == 7) ? "'{NULL}'" : (i % nullModulo == 15) ? "'{}'" : String.format("'{%f, NULL}'", i + Math.PI))                                                                                                // DataType.FLOAT8ARRAY
                .add((i % nullModulo == 8) ? "'{NULL}'" : (i % nullModulo == 16) ? "'{}'" : String.format("'{\"%s\", \"%s\", \"%s\", NULL}'", i, i + 1, i + 2))                                                                        // DataType.BPCHARARRAY
                .add((i % nullModulo == 9) ? "'{NULL}'" : (i % nullModulo == 0) ? "'{}'" : String.format("'{NULL, \"var%02d\", \"var%02d\"}'", i, i + 2))                                                                              // DataType.VARCHARARRAY
                .add((i % nullModulo == 10) ? "'{NULL}'" : (i % nullModulo == 1) ? "'{}'" : String.format("'{\"longer string var%02d\", \"longer string var%02d\", NULL, \"longer string var%02d\"}'", i, i + 2, i + 3))               // DataType.VARCHARARRAY no limit
                .add((i % nullModulo == 11) ? "'{NULL}'" : (i % nullModulo == 2) ? "'{}'" : String.format("'{\"2010-01-%02d\", NULL, \"2010-01-%02d\"}'", (i % 30) + 1, ((i * 2) % 30) + 1))                                           // DataType.DATEARRAY
                .add((i % nullModulo == 12) ? "'{NULL}'" : (i % nullModulo == 3) ? "'{}'" : String.format("'{NULL, \"10:11:%02d\", \"10:11:%02d\"}'", i % 60, (i * 3) % 60))                                                           // DataType.TIMEARRAY
                .add((i % nullModulo == 13) ? "'{NULL}'" : (i % nullModulo == 4) ? "'{}'" : String.format("'{\"2013-07-13 21:00:05.%03d456\", NULL}'", i % 1000))                                                                      // DataType.TIMESTAMPARRAY
                .add((i % nullModulo == 14) ? "'{NULL}'" : (i % nullModulo == 5) ? "'{}'" : String.format("'{\"2013-07-13 21:00:05.987%03d-07\", \"2013-07-13 21:00:05.987%03d-07\", NULL}'", i % 1000, i % 999))                      // DataType.TIMESTAMP_WITH_TIME_ZONE_ARRAY
                .add((i % nullModulo == 15) ? "'{NULL}'" : (i % nullModulo == 6) ? "'{}'" : String.format("'{NULL, 12345678900000.00000%s, 12345678900000.00000%s}'", i, i + 1))                                                       // DataType.NUMERICARRAY
                .add((i % nullModulo == 16) ? "'{NULL}'" : (i % nullModulo == 7) ? "'{}'" : String.format("'{\"476f35e4-da1a-43cf-8f7c-950a%08d\", NULL, \"476f35e4-da1a-43cf-8f7c-950a%08d\"}'", i % 100000000,  (i+2) % 100000000))  // DataType.UUIDARRAY
                ;
            insertStatement += statementBuilder.toString().concat((i < (numRows - 1)) ? "," : ";");
        }
        gpdb.runQuery(insertStatement);
    }

    private void insertMultidimensionalArrayData(String exTable, int numRows, int nullModulo) throws Exception {
        String insertStatement = "INSERT INTO " + exTable + " VALUES ";
        for (int i = 0; i < numRows; i++) {
            StringJoiner statementBuilder = new StringJoiner(",", "(", ")")
                .add(String.valueOf(i))    // always not-null row index, column index starts with 0 after it
                .add((i % nullModulo == 0) ? "'{NULL}'" : (i % nullModulo == 8) ? "'{}'" : String.format("'{{\"%b\", \"%b\"}, {NULL, \"%b\"}}'::bool[]", i % 2 != 0, i % 3 != 0, i % 7 != 0))                                                // DataType.BOOLEANARRAY
                .add((i % nullModulo == 1) ? "'{NULL}'" : (i % nullModulo == 9) ? "'{}'" : String.format("'{{\\\\x%02d%02d}, {\\\\x%02d%02d}}'::bytea[]", i % 100, (i + 1) % 100,  (i + 2) % 100, (i + 3) % 100))                            // DataType.BYTEAARRAY
                .add((i % nullModulo == 2) ? "'{NULL}'" : (i % nullModulo == 10) ? "'{}'" : String.format("'{{NULL, %d}}'", 123456789000000000L + i))                                                                                        // DataType.INT8ARRAY
                .add((i % nullModulo == 3) ? "'{NULL}'" : (i % nullModulo == 11) ? "'{}'" : String.format("'{{%d, NULL}}'", 10L + i % 32000))                                                                                                // DataType.INT2ARRAY
                .add((i % nullModulo == 4) ? "'{NULL}'" : (i % nullModulo == 12) ? "'{}'" : String.format("'{{%d}, {%d}}'", 100L + i, 200L + i))                                                                                             // DataType.INT4ARRAY
                .add((i % nullModulo == 5) ? "'{NULL}'" : (i % nullModulo == 13) ? "'{}'" : String.format("'{{\"row-%02d\", \"row-%02d\"}, {NULL, \"\"}}'", i, i * 2))                                                                       // DataType.TEXTARRAY
                .add((i % nullModulo == 6) ? "'{NULL}'" : (i % nullModulo == 14) ? "'{}'" : String.format("'{{NULL, %f}}'", Float.valueOf(i + 0.00001f * i).doubleValue()))                                                                  // DataType.FLOAT4ARRAY
                .add((i % nullModulo == 7) ? "'{NULL}'" : (i % nullModulo == 15) ? "'{}'" : String.format("'{{%f, NULL}}'", i + Math.PI))                                                                                                    // DataType.FLOAT8ARRAY
                .add((i % nullModulo == 8) ? "'{NULL}'" : (i % nullModulo == 16) ? "'{}'" : String.format("'{{\"%s\", \"%s\"}, {\"%s\", NULL}}'", i, i + 1, i + 2))                                                                          // DataType.BPCHARARRAY
                .add((i % nullModulo == 9) ? "'{NULL}'" : (i % nullModulo == 0) ? "'{}'" : String.format("'{{\"var%02d\"}, {\"var%02d\"}}'", i, i + 2))                                                                                      // DataType.VARCHARARRAY
                .add((i % nullModulo == 10) ? "'{NULL}'" : (i % nullModulo == 1) ? "'{}'" : String.format("'{{\"longer string var%02d\", \"longer string var%02d\"}, {NULL, \"longer string var%02d\"}}'", i, i + 2, i + 3))                 // DataType.VARCHARARRAY no limit
                .add((i % nullModulo == 11) ? "'{NULL}'" : (i % nullModulo == 2) ? "'{}'" : String.format("'{{\"2010-01-%02d\", NULL, \"2010-01-%02d\"}}'", (i % 30) + 1, ((i * 2) % 30) + 1))                                               // DataType.DATEARRAY
                .add((i % nullModulo == 12) ? "'{NULL}'" : (i % nullModulo == 3) ? "'{}'" : String.format("'{{NULL}, {\"10:11:%02d\"}, {\"10:11:%02d\"}}'", i % 60, (i * 3) % 60))                                                           // DataType.TIMEARRAY
                .add((i % nullModulo == 13) ? "'{NULL}'" : (i % nullModulo == 4) ? "'{}'" : String.format("'{{\"2013-07-13 21:00:05.%03d456\"}, {NULL}}'", i % 1000))                                                                        // DataType.TIMESTAMPARRAY
                .add((i % nullModulo == 14) ? "'{NULL}'" : (i % nullModulo == 5) ? "'{}'" : String.format("'{{\"2013-07-13 21:00:05.987%03d-07\", \"2013-07-13 21:00:05.987%03d-07\"}, {NULL, NULL}}'", i % 1000, i % 999))                  // DataType.TIMESTAMP_WITH_TIME_ZONE_ARRAY
                .add((i % nullModulo == 15) ? "'{NULL}'" : (i % nullModulo == 6) ? "'{}'" : String.format("'{{NULL, 12345678900000.00000%s}, {12345678900000.00000%s, NULL}}'", i, i + 1))                                                   // DataType.NUMERICARRAY
                .add((i % nullModulo == 16) ? "'{NULL}'" : (i % nullModulo == 7) ? "'{}'" : String.format("'{{\"476f35e4-da1a-43cf-8f7c-950a%08d\"}, {NULL}, {\"476f35e4-da1a-43cf-8f7c-950a%08d\"}}'", i % 100000000,  (i+2) % 100000000))  // DataType.UUIDARRAY
                ;
            insertStatement += statementBuilder.toString().concat((i < (numRows - 1)) ? "," : ";");
        }
        gpdb.runQuery(insertStatement);
    }

    private String getRecordCSV(int row, boolean[] isNull) {
        // refer to ORCVectorizedResolverWriteTest unit test where this data is used
        StringJoiner rowBuilder = new StringJoiner(",", "(", ")")
            .add(String.valueOf(row))    // always not-null row index, column index starts with 0 after it
            .add(isNull [0] ? "NULL" : String.valueOf(row % 2 != 0))                                         // DataType.BOOLEAN
            .add(isNull [1] ? "NULL" : String.format("'\\x%02d%02d'::bytea", row%100, (row + 1) % 100))      // DataType.BYTEA
            .add(isNull [2] ? "NULL" : String.valueOf(123456789000000000L + row))                            // DataType.BIGINT
            .add(isNull [3] ? "NULL" : String.valueOf(10L + row % 32000))                                    // DataType.SMALLINT
            .add(isNull [4] ? "NULL" : String.valueOf(100L + row))                                           // DataType.INTEGER
            .add(isNull [5] ? "NULL" : String.format("'row-%02d'", row))                                     // DataType.TEXT
            .add(isNull [6] ? "NULL" : Float.valueOf(row + 0.00001f * row).toString())                       // DataType.REAL
            .add(isNull [7] ? "NULL" : String.valueOf(row + Math.PI))                                        // DataType.FLOAT8
            .add(isNull [8] ? "NULL" : String.format("'%s'", row))                                           // DataType.BPCHAR
            .add(isNull [9] ? "NULL" : String.format("'var%02d'", row))                                      // DataType.VARCHAR
            .add(isNull[10] ? "NULL" : String.format("'var-no-length-%02d'", row))                           // DataType.VARCHAR no length
            .add(isNull[11] ? "NULL" : String.format("'2010-01-%02d'", (row % 30) + 1))                      // DataType.DATE
            .add(isNull[12] ? "NULL" : String.format("'10:11:%02d'", row % 60))                              // DataType.TIME
            .add(isNull[13] ? "NULL" : String.format("'2013-07-13 21:00:05.%03d456'", row % 1000))           // DataType.TIMESTAMP
            .add(isNull[14] ? "NULL" : String.format("'12345678900000.00000%s'", row))                       // DataType.NUMERIC
            .add(isNull[15] ? "NULL" : String.format("'476f35e4-da1a-43cf-8f7c-950a%08d'", row % 100000000)) // DataType.UUID
            ;
        return rowBuilder.toString();
    }

    public interface ThrowingConsumer<T, E extends Exception> {
        void accept() throws E;
    }

    private void attemptInsert(ThrowingConsumer operation, Integer numRows, String readableTableName, Integer retryCount) throws Exception {
        if (retryCount == 0) {
            return;
        }

        try {
            operation.accept();
        } catch (PSQLException e) {
            if (e.getMessage().contains("Operation could not be completed within the specified time")) {
                ReportUtils.startLevel(null, getClass(), String.format("Operation timed out, trying again. Retries left: %d : '%s'", retryCount, e.getMessage()));
                Table analyticResult = new Table("analyticResult", null);
                // see how much data was written before the operation timed out
                gpdb.queryResults(analyticResult, String.format("SELECT COUNT(*) FROM %s", readableTableName));
                String result = analyticResult.getData().get(0).get(0);

                ReportUtils.startLevel(null, getClass(), String.format("Found %s records, expected %d", result, numRows));

                // operation timed out in the middle and not all the data was added, delete all the files and try again
                if (Integer.valueOf(result) < numRows) {
                    hdfs.removeDirectory(fullTestPath);
                    attemptInsert(operation, numRows, readableTableName, retryCount - 1);
                }
            }
        }
    }

    private void prepareWritableExternalTable(String name, String[] fields, String path) throws Exception {
        exTable = TableFactory.getPxfHcfsWritableTable(name + "_writable", fields, path, hdfs.getBasePath(), "orc");
        createTable(exTable);
    }

    private void prepareReadableExternalTable(String name, String[] fields, String path, boolean mapByPosition) throws Exception {
        exTable = TableFactory.getPxfHcfsReadableTable(name + "_readable", fields, path, hdfs.getBasePath(), "orc");
        if (mapByPosition) {
            exTable.setUserParameters(new String[]{"MAP_BY_POSITION=true"});
        }
        createTable(exTable);
    }

}
