package org.greenplum.pxf.automation.features.json;

import annotations.FailsWithFDW;
import annotations.WorksWithFDW;
import org.apache.commons.lang.StringUtils;
import org.greenplum.pxf.automation.features.BaseFeature;
import org.greenplum.pxf.automation.structures.tables.utils.TableFactory;
import org.greenplum.pxf.automation.utils.system.ProtocolEnum;
import org.greenplum.pxf.automation.utils.system.ProtocolUtils;
import org.testng.annotations.Test;

/**
 * Tests for Json plugin to read HDFS files in JSON format.
 */
@WorksWithFDW
public class JsonReadTest extends BaseFeature {

    private String hdfsPath;
    private String resourcePath;

    private final String SUFFIX_JSON = ".json";

    private final String FILENAME_SIMPLE = "simple";
    private final String FILENAME_TYPES = "supported_primitive_types";
    private final String FILENAME_PRETTY_PRINT = "tweets-pp";
    private final String FILENAME_PRETTY_PRINT_W_DELETE = "tweets-pp-with-delete";
    private final String FILENAME_BROKEN = "tweets-broken";
    private final String FILENAME_MISMATCHED_TYPES = "supported_primitive_mismatched_types";
    private final String FILENAME_JSON_ARRAY = "simple_array";

    private static final String[] TWEETS_FIELDS = new String[]{
            "created_at                     text",
            "id                             bigint",
            "text                           text",
            "\"user.screen_name\"           text",
            "\"entities.hashtags[0]\"       text",
            "\"coordinates.coordinates[0]\" float8",
            "\"coordinates.coordinates[1]\" float8",
    };

    private static final String[] SUPPORTED_PRIMITIVE_FIELDS = new String[]{
            "type_int      int",
            "type_bigint   bigint",
            "type_smallint smallint",
            "type_float    real",
            "type_double   float8",
            "type_string1  text",
            "type_string2  varchar",
            "type_string3  bpchar",
            "type_char     char",
            "type_boolean  bool",
    };

    private static final String[] ARRAYS_AS_TEXT_FIELDS = new String[]{
            "id       int",
            "emp_arr  text",
            "emp_obj  text",
            "num_arr  text",
            "bool_arr text",
            "str_arr  text",
            "arr_arr  text",
            "obj_arr  text",
            "obj      text"
    };

    private static final String[] ARRAYS_AS_VARCHAR_FIELDS = new String[]{
            "id       int",
            "emp_arr  varchar",      // unlimited
            "emp_obj  varchar(10)",  // more than actual
            "num_arr  varchar(40)",  // actual size
            "bool_arr varchar(255)", // way more than actual
            "str_arr  varchar",
            "arr_arr  varchar",
            "obj_arr  varchar",
            "obj      varchar"
    };

    private static final String[] ARRAYS_AS_BPCHAR_FIELDS = new String[]{
            "id       int",
            "emp_arr  bpchar(4)",
            "emp_obj  bpchar(10)",
            "num_arr  bpchar(42)", // 2 more than actual size of data
            "bool_arr bpchar(17)", // actual size
            "str_arr  bpchar(25)", // 1 more than actual
            "arr_arr  bpchar(25)", // 3 more than actual
            "obj_arr  bpchar(23)", // 1 more than actual
            "obj      bpchar(100)" // actual size
    };

    private static final String[] ARRAYS_AS_TEXT_PROJECTIONS_FIELDS = new String[]{
            "id                        int",
            "\"emp_arr[0]\"            text",
            "\"emp_arr[1]\"            text",     // out of bounds
            "\"num_arr[0]\"            real",
            "\"num_arr[1]\"            real",
            "\"num_arr[100]\"          real",     // out of bounds
            "\"bool_arr[0]\"           boolean",
            "\"str_arr[2]\"            text",     // quoted value
            "\"arr_arr[0]\"            text",
            "\"arr_arr[1]\"            text",
            "\"arr_arr[100]\"          text",     // out of bounds
            "\"obj_arr[0]\"            text",
            "\"obj_arr[1]\"            text",
            "\"obj_arr[100]\"          text",     // out of bounds
            "\"obj_arr[0].a\"          text",     // this returns NULL now, not the actual value
            "\"obj.data.data.data\"    text",
            "\"obj.data.data.data[0]\" text",
            "\"obj.data.data\"         text"
    };

    @Override
    public void beforeClass() throws Exception {
        // path for storing data on HDFS (for processing by PXF)
        hdfsPath = hdfs.getWorkingDirectory() + "/json/";

        // location of schema and data files
        resourcePath = localDataResourcesFolder + "/json/";

        // create and copy data to hdfs
        prepareData();
    }

    private void prepareData() throws Exception {

        hdfs.copyFromLocal(resourcePath + FILENAME_SIMPLE + SUFFIX_JSON,
                hdfsPath + FILENAME_SIMPLE + SUFFIX_JSON);
        hdfs.copyFromLocal(resourcePath + FILENAME_TYPES + SUFFIX_JSON,
                hdfsPath + FILENAME_TYPES + SUFFIX_JSON);
        hdfs.copyFromLocal(resourcePath + FILENAME_PRETTY_PRINT + SUFFIX_JSON,
                hdfsPath + FILENAME_PRETTY_PRINT + SUFFIX_JSON);
        hdfs.copyFromLocal(resourcePath + FILENAME_PRETTY_PRINT_W_DELETE
                + SUFFIX_JSON, hdfsPath + FILENAME_PRETTY_PRINT_W_DELETE
                + SUFFIX_JSON);
        hdfs.copyFromLocal(resourcePath + FILENAME_BROKEN + SUFFIX_JSON,
                hdfsPath + FILENAME_BROKEN + SUFFIX_JSON);
        hdfs.copyFromLocal(resourcePath + FILENAME_MISMATCHED_TYPES + SUFFIX_JSON,
                hdfsPath + FILENAME_MISMATCHED_TYPES + SUFFIX_JSON);
        hdfs.copyFromLocal(resourcePath + FILENAME_JSON_ARRAY + SUFFIX_JSON,
                hdfsPath + FILENAME_JSON_ARRAY + SUFFIX_JSON);
    }

    /**
     * Test simple json file
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"features", "gpdb", "security", "hcfs"})
    public void jsonSimple() throws Exception {
        prepareExternalTable("jsontest_simple", new String[]{"name text", "age int"}, hdfsPath + FILENAME_SIMPLE + SUFFIX_JSON, "custom");
        gpdb.createTableAndVerify(exTable);
        // Verify results
        runSqlTest("features/hdfs/readable/json/simple");
    }

    /**
     * Test all JSON plugin supported types. TODO: no support for bytea type
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"features", "gpdb", "security", "hcfs"})
    public void jsonSupportedPrimitives() throws Exception {
        prepareExternalTable("jsontest_supported_primitive_types", SUPPORTED_PRIMITIVE_FIELDS, hdfsPath + FILENAME_TYPES + SUFFIX_JSON, "custom");
        gpdb.createTableAndVerify(exTable);
        // Verify results
        runSqlTest("features/hdfs/readable/json/supported_primitive_types");
    }

    /**
     * Test all JSON plugin supported types. TODO: no support for bytea type
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"features", "gpdb", "security", "hcfs"})
    public void jsonSupportedPrimitivesWithCsvWireFormat() throws Exception {
        prepareExternalTable("jsontest_supported_primitive_types", SUPPORTED_PRIMITIVE_FIELDS, hdfsPath + FILENAME_TYPES + SUFFIX_JSON, "CSV");
        gpdb.createTableAndVerify(exTable);
        // Verify results
        runSqlTest("features/hdfs/readable/json/supported_primitive_types");
    }

    /**
     * Test JSON file with pretty print format. Some of the fields return null
     * value because the field is missing of because the array doesn't contain
     * the requested item.
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"features", "gpdb", "security", "hcfs"})
    public void jsonPrettyPrint() throws Exception {
        prepareExternalTable("jsontest_pretty_print", TWEETS_FIELDS, hdfsPath + FILENAME_PRETTY_PRINT + SUFFIX_JSON, "custom");
        exTable.setUserParameters(new String[]{"IDENTIFIER=created_at"});
        gpdb.createTableAndVerify(exTable);

        prepareExternalTable("jsontest_pretty_print_filefrag", TWEETS_FIELDS, hdfsPath + FILENAME_PRETTY_PRINT + SUFFIX_JSON, "custom");
        exTable.setUserParameters(new String[]{
                "IDENTIFIER=created_at",
                "SPLIT_BY_FILE=true"});
        gpdb.createTableAndVerify(exTable);
        // Verify results
        runSqlTest("features/hdfs/readable/json/pretty_print");
    }

    /**
     * Test JSON file with pretty print format. Some of the records don't
     * contain the identifier and will be ignored.
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"features", "gpdb", "security", "hcfs"})
    public void missingIdentifier() throws Exception {
        prepareExternalTable("jsontest_missing_identifier", TWEETS_FIELDS, hdfsPath + FILENAME_PRETTY_PRINT_W_DELETE + SUFFIX_JSON, "custom");
        exTable.setUserParameters(new String[]{"IDENTIFIER=created_at"});
        gpdb.createTableAndVerify(exTable);

        prepareExternalTable("jsontest_missing_identifier_filefrag", TWEETS_FIELDS, hdfsPath + FILENAME_PRETTY_PRINT_W_DELETE + SUFFIX_JSON, "custom");
        exTable.setUserParameters(new String[]{
                "IDENTIFIER=created_at",
                "SPLIT_BY_FILE=true"});
        gpdb.createTableAndVerify(exTable);
        // Verify results
        runSqlTest("features/hdfs/readable/json/missing_identifier");
    }

    /**
     * Test JSON file with pretty print format. Some of the records exceed the
     * max size (MAXLENGTH) and will be ignored.
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"features", "gpdb", "security", "hcfs"})
    public void exceedsMaxSize() throws Exception {
        prepareExternalTable("jsontest_max_size", TWEETS_FIELDS, hdfsPath + FILENAME_PRETTY_PRINT + SUFFIX_JSON, "custom");
        exTable.setUserParameters(new String[]{
                "IDENTIFIER=created_at",
                "MAXLENGTH=566"});
        gpdb.createTableAndVerify(exTable);

        prepareExternalTable("jsontest_max_size_filefrag", TWEETS_FIELDS, hdfsPath + FILENAME_PRETTY_PRINT + SUFFIX_JSON, "custom");
        exTable.setUserParameters(new String[]{
                "IDENTIFIER=created_at",
                "MAXLENGTH=566",
                "SPLIT_BY_FILE=true"});
        gpdb.createTableAndVerify(exTable);
        // Verify results
        runSqlTest("features/hdfs/readable/json/exceed_max_size");
    }

    /**
     * Test JSON file with pretty print format. One of the records
     * is malformed. In that case the whole line will be
     * replaced by NULLs.
     *
     * @throws Exception if test fails to run
     */
    // Some of these tests failing because the returned error has extra quotes "
    // For e.g.:
    // ""[truncated 73 chars]; line: 3, column: 22]'. invalid JSON record
    //  Instead of
    // "[truncated 73 chars]; line: 3, column: 22]'. invalid JSON record
    // This is getting appended in toCsvField method of GreenplumCSV.java
    // And is happening only for the error cases. Adding escape "\" doesn't help here as well.

    // TODO: May be create a separate test suite for all these failing tests
    @FailsWithFDW
    @Test(groups = {"features", "gpdb", "security", "hcfs"})
    public void malformedRecord() throws Exception {
        prepareExternalTable("jsontest_malformed_record", TWEETS_FIELDS, hdfsPath + FILENAME_BROKEN + SUFFIX_JSON, "custom");
        exTable.setUserParameters(new String[]{"IDENTIFIER=created_at"});
        gpdb.createTableAndVerify(exTable);

        prepareExternalTable("jsontest_malformed_record_filefrag", TWEETS_FIELDS, hdfsPath + FILENAME_BROKEN + SUFFIX_JSON, "custom");
        exTable.setUserParameters(new String[]{
                "IDENTIFIER=created_at",
                "SPLIT_BY_FILE=true"});
        gpdb.createTableAndVerify(exTable);
        // Verify results
        runSqlTest("features/hdfs/readable/json/malformed_record");
    }

    /**
     * Test JSON file with pretty print format. One of the records
     * is malformed. In that case the whole line will be
     * replaced by NULLs.
     *
     * @throws Exception if test fails to run
     */
    @FailsWithFDW
    @Test(groups = {"features", "gpdb", "security"})
    public void malformedRecordWithCsvWireFormat() throws Exception {
        prepareExternalTable("jsontest_malformed_record", TWEETS_FIELDS, hdfsPath + FILENAME_BROKEN + SUFFIX_JSON, "CSV");
        exTable.setUserParameters(new String[]{"IDENTIFIER=created_at"});
        gpdb.createTableAndVerify(exTable);

        prepareExternalTable("jsontest_malformed_record_filefrag", TWEETS_FIELDS, hdfsPath + FILENAME_BROKEN + SUFFIX_JSON, "CSV");
        exTable.setUserParameters(new String[]{
                "IDENTIFIER=created_at",
                "SPLIT_BY_FILE=true"});
        gpdb.createTableAndVerify(exTable);
        // Verify results
        runSqlTest("features/hdfs/readable/json/malformed_record_csv");
    }

    /**
     * Test JSON file with pretty print format with reject limit configured. One of the records
     * is malformed. The query is allowed and a table is created.
     *
     * @throws Exception if test fails to run
     */
    @FailsWithFDW
    @Test(groups = {"features", "gpdb", "security", "hcfs"})
    public void malformedRecordWithRejectLimit() throws Exception {
        prepareExternalTable("jsontest_malformed_record_with_reject_limit", TWEETS_FIELDS, hdfsPath + FILENAME_BROKEN + SUFFIX_JSON, "custom");
        exTable.setUserParameters(new String[]{"IDENTIFIER=created_at"});
        exTable.setSegmentRejectLimit(2);
        exTable.setErrorTable("true");
        gpdb.createTableAndVerify(exTable);

        prepareExternalTable("jsontest_malformed_record_with_reject_limit_filefrag", TWEETS_FIELDS, hdfsPath + FILENAME_BROKEN + SUFFIX_JSON, "custom");
        exTable.setUserParameters(new String[]{
                "IDENTIFIER=created_at",
                "SPLIT_BY_FILE=true"});
        exTable.setSegmentRejectLimit(2);
        exTable.setErrorTable("true");
        gpdb.createTableAndVerify(exTable);
        // Verify results
        runSqlTest("features/hdfs/readable/json/malformed_record_with_reject_limit");
    }

    /**
     * Test JSON file with pretty print format with reject limit configured. One of the records
     * is malformed. The query is allowed and a table is created.
     *
     * @throws Exception if test fails to run
     */
    @FailsWithFDW
    @Test(groups = {"features", "gpdb", "security", "hcfs"})
    public void malformedRecordWithRejectLimitWithCsvWireFormat() throws Exception {
        prepareExternalTable("jsontest_malformed_record_with_reject_limit", TWEETS_FIELDS, hdfsPath + FILENAME_BROKEN + SUFFIX_JSON, "CSV");
        exTable.setUserParameters(new String[]{"IDENTIFIER=created_at"});
        exTable.setSegmentRejectLimit(2);
        exTable.setErrorTable("true");
        gpdb.createTableAndVerify(exTable);

        prepareExternalTable("jsontest_malformed_record_with_reject_limit_filefrag", TWEETS_FIELDS, hdfsPath + FILENAME_BROKEN + SUFFIX_JSON, "CSV");
        exTable.setUserParameters(new String[]{
                "IDENTIFIER=created_at",
                "SPLIT_BY_FILE=true"});
        exTable.setSegmentRejectLimit(2);
        exTable.setErrorTable("true");
        gpdb.createTableAndVerify(exTable);
        // Verify results
        runSqlTest("features/hdfs/readable/json/malformed_record_with_reject_limit_csv");
    }

    /**
     * Test JSON file with all supported types. Some of the records
     * have type mismatches (e.g. an integer entered as '(').
     * In that case, the line will be sent to GPDB as TEXT, and we
     * expect GPDB to raise a type error.
     *
     * @throws Exception if test fails to run
     */
    @FailsWithFDW
    @Test(groups = {"features", "gpdb", "security", "hcfs"})
    public void mismatchedTypes() throws Exception {
        prepareExternalTable("jsontest_mismatched_types", SUPPORTED_PRIMITIVE_FIELDS, hdfsPath + FILENAME_MISMATCHED_TYPES + SUFFIX_JSON, "custom");
        gpdb.createTableAndVerify(exTable);
        // Verify results
        runSqlTest("features/hdfs/readable/json/mismatched_types");
    }

    /**
     * Test JSON file with all supported types. Some of the records
     * have type mismatches (e.g. an integer entered as '(').
     * In that case, the line will be sent to GPDB as TEXT, and we
     * expect GPDB to raise a type error. This table has reject limit
     * set high enough to get some data back.
     *
     * @throws Exception if test fails to run
     */
    @FailsWithFDW
    @Test(groups = {"features", "gpdb", "security", "hcfs"})
    public void mismatchedTypesWithRejectLimit() throws Exception {
        prepareExternalTable("jsontest_mismatched_types_with_reject_limit", SUPPORTED_PRIMITIVE_FIELDS, hdfsPath + FILENAME_MISMATCHED_TYPES + SUFFIX_JSON, "custom");
        exTable.setSegmentRejectLimit(7);
        exTable.setErrorTable("true");
        gpdb.createTableAndVerify(exTable);
        // Verify results
        runSqlTest("features/hdfs/readable/json/mismatched_types_with_reject_limit");
    }

    @Test(groups = {"features", "gpdb", "security", "hcfs"})
    public void jsonStringArrayAsGpdbText() throws Exception {
        // tables where columns do not reference inside JSON arrays
        prepareExternalTable("jsontest_array_as_text", ARRAYS_AS_TEXT_FIELDS, hdfsPath + FILENAME_JSON_ARRAY + SUFFIX_JSON, "custom");
        gpdb.createTableAndVerify(exTable);

        prepareExternalTable("jsontest_array_as_varchar", ARRAYS_AS_VARCHAR_FIELDS, hdfsPath + FILENAME_JSON_ARRAY + SUFFIX_JSON, "custom");
        gpdb.createTableAndVerify(exTable);

        prepareExternalTable("jsontest_array_as_bpchar", ARRAYS_AS_BPCHAR_FIELDS, hdfsPath + FILENAME_JSON_ARRAY + SUFFIX_JSON, "custom");
        gpdb.createTableAndVerify(exTable);

        // table where columns reference inside JSON arrays
        prepareExternalTable("jsontest_array_as_text_projections", ARRAYS_AS_TEXT_PROJECTIONS_FIELDS, hdfsPath + FILENAME_JSON_ARRAY + SUFFIX_JSON, "custom");
        gpdb.createTableAndVerify(exTable);

        runSqlTest("features/hdfs/readable/json/array_as_text");

        // test using JSON functions (available in Greenplum 6+) to convert from TEXT fields into native arrays
        if (gpdb.getVersion() >= 6) {
            runSqlTest("features/hdfs/readable/json/json_functions");
        }
    }

    private void prepareExternalTable(String name, String[] fields, String path, String format) {
        ProtocolEnum protocol = ProtocolUtils.getProtocol();
        exTable = TableFactory.getPxfReadableJsonTable(name, fields,
                protocol.getExternalTablePath(hdfs.getBasePath(), path), format);
        exTable.setHost(pxfHost);
        exTable.setPort(pxfPort);
        if (StringUtils.equals(format, "custom")) {
            exTable.setFormatter("pxfwritable_import");
        }
        exTable.setProfile(protocol.value() + ":json");
    }
}
