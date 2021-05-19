package org.greenplum.pxf.automation.features.json;

import org.apache.commons.lang.StringUtils;
import org.greenplum.pxf.automation.features.BaseFeature;
import org.greenplum.pxf.automation.structures.tables.pxf.ReadableExternalTable;
import org.greenplum.pxf.automation.utils.system.ProtocolEnum;
import org.greenplum.pxf.automation.utils.system.ProtocolUtils;
import org.testng.annotations.Test;

/**
 * Tests for Json plugin to read HDFS files in JSON format.
 */
public class JsonTest extends BaseFeature {

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

    private final String[] tweetsFields = new String[]{
            "created_at text",
            "id bigint",
            "text text",
            "\"user.screen_name\" text",
            "\"entities.hashtags[0]\" text",
            "\"coordinates.coordinates[0]\" float8",
            "\"coordinates.coordinates[1]\" float8",
    };

    private final String[] supportedPrimitiveFields = new String[]{
            "type_int int",
            "type_bigint bigint",
            "type_smallint smallint",
            "type_float real",
            "type_double float8",
            "type_string1 text",
            "type_string2 varchar",
            "type_string3 bpchar",
            "type_char char",
            "type_boolean bool",
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
        runTincTest("pxf.features.hdfs.readable.json.simple.runTest");
    }

    /**
     * Test all JSON plugin supported types. TODO: no support for bytea type
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"features", "gpdb", "security", "hcfs"})
    public void jsonSupportedPrimitives() throws Exception {
        prepareExternalTable("jsontest_supported_primitive_types", supportedPrimitiveFields, hdfsPath + FILENAME_TYPES + SUFFIX_JSON, "custom");
        gpdb.createTableAndVerify(exTable);
        // Verify results
        runTincTest("pxf.features.hdfs.readable.json.supported_primitive_types.runTest");
    }

    /**
     * Test all JSON plugin supported types. TODO: no support for bytea type
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"features", "gpdb", "security", "hcfs"})
    public void jsonSupportedPrimitivesWithCsvWireFormat() throws Exception {
        prepareExternalTable("jsontest_supported_primitive_types", supportedPrimitiveFields, hdfsPath + FILENAME_TYPES + SUFFIX_JSON, "CSV");
        gpdb.createTableAndVerify(exTable);
        // Verify results
        runTincTest("pxf.features.hdfs.readable.json.supported_primitive_types.runTest");
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
        prepareExternalTable("jsontest_pretty_print", tweetsFields, hdfsPath + FILENAME_PRETTY_PRINT + SUFFIX_JSON, "custom");
        exTable.setUserParameters(new String[]{"IDENTIFIER=created_at"});
        gpdb.createTableAndVerify(exTable);
        // Verify results
        runTincTest("pxf.features.hdfs.readable.json.pretty_print.runTest");
    }

    /**
     * Test JSON file with pretty print format. Some of the records don't
     * contain the identifier and will be ignored.
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"features", "gpdb", "security", "hcfs"})
    public void missingIdentifier() throws Exception {
        prepareExternalTable("jsontest_missing_identifier", tweetsFields, hdfsPath + FILENAME_PRETTY_PRINT_W_DELETE + SUFFIX_JSON, "custom");
        exTable.setUserParameters(new String[]{"IDENTIFIER=created_at"});
        gpdb.createTableAndVerify(exTable);
        // Verify results
        runTincTest("pxf.features.hdfs.readable.json.missing_identifier.runTest");
    }

    /**
     * Test JSON file with pretty print format. Some of the records exceed the
     * max size (MAXLENGTH) and will be ignored.
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"features", "gpdb", "security", "hcfs"})
    public void exceedsMaxSize() throws Exception {
        prepareExternalTable("jsontest_max_size", tweetsFields, hdfsPath + FILENAME_PRETTY_PRINT + SUFFIX_JSON, "custom");
        exTable.setUserParameters(new String[]{
                "IDENTIFIER=created_at",
                "MAXLENGTH=566"});
        gpdb.createTableAndVerify(exTable);
        // Verify results
        runTincTest("pxf.features.hdfs.readable.json.exceed_max_size.runTest");
    }

    /**
     * Test JSON file with pretty print format. One of the records
     * is malformed. In that case the whole line will be
     * replaced by NULLs.
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"features", "gpdb", "security", "hcfs"})
    public void malformedRecord() throws Exception {
        prepareExternalTable("jsontest_malformed_record", tweetsFields, hdfsPath + FILENAME_BROKEN + SUFFIX_JSON, "custom");
        exTable.setUserParameters(new String[]{"IDENTIFIER=created_at"});
        gpdb.createTableAndVerify(exTable);
        // Verify results
        runTincTest("pxf.features.hdfs.readable.json.malformed_record.runTest");
    }

    /**
     * Test JSON file with pretty print format. One of the records
     * is malformed. In that case the whole line will be
     * replaced by NULLs.
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"features", "gpdb", "security"})
    public void malformedRecordWithCsvWireFormat() throws Exception {
        prepareExternalTable("jsontest_malformed_record", tweetsFields, hdfsPath + FILENAME_BROKEN + SUFFIX_JSON, "CSV");
        exTable.setUserParameters(new String[]{"IDENTIFIER=created_at"});
        gpdb.createTableAndVerify(exTable);
        // Verify results
        runTincTest("pxf.features.hdfs.readable.json.malformed_record_csv.runTest");
    }

    /**
     * Test JSON file with pretty print format with reject limit configured. One of the records
     * is malformed. The query is allowed and a table is created.
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"features", "gpdb", "security", "hcfs"})
    public void malformedRecordWithRejectLimit() throws Exception {
        prepareExternalTable("jsontest_malformed_record_with_reject_limit", tweetsFields, hdfsPath + FILENAME_BROKEN + SUFFIX_JSON, "custom");
        exTable.setUserParameters(new String[]{"IDENTIFIER=created_at"});
        exTable.setSegmentRejectLimit(2);
        exTable.setErrorTable("true");
        gpdb.createTableAndVerify(exTable);
        // Verify results
        runTincTest("pxf.features.hdfs.readable.json.malformed_record_with_reject_limit.runTest");
    }

    /**
     * Test JSON file with pretty print format with reject limit configured. One of the records
     * is malformed. The query is allowed and a table is created.
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"features", "gpdb", "security", "hcfs"})
    public void malformedRecordWithRejectLimitWithCsvWireFormat() throws Exception {
        prepareExternalTable("jsontest_malformed_record_with_reject_limit", tweetsFields, hdfsPath + FILENAME_BROKEN + SUFFIX_JSON, "CSV");
        exTable.setUserParameters(new String[]{"IDENTIFIER=created_at"});
        exTable.setSegmentRejectLimit(2);
        exTable.setErrorTable("true");
        gpdb.createTableAndVerify(exTable);
        // Verify results
        runTincTest("pxf.features.hdfs.readable.json.malformed_record_with_reject_limit_csv.runTest");
    }

    /**
     * Test JSON file with all supported types. Some of the records
     * have type mismatches (e.g. an integer entered as '(').
     * In that case, the line will be sent to GPDB as TEXT, and we
     * expect GPDB to raise a type error.
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"features", "gpdb", "security", "hcfs"})
    public void mismatchedTypes() throws Exception {
        prepareExternalTable("jsontest_mismatched_types", supportedPrimitiveFields, hdfsPath + FILENAME_MISMATCHED_TYPES + SUFFIX_JSON, "custom");
        gpdb.createTableAndVerify(exTable);
        // Verify results
        runTincTest("pxf.features.hdfs.readable.json.mismatched_types.runTest");
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
    @Test(groups = {"features", "gpdb", "security", "hcfs"})
    public void mismatchedTypesWithRejectLimit() throws Exception {
        prepareExternalTable("jsontest_mismatched_types_with_reject_limit", supportedPrimitiveFields, hdfsPath + FILENAME_MISMATCHED_TYPES + SUFFIX_JSON, "custom");
        exTable.setSegmentRejectLimit(7);
        exTable.setErrorTable("true");
        gpdb.createTableAndVerify(exTable);
        // Verify results
        runTincTest("pxf.features.hdfs.readable.json.mismatched_types_with_reject_limit.runTest");
    }

    @Test(groups = {"features", "gpdb", "security", "hcfs"})
    public void jsonStringArrayAsGpdbText() throws Exception {
        prepareExternalTable("jsontest_array_as_text", new String[]{"id INTEGER", "values TEXT"}, hdfsPath + FILENAME_JSON_ARRAY + SUFFIX_JSON, "custom");
        gpdb.createTableAndVerify(exTable);

        runTincTest("pxf.features.hdfs.readable.json.array_as_text.runTest");
    }

    // TODO: JSON array of other primitive types (e.g., int/float/bool)

    // TODO: JSON array of objects
    // TODO: test index into JSON array and then JSON object

    // TODO: JSON array of arrays

    // TODO: JSON array of mixed types (e.g., `[ture, 123, "a string"]`)

    // TODO: special characters in strings (`{"key": "This is a quote \"a quote.\" from foo."}`)
    /*
     * {"id": 1, "values": "This is a string with an embedded quote \"Hello, World!\""}
     * {"id": 2, "values": [{"key": "foo"}, {"key": "bizz"}, {"key": "The Fonz says \"Eh!\"."}]}
     * {"id": 3, "values": "{1,2} this is a valid JSON string that looks like a postgres array"}
     *
     * 1. Should be no problem reading this data into GPDB as text
     * 2. SELECT id, ((values::json)->2)->>'key' FROM pxf_json WHERE id = 2;
 id |       ?column?
----+----------------------
  2 | The Fonz says "Eh!".
     */

    // TODO: test using BPCHAR and/or VARCHAR

    // TODO: test for varchar(n)

    private void prepareExternalTable(String name, String[] fields, String path, String format) {
        ProtocolEnum protocol = ProtocolUtils.getProtocol();
        exTable = new ReadableExternalTable(name, fields,
                protocol.getExternalTablePath(hdfs.getBasePath(), path), format);
        exTable.setHost(pxfHost);
        exTable.setPort(pxfPort);
        if (StringUtils.equals(format, "custom")) {
            exTable.setFormatter("pxfwritable_import");
        }
        exTable.setProfile(protocol.value() + ":json");
    }
}
