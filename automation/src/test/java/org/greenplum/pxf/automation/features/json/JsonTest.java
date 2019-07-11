package org.greenplum.pxf.automation.features.json;

import org.greenplum.pxf.automation.structures.tables.pxf.ReadableExternalTable;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.greenplum.pxf.automation.features.BaseFeature;
import org.greenplum.pxf.automation.utils.system.ProtocolUtils;

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

    private String[] tweetsFields = new String[]{
            "created_at text",
            "id bigint",
            "text text",
            "\"user.screen_name\" text",
            "\"entities.hashtags[0]\" text",
            "\"coordinates.coordinates[0]\" float8",
            "\"coordinates.coordinates[1]\" float8",
    };

    private String[] supportedPrimitiveFields = new String[]{
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
    }

    @BeforeMethod(alwaysRun = true)
    public void setUp() throws Exception {

        // default external table with common settings
        exTable = new ReadableExternalTable("jsonSimple", null, "", "custom");
        exTable.setHost(pxfHost);
        exTable.setPort(pxfPort);
        exTable.setFormatter("pxfwritable_import");
    }

    /**
     * Test simple json file
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"features", "gpdb", "hcfs"})
    public void jsonSimple() throws Exception {

        exTable.setName("jsontest_simple");
        exTable.setProfile(ProtocolUtils.getProtocol().value() + ":json");
        exTable.setPath(hdfsPath + FILENAME_SIMPLE + SUFFIX_JSON);
        exTable.setFields(new String[]{"name text", "age int"});

        gpdb.createTableAndVerify(exTable);

        // Verify results
        runTincTest("pxf.features.hdfs.readable.json.simple.runTest");
    }

    /**
     * Test all JSON plugin supported types. TODO: no support for bytea type
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"features", "gpdb", "hcfs"})
    public void jsonSupportedPrimitives() throws Exception {

        exTable.setName("jsontest_supported_primitive_types");
        exTable.setProfile(ProtocolUtils.getProtocol().value() + ":json");
        exTable.setPath(hdfsPath + FILENAME_TYPES + SUFFIX_JSON);
        exTable.setFields(supportedPrimitiveFields);

        gpdb.createTableAndVerify(exTable);

        // Verify results
        runTincTest("pxf.features.hdfs.readable.json.supported_primitive_types.runTest");
    }

    /**
     * Test all JSON plugin supported types. TODO: no support for bytea type
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"features", "gpdb", "hcfs"})
    public void jsonSupportedPrimitivesWithCsvWireFormat() throws Exception {

        exTable = new ReadableExternalTable("jsontest_supported_primitive_types", supportedPrimitiveFields, hdfsPath + FILENAME_TYPES + SUFFIX_JSON, "CSV");
        exTable.setHost(pxfHost);
        exTable.setPort(pxfPort);
        exTable.setProfile(ProtocolUtils.getProtocol().value() + ":json");

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
    @Test(groups = {"features", "gpdb", "hcfs"})
    public void jsonPrettyPrint() throws Exception {

        exTable.setName("jsontest_pretty_print");
        exTable.setProfile(ProtocolUtils.getProtocol().value() + ":json");
        exTable.setPath(hdfsPath + FILENAME_PRETTY_PRINT + SUFFIX_JSON);
        exTable.setFields(tweetsFields);
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
    @Test(groups = {"features", "gpdb", "hcfs"})
    public void missingIdentifier() throws Exception {

        exTable.setName("jsontest_missing_identifier");
        exTable.setProfile(ProtocolUtils.getProtocol().value() + ":json");
        exTable.setPath(hdfsPath + FILENAME_PRETTY_PRINT_W_DELETE + SUFFIX_JSON);
        exTable.setFields(tweetsFields);
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
    @Test(groups = {"features", "gpdb", "hcfs"})
    public void exceedsMaxSize() throws Exception {

        exTable.setName("jsontest_max_size");
        exTable.setProfile(ProtocolUtils.getProtocol().value() + ":json");
        exTable.setPath(hdfsPath + FILENAME_PRETTY_PRINT + SUFFIX_JSON);
        exTable.setFields(tweetsFields);
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
    @Test(groups = {"features", "gpdb", "hcfs"})
    public void malformedRecord() throws Exception {

        exTable.setName("jsontest_malformed_record");
        exTable.setProfile(ProtocolUtils.getProtocol().value() + ":json");
        exTable.setPath(hdfsPath + FILENAME_BROKEN + SUFFIX_JSON);
        exTable.setFields(tweetsFields);
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
    @Test(groups = {"features", "gpdb"})
    public void malformedRecordWithCsvWireFormat() throws Exception {

        exTable = new ReadableExternalTable("jsontest_malformed_record", tweetsFields, hdfsPath + FILENAME_BROKEN + SUFFIX_JSON, "CSV");
        exTable.setHost(pxfHost);
        exTable.setPort(pxfPort);
        exTable.setProfile(ProtocolUtils.getProtocol().value() + ":json");
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
    @Test(groups = {"features", "gpdb", "hcfs"})
    public void malformedRecordWithRejectLimit() throws Exception {

        exTable.setName("jsontest_malformed_record_with_reject_limit");
        exTable.setProfile(ProtocolUtils.getProtocol().value() + ":json");
        exTable.setPath(hdfsPath + FILENAME_BROKEN + SUFFIX_JSON);
        exTable.setFields(tweetsFields);
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
    @Test(groups = {"features", "gpdb", "hcfs"})
    public void malformedRecordWithRejectLimitWithCsvWireFormat() throws Exception {

        exTable = new ReadableExternalTable("jsontest_malformed_record_with_reject_limit", tweetsFields, hdfsPath + FILENAME_BROKEN + SUFFIX_JSON, "CSV");
        exTable.setHost(pxfHost);
        exTable.setPort(pxfPort);
        exTable.setProfile(ProtocolUtils.getProtocol().value() + ":json");
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
    @Test(groups = {"features", "gpdb", "hcfs"})
    public void mismatchedTypes() throws Exception {

        exTable.setName("jsontest_mismatched_types");
        exTable.setProfile(ProtocolUtils.getProtocol().value() + ":json");
        exTable.setPath(hdfsPath + FILENAME_MISMATCHED_TYPES + SUFFIX_JSON);
        exTable.setFields(supportedPrimitiveFields);

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
    @Test(groups = {"features", "gpdb", "hcfs"})
    public void mismatchedTypesWithRejectLimit() throws Exception {

        exTable.setName("jsontest_mismatched_types_with_reject_limit");
        exTable.setProfile(ProtocolUtils.getProtocol().value() + ":json");
        exTable.setPath(hdfsPath + FILENAME_MISMATCHED_TYPES + SUFFIX_JSON);
        exTable.setFields(supportedPrimitiveFields);
        exTable.setSegmentRejectLimit(7);
        exTable.setErrorTable("true");

        gpdb.createTableAndVerify(exTable);

        // Verify results
        runTincTest("pxf.features.hdfs.readable.json.mismatched_types_with_reject_limit.runTest");
    }
}
