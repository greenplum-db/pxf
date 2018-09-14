package org.greenplum.pxf.automation.features.json;

import org.greenplum.pxf.automation.structures.tables.pxf.ReadableExternalTable;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.greenplum.pxf.automation.features.BaseFeature;

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

    private String[] tweetsFields = new String[] {
            "created_at text",
            "id bigint",
            "text text",
            "\"user.screen_name\" text",
            "\"entities.hashtags[0]\" text",
            "\"coordinates.coordinates[0]\" float8",
            "\"coordinates.coordinates[1]\" float8", };

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
    @Test(groups = { "features", "gpdb" })
    public void jsonSimple() throws Exception {

        exTable.setName("jsontest_simple");
        exTable.setProfile("Json");
        exTable.setPath(hdfsPath + FILENAME_SIMPLE + SUFFIX_JSON);
        exTable.setFields(new String[] { "name text", "age int" });

        gpdb.createTableAndVerify(exTable);

        // Verify results
        runTincTest("pxf.features.hdfs.readable.json.simple.runTest");
    }

    /**
     * Test all JSON plugin supported types. TODO: no support for bytea type
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = { "features", "gpdb" })
    public void jsonSupportedPrimitives() throws Exception {

        exTable.setName("jsontest_supported_primitive_types");
        exTable.setProfile("Json");
        exTable.setPath(hdfsPath + FILENAME_TYPES + SUFFIX_JSON);
        exTable.setFields(new String[] {
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
        // "type_bytes bytea",
        });

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
    @Test(groups = { "features", "gpdb" })
    public void jsonPrettyPrint() throws Exception {

        exTable.setName("jsontest_pretty_print");
        exTable.setProfile("Json");
        exTable.setPath(hdfsPath + FILENAME_PRETTY_PRINT + SUFFIX_JSON);
        exTable.setFields(tweetsFields);
        exTable.setUserParameters(new String[] { "IDENTIFIER=created_at" });

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
    @Test(groups = { "features", "gpdb" })
    public void missingIdentifier() throws Exception {

        exTable.setName("jsontest_missing_identifier");
        exTable.setProfile("Json");
        exTable.setPath(hdfsPath + FILENAME_PRETTY_PRINT_W_DELETE + SUFFIX_JSON);
        exTable.setFields(tweetsFields);
        exTable.setUserParameters(new String[] { "IDENTIFIER=created_at" });

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
    @Test(groups = { "features", "gpdb" })
    public void exceedsMaxSize() throws Exception {

        exTable.setName("jsontest_max_size");
        exTable.setProfile("Json");
        exTable.setPath(hdfsPath + FILENAME_PRETTY_PRINT + SUFFIX_JSON);
        exTable.setFields(tweetsFields);
        exTable.setUserParameters(new String[] {
                "IDENTIFIER=created_at",
                "MAXLENGTH=566" });

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
    @Test(groups = { "features", "gpdb" })
    public void malFormatedRecord() throws Exception {

        exTable.setName("jsontest_malformed_record");
        exTable.setProfile("Json");
        exTable.setPath(hdfsPath + FILENAME_BROKEN + SUFFIX_JSON);
        exTable.setFields(tweetsFields);
        exTable.setUserParameters(new String[] { "IDENTIFIER=created_at" });

        gpdb.createTableAndVerify(exTable);

        // Verify results
        runTincTest("pxf.features.hdfs.readable.json.malformed_record.runTest");
    }
}
