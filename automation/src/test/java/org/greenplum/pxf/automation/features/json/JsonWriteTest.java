package org.greenplum.pxf.automation.features.json;

import org.greenplum.pxf.automation.features.BaseWritableFeature;
import org.greenplum.pxf.automation.structures.tables.basic.Table;
import org.greenplum.pxf.automation.structures.tables.utils.TableFactory;
import org.greenplum.pxf.automation.utils.system.ProtocolEnum;
import org.greenplum.pxf.automation.utils.system.ProtocolUtils;
import org.testng.annotations.Test;

import java.io.File;
import java.util.StringJoiner;
import java.util.function.Consumer;

import static java.lang.Thread.sleep;
import static org.testng.Assert.assertTrue;

/**
 * Test class for testing writing use cases of Json profiles.
 */
public class JsonWriteTest extends BaseWritableFeature {

    public static final String[] PRIMITIVE_TYPES_FIELDS = {
            "id          integer",                     // DataType.INTEGER
            "name        text",                        // DataType.TEXT
            "sml         smallint",                    // DataType.SMALLINT
            "integ       integer",                     // DataType.INTEGER
            "bg          bigint",                      // DataType.BIGINT
            "r           real",                        // DataType.REAL
            "dp          double precision",            // DataType.FLOAT8
            "dec         numeric",                     // DataType.NUMERIC
            "bool        boolean",                     // DataType.BOOLEAN
            "cdate       date",                        // DataType.DATE
            "ctime       time",                        // DataType.TIME
            "tm          timestamp without time zone", // DataType.TIMESTAMP
            "tmz         timestamp with time zone",    // DataType.TIMESTAMP_WITH_TIME_ZONE
            "c1          character(3)",                // DataType.BPCHAR
            "vc1         character varying(5)",        // DataType.VARCHAR
            "bin         bytea"                        // DataType.BYTEA
    };

    public static final String[] ESCAPING_PRIMITIVE_TYPES_FIELDS = {
            "id             integer",                     // DataType.INTEGER
            "\"col space\"  text",                        // DataType.TEXT
            "text_escape    text",                        // DataType.TEXT
            "char_escape    character(14)",               // DataType.BPCHAR
            "varchar_escape character varying(14)"        // DataType.VARCHAR
    };

    private static final String PRIMITIVE_TYPES = "primitive_types";

    private static final Consumer<String> JSON_EXTENSION_ASSERTER = (filename) -> {
        assertTrue(filename.endsWith(".json"), "file " + filename + " does not end with .json");
    };

    private static final Consumer<String> JSON_GZ_EXTENSION_ASSERTER = (filename) -> {
        assertTrue(filename.endsWith(".json.gz"), "file " + filename + " does not end with .json.gz");
    };

    private String resourcePath;

    // table that stores seed data for insertion into writable external tables
    private Table gpdbPrimitiveTypesTable;

    @Override
    public void beforeClass() throws Exception {
        // path for storing data on HDFS that will be auto-cleanup after each test method
        hdfsWritePath = hdfs.getWorkingDirectory() + "/json/";
        resourcePath = localDataResourcesFolder + "/json/";

        // seed the source data in GPDB internal table
        gpdbPrimitiveTypesTable = new Table("gpdb_" + PRIMITIVE_TYPES, PRIMITIVE_TYPES_FIELDS);
        gpdbPrimitiveTypesTable.setDistributionFields(new String[]{"id"});
        gpdb.createTableAndVerify(gpdbPrimitiveTypesTable);
        gpdb.copyFromFile(gpdbPrimitiveTypesTable, new File(resourcePath + PRIMITIVE_TYPES + ".csv"), ",", true);
    }

    @Test(groups = {"gpdb", "security", "hcfs"})
    public void writePrimitiveTypesObject() throws Exception {
        runScenario(PRIMITIVE_TYPES + "_object", PRIMITIVE_TYPES_FIELDS, gpdbPrimitiveTypesTable,
                null, new String[]{"IDENTIFIER=id"}, JSON_EXTENSION_ASSERTER);
    }

    @Test(groups = {"gpdb", "security", "hcfs"})
    public void writePrimitiveTypesObjectWithCustomKey() throws Exception {
        runScenario(PRIMITIVE_TYPES + "_object_key", PRIMITIVE_TYPES_FIELDS, gpdbPrimitiveTypesTable,
                new String[]{"KEY=tuples"}, new String[]{"IDENTIFIER=id"}, JSON_EXTENSION_ASSERTER);
    }

    @Test(groups = {"gpdb", "security", "hcfs"})
    public void writePrimitiveTypesRows() throws Exception {
        runScenario(PRIMITIVE_TYPES + "_rows", PRIMITIVE_TYPES_FIELDS, gpdbPrimitiveTypesTable,
                new String[]{"LAYOUT=rows"}, null, JSON_EXTENSION_ASSERTER);
    }

    @Test(groups = {"gpdb", "security", "hcfs"})
    public void writePrimitiveTypesCompressed() throws Exception {
        runScenario(PRIMITIVE_TYPES + "_compressed", PRIMITIVE_TYPES_FIELDS, gpdbPrimitiveTypesTable,
                new String[]{"COMPRESSION_CODEC=gzip"}, new String[]{"IDENTIFIER=id"}, JSON_GZ_EXTENSION_ASSERTER);
    }

    @Test(groups = {"gpdb", "security", "hcfs"})
    public void writePrimitiveTypesEscaping() throws Exception {
        String escapingData = new StringJoiner(",")
                .add("(1, 'col space 1', 'text', 'char', 'varchar')")
                .add("(2, 'col space 2', 's\"b\\{},:[]', 's\"b\\{},:[]', 's\"b\\{},:[]')")                         // single sequence
                .add("(3, 'col space 3', 'd\"\"b\\\\{},:[]', 'd\"\"b\\\\{},:[]', 'd\"\"b\\\\{},:[]')")             // double sequence
                .add("(4, 'col space 4', 't\"\"\"b\\\\\\{},:[]', 't\"\"\"b\\\\\\{},:[]', 't\"\"\"b\\\\\\{},:[]')") // triple sequence
                .toString();
        runScenario(PRIMITIVE_TYPES + "_escaping", ESCAPING_PRIMITIVE_TYPES_FIELDS, escapingData,
                null, new String[]{"IDENTIFIER=id"}, JSON_EXTENSION_ASSERTER);
    }

    @Test(groups = {"gpdb", "security", "hcfs"})
    public void errorInvalidEncoding() throws Exception {
        // 1. prepare writable external table ready to receive data for writing from internal table
        writableExTable = TableFactory.getPxfHcfsWritableTable(
                "pxf_invalid_encoding_json_write", PRIMITIVE_TYPES_FIELDS, hdfsWritePath + "invalid_encoding", hdfs.getBasePath(), "json");
        writableExTable.setEncoding("LATIN1"); // set a non-UTF8 encoding for the table
        createTable(writableExTable);

        // 2. run the Tinc test that inserts data as CTAS, which should fail and verifies the error message
        // for external table with pxfwritable_export formatter (default setup for this test) the actual error
        // will be produced by the formatter, not PXF, but for FDW or CSV wire format PXF error should show
        runTincTest("pxf.features.hdfs.writable.json.invalid_encoding.runTest");
    }

    @Test(groups = {"gpdb", "security", "hcfs"})
    public void errorInvalidLayout() throws Exception {
        // 1. prepare writable external table ready to receive data for writing from internal table
        writableExTable = TableFactory.getPxfHcfsWritableTable(
                "pxf_invalid_layout_json_write", PRIMITIVE_TYPES_FIELDS, hdfsWritePath + "invalid_layout", hdfs.getBasePath(), "json");
        writableExTable.setUserParameters(new String[]{"LAYOUT=foo"});
        createTable(writableExTable);

        // 2. run the Tinc test that inserts data as CTAS, which should fail and verifies the error message
        runTincTest("pxf.features.hdfs.writable.json.invalid_layout.runTest");
    }

    /**
     * Runs a test scenario with a given name. The test creates a writable external table, inserts data into it, then
     * creates a readable external table and calls a Tinc test to read the data and compare with the expected output.
     *
     * @param scenario         name of the scenario, gets factored into the names of tables
     * @param fields           schema for the external tables
     * @param source           a string with data or a Table object for existing Greenplum table that serves as the source of data
     * @param writeOptions     options to add to the writable table, null if no additional options are needed
     * @param readOptions      options to add to the readable table, null if no additional options are needed
     * @param filenameAsserter a function that asserts names of produced files
     * @throws Exception if any operation fails
     */
    private void runScenario(String scenario, String[] fields, Object source, String[] writeOptions, String[] readOptions, Consumer<String> filenameAsserter) throws Exception {

        // 1. prepare writable external table ready to receive data for writing from internal table
        writableExTable = TableFactory.getPxfHcfsWritableTable(
                "pxf_" + scenario + "_json_write", fields, hdfsWritePath + scenario, hdfs.getBasePath(), "json");
        if (writeOptions != null) {
            writableExTable.setUserParameters(writeOptions);
        }
        createTable(writableExTable);

        // 2. insert data into the writable table
        if (source instanceof String) {
            // as INSERT INTO .. VALUES (..) SQL query
            gpdb.insertData((String) source, writableExTable);
        } else if (source instanceof Table) {
            // as CTAS from the corresponding internal table
            gpdb.copyData((Table) source, writableExTable);
        } else {
            throw new UnsupportedOperationException("Invalid type of source parameter");
        }

        // 3. wait until the data is written (potentially asynchronously) to the backend system, assert filenames
        if (ProtocolUtils.getProtocol() != ProtocolEnum.HDFS) {
            // for HCFS on Cloud, wait a bit for async write in previous steps to finish
            sleep(10000);
            hdfs.list(hdfsWritePath + scenario).forEach(filenameAsserter);
        }

        // 4. prepare readable external table ready to read data written by the writable external table
        readableExTable = TableFactory.getPxfHcfsReadableTable(
                "pxf_" + scenario + "_json_read", fields, hdfsWritePath + scenario, hdfs.getBasePath(), "json");
        if (readOptions != null) {
            readableExTable.setUserParameters(readOptions);
        }
        createTable(readableExTable);

        // 5. run the Tinc test that queries the data back using the corresponding readable external table
        runTincTest("pxf.features.hdfs.writable.json." + scenario + ".runTest");
    }

}
