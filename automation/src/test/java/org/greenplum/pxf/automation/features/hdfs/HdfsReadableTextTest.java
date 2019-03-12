package org.greenplum.pxf.automation.features.hdfs;

import java.io.File;
import java.nio.charset.StandardCharsets;

import org.greenplum.pxf.automation.components.cluster.PhdCluster;
import org.greenplum.pxf.automation.structures.tables.basic.Table;
import org.greenplum.pxf.automation.structures.tables.pxf.ErrorTable;
import org.greenplum.pxf.automation.structures.tables.pxf.ReadableExternalTable;
import org.greenplum.pxf.automation.utils.fileformats.FileFormatsUtils;
import org.greenplum.pxf.automation.utils.jsystem.report.ReportUtils;
import org.greenplum.pxf.automation.utils.system.ProtocolUtils;
import org.greenplum.pxf.automation.utils.tables.ComparisonUtils;
import org.junit.Assert;
import org.testng.annotations.Test;

import org.greenplum.pxf.automation.enums.EnumPxfDefaultProfiles;
import org.greenplum.pxf.automation.utils.csv.CsvUtils;
import org.greenplum.pxf.automation.datapreparer.CustomTextPreparer;
import org.greenplum.pxf.automation.datapreparer.QuotedLineTextPreparer;
import org.greenplum.pxf.automation.features.BaseFeature;

/**
 * Collection of Test cases for PXF ability to read Text/CSV files from HDFS.
 * Relates to cases located in "PXF Test Suite" in testrail:
 * https://testrail.greenplum.com/index.php?/suites/view/1099 in
 * "HDFS Readable - Text/CSV" section.
 */
public class HdfsReadableTextTest extends BaseFeature {
    // holds data for file generation
    Table dataTable = null;
    // path for storing data on HDFS
    String hdfsFilePath = "";

    private String resourcePath;

    private final String SUFFIX_CLASS = ".class";

    String testPackageLocation = "/org/greenplum/pxf/automation/testplugin/";
    String testPackage = "org.greenplum.pxf.automation.testplugin.";

    String throwOn10000Accessor = "ThrowOn10000Accessor";

    /**
     * Prepare all components and all data flow (Hdfs to GPDB)
     */
    @Override
    public void beforeClass() throws Exception {
        // location of test plugin files
        resourcePath = "target/classes" + testPackageLocation;

        String newPath = "/tmp/publicstage/pxf";
        // copy additional plugins classes to cluster nodes, used for filter
        // pushdown cases
        cluster.copyFileToNodes(new File(resourcePath + throwOn10000Accessor
                + SUFFIX_CLASS).getAbsolutePath(), newPath
                + testPackageLocation, true, false);

        // add new path to classpath file and restart PXF service
        cluster.addPathToPxfClassPath(newPath);
        cluster.restart(PhdCluster.EnumClusterServices.pxf);
    }

    /**
     * Before every method determine default hdfs data Path, default data, and
     * default external table structure. Each case change it according to it
     * needs.
     *
     * @throws Exception
     */
    @Override
    protected void beforeMethod() throws Exception {
        super.beforeMethod();
        // path for storing data on HDFS
        hdfsFilePath = hdfs.getWorkingDirectory() + "/data";
        // prepare data in table
        dataTable = new Table("dataTable", null);
        FileFormatsUtils.prepareData(new CustomTextPreparer(), 100, dataTable);
        // default definition of external table
        exTable = new ReadableExternalTable("pxf_hdfs_small_data",
                new String[]{
                        "s1 text",
                        "s2 text",
                        "s3 text",
                        "d1 timestamp",
                        "n1 int",
                        "n2 int",
                        "n3 int",
                        "n4 int",
                        "n5 int",
                        "n6 int",
                        "n7 int",
                        "s11 text",
                        "s12 text",
                        "s13 text",
                        "d11 timestamp",
                        "n11 int",
                        "n12 int",
                        "n13 int",
                        "n14 int",
                        "n15 int",
                        "n16 int",
                        "n17 int"}, hdfsFilePath, "TEXT");

        exTable.setHost(pxfHost);
        exTable.setPort(pxfPort);
    }

    /**
     * Read delimited text file from HDFS using explicit plugins and TEXT
     * format.
     *
     * @throws Exception
     */
    @Test(groups = {"features", "sanity", "gpdb"})
    public void readDelimitedTextUsingTextFormat() throws Exception {
        // set plugins and delimiter
        exTable.setFragmenter("org.greenplum.pxf.plugins.hdfs.HdfsDataFragmenter");
        exTable.setAccessor("org.greenplum.pxf.plugins.hdfs.LineBreakAccessor");
        exTable.setResolver("org.greenplum.pxf.plugins.hdfs.StringPassResolver");
        exTable.setDelimiter(",");
        // create external table
        gpdb.createTableAndVerify(exTable);
        // write data to HDFS
        hdfs.writeTableToFile(hdfsFilePath, dataTable, ",");
        // verify results
        runTincTest("pxf.features.hdfs.readable.text.small_data.runTest");
    }

    /**
     * Read quoted CSV file from HDFS using explicit plugins and CSV format.
     *
     * @throws Exception
     */
    @Test(groups = {"features", "gpdb"})
    public void readCsvUsingCsvFormat() throws Exception {
        // set plugins and format
        exTable.setFragmenter("org.greenplum.pxf.plugins.hdfs.HdfsDataFragmenter");
        exTable.setAccessor("org.greenplum.pxf.plugins.hdfs.LineBreakAccessor");
        exTable.setResolver("org.greenplum.pxf.plugins.hdfs.StringPassResolver");
        exTable.setFormat("CSV");
        // create external table
        gpdb.createTableAndVerify(exTable);
        // create local CSV file
        String tempLocalDataPath = dataTempFolder + "/data.csv";
        CsvUtils.writeTableToCsvFile(dataTable, tempLocalDataPath);
        // copy local CSV to HDFS
        hdfs.copyFromLocal(tempLocalDataPath, hdfsFilePath);
        // verify results
        runTincTest("pxf.features.hdfs.readable.text.small_data.runTest");
    }

    /**
     * Read quoted CSV file from HDFS using HdfsTextSimple profile and CSV
     * format.
     *
     * @throws Exception
     */
    @Test(groups = {"features", "gpdb", "hcfs"})
    public void readCsvUsingProfile() throws Exception {
        // set profile and format
        exTable.setProfile(ProtocolUtils.getProtocol().value() + ":text");
        exTable.setFormat("CSV");
        // create external table
        gpdb.createTableAndVerify(exTable);
        // create local CSV file
        String tempLocalDataPath = dataTempFolder + "/data.csv";
        CsvUtils.writeTableToCsvFile(dataTable, tempLocalDataPath);
        // copy local CSV to HDFS
        hdfs.copyFromLocal(tempLocalDataPath, hdfsFilePath);
        // verify results
        runTincTest("pxf.features.hdfs.readable.text.small_data.runTest");
    }

    /**
     * Create multi lined CSV data, use
     * QuotedLineBreakAccessor to read.
     *
     * @throws Exception
     */
    @Test(groups = {"features", "gpdb"})
    public void readMultiBlockedMultiLinedCsv() throws Exception {
        // prepare local CSV file
        dataTable = new Table("dataTable", null);
        String tempLocalDataPath = dataTempFolder + "/data.csv";
        // prepare template data of 10 lines
        FileFormatsUtils.prepareData(new QuotedLineTextPreparer(), 10,
                dataTable);
        // multiple it to file
        FileFormatsUtils.prepareDataFile(dataTable, 32, tempLocalDataPath);
        // copy local file to HDFS
        hdfs.copyFromLocal(tempLocalDataPath, hdfsFilePath);
        // define and create external table
        exTable = new ReadableExternalTable("pxf_multi_csv", new String[]{
                "num1 int",
                "word text",
                "num2 int"}, hdfsFilePath, "CSV");
        exTable.setFragmenter("org.greenplum.pxf.plugins.hdfs.HdfsDataFragmenter");
        exTable.setAccessor("org.greenplum.pxf.plugins.hdfs.QuotedLineBreakAccessor");
        exTable.setResolver("org.greenplum.pxf.plugins.hdfs.StringPassResolver");
        exTable.setHost(pxfHost);
        exTable.setPort(pxfPort);
        gpdb.createTableAndVerify(exTable);
        // Verify results
        runTincTest("pxf.features.hdfs.readable.text.multiblocked_csv_data.runTest");
    }

    /**
     * Create multi lined CSV data, use HdfsTextMulti pxf
     * profile to read.
     *
     * @throws Exception
     */
    @Test(groups = {"features", "gpdb", "hcfs"})
    public void readMultiBlockedMultiLinedCsvUsingProfile() throws Exception {
        // prepare local CSV file
        dataTable = new Table("dataTable", null);
        String tempLocalDataPath = dataTempFolder + "/data.csv";
        // prepare template data of 10 lines
        FileFormatsUtils.prepareData(new QuotedLineTextPreparer(), 10,
                dataTable);
        // multiple it to file
        FileFormatsUtils.prepareDataFile(dataTable, 32, tempLocalDataPath);
        // copy local file to HDFS
        hdfs.copyFromLocal(tempLocalDataPath, hdfsFilePath);
        // define and create external table
        exTable = new ReadableExternalTable("pxf_multi_csv", new String[]{
                "num1 int",
                "word text",
                "num2 int"}, hdfsFilePath, "CSV");
        exTable.setProfile(ProtocolUtils.getProtocol().value() + ":text:multi");
        exTable.setHost(pxfHost);
        exTable.setPort(pxfPort);
        gpdb.createTableAndVerify(exTable);
        // Verify results
        runTincTest("pxf.features.hdfs.readable.text.multiblocked_csv_data.runTest");
    }

    /**
     * Create 2 files located under the same HDFS directory and read it using
     * wildcard
     *
     * @throws Exception
     */
    @Test(groups = {"features", "gpdb", "hcfs"})
    public void wildcardLocation() throws Exception {
        // define HDFS data directory
        String wildcardHdfsPath = hdfs.getWorkingDirectory() + "/wild/";
        // write 2 files to same HDFS directory
        hdfs.writeTableToFile(wildcardHdfsPath + "/data1.txt", dataTable, ",");
        hdfs.writeTableToFile(wildcardHdfsPath + "/data2.txt", dataTable, ",");
        // define and create external table
        exTable.setProfile(ProtocolUtils.getProtocol().value() + ":text");
        exTable.setDelimiter(",");
        exTable.setPath(wildcardHdfsPath + "/*.txt");
        gpdb.createTableAndVerify(exTable);
        // verify results
        runTincTest("pxf.features.hdfs.readable.text.wildcard.runTest");

        // test ? wildcard
        exTable.setPath(wildcardHdfsPath + "/data?.txt");
        gpdb.createTableAndVerify(exTable);
        // verify results
        runTincTest("pxf.features.hdfs.readable.text.wildcard.runTest");
    }

    /**
     * Create "recursive" directory structure in HDFS, copy files to different
     * directories, read all data by specifying parent directory as the
     * location.
     *
     * @throws Exception
     */
    @Test(groups = {"features", "gpdb", "hcfs"})
    public void recursiveHdfsDirectories() throws Exception {
        // define base HDFS directory
        String baseDirectory = hdfs.getWorkingDirectory() + "/base/";
        // create "recursive" structure
        hdfs.createDirectory(baseDirectory + "A/B/A/B/C/A");
        // create data in different directories
        hdfs.writeTableToFile(baseDirectory + "A/data_file1", dataTable, ",");
        hdfs.writeTableToFile(baseDirectory + "A/B/A/data_file2", dataTable,
                ",");
        hdfs.writeTableToFile(baseDirectory + "A/B/A/B/C/data_file3",
                dataTable, ",");
        hdfs.writeTableToFile(baseDirectory + "A/B/A/B/C/A/data_file4",
                dataTable, ",");
        // define and create external table
        exTable.setProfile(ProtocolUtils.getProtocol().value() + ":text");
        exTable.setDelimiter(",");
        exTable.setPath(baseDirectory);
        gpdb.createTableAndVerify(exTable);
        // verify results
        runTincTest("pxf.features.hdfs.readable.text.recursive.runTest");
    }

    /**
     * Create empty file in HDFS and read it.
     *
     * @throws Exception
     */
    @Test(groups = {"features", "gpdb", "hcfs"})
    public void emptyTextFile() throws Exception {
        // define and create external table
        exTable.setProfile(ProtocolUtils.getProtocol().value() + ":text");
        exTable.setDelimiter(",");
        gpdb.createTableAndVerify(exTable);
        // write empty data to HDFS
        hdfs.writeTableToFile(hdfsFilePath, new Table("emptyTable", null), ",");
        // verify results
        runTincTest("pxf.features.hdfs.readable.text.empty_file.runTest");
    }

    /**
     * Create HDFS text file using "ISO_8859_1" encoding, define external table
     * with same encoding (called LATIN1 in gpdb) and read data.
     *
     * @throws Exception
     */
    @Test(groups = {"features"})//, "gpdb", "hcfs" })
    public void differentEncoding() throws Exception {
        // define and create external table
        exTable.setFields(new String[]{"num1 int", "word text"});
        exTable.setFragmenter("org.greenplum.pxf.plugins.hdfs.HdfsDataFragmenter");
        exTable.setAccessor("org.greenplum.pxf.plugins.hdfs.LineBreakAccessor");
        exTable.setResolver("org.greenplum.pxf.plugins.hdfs.StringPassResolver");
        exTable.setDelimiter(",");
        exTable.setEncoding("LATIN1");
        gpdb.createTableAndVerify(exTable);
        // prepare data and write to HDFS
        dataTable = new Table("data", null);
        dataTable.addRow(new String[]{"4", "tá sé seo le tástáil dea-"});
        dataTable.addRow(new String[]{"3", "règles d'automation"});
        dataTable.addRow(new String[]{
                "5",
                "minden amire szüksége van a szeretet"});
        hdfs.writeTableToFile(hdfsFilePath, dataTable, ",",
                StandardCharsets.ISO_8859_1);
        // verify results
        runTincTest("pxf.features.hdfs.readable.text.encoding.runTest");
    }

    /**
     * perform analyze over external table, first with
     * pxf_enable_stat_collection=false to see default values, and than with
     * pxf_enable_stat_collection=true to see estimates results written to
     * pg_class table.
     *
     * @throws Exception
     */
    @Test(groups = "features")
    public void analyze() throws Exception {
        // write data to HDFS
        hdfs.writeTableToFile(hdfsFilePath, dataTable, ",");
        // define and create external table
        exTable.setProfile(EnumPxfDefaultProfiles.HdfsTextSimple.toString());
        exTable.setDelimiter(",");
        gpdb.createTableAndVerify(exTable);
        // set pxf_enable_stat_collection=false
        gpdb.runQuery("SET pxf_enable_stat_collection = false");
        // analyze table with expected warning about GUC
        gpdb.analyze(exTable, true);
        // query results from pg_class table
        Table analyzeResults = new Table("analyzeResults", null);
        gpdb.queryResults(
                analyzeResults,
                "SELECT reltuples FROM pg_class WHERE relname='"
                        + exTable.getName() + "'");
        // prepare expected default results and verify
        Table expectedAnalyzeResults = new Table("expectedAnalyzeResults", null);
        expectedAnalyzeResults.addRow(new String[]{"1000000"});
        ComparisonUtils.compareTables(analyzeResults, expectedAnalyzeResults,
                null);
        // set pxf_enable_stat_collection=true
        gpdb.runQuery("SET pxf_enable_stat_collection = true");
        // analyze
        gpdb.analyze(exTable, false);
        // query results from pg_class table
        analyzeResults.initDataStructures();
        gpdb.queryResults(
                analyzeResults,
                "SELECT reltuples FROM pg_class WHERE relname='"
                        + exTable.getName() + "'");
        // prepare expected default results and verify
        expectedAnalyzeResults.initDataStructures();
        expectedAnalyzeResults.addRow(new String[]{"100"});
        ComparisonUtils.compareTables(analyzeResults, expectedAnalyzeResults,
                null);
    }

    /**
     * Test error table option:
     * {@code LOG ERRORS INTO <err_table> SEGMENT REJECT LIMIT
     * <number_of_errors>}. When reading data from external table, errors in
     * formatting are stored in an error table until the limit of allowed errors
     * is reached.
     * <p>
     * The test covers a case when the number of errors is lower than limit and
     * a case when the errors breach the limit. It also tests the cleanup of
     * segwork and metadata information in the filename parameter (the pxf URI)
     * in the error table (GPSQL-1708).
     *
     * @throws Exception
     */
    @Test(groups = {"features", "gpdb", "hcfs"})
    public void errorTable() throws Exception {

        String[] fields = new String[]{"num int", "words text"};

        Table dataTable = new Table("dataTable", null);

        dataTable.addRow(new String[]{"All Together Now", "The Beatles"});
        dataTable.addRow(new String[]{"1", "one"});
        dataTable.addRow(new String[]{"2", "two"});
        dataTable.addRow(new String[]{"3", "three"});
        dataTable.addRow(new String[]{"4", "four"});
        dataTable.addRow(new String[]{"can", "I"});
        dataTable.addRow(new String[]{"have", "a"});
        dataTable.addRow(new String[]{"little", "more"});
        dataTable.addRow(new String[]{"5", "five"});
        dataTable.addRow(new String[]{"6", "six"});
        dataTable.addRow(new String[]{"7", "seven"});
        dataTable.addRow(new String[]{"8", "eight"});
        dataTable.addRow(new String[]{"9", "nine"});
        dataTable.addRow(new String[]{"10", "ten - I love you!"});

        hdfs.writeTableToFile(hdfsFilePath, dataTable, ",");

        ErrorTable errorTable = new ErrorTable("err_table");
        gpdb.runQueryWithExpectedWarning(
                errorTable.constructDropStmt(true),
                "(drop cascades to external table err_table_test|" + "table \""
                        + errorTable.getName() + "\" does not exist, skipping)",
                true, true);

        exTable.setName("err_table_test");
        exTable.setFields(fields);
        exTable.setProfile(ProtocolUtils.getProtocol().value() + ":text");
        exTable.setDelimiter(",");
        exTable.setErrorTable(errorTable.getName());
        exTable.setSegmentRejectLimit(10);

        ReportUtils.startLevel(null, getClass(),
                "Drop and create table, expect error table notice");
        gpdb.dropTable(exTable, false);
        gpdb.runQueryWithExpectedWarning(exTable.constructCreateStmt(),
                "Error table \"" + errorTable.getName() + "\" does not exist. "
                        + "Auto generating an error table with the same name",
                true, true);

        Assert.assertTrue(gpdb.checkTableExists(exTable));
        ReportUtils.stopLevel(null);

        runTincTest("pxf.features.hdfs.readable.text.error_table_gpdb.runTest");
        ReportUtils.startLevel(null, getClass(), "table with too many errors");
        exTable.setSegmentRejectLimit(3);
        gpdb.createTableAndVerify(exTable);

        runTincTest("pxf.features.hdfs.readable.text.errors.error_table_breached.runTest");

        ReportUtils.stopLevel(null);
    }

    /**
     * Verify use of LIMIT
     * <p>
     * TODO The test doesn't verify whether Gpdb got all tuples or just the
     * LIMIT. We should test LIMIT cancels the query once it gets LIMIT tuples.
     */
    @Test(groups = {"features", "gpdb", "hcfs"})
    public void limit() throws Exception {

        Table dataTable = new Table("dataTable", null);
        Table limitTable = new Table("limitTable", null);
        String[] line = new String[]{"Same shirt", "different day"};
        for (int i = 0; i < 1000; i++) {
            limitTable.addRow(line);
        }
        for (int i = 0; i < 10; i++) {
            dataTable.appendRows(limitTable);
        }

        hdfs.writeTableToFile(hdfsFilePath, dataTable, ",");

        String[] fields = new String[]{"s1 text", "s2 text"};
        exTable.setFields(fields);
        exTable.setName("text_limit");
        exTable.setProfile(ProtocolUtils.getProtocol().value() + ":text");
        exTable.setDelimiter(",");

        gpdb.createTableAndVerify(exTable);

        runTincTest("pxf.features.hdfs.readable.text.limit.runTest");
    }

    /**
     * Verify query fails when conversion to int (for example) fails (without an
     * error table) and a proper error message is printed
     */
    @Test(groups = {"features", "gpdb", "hcfs"})
    public void negativeBadTextData() throws Exception {

        Table dataTable = new Table("dataTable", null);
        for (int i = 0; i < 50; i++) {
            dataTable.addRow(new String[]{"" + i, Integer.toHexString(i)});
        }
        dataTable.addRow(new String[]{"joker", "ha"});

        hdfs.writeTableToFile(hdfsFilePath, dataTable, ",");

        String[] fields = new String[]{"num int", "string text"};
        exTable.setName("bad_text");
        exTable.setFields(fields);
        exTable.setProfile(ProtocolUtils.getProtocol().value() + ":text");
        exTable.setDelimiter(",");

        gpdb.createTableAndVerify(exTable);

        runTincTest("pxf.features.hdfs.readable.text.errors.wrong_type.runTest");
    }

    /**
     * Verify we detect errors in the read bridge if they occur after the first
     * HTTP packet was sent. TcServer (2.9.7) detects error and doesn't add the
     * end chunk. PXF on the client side should detect and throw an error.
     * <p>
     * The test is done by running a query over a file with >10000 records,
     * using ThrowOn10000Accessor which throw an exception on the 10000th
     * record.
     * <p>
     * see GPSQL-2272
     */
    @Test(groups = {"features", "gpdb"})
    public void errorInTheMiddleOfStream() throws Exception {

        Table dataTable = new Table("dataTable", null);
        for (int i = 0; i < 10005; i++) {
            dataTable.addRow(new String[]{"" + i, Integer.toHexString(i)});
        }

        hdfs.writeTableToFile(hdfsFilePath, dataTable, ",");

        String[] fields = new String[]{"num int", "string text"};

        exTable.setName("error_on_10000");
        exTable.setFields(fields);
        exTable.setFragmenter("org.greenplum.pxf.plugins.hdfs.HdfsDataFragmenter");
        exTable.setAccessor(testPackage + throwOn10000Accessor);
        exTable.setResolver("org.greenplum.pxf.plugins.hdfs.StringPassResolver");
        exTable.setDelimiter(",");

        gpdb.createTableAndVerify(exTable);

        runTincTest("pxf.features.hdfs.readable.text.errors.middle_of_stream.runTest");
    }
}
