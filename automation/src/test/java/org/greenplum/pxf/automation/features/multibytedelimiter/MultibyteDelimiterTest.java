package org.greenplum.pxf.automation.features.multibytedelimiter;

import au.com.bytecode.opencsv.CSVWriter;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.SystemUtils;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.greenplum.pxf.automation.components.cluster.PhdCluster;
import org.greenplum.pxf.automation.datapreparer.CustomTextPreparer;
import org.greenplum.pxf.automation.datapreparer.QuotedLineTextPreparer;
import org.greenplum.pxf.automation.enums.EnumPxfDefaultProfiles;
import org.greenplum.pxf.automation.features.BaseFeature;
import org.greenplum.pxf.automation.structures.tables.basic.Table;
import org.greenplum.pxf.automation.structures.tables.pxf.ErrorTable;
import org.greenplum.pxf.automation.structures.tables.pxf.ReadableExternalTable;
import org.greenplum.pxf.automation.structures.tables.utils.TableFactory;
import org.greenplum.pxf.automation.utils.csv.CsvUtils;
import org.greenplum.pxf.automation.utils.fileformats.FileFormatsUtils;
import org.greenplum.pxf.automation.utils.jsystem.report.ReportUtils;
import org.greenplum.pxf.automation.utils.system.ProtocolEnum;
import org.greenplum.pxf.automation.utils.system.ProtocolUtils;
import org.greenplum.pxf.automation.utils.tables.ComparisonUtils;
import org.junit.Assert;
import org.testng.annotations.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.greenplum.pxf.automation.features.tpch.LineItem.LINEITEM_SCHEMA;

/**
 * Collection of Test cases for PXF ability to read Text/CSV files from HDFS.
 * Relates to cases located in "PXF Test Suite" in testrail:
 * https://testrail.greenplum.com/index.php?/suites/view/1099 in
 * "HDFS Readable - Text/CSV" section.
 */
public class MultibyteDelimiterTest extends BaseFeature {

    private static final String SUFFIX_CLASS = ".class";

    public static final String[] SMALL_DATA_FIELDS = {
            "name text",
            "num integer",
            "dub double precision",
            "longNum bigint",
            "bool boolean"
    };

    ProtocolEnum protocol;

    // holds data for file generation
    Table dataTable = null;
    // path for storing data on HDFS
    String hdfsFilePath = "";

    String testPackageLocation = "/org/greenplum/pxf/automation/testplugin/";
    String testPackage = "org.greenplum.pxf.automation.testplugin.";

    String throwOn10000Accessor = "ThrowOn10000Accessor";

    /**
     * Prepare all components and all data flow (Hdfs to GPDB)
     */
    @Override
    public void beforeClass() throws Exception {
        // location of test plugin files
        String resourcePath = "target/classes" + testPackageLocation;

        String newPath = "/tmp/publicstage/pxf";
        // copy additional plugins classes to cluster nodes, used for filter
        // pushdown cases
        cluster.copyFileToNodes(new File(resourcePath + throwOn10000Accessor
                + SUFFIX_CLASS).getAbsolutePath(), newPath
                + testPackageLocation, true, false);

        // add new path to classpath file and restart PXF service
        cluster.addPathToPxfClassPath(newPath);
        cluster.restart(PhdCluster.EnumClusterServices.pxf);

        protocol = ProtocolUtils.getProtocol();
    }

    /**
     * Before every method determine default hdfs data Path, default data, and
     * default external table structure. Each case change it according to it
     * needs.
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
        exTable = TableFactory.getPxfReadableTextTable("pxf_hdfs_small_data",
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
                        "n17 int"},
                protocol.getExternalTablePath(hdfs.getBasePath(), hdfsFilePath),
                ",");
        exTable.setFormat("CUSTOM");
        exTable.setFormatter("pxfdelimited_import");
    }

    @Test(groups = {"features", "gpdb", "hcfs", "security"})
    public void readCsvTwoByteDelimiter() throws Exception {
        // set profile and format
        exTable.setName("pxf_multibyte_twobyte_data");
        exTable.setProfile(protocol.value() + ":csv");
        exTable.setDelimiter("¤");
        // create external table
        gpdb.createTableAndVerify(exTable);
        // create local CSV file
        String tempLocalDataPath = dataTempFolder + "/data.csv";
        CsvUtils.writeTableToCsvFileOptions(dataTable, tempLocalDataPath, '¤', ' ', CSVWriter.DEFAULT_ESCAPE_CHARACTER, CSVWriter.DEFAULT_LINE_END);
        // copy local CSV to HDFS
        hdfs.copyFromLocal(tempLocalDataPath, hdfsFilePath);

        // create a new table with the SKIP_HEADER_COUNT parameter
        exTable.setName("pxf_multibyte_twobyte_data_with_skip");
        exTable.setUserParameters(new String[]{"SKIP_HEADER_COUNT=10"});
        // create external table
        gpdb.createTableAndVerify(exTable);
        // run the query skipping the first 10 lines of the text
        // verify results
        runTincTest("pxf.features.multibyte_delimiter.two_byte.runTest");
    }

    @Test(groups = {"features", "gpdb", "hcfs", "security"})
    public void readCsvThreeByteDelimiter() throws Exception {
        // set profile and format
        exTable.setName("pxf_multibyte_threebyte_data");
        exTable.setProfile(protocol.value() + ":csv");
        exTable.setDelimiter("停");
        // create external table
        gpdb.createTableAndVerify(exTable);
        // create local CSV file
        String tempLocalDataPath = dataTempFolder + "/data.csv";
        CsvUtils.writeTableToCsvFileOptions(dataTable, tempLocalDataPath, '停', ' ', CSVWriter.DEFAULT_ESCAPE_CHARACTER, CSVWriter.DEFAULT_LINE_END);
        // copy local CSV to HDFS
        hdfs.copyFromLocal(tempLocalDataPath, hdfsFilePath);

        // create a new table with the SKIP_HEADER_COUNT parameter
        exTable.setName("pxf_multibyte_threebyte_data_with_skip");
        exTable.setUserParameters(new String[]{"SKIP_HEADER_COUNT=10"});
        // create external table
        gpdb.createTableAndVerify(exTable);
        // verify results
        // run the query skipping the first 10 lines of the text
        runTincTest("pxf.features.multibyte_delimiter.three_byte.runTest");
    }

    @Test(groups = {"features", "gpdb", "hcfs", "security"})
    public void readCsvFourByteDelimiter() throws Exception {
        // set profile and format
        exTable.setName("pxf_multibyte_fourbyte_data");
        exTable.setProfile(protocol.value() + ":csv");
        exTable.setDelimiter("\uD83D\uDE42");
        // create external table
        gpdb.createTableAndVerify(exTable);
        // create local CSV file
        String tempLocalDataPath = dataTempFolder + "/data.csv";
        CsvUtils.writeTableToCsvFileOptions(dataTable, tempLocalDataPath, '|', ' ', CSVWriter.DEFAULT_ESCAPE_CHARACTER, CSVWriter.DEFAULT_LINE_END);
        // replace pipe delimiter with actual delimiter
        CsvUtils.updateDelim(tempLocalDataPath, '|', "\uD83D\uDE42");
        // copy local CSV to HDFS
        hdfs.copyFromLocal(tempLocalDataPath, hdfsFilePath);

        // create a new table with the SKIP_HEADER_COUNT parameter
        exTable.setName("pxf_multibyte_fourbyte_data_with_skip");
        exTable.setUserParameters(new String[]{"SKIP_HEADER_COUNT=10"});
        // create external table
        gpdb.createTableAndVerify(exTable);
        // verify results
        // run the query skipping the first 10 lines of the text
        runTincTest("pxf.features.multibyte_delimiter.four_byte.runTest");
    }

    @Test(groups = {"features", "gpdb", "hcfs", "security"})
    public void readCsvMultiCharStringDelimiter() throws Exception {
        // set profile and format
        exTable.setName("pxf_multibyte_multichar_data");
        exTable.setProfile(protocol.value() + ":csv");
        exTable.setDelimiter("DELIM");
        // create external table
        gpdb.createTableAndVerify(exTable);
        // create local CSV file
        String tempLocalDataPath = dataTempFolder + "/data.csv";
        CsvUtils.writeTableToCsvFileOptions(dataTable, tempLocalDataPath, '|', ' ', CSVWriter.DEFAULT_ESCAPE_CHARACTER, CSVWriter.DEFAULT_LINE_END);
        // replace pipe delimiter with actual delimiter
        CsvUtils.updateDelim(tempLocalDataPath, '|', "DELIM");
        // copy local CSV to HDFS
        hdfs.copyFromLocal(tempLocalDataPath, hdfsFilePath);

        // create a new table with the SKIP_HEADER_COUNT parameter
        exTable.setName("pxf_multibyte_multichar_data_with_skip");
        exTable.setUserParameters(new String[]{"SKIP_HEADER_COUNT=10"});
        // create external table
        gpdb.createTableAndVerify(exTable);
        // verify results
        // run the query skipping the first 10 lines of the text
        runTincTest("pxf.features.multibyte_delimiter.multi_char.runTest");
    }

    @Test(groups = {"features", "gpdb", "hcfs", "security"})
    public void readCsvTwoByteDelimiterWithCRLF() throws Exception {
        // set profile and format
        exTable.setName("pxf_multibyte_twobyte_withcrlf_data");
        exTable.setProfile(protocol.value() + ":csv");
        exTable.setDelimiter("¤");
        exTable.setNewLine("\\r\\n");
        // create external table
        gpdb.createTableAndVerify(exTable);
        // create local CSV file
        String tempLocalDataPath = dataTempFolder + "/data.csv";
        CsvUtils.writeTableToCsvFileOptions(dataTable, tempLocalDataPath, '¤', ' ', CSVWriter.DEFAULT_ESCAPE_CHARACTER, "\r\n");
        // copy local CSV to HDFS
        hdfs.copyFromLocal(tempLocalDataPath, hdfsFilePath);

        // verify results
        runTincTest("pxf.features.multibyte_delimiter.two_byte_with_crlf.runTest");
    }

    @Test(groups = {"features", "gpdb", "hcfs", "security"})
    public void readCsvTwoByteDelimiterWithQuote() throws Exception {
        // set profile and format
        exTable.setName("pxf_multibyte_twobyte_withquote_data");
        exTable.setProfile(protocol.value() + ":csv");
        exTable.setDelimiter("¤");
        exTable.setQuote("\"");
        // create external table
        gpdb.createTableAndVerify(exTable);
        // create local CSV file
        String tempLocalDataPath = dataTempFolder + "/data.csv";
        CsvUtils.writeTableToCsvFileOptions(dataTable, tempLocalDataPath, '¤', CSVWriter.DEFAULT_QUOTE_CHARACTER, CSVWriter.DEFAULT_ESCAPE_CHARACTER, CSVWriter.DEFAULT_LINE_END);
        // copy local CSV to HDFS
        hdfs.copyFromLocal(tempLocalDataPath, hdfsFilePath);

        // verify results
        runTincTest("pxf.features.multibyte_delimiter.two_byte_with_quote.runTest");
    }

    @Test(groups = {"features", "gpdb", "hcfs", "security"})
    public void readCsvTwoByteDelimiterWithQuoteAndEscape() throws Exception {
        // set profile and format
        exTable.setName("pxf_multibyte_twobyte_withquote_withescape_data");
        exTable.setProfile(protocol.value() + ":csv");
        exTable.setDelimiter("¤");
        exTable.setQuote("|");
        exTable.setEscape("\\");
        // create external table
        gpdb.createTableAndVerify(exTable);
        // create local CSV file
        String tempLocalDataPath = dataTempFolder + "/data.csv";
        CsvUtils.writeTableToCsvFileOptions(dataTable, tempLocalDataPath, '¤', '|', '\\', CSVWriter.DEFAULT_LINE_END);
        // copy local CSV to HDFS
        hdfs.copyFromLocal(tempLocalDataPath, hdfsFilePath);

        // verify results
        runTincTest("pxf.features.multibyte_delimiter.two_byte_with_quote_and_escape.runTest");
    }

//    /**
//     * Read multiple CSV files with headers from HCFS using *:text profile and
//     * CSV format.
//     *
//     * @throws Exception when the test fails
//     */
//    @Test(groups = {"features", "gpdb", "hcfs", "security"})
//    public void readCsvFilesWithHeader() throws Exception {
//        // set profile and format
//        prepareReadableTable("pxf_hcfs_csv_files_with_header", LINEITEM_SCHEMA, hdfs.getWorkingDirectory() + "/csv_files_with_header", "CSV");
//        exTable.setUserParameters(new String[]{"SKIP_HEADER_COUNT=1"});
//        exTable.setDelimiter("|");
//        // create external table
//        gpdb.createTableAndVerify(exTable);
//        // copy local CSV to HCFS
//        hdfs.copyFromLocal(localDataResourcesFolder + "/csv/sample1.csv", hdfs.getWorkingDirectory() + "/csv_files_with_header/sample1.csv");
//        hdfs.copyFromLocal(localDataResourcesFolder + "/csv/sample2.csv", hdfs.getWorkingDirectory() + "/csv_files_with_header/sample2.csv");
//        hdfs.copyFromLocal(localDataResourcesFolder + "/csv/sample3.csv", hdfs.getWorkingDirectory() + "/csv_files_with_header/sample3.csv");
//        // verify results
//        runTincTest("pxf.features.hdfs.readable.text.csv_files_with_header.runTest");
//    }

    private void prepareReadableTable(String name, String[] fields, String path, String format) {
        exTable.setName(name);
        exTable.setFormat(format);
        exTable.setPath(protocol.getExternalTablePath(hdfs.getBasePath(), path));
        exTable.setFields(fields);
        exTable.setProfile(protocol.value() + ":text");
    }
}
