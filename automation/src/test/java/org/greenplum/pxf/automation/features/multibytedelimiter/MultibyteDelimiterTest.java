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
import org.greenplum.pxf.automation.utils.exception.ExceptionUtils;
import org.greenplum.pxf.automation.utils.fileformats.FileFormatsUtils;
import org.greenplum.pxf.automation.utils.jsystem.report.ReportUtils;
import org.greenplum.pxf.automation.utils.system.ProtocolEnum;
import org.greenplum.pxf.automation.utils.system.ProtocolUtils;
import org.greenplum.pxf.automation.utils.tables.ComparisonUtils;
import org.junit.Assert;
import org.postgresql.util.PSQLException;
import org.testng.annotations.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static java.lang.Thread.sleep;
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
        exTable = TableFactory.getPxfReadableTextTable("pxf_multibyte_small_data",
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
    public void readTwoByteDelimiter() throws Exception {
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
    public void readTwoByteDelimiterDelimNotProvided() throws Exception {
        // set profile and format
        exTable.setName("pxf_multibyte_twobyte_nodelim_data");
        exTable.setProfile(protocol.value() + ":csv");
        exTable.setDelimiter(null);
        // create external table
        gpdb.createTableAndVerify(exTable);
        // create local CSV file
        String tempLocalDataPath = dataTempFolder + "/data.csv";
        CsvUtils.writeTableToCsvFileOptions(dataTable, tempLocalDataPath, '¤', ' ', CSVWriter.DEFAULT_ESCAPE_CHARACTER, CSVWriter.DEFAULT_LINE_END);
        // copy local CSV to HDFS
        hdfs.copyFromLocal(tempLocalDataPath, hdfsFilePath);

        // verify results
        runTincTest("pxf.features.multibyte_delimiter.two_byte_no_delim.runTest");
    }

    @Test(groups = {"features", "gpdb", "hcfs", "security"})
    public void readTwoByteDelimiterWrongFormatter() throws Exception {
        // set profile and format
        exTable.setName("pxf_multibyte_twobyte_wrongformatter_data");
        exTable.setProfile(protocol.value() + ":csv");
        exTable.setDelimiter("¤");
        exTable.setDelimiter("pxfwritable_import");
        // create external table
        gpdb.createTableAndVerify(exTable);
        // create local CSV file
        String tempLocalDataPath = dataTempFolder + "/data.csv";
        CsvUtils.writeTableToCsvFileOptions(dataTable, tempLocalDataPath, '¤', ' ', CSVWriter.DEFAULT_ESCAPE_CHARACTER, CSVWriter.DEFAULT_LINE_END);
        // copy local CSV to HDFS
        hdfs.copyFromLocal(tempLocalDataPath, hdfsFilePath);

        // verify results
        runTincTest("pxf.features.multibyte_delimiter.two_byte_wrong_formatter.runTest");
    }

    @Test(groups = {"features", "gpdb", "hcfs", "security"})
    public void readThreeByteDelimiter() throws Exception {
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
    public void readFourByteDelimiter() throws Exception {
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
    public void readMultiCharStringDelimiter() throws Exception {
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
        // wait a bit for async write in previous steps to finish
        if (protocol == ProtocolEnum.FILE) {
            sleep(10000);
        }

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
    public void readTwoByteDelimiterWithCRLF() throws Exception {
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
    public void readTwoByteDelimiterWithQuote() throws Exception {
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
    public void readTwoByteDelimiterWithQuoteAndEscape() throws Exception {
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

    @Test(groups = {"features", "gpdb", "hcfs", "security"})
    public void readBzip2CompressedCsvTwoByteDelimiter() throws Exception {
        BZip2Codec codec = new BZip2Codec();
        codec.setConf(hdfs.getConfiguration());
        char c = 'a';

        for (int i = 0; i < 10; i++, c++) {
            Table dataTable = getSmallData(StringUtils.repeat(String.valueOf(c), 2), 10);
            hdfs.writeTableToFile(hdfs.getWorkingDirectory() + "/bzip2/" + c + "_" + fileName + ".bz2",
                    dataTable, "¤", StandardCharsets.UTF_8, codec);
        }

        exTable =
                TableFactory.getPxfReadableCSVTable("pxf_multibyte_twobyte_withbzip2_data", SMALL_DATA_FIELDS,
                        protocol.getExternalTablePath(hdfs.getBasePath(), hdfs.getWorkingDirectory()) + "/bzip2/", "¤");
        exTable.setFormat("CUSTOM");
        exTable.setFormatter("pxfdelimited_import");
        gpdb.createTableAndVerify(exTable);

        runTincTest("pxf.features.multibyte_delimiter.two_byte_with_bzip2.runTest");
    }

    @Test(groups = {"features", "gpdb", "hcfs", "security"})
    public void invalidCodePoint() throws Exception {
        // set profile and format
        exTable.setName("pxf_multibyte_invalid_codepoint_data");
        exTable.setProfile(protocol.value() + ":csv");
        exTable.setDelimiter("E\'\\xA4\'");
        // create external table
        try {
            gpdb.createTableAndVerify(exTable);
            Assert.fail("Insert data should fail because of unsupported type");
        } catch (PSQLException e) {
            ExceptionUtils.validate(null, e, new PSQLException("ERROR.*invalid byte sequence for encoding.*?", null), true);
        }
    }

//    @Test(groups = {"features", "gpdb", "security"})
//    public void readMultiBlockedMultiLinedCsv() throws Exception {
//        // prepare local CSV file
//        dataTable = new Table("dataTable", null);
//        String tempLocalDataPath = dataTempFolder + "/data.csv";
//        // prepare template data of 10 lines
//        FileFormatsUtils.prepareData(new QuotedLineTextPreparer(), 10,
//                dataTable);
//        // multiply it to file
//        FileFormatsUtils.prepareDataFile(dataTable, 32, tempLocalDataPath, "¤");
//        // copy local file to HDFS
//        hdfs.copyFromLocal(tempLocalDataPath, hdfsFilePath);
//        // define and create external table
//        exTable = TableFactory.getPxfReadableCSVTable("pxf_multi_csv", new String[]{
//                        "num1 int",
//                        "word text",
//                        "num2 int"},
//                protocol.getExternalTablePath(hdfs.getBasePath(), hdfsFilePath), "¤");
//        exTable.setProfile(protocol.value() + ":text:multi");
//        exTable.setFormat("CUSTOM");
//        exTable.setFormatter("pxfdelimited_import");
//        exTable.setDelimiter("¤");
//        gpdb.createTableAndVerify(exTable);
//        // Verify results
//        runTincTest("pxf.features.multibyte_delimiter.multiblocked_csv_data.runTest");
//    }

//    @Test(groups = {"features", "gpdb", "hdfs", "security"})
//    public void readMultiLineCsvFilesWithSkipHeaderCount() throws Exception {
//        String hdfs_dir = hdfs.getWorkingDirectory() + "/csv_files_with_header/multi_line_csv_with_header.csv";
//        hdfs.copyFromLocal(localDataResourcesFolder + "/csv/multi_line_csv_with_header.csv", hdfs_dir);
//
//        // define and create external table
//        exTable = TableFactory.getPxfReadableCSVTable("pxf_multi_csv_with_header", new String[]{
//                        "num1 int",
//                        "word text",
//                        "num2 int"},
//                protocol.getExternalTablePath(hdfs.getBasePath(), hdfs_dir), "¤");
//        exTable.setProfile(protocol.value() + ":text:multi");
//        exTable.setFormat("CUSTOM");
//        exTable.setFormatter("pxfdelimited_import");
//        exTable.setDelimiter("¤");
//        exTable.setUserParameters(new String[]{"SKIP_HEADER_COUNT=1"});
//        gpdb.createTableAndVerify(exTable);
//        // Verify results
//        runTincTest("pxf.features.multibyte_delimiter.multiline_csv_data_with_header.runTest");
//    }
//
//    /**
//     * Read the multi-line CSV file with headers from HDFS using *:text:multi profile and
//     * CSV format and SKIP_HEADER_COUNT=1 and FILE_AS_ROW flag set to True.
//     *
//     * @throws Exception when the test fails
//     */
//    @Test(groups = {"features", "gpdb", "hdfs", "security"})
//    public void readMultiLineCsvFileAsRowSkipHeaderCount() throws Exception {
//        String hdfs_dir = hdfs.getWorkingDirectory() + "/csv_files_with_header/multi_line_csv_with_header.csv";
//        hdfs.copyFromLocal(localDataResourcesFolder + "/csv/multi_line_csv_with_header.csv", hdfs_dir);
//
//        // define and create external table
//        exTable = TableFactory.getPxfReadableCSVTable("pxf_file_as_row_with_header", new String[]{
//                        "data text"},
//                protocol.getExternalTablePath(hdfs.getBasePath(), hdfs_dir), ",");
//
//        exTable.setProfile(protocol.value() + ":text:multi");
//        exTable.setUserParameters(new String[]{"SKIP_HEADER_COUNT=1","FILE_AS_ROW=true"});
//        gpdb.createTableAndVerify(exTable);
//        // Verify results
//        runTincTest("pxf.features.hdfs.readable.text.multiline_file_as_row_with_header.runTest");
//    }

    @Test(groups = {"features", "gpdb", "hcfs", "security"})
    public void readFileWithLatin1Encoding() throws Exception {
        ProtocolEnum protocol = ProtocolUtils.getProtocol();
        // define and create external table
        exTable.setName("pxf_multibyte_encoding");
        exTable.setFields(new String[]{"num1 int", "word text"});
        exTable.setProfile(protocol.value() + ":text");
        exTable.setDelimiter("¤");
        exTable.setEncoding("LATIN1");
        gpdb.createTableAndVerify(exTable);
        // prepare data and write to HDFS
        dataTable = new Table("data", null);
        dataTable.addRow(new String[]{"4", "tá sé seo le tástáil dea-"});
        dataTable.addRow(new String[]{"3", "règles d'automation"});
        dataTable.addRow(new String[]{
                "5",
                "minden amire szüksége van a szeretet"});
        hdfs.writeTableToFile(hdfsFilePath, dataTable, "¤",
                StandardCharsets.ISO_8859_1);

        // verify results
        runTincTest("pxf.features.multibyte_delimiter.encoding.runTest");
    }

    @Test(groups = {"features", "gpdb", "hcfs", "security"})
    public void readFileWithLatin1EncodingByteRepresentation() throws Exception {
        ProtocolEnum protocol = ProtocolUtils.getProtocol();
        // define and create external table
        exTable.setName("pxf_multibyte_encoding_bytes");
        exTable.setFields(new String[]{"num1 int", "word text"});
        exTable.setProfile(protocol.value() + ":text");
        exTable.setDelimiter("E\'\\xC2\\xA4\'");
        exTable.setEncoding("LATIN1");
        gpdb.createTableAndVerify(exTable);
        // prepare data and write to HDFS
        dataTable = new Table("data", null);
        dataTable.addRow(new String[]{"4", "tá sé seo le tástáil dea-"});
        dataTable.addRow(new String[]{"3", "règles d'automation"});
        dataTable.addRow(new String[]{
                "5",
                "minden amire szüksége van a szeretet"});
        hdfs.writeTableToFile(hdfsFilePath, dataTable, "¤",
                StandardCharsets.ISO_8859_1);

        // verify results
        runTincTest("pxf.features.multibyte_delimiter.encoding_bytes.runTest");
    }

}
