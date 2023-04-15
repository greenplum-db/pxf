package org.greenplum.pxf.automation.features.multibytedelimiter;

import au.com.bytecode.opencsv.CSVWriter;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.greenplum.pxf.automation.datapreparer.CustomTextPreparer;
import org.greenplum.pxf.automation.features.BaseFeature;
import org.greenplum.pxf.automation.structures.tables.basic.Table;
import org.greenplum.pxf.automation.structures.tables.utils.TableFactory;
import org.greenplum.pxf.automation.utils.csv.CsvUtils;
import org.greenplum.pxf.automation.utils.exception.ExceptionUtils;
import org.greenplum.pxf.automation.utils.fileformats.FileFormatsUtils;
import org.greenplum.pxf.automation.utils.system.ProtocolEnum;
import org.greenplum.pxf.automation.utils.system.ProtocolUtils;
import org.junit.Assert;
import org.postgresql.util.PSQLException;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;

import static java.lang.Thread.sleep;

/**
 * Collection of Test cases for PXF ability to read Text/CSV files with pxfdelimited_import.
 */
public class MultibyteDelimiterTest extends BaseFeature {
    ProtocolEnum protocol;

    // holds data for file generation
    Table dataTable = null;

    // holds data for encoded file generation
    Table encodedDataTable = null;

    // path for storing data on HDFS
    String hdfsFilePath = "";

    String testPackageLocation = "/org/greenplum/pxf/automation/testplugin/";
    private static final  String[] ROW_WITH_ESCAPE = {"s_101",
            "s_1001",
            "s_10001",
            "2299-11-28 05:46:40",
            "101",
            "1001",
            "10001",
            "10001",
            "10001",
            "10001",
            "10001",
            "s_101 | escaped!",
            "s_1001",
            "s_10001",
            "2299-11-28 05:46:40",
            "101",
            "1001",
            "10001",
            "10001",
            "10001",
            "10001",
            "10001"};

    /**
     * Prepare all components and all data flow (Hdfs to GPDB)
     */
    @Override
    public void beforeClass() throws Exception {
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
        hdfsFilePath = hdfs.getWorkingDirectory() + "/multibyteDelimiter";
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
                null);
        exTable.setFormat("CUSTOM");
        exTable.setFormatter("pxfdelimited_import");


        encodedDataTable = new Table("data", null);
        encodedDataTable.addRow(new String[]{"4", "tá sé seo le tástáil dea-"});
        encodedDataTable.addRow(new String[]{"3", "règles d'automation"});
        encodedDataTable.addRow(new String[]{
                "5",
                "minden amire szüksége van a szeretet"});
    }

    @Test(groups = {"gpdb", "hcfs", "security"})
    public void readTwoByteDelimiter() throws Exception {
        updateExternalTableOptions("pxf_multibyte_twobyte_data", new String[] {"delimiter='¤'"}, ":csv");

        // create external table
        gpdb.createTableAndVerify(exTable);
        // create local CSV file
        String tempLocalDataPath = dataTempFolder + "/data.csv";
        CsvUtils.writeTableToCsvFile(dataTable, tempLocalDataPath, StandardCharsets.UTF_8,
                '¤', CSVWriter.NO_QUOTE_CHARACTER, CSVWriter.NO_ESCAPE_CHARACTER, CSVWriter.DEFAULT_LINE_END);
        // copy local CSV to HDFS
        hdfs.copyFromLocal(tempLocalDataPath, hdfsFilePath);

        // create a new table with the SKIP_HEADER_COUNT parameter
        exTable.setName("pxf_multibyte_twobyte_data_with_skip");
        exTable.setUserParameters(new String[]{"SKIP_HEADER_COUNT=10"});
        // create external table
        gpdb.createTableAndVerify(exTable);

        // verify results
        runTincTest("pxf.features.multibyte_delimiter.two_byte.runTest");
    }

    @Test(groups = {"gpdb", "hcfs", "security"})
    public void readThreeByteDelimiter() throws Exception {
        updateExternalTableOptions("pxf_multibyte_threebyte_data", new String[] {"delimiter='停'"}, ":csv");

        // create external table
        gpdb.createTableAndVerify(exTable);
        // create local CSV file
        String tempLocalDataPath = dataTempFolder + "/data.csv";
        CsvUtils.writeTableToCsvFile(dataTable, tempLocalDataPath, StandardCharsets.UTF_8,
                '停', CSVWriter.NO_QUOTE_CHARACTER, CSVWriter.NO_ESCAPE_CHARACTER, CSVWriter.DEFAULT_LINE_END);
        // copy local CSV to HDFS
        hdfs.copyFromLocal(tempLocalDataPath, hdfsFilePath);
        // wait a bit for async write in previous steps to finish
        if (protocol == ProtocolEnum.FILE) {
            sleep(10000);
        }

        // create a new table with the SKIP_HEADER_COUNT parameter
        exTable.setName("pxf_multibyte_threebyte_data_with_skip");
        exTable.setUserParameters(new String[]{"SKIP_HEADER_COUNT=10"});
        // create external table
        gpdb.createTableAndVerify(exTable);

        // verify results
        runTincTest("pxf.features.multibyte_delimiter.three_byte.runTest");
    }

    @Test(groups = {"gpdb", "hcfs", "security"})
    public void readFourByteDelimiter() throws Exception {
        updateExternalTableOptions("pxf_multibyte_fourbyte_data", new String[] {"delimiter='\uD83D\uDE42'"}, ":csv");

        // create external table
        gpdb.createTableAndVerify(exTable);
        // create local CSV file
        String tempLocalDataPath = dataTempFolder + "/data.csv";
        CsvUtils.writeTableToCsvFile(dataTable, tempLocalDataPath, StandardCharsets.UTF_8,
                '|', CSVWriter.NO_QUOTE_CHARACTER, CSVWriter.NO_ESCAPE_CHARACTER, CSVWriter.DEFAULT_LINE_END);
        // replace pipe delimiter with actual delimiter
        CsvUtils.updateDelim(tempLocalDataPath, '|', "\uD83D\uDE42");
        // copy local CSV to HDFS
        hdfs.copyFromLocal(tempLocalDataPath, hdfsFilePath);
        // wait a bit for async write in previous steps to finish
        if (protocol == ProtocolEnum.FILE) {
            sleep(10000);
        }

        // create a new table with the SKIP_HEADER_COUNT parameter
        exTable.setName("pxf_multibyte_fourbyte_data_with_skip");
        exTable.setUserParameters(new String[]{"SKIP_HEADER_COUNT=10"});
        // create external table
        gpdb.createTableAndVerify(exTable);

        // verify results
        runTincTest("pxf.features.multibyte_delimiter.four_byte.runTest");
    }

    @Test(groups = {"gpdb", "hcfs", "security"})
    public void readMultiCharStringDelimiter() throws Exception {
        updateExternalTableOptions("pxf_multibyte_multichar_data", new String[] {"delimiter='DELIM'"}, ":csv");

        // create external table
        gpdb.createTableAndVerify(exTable);
        // create local CSV file
        String tempLocalDataPath = dataTempFolder + "/data.csv";
        CsvUtils.writeTableToCsvFile(dataTable, tempLocalDataPath, StandardCharsets.UTF_8,
                '|', CSVWriter.NO_QUOTE_CHARACTER, CSVWriter.NO_ESCAPE_CHARACTER, CSVWriter.DEFAULT_LINE_END);
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
        runTincTest("pxf.features.multibyte_delimiter.multi_char.runTest");
    }

    @Test(groups = {"gpdb", "hcfs", "security"})
    public void readTwoByteDelimiterWithCRLF() throws Exception {
        updateExternalTableOptions("pxf_multibyte_twobyte_withcrlf_data", new String[] {"delimiter='¤'", "newline='CRLF'"}, ":csv");

        // create external table
        gpdb.createTableAndVerify(exTable);
        // create local CSV file
        String tempLocalDataPath = dataTempFolder + "/data.csv";
        CsvUtils.writeTableToCsvFile(dataTable, tempLocalDataPath, StandardCharsets.UTF_8,
                '¤', CSVWriter.NO_QUOTE_CHARACTER, CSVWriter.NO_ESCAPE_CHARACTER, "\r\n");
        // copy local CSV to HDFS
        hdfs.copyFromLocal(tempLocalDataPath, hdfsFilePath);

        // verify results
        runTincTest("pxf.features.multibyte_delimiter.two_byte_with_crlf.runTest");
    }

    @Test(groups = {"gpdb", "hcfs", "security"})
    public void readTwoByteDelimiterWithCR() throws Exception {
        updateExternalTableOptions("pxf_multibyte_twobyte_withcr_data", new String[] {"delimiter='¤'", "newline='CR'"}, ":csv");
        exTable.setUserParameters(new String[] {"NEWLINE=CR"});

        // create external table
        gpdb.createTableAndVerify(exTable);
        // create local CSV file
        String tempLocalDataPath = dataTempFolder + "/data.csv";
        CsvUtils.writeTableToCsvFile(dataTable, tempLocalDataPath, StandardCharsets.UTF_8,
                '¤', CSVWriter.NO_QUOTE_CHARACTER, CSVWriter.NO_ESCAPE_CHARACTER, "\r");
        // copy local CSV to HDFS
        hdfs.copyFromLocal(tempLocalDataPath, hdfsFilePath);

        // verify results
        runTincTest("pxf.features.multibyte_delimiter.two_byte_with_cr.runTest");
    }

    @Test(groups = {"gpdb", "hcfs", "security"})
    public void readTwoByteDelimiterWrongFormatter() throws Exception {
        updateExternalTableOptions("pxf_multibyte_twobyte_wrongformatter_data", new String[] {"delimiter='¤'"}, ":csv");
        exTable.setFormatter("pxfwritable_import");

        // create external table
        gpdb.createTableAndVerify(exTable);
        // create local CSV file
        String tempLocalDataPath = dataTempFolder + "/data.csv";
        CsvUtils.writeTableToCsvFile(dataTable, tempLocalDataPath, StandardCharsets.UTF_8,
                '¤', CSVWriter.NO_QUOTE_CHARACTER, CSVWriter.NO_ESCAPE_CHARACTER, CSVWriter.DEFAULT_LINE_END);
        // copy local CSV to HDFS
        hdfs.copyFromLocal(tempLocalDataPath, hdfsFilePath);

        // verify results
        runTincTest("pxf.features.multibyte_delimiter.two_byte_wrong_formatter.runTest");
    }

    @Test(groups = {"gpdb", "hcfs", "security"})
    public void readTwoByteDelimiterDelimNotProvided() throws Exception {
        updateExternalTableOptions("pxf_multibyte_twobyte_nodelim_data", new String[] {}, ":csv");

        // create external table
        gpdb.createTableAndVerify(exTable);
        // create local CSV file
        String tempLocalDataPath = dataTempFolder + "/data.csv";
        CsvUtils.writeTableToCsvFile(dataTable, tempLocalDataPath, StandardCharsets.UTF_8,
                '¤', CSVWriter.NO_QUOTE_CHARACTER, CSVWriter.NO_ESCAPE_CHARACTER, CSVWriter.DEFAULT_LINE_END);
        // copy local CSV to HDFS
        hdfs.copyFromLocal(tempLocalDataPath, hdfsFilePath);

        // verify results
        runTincTest("pxf.features.multibyte_delimiter.two_byte_no_delim.runTest");
    }

    @Test(groups = {"gpdb", "hcfs", "security"})
    public void readTwoByteDelimiterWithWrongDelimiter() throws Exception {
        updateExternalTableOptions("pxf_multibyte_twobyte_wrong_delim_data", new String[] {"delimiter='停'"}, ":csv");

        // create external table
        gpdb.createTableAndVerify(exTable);
        // create local CSV file
        String tempLocalDataPath = dataTempFolder + "/data.csv";
        CsvUtils.writeTableToCsvFile(dataTable, tempLocalDataPath, StandardCharsets.UTF_8,
                '¤', CSVWriter.NO_QUOTE_CHARACTER, CSVWriter.NO_ESCAPE_CHARACTER, CSVWriter.DEFAULT_LINE_END);
        // copy local CSV to HDFS
        hdfs.copyFromLocal(tempLocalDataPath, hdfsFilePath);

        // verify results
        runTincTest("pxf.features.multibyte_delimiter.two_byte_with_wrong_delim.runTest");
    }

    @Test(groups = {"gpdb", "hcfs", "security"})
    public void readTwoByteDelimiterWithQuote() throws Exception {
        updateExternalTableOptions("pxf_multibyte_twobyte_withquote_data", new String[] {"delimiter='¤'", "quote='\"'"}, ":csv");

        // create external table
        gpdb.createTableAndVerify(exTable);
        // create local CSV file
        String tempLocalDataPath = dataTempFolder + "/data.csv";
        CsvUtils.writeTableToCsvFile(dataTable, tempLocalDataPath, StandardCharsets.UTF_8,
                '¤', CSVWriter.DEFAULT_QUOTE_CHARACTER, CSVWriter.DEFAULT_ESCAPE_CHARACTER, CSVWriter.DEFAULT_LINE_END);
        // copy local CSV to HDFS
        hdfs.copyFromLocal(tempLocalDataPath, hdfsFilePath);
        // wait a bit for async write in previous steps to finish
        if (protocol == ProtocolEnum.FILE) {
            sleep(10000);
        }

        // verify results
        runTincTest("pxf.features.multibyte_delimiter.two_byte_with_quote.runTest");
    }

    @Test(groups = {"gpdb", "hcfs", "security"})
    public void readTwoByteDelimiterWithWrongEol() throws Exception {
        updateExternalTableOptions("pxf_multibyte_twobyte_wrong_eol_data", new String[] {"delimiter='¤'", "quote='|'", "newline='CR'"}, ":csv");

        // create external table
        gpdb.createTableAndVerify(exTable);
        // create local CSV file
        String tempLocalDataPath = dataTempFolder + "/data.csv";
        CsvUtils.writeTableToCsvFile(dataTable, tempLocalDataPath, StandardCharsets.UTF_8,
                '¤', CSVWriter.DEFAULT_QUOTE_CHARACTER, CSVWriter.DEFAULT_ESCAPE_CHARACTER, CSVWriter.DEFAULT_LINE_END);
        // copy local CSV to HDFS
        hdfs.copyFromLocal(tempLocalDataPath, hdfsFilePath);

        // verify results
        // in newer versions of GP6 and in GP7, GPDB calls into the formatter one more time to handle EOF properly
        // however, this is not the case for GP5 and for versions of GP6 older than 6.24.0
        // therefore, we must run 2 different sets of tests to check for the expected error
        if (gpdb.getVersion() >= 6) {
            runTincTest("pxf.features.multibyte_delimiter.two_byte_with_wrong_eol.runTest");
        } else {
            runTincTest("pxf.features.multibyte_delimiter.two_byte_with_wrong_eol_5X.runTest");
        }
    }

    @Test(groups = {"gpdb", "hcfs", "security"})
    public void readTwoByteDelimiterWithWrongQuote() throws Exception {
        updateExternalTableOptions("pxf_multibyte_twobyte_wrong_quote_data", new String[] {"delimiter='¤'", "quote='|'"}, ":csv");

        // create external table
        gpdb.createTableAndVerify(exTable);
        // create local CSV file
        String tempLocalDataPath = dataTempFolder + "/data.csv";
        CsvUtils.writeTableToCsvFile(dataTable, tempLocalDataPath, StandardCharsets.UTF_8,
                '¤', CSVWriter.DEFAULT_QUOTE_CHARACTER, CSVWriter.DEFAULT_ESCAPE_CHARACTER, CSVWriter.DEFAULT_LINE_END);
        // copy local CSV to HDFS
        hdfs.copyFromLocal(tempLocalDataPath, hdfsFilePath);

        // verify results
        // in newer versions of GP6 and in GP7, GPDB calls into the formatter one more time to handle EOF properly
        // however, this is not the case for GP5 and for versions of GP6 older than 6.24.0
        // therefore, we must run 2 different sets of tests to check for the expected error
        if (gpdb.getVersion() >= 6) {
            runTincTest("pxf.features.multibyte_delimiter.two_byte_with_wrong_quote.runTest");
        } else {
            runTincTest("pxf.features.multibyte_delimiter.two_byte_with_wrong_quote_5X.runTest");
        }
    }

    @Test(groups = {"gpdb", "hcfs", "security"})
    public void readTwoByteDelimiterWithQuoteAndEscape() throws Exception {
        updateExternalTableOptions("pxf_multibyte_twobyte_withquote_withescape_data", new String[] {"delimiter='¤'", "quote='|'", "escape='\\'"}, ":csv");

        // create external table
        gpdb.createTableAndVerify(exTable);
        // create local CSV file
        dataTable.addRow(ROW_WITH_ESCAPE);
        String tempLocalDataPath = dataTempFolder + "/data.csv";
        CsvUtils.writeTableToCsvFile(dataTable, tempLocalDataPath, StandardCharsets.UTF_8,
                '¤', '|', '\\', CSVWriter.DEFAULT_LINE_END);
        // copy local CSV to HDFS
        hdfs.copyFromLocal(tempLocalDataPath, hdfsFilePath);

        // verify results
        runTincTest("pxf.features.multibyte_delimiter.two_byte_with_quote_and_escape.runTest");
    }

    @Test(groups = {"gpdb", "hcfs", "security"})
    public void readTwoByteDelimiterWithWrongEscape() throws Exception {
        updateExternalTableOptions("pxf_multibyte_twobyte_wrong_escape_data", new String[] {"delimiter='¤'", "quote='|'", "escape='#'"}, ":csv");

        // create external table
        gpdb.createTableAndVerify(exTable);
        // create local CSV file
        dataTable.addRow(ROW_WITH_ESCAPE);
        String tempLocalDataPath = dataTempFolder + "/data.csv";
        CsvUtils.writeTableToCsvFile(dataTable, tempLocalDataPath, StandardCharsets.UTF_8,
                '¤', '|', '\\', CSVWriter.DEFAULT_LINE_END);;
        // copy local CSV to HDFS
        hdfs.copyFromLocal(tempLocalDataPath, hdfsFilePath);
        // wait a bit for async write in previous steps to finish
        if (protocol == ProtocolEnum.FILE) {
            sleep(10000);
        }

        // verify results
        runTincTest("pxf.features.multibyte_delimiter.two_byte_with_wrong_escape.runTest");
    }

    // users should still be able to use a normal delimiter with this formatter
    @Test(groups = {"gpdb", "hcfs", "security"})
    public void readOneByteDelimiter() throws Exception {
        updateExternalTableOptions("pxf_multibyte_onebyte_data", new String[] {"delimiter='|'"}, ":csv");

        // create external table
        gpdb.createTableAndVerify(exTable);
        // create local CSV file
        String tempLocalDataPath = dataTempFolder + "/data.csv";
        CsvUtils.writeTableToCsvFile(dataTable, tempLocalDataPath, StandardCharsets.UTF_8,
                '|', CSVWriter.NO_QUOTE_CHARACTER, CSVWriter.NO_ESCAPE_CHARACTER, CSVWriter.DEFAULT_LINE_END);
        // copy local CSV to HDFS
        hdfs.copyFromLocal(tempLocalDataPath, hdfsFilePath);
        // verify results
        runTincTest("pxf.features.multibyte_delimiter.one_byte.runTest");
    }


    @Test(groups = {"gpdb", "hcfs", "security"})
    public void readOneCol() throws Exception {
        // set profile and format
        exTable = TableFactory.getPxfReadableTextTable("pxf_multibyte_onecol_data",
                new String[]{"s1 text"},
                protocol.getExternalTablePath(hdfs.getBasePath(), hdfsFilePath),
                null);
        exTable.setFormat("CUSTOM");
        exTable.setFormatter("pxfdelimited_import");
        exTable.setProfile(protocol.value() + ":csv");
        exTable.addFormatterOption("delimiter='¤'");
        // create external table
        gpdb.createTableAndVerify(exTable);
        // prepare data and write to HDFS
        dataTable = new Table("data", null);
        dataTable.addRow(new String[]{"tá sé seo le tástáil dea-"});
        dataTable.addRow(new String[]{"règles d'automation"});
        dataTable.addRow(new String[]{"minden amire szüksége van a szeretet"});
        String tempLocalDataPath = dataTempFolder + "/data.csv";
        CsvUtils.writeTableToCsvFile(dataTable, tempLocalDataPath, StandardCharsets.UTF_8,
                '¤', CSVWriter.NO_QUOTE_CHARACTER, CSVWriter.NO_ESCAPE_CHARACTER, CSVWriter.DEFAULT_LINE_END);
        // copy local CSV to HDFS
        hdfs.copyFromLocal(tempLocalDataPath, hdfsFilePath);

        // verify results
        runTincTest("pxf.features.multibyte_delimiter.one_col.runTest");
    }

    @Test(groups = {"gpdb", "hcfs", "security"})
    public void readOneColQuote() throws Exception {
        // set profile and format
        exTable = TableFactory.getPxfReadableTextTable("pxf_multibyte_onecol_quote_data",
                new String[]{"s1 text"},
                protocol.getExternalTablePath(hdfs.getBasePath(), hdfsFilePath),
                null);
        exTable.setFormat("CUSTOM");
        exTable.setFormatter("pxfdelimited_import");
        exTable.setProfile(protocol.value() + ":csv");
        exTable.setFormatterOptions(new String[] {"delimiter='¤'", "quote='|'"});
        // create external table
        gpdb.createTableAndVerify(exTable);
        // prepare data and write to HDFS
        dataTable = new Table("data", null);
        dataTable.addRow(new String[]{"tá sé seo le tástáil dea-"});
        dataTable.addRow(new String[]{"règles d'automation"});
        dataTable.addRow(new String[]{"minden amire szüksége van a szeretet"});
        String tempLocalDataPath = dataTempFolder + "/data.csv";
        CsvUtils.writeTableToCsvFile(dataTable, tempLocalDataPath, StandardCharsets.UTF_8,
                '¤', '|', CSVWriter.NO_ESCAPE_CHARACTER, CSVWriter.DEFAULT_LINE_END);
        // copy local CSV to HDFS
        hdfs.copyFromLocal(tempLocalDataPath, hdfsFilePath);

        // verify results
        runTincTest("pxf.features.multibyte_delimiter.one_col_quote.runTest");
    }

    @Test(groups = {"gpdb", "hcfs", "security"})
    public void readBzip2CompressedCsv() throws Exception {
        BZip2Codec codec = new BZip2Codec();
        codec.setConf(hdfs.getConfiguration());
        char c = 'a';

        for (int i = 0; i < 10; i++, c++) {
            Table dataTable = getSmallData(StringUtils.repeat(String.valueOf(c), 2), 10);
            hdfs.writeTableToFile(hdfs.getWorkingDirectory() + "/bzip2/" + c + "_" + fileName + ".bz2",
                    dataTable, "¤", StandardCharsets.UTF_8, codec);
        }

        exTable = TableFactory.getPxfReadableCSVTable("pxf_multibyte_twobyte_withbzip2_data",
                new String[] {
                        "name text",
                        "num integer",
                        "dub double precision",
                        "longNum bigint",
                        "bool boolean"
                },
                protocol.getExternalTablePath(hdfs.getBasePath(),
                hdfs.getWorkingDirectory()) + "/bzip2/", null);
        exTable.setFormat("CUSTOM");
        exTable.setFormatter("pxfdelimited_import");
        exTable.addFormatterOption("delimiter='¤'");
        gpdb.createTableAndVerify(exTable);

        runTincTest("pxf.features.multibyte_delimiter.two_byte_with_bzip2.runTest");
    }

    @Test(groups = {"gpdb", "hcfs", "security"})
    public void readTwoByteWithQuoteEscapeNewLine() throws Exception {
        updateExternalTableOptions("pxf_multibyte_quote_escape_newline_data", new String[] {"delimiter='¤'", "quote='|'", "escape='\\'", "newline='EOL'"}, ":csv");

        // create external table
        gpdb.createTableAndVerify(exTable);
        // create local CSV file
        dataTable.addRow(ROW_WITH_ESCAPE);
        String tempLocalDataPath = dataTempFolder + "/data.csv";
        CsvUtils.writeTableToCsvFile(dataTable, tempLocalDataPath, StandardCharsets.UTF_8,
                '¤', '|', '\\', "EOL");;
        // copy local CSV to HDFS
        hdfs.copyFromLocal(tempLocalDataPath, hdfsFilePath);

        // verify results
        runTincTest("pxf.features.multibyte_delimiter.quote_escape_newline.runTest");
    }

    @Test(groups = {"gpdb", "hcfs", "security"})
    public void invalidCodePoint() throws Exception {
        updateExternalTableOptions("pxf_multibyte_invalid_codepoint_data", new String[] {"delimiter=E'\\xA4'"}, ":csv");

        // create external table
        try {
            gpdb.createTableAndVerify(exTable);
            Assert.fail("Insert data should fail because of unsupported type");
        } catch (PSQLException e) {
            ExceptionUtils.validate(null, e, new PSQLException("ERROR.*invalid byte sequence for encoding.*?", null), true);
        }
    }

    @Test(groups = {"gpdb", "hcfs", "security"})
    public void readFileWithLatin1EncodingTextProfile() throws Exception {
        updateExternalTableOptions("pxf_multibyte_encoding", new String[] {"delimiter='¤'"}, ":text");
        exTable.setFields(new String[]{"num1 int", "word text"});
        exTable.setEncoding("LATIN1");

        gpdb.createTableAndVerify(exTable);

        String tempLocalDataPath = dataTempFolder + "/data.csv";
        CsvUtils.writeTableToCsvFile(encodedDataTable, tempLocalDataPath, StandardCharsets.ISO_8859_1,
                '¤', ' ', ' ', CSVWriter.DEFAULT_LINE_END);
        // copy local CSV to HDFS
        hdfs.copyFromLocal(tempLocalDataPath, hdfsFilePath);

        // verify results
        runTincTest("pxf.features.multibyte_delimiter.encoding.runTest");
    }

    @Test(groups = {"gpdb", "hcfs", "security"})
    public void readFileWithLatin1EncodingByteRepresentationTextProfile() throws Exception {
        updateExternalTableOptions("pxf_multibyte_encoding_bytes", new String[] {"delimiter=E'\\xC2\\xA4'"}, ":text");
        exTable.setFields(new String[]{"num1 int", "word text"});
        exTable.setEncoding("LATIN1");

        gpdb.createTableAndVerify(exTable);

        // create local CSV file
        String tempLocalDataPath = dataTempFolder + "/data.csv";
        CsvUtils.writeTableToCsvFile(encodedDataTable, tempLocalDataPath, StandardCharsets.ISO_8859_1,
                '¤', ' ', ' ', CSVWriter.DEFAULT_LINE_END);
        // copy local CSV to HDFS
        hdfs.copyFromLocal(tempLocalDataPath, hdfsFilePath);

        // verify results
        runTincTest("pxf.features.multibyte_delimiter.encoding_bytes.runTest");
    }

    @Test(groups = {"gpdb", "hcfs", "security"})
    public void readFileWithLatin1EncodingWithQuoteTextProfile() throws Exception {
        updateExternalTableOptions("pxf_multibyte_encoding_quote", new String[] {"delimiter='¤'", "quote='|'"}, ":text");
        exTable.setFields(new String[]{"num1 int", "word text"});
        exTable.setEncoding("LATIN1");

        gpdb.createTableAndVerify(exTable);

        String tempLocalDataPath = dataTempFolder + "/data.csv";
        CsvUtils.writeTableToCsvFile(encodedDataTable, tempLocalDataPath, StandardCharsets.ISO_8859_1,
                '¤', '|', '|', CSVWriter.DEFAULT_LINE_END);
        // copy local CSV to HDFS
        hdfs.copyFromLocal(tempLocalDataPath, hdfsFilePath);

        // verify results
        runTincTest("pxf.features.multibyte_delimiter.encoding_quote.runTest");
    }

    @Test(groups = {"gpdb", "hcfs", "security"})
    public void readFileWithLatin1EncodingWithQuoteAndEscapeTextProfile() throws Exception {
        updateExternalTableOptions("pxf_multibyte_encoding_quote_escape", new String[] {"delimiter='¤'", "quote='|'", "escape='\"'"}, ":text");
        exTable.setFields(new String[]{"num1 int", "word text"});
        exTable.setEncoding("LATIN1");

        gpdb.createTableAndVerify(exTable);

        // create local CSV file
        String tempLocalDataPath = dataTempFolder + "/data.csv";
        CsvUtils.writeTableToCsvFile(encodedDataTable, tempLocalDataPath, StandardCharsets.ISO_8859_1,
                '¤', '|', '\"', CSVWriter.DEFAULT_LINE_END);
        // copy local CSV to HDFS
        hdfs.copyFromLocal(tempLocalDataPath, hdfsFilePath);

        // verify results
        runTincTest("pxf.features.multibyte_delimiter.encoding_quote_escape.runTest");
    }

    @Test(groups = {"gpdb", "hcfs", "security"})
    public void wrongProfileWithFormatter() throws Exception {
        updateExternalTableOptions("pxf_multibyte_wrong_profile", new String[] {"delimiter='¤'", "quote='|'", "escape='\"'"}, ":avro");
        exTable.setFields(new String[]{"name text", "age int"});

        gpdb.createTableAndVerify(exTable);
        // prepare data and write to HDFS
        gpdb.createTableAndVerify(exTable);
        // location of schema and data files
        String absolutePath = getClass().getClassLoader().getResource("data").getPath();
        String resourcePath = absolutePath + "/avro/";
        hdfs.writeAvroFileFromJson(hdfsFilePath + "simple.avro",
                "file://" + resourcePath + "simple.avsc",
                "file://" + resourcePath + "simple.json", null);
        // verify results
        runTincTest("pxf.features.multibyte_delimiter.wrong_profile.runTest");
    }

    private void updateExternalTableOptions(String name, String[] formatterOptions, String profile) {
        exTable.setName(name);
        exTable.setFormatterOptions(formatterOptions);
        exTable.setProfile(protocol.value() + profile);
    }
}