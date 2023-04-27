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

import java.nio.charset.Charset;
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

    private class CsvSpec  {
        String delimiter;
        char quote;
        char escape;
        String eol;
        Charset encoding;
        public CsvSpec(String delimiter, char quote, char escape, String eol) {
            this.delimiter = delimiter;
            this.quote = quote;
            this.escape = escape;
            this.eol = eol;
            this.encoding = StandardCharsets.UTF_8;
        }

        public CsvSpec(String delimiter, char quote, char escape) {
            this(delimiter, quote, escape, CSVWriter.DEFAULT_LINE_END);
        }

        public CsvSpec(String delimiter) {
            this(delimiter, CSVWriter.NO_QUOTE_CHARACTER, CSVWriter.NO_ESCAPE_CHARACTER, CSVWriter.DEFAULT_LINE_END);
        }

        public void setDelimiter(String delimiter) {
            this.delimiter = delimiter;
        }

        public void setQuote(char quote) {
            this.quote = quote;
        }

        public void setEscape(char escape) {
            this.escape = escape;
        }

        public void setEol(String eol) {
            this.eol = eol;
        }

        public void setEncoding(Charset encoding) {
            this.encoding = encoding;
        }

        /**
         * This function takes the CsvSpec used for writing the file
         * and clones it to be used as table formatter options.
         *
         * In the case of EOL handling, we do not want to include the
         * EOL value as a formatter option if it is the default (\n).
         * However, for '\r' the corresponding value is 'CR' and for '\r\n'
         * the corresponding value is 'CRLF'. Anything else, we return as is.
         *
         * @return A clone of the CsvSpec to be used as formatter options for the table DDL
         */
        public CsvSpec cloneForFormatting() {
            // handle EOL situation
            String eol = this.eol;
            switch (eol) {
                case "\r":
                    eol = "CR";
                    break;
                case "\r\n":
                    eol = "CRLF";
                    break;
                case CSVWriter.DEFAULT_LINE_END:
                    // for the default case, we do not want to set the eol value in the formatter options
                    eol = null;
                    break;
                default:
                    eol = this.eol;
            }

            CsvSpec clone = new CsvSpec(this.delimiter, this.quote, this.escape, eol);
            // we do not care about the encoding value as a formatter option in the table DDL
            clone.setEncoding(null);

            return clone;
        }
    }

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
        exTable.setProfile(protocol.value() + ":csv");

        encodedDataTable = new Table("data", null);
        encodedDataTable.addRow(new String[]{"4", "tá sé seo le tástáil dea-"});
        encodedDataTable.addRow(new String[]{"3", "règles d'automation"});
        encodedDataTable.addRow(new String[]{"5", "minden amire szüksége van a szeretet"});
    }

    @Test(groups = {"gpdb", "hcfs", "security"})
    public void readTwoByteDelimiter() throws Exception {
        // used for creating the CSV file
        CsvSpec fileSpec = new CsvSpec("¤");
        // used for setting up formatterOptions
        CsvSpec tableSpec = fileSpec.cloneForFormatting();

        runScenario("pxf_multibyte_twobyte_data", tableSpec, dataTable, fileSpec);

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
        CsvSpec fileSpec = new CsvSpec("停");
        CsvSpec tableSpec = fileSpec.cloneForFormatting();

        runScenario("pxf_multibyte_threebyte_data", tableSpec, dataTable, fileSpec);

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
        CsvSpec fileSpec = new CsvSpec("\uD83D\uDE42");
        CsvSpec tableSpec = fileSpec.cloneForFormatting();

        runScenario("pxf_multibyte_fourbyte_data", tableSpec, dataTable, fileSpec);

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
        CsvSpec fileSpec = new CsvSpec("DELIM");
        CsvSpec tableSpec = fileSpec.cloneForFormatting();

        runScenario("pxf_multibyte_multichar_data", tableSpec, dataTable, fileSpec);

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
        CsvSpec fileSpec = new CsvSpec("¤", CSVWriter.NO_QUOTE_CHARACTER, CSVWriter.NO_ESCAPE_CHARACTER, "\r\n");
        CsvSpec tableSpec = fileSpec.cloneForFormatting();

        runScenario("pxf_multibyte_twobyte_withcrlf_data", tableSpec, dataTable, fileSpec);

        // verify results
        runTincTest("pxf.features.multibyte_delimiter.two_byte_with_crlf.runTest");
    }

    @Test(groups = {"gpdb", "hcfs", "security"})
    public void readTwoByteDelimiterWithCR() throws Exception {
        CsvSpec fileSpec = new CsvSpec("¤", CSVWriter.NO_QUOTE_CHARACTER, CSVWriter.NO_ESCAPE_CHARACTER, "\r");
        CsvSpec tableSpec = fileSpec.cloneForFormatting();

        // we need to add the eol value to the URL to be able to parse the data on PXF Java side
        exTable.setUserParameters(new String[] {"NEWLINE=CR"});

        runScenario("pxf_multibyte_twobyte_withcr_data", tableSpec, dataTable, fileSpec);

        // verify results
        runTincTest("pxf.features.multibyte_delimiter.two_byte_with_cr.runTest");
    }

    @Test(groups = {"gpdb", "hcfs", "security"})
    public void readTwoByteDelimiterWrongFormatter() throws Exception {
        CsvSpec fileSpec = new CsvSpec("¤");
        CsvSpec tableSpec = fileSpec.cloneForFormatting();

        exTable.setFormatter("pxfwritable_import");

        runScenario("pxf_multibyte_twobyte_wrongformatter_data", tableSpec, dataTable, fileSpec);

        // verify results
        runTincTest("pxf.features.multibyte_delimiter.two_byte_wrong_formatter.runTest");
    }

    @Test(groups = {"gpdb", "hcfs", "security"})
    public void readTwoByteDelimiterDelimNotProvided() throws Exception {
        CsvSpec fileSpec = new CsvSpec("¤");
        CsvSpec tableSpec = fileSpec.cloneForFormatting();
        // remove the delimiter for the formatterOptions
        tableSpec.setDelimiter(null);

        runScenario("pxf_multibyte_twobyte_nodelim_data", tableSpec, dataTable, fileSpec);

        // verify results
        runTincTest("pxf.features.multibyte_delimiter.two_byte_no_delim.runTest");
    }

    @Test(groups = {"gpdb", "hcfs", "security"})
    public void readTwoByteDelimiterWithWrongDelimiter() throws Exception {
        CsvSpec fileSpec = new CsvSpec("¤");
        CsvSpec tableSpec = fileSpec.cloneForFormatting();
        // set the wrong delimiter for the formatterOptions
        tableSpec.setDelimiter("停");

        runScenario("pxf_multibyte_twobyte_wrong_delim_data", tableSpec, dataTable, fileSpec);

        // verify results
        runTincTest("pxf.features.multibyte_delimiter.two_byte_with_wrong_delim.runTest");
    }

    @Test(groups = {"gpdb", "hcfs", "security"})
    public void readTwoByteDelimiterWithQuote() throws Exception {
        CsvSpec fileSpec = new CsvSpec("¤", CSVWriter.DEFAULT_QUOTE_CHARACTER, CSVWriter.NO_ESCAPE_CHARACTER);
        CsvSpec tableSpec = fileSpec.cloneForFormatting();

        runScenario("pxf_multibyte_twobyte_withquote_data", tableSpec, dataTable, fileSpec);

        // verify results
        runTincTest("pxf.features.multibyte_delimiter.two_byte_with_quote.runTest");
    }

    @Test(groups = {"gpdb", "hcfs", "security"})
    public void readTwoByteDelimiterWithWrongEol() throws Exception {
        CsvSpec fileSpec = new CsvSpec("¤", CSVWriter.DEFAULT_QUOTE_CHARACTER, CSVWriter.DEFAULT_ESCAPE_CHARACTER);
        CsvSpec tableSpec = fileSpec.cloneForFormatting();
        // set the wrong eol for the formatterOptions
        tableSpec.setEol("CR");

        runScenario("pxf_multibyte_twobyte_wrong_eol_data", tableSpec, dataTable, fileSpec);

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
        CsvSpec fileSpec = new CsvSpec("¤", CSVWriter.DEFAULT_QUOTE_CHARACTER, CSVWriter.DEFAULT_ESCAPE_CHARACTER);
        CsvSpec tableSpec = fileSpec.cloneForFormatting();
        // set the wrong quote for the formatterOptions
        tableSpec.setQuote('|');

        runScenario("pxf_multibyte_twobyte_wrong_quote_data", tableSpec, dataTable, fileSpec);

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
        CsvSpec fileSpec = new CsvSpec("¤", '|', '\\');
        CsvSpec tableSpec = fileSpec.cloneForFormatting();

        dataTable.addRow(ROW_WITH_ESCAPE);

        runScenario("pxf_multibyte_twobyte_withquote_withescape_data", tableSpec, dataTable, fileSpec);

        // verify results
        runTincTest("pxf.features.multibyte_delimiter.two_byte_with_quote_and_escape.runTest");
    }

    @Test(groups = {"gpdb", "hcfs", "security"})
    public void readTwoByteDelimiterWithWrongEscape() throws Exception {
        CsvSpec fileSpec = new CsvSpec("¤", '|', '\\');
        CsvSpec tableSpec = fileSpec.cloneForFormatting();
        tableSpec.setEscape('#');

        dataTable.addRow(ROW_WITH_ESCAPE);

        runScenario("pxf_multibyte_twobyte_wrong_escape_data", tableSpec, dataTable, fileSpec);

        // verify results
        runTincTest("pxf.features.multibyte_delimiter.two_byte_with_wrong_escape.runTest");
    }

    // users should still be able to use a normal delimiter with this formatter
    @Test(groups = {"gpdb", "hcfs", "security"})
    public void readOneByteDelimiter() throws Exception {
        CsvSpec fileSpec = new CsvSpec("|");
        CsvSpec tableSpec = fileSpec.cloneForFormatting();

        runScenario("pxf_multibyte_onebyte_data", tableSpec, dataTable, fileSpec);

        // verify results
        runTincTest("pxf.features.multibyte_delimiter.one_byte.runTest");
    }


    @Test(groups = {"gpdb", "hcfs", "security"})
    public void readOneCol() throws Exception {
        CsvSpec fileSpec = new CsvSpec("¤");
        CsvSpec tableSpec = fileSpec.cloneForFormatting();

        exTable.setFields(new String[]{"s1 text"});

        dataTable = new Table("data", null);
        dataTable.addRow(new String[]{"tá sé seo le tástáil dea-"});
        dataTable.addRow(new String[]{"règles d'automation"});
        dataTable.addRow(new String[]{"minden amire szüksége van a szeretet"});

        runScenario("pxf_multibyte_onecol_data", tableSpec, dataTable, fileSpec);

        // verify results
        runTincTest("pxf.features.multibyte_delimiter.one_col.runTest");
    }

    @Test(groups = {"gpdb", "hcfs", "security"})
    public void readOneColQuote() throws Exception {
        CsvSpec fileSpec = new CsvSpec("¤", '|', CSVWriter.NO_ESCAPE_CHARACTER);
        CsvSpec tableSpec = fileSpec.cloneForFormatting();

        exTable.setFields(new String[]{"s1 text"});

        dataTable = new Table("data", null);
        dataTable.addRow(new String[]{"tá sé seo le tástáil dea-"});
        dataTable.addRow(new String[]{"règles d'automation"});
        dataTable.addRow(new String[]{"minden amire szüksége van a szeretet"});

        runScenario("pxf_multibyte_onecol_quote_data", tableSpec, dataTable, fileSpec);

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

        createCsvExternalTable("pxf_multibyte_twobyte_withbzip2_data",
                new String[] {
                        "name text",
                        "num integer",
                        "dub double precision",
                        "longNum bigint",
                        "bool boolean"
                },
                protocol.getExternalTablePath(hdfs.getBasePath(), hdfs.getWorkingDirectory())+ "/bzip2/",
                new String[] {"delimiter='¤'"});

        runTincTest("pxf.features.multibyte_delimiter.two_byte_with_bzip2.runTest");
    }

    @Test(groups = {"gpdb", "hcfs", "security"})
    public void readTwoByteWithQuoteEscapeNewLine() throws Exception {
        CsvSpec fileSpec = new CsvSpec("¤", '|', '\\', "EOL");
        CsvSpec tableSpec = fileSpec.cloneForFormatting();

        dataTable.addRow(ROW_WITH_ESCAPE);

        runScenario("pxf_multibyte_quote_escape_newline_data", tableSpec, dataTable, fileSpec);

        // verify results
        runTincTest("pxf.features.multibyte_delimiter.quote_escape_newline.runTest");
    }

    @Test(groups = {"gpdb", "hcfs", "security"})
    public void invalidCodePoint() throws Exception {
        exTable.setName("pxf_multibyte_invalid_codepoint_data");
        exTable.setFormatterOptions(new String[] {"delimiter=E'\\xA4'"});

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
        CsvSpec fileSpec = new CsvSpec("¤");
        // set the encoding value since the default value in CsvSpec is UTF-8
        fileSpec.setEncoding(StandardCharsets.ISO_8859_1);
        CsvSpec tableSpec = fileSpec.cloneForFormatting();

        exTable.setFields(new String[]{"num1 int", "word text"});
        exTable.setEncoding("LATIN1");

        runScenario("pxf_multibyte_encoding", tableSpec, encodedDataTable, fileSpec);

        // verify results
        runTincTest("pxf.features.multibyte_delimiter.encoding.runTest");
    }

    @Test(groups = {"gpdb", "hcfs", "security"})
    public void readFileWithLatin1EncodingByteRepresentationTextProfile() throws Exception {
        CsvSpec fileSpec = new CsvSpec("¤");
        // set the encoding value since the default value in CsvSpec is UTF-8
        fileSpec.setEncoding(StandardCharsets.ISO_8859_1);
        CsvSpec tableSpec = fileSpec.cloneForFormatting();
        // use byte encoding instead
        tableSpec.setDelimiter("\\xC2\\xA4");

        exTable.setFields(new String[]{"num1 int", "word text"});
        exTable.setEncoding("LATIN1");
        exTable.setProfile(protocol.value() + ":text");

        runScenario("pxf_multibyte_encoding_bytes", tableSpec, encodedDataTable, fileSpec);

        // verify results
        runTincTest("pxf.features.multibyte_delimiter.encoding_bytes.runTest");
    }

    @Test(groups = {"gpdb", "hcfs", "security"})
    public void readFileWithLatin1EncodingWithQuoteTextProfile() throws Exception {
        CsvSpec fileSpec = new CsvSpec("¤", '|', '|');
        // set the encoding value since the default value in CsvSpec is UTF-8
        fileSpec.setEncoding(StandardCharsets.ISO_8859_1);
        CsvSpec tableSpec = fileSpec.cloneForFormatting();

        exTable.setFields(new String[]{"num1 int", "word text"});
        exTable.setEncoding("LATIN1");
        exTable.setProfile(protocol.value() + ":text");

        runScenario("pxf_multibyte_encoding_quote", tableSpec, encodedDataTable, fileSpec);

        // verify results
        runTincTest("pxf.features.multibyte_delimiter.encoding_quote.runTest");
    }

    @Test(groups = {"gpdb", "hcfs", "security"})
    public void readFileWithLatin1EncodingWithQuoteAndEscapeTextProfile() throws Exception {
        CsvSpec fileSpec = new CsvSpec("¤", '|', '\"');
        // set the encoding value since the default value in CsvSpec is UTF-8
        fileSpec.setEncoding(StandardCharsets.ISO_8859_1);
        CsvSpec tableSpec = fileSpec.cloneForFormatting();

        exTable.setFields(new String[]{"num1 int", "word text"});
        exTable.setEncoding("LATIN1");
        exTable.setProfile(protocol.value() + ":text");

        runScenario("pxf_multibyte_encoding_quote_escape", tableSpec, encodedDataTable, fileSpec);

        // verify results
        runTincTest("pxf.features.multibyte_delimiter.encoding_quote_escape.runTest");
    }

    @Test(groups = {"gpdb", "hcfs", "security"})
    public void wrongProfileWithFormatter() throws Exception {
        exTable.setName("pxf_multibyte_wrong_profile");
        exTable.setFormatterOptions(new String[] {"delimiter='¤'", "quote='|'", "escape='\"'"});
        exTable.setProfile(protocol.value() + ":avro");
        exTable.setFields(new String[]{"name text", "age int"});

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

    private void createCsvExternalTable(String name, String[] cols, String path, String[] formatterOptions) throws Exception {
        exTable = TableFactory.getPxfReadableTextTable(name, cols, path, null);
        exTable.setFormat("CUSTOM");
        exTable.setFormatter("pxfdelimited_import");
        exTable.setProfile(protocol.value() + ":csv");
        exTable.setFormatterOptions(formatterOptions);

        gpdb.createTableAndVerify(exTable);
    }

    private void writeCsvFileToHdfs(Table dataTable, CsvSpec spec) throws Exception {
        // create local CSV
        String tempLocalDataPath = dataTempFolder + "/data.csv";
        if (spec.delimiter.length() > 1) {
            CsvUtils.writeTableToCsvFile(dataTable, tempLocalDataPath, spec.encoding,
                    '|', spec.quote, spec.escape, spec.eol);
            CsvUtils.updateDelim(tempLocalDataPath, '|', spec.delimiter);
        } else {
            CsvUtils.writeTableToCsvFile(dataTable, tempLocalDataPath, spec.encoding,
                    spec.delimiter.charAt(0), spec.quote, spec.escape, spec.eol);
        }

        // copy local CSV to HDFS
        hdfs.copyFromLocal(tempLocalDataPath, hdfsFilePath);
        sleep(500);
    }

    private void runScenario(String tableName, CsvSpec tableSpec, Table dataTable, CsvSpec fileSpec) throws Exception {
        exTable.setName(tableName);
        // set the formatter options using the table spec
        if (tableSpec.delimiter != null) {
            exTable.addFormatterOption("delimiter=E'" + tableSpec.delimiter + "'");
        }
        if (tableSpec.quote != CSVWriter.NO_QUOTE_CHARACTER) {
            exTable.addFormatterOption("quote='" + tableSpec.quote + "'");
        }
        if (tableSpec.escape != CSVWriter.NO_ESCAPE_CHARACTER) {
            exTable.addFormatterOption("escape='" + tableSpec.escape + "'");
        }
        if (tableSpec.eol != null) {
            exTable.addFormatterOption("newline='" + tableSpec.eol + "'");
        }

        // create external table
        gpdb.createTableAndVerify(exTable);

        // create CSV file in hdfs using the provided data table and file spec
        writeCsvFileToHdfs(dataTable, fileSpec);
    }
}
