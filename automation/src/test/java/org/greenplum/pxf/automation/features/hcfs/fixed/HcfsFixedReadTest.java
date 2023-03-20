package org.greenplum.pxf.automation.features.hcfs.fixed;

import org.greenplum.pxf.automation.features.BaseFeature;
import org.greenplum.pxf.automation.structures.tables.utils.TableFactory;
import org.testng.annotations.Test;

/**
 * Functional Test for reading fixed data format text files in HCFS
 * The dataset is based on a set of tests available in Greenplum
 * https://github.com/greenplum-db/gpdb/blob/main/contrib/formatter_fixedwidth/data/fixedwidth_small_correct.tbl
 */
public class HcfsFixedReadTest extends BaseFeature {

    private static final String[] SMALL_DATA_FIELDS = new String[]{
            "s1 char(10)",
            "s2 varchar(10)",
            "s3 text",
            "dt timestamp",
            "n1 smallint",
            "n2 integer",
            "n3 bigint",
            "n4 decimal",
            "n5 numeric",
            "n6 real",
            "n7 double precision"
    };

    private static final String[] SMALL_DATA_FORMATTER_OPTIONS = new String[]{
            "s1='10'",
            "s2='10'",
            "s3='10'",
            "dt='20'",
            "n1='5'",
            "n2='10'",
            "n3='10'",
            "n4='10'",
            "n5='10'",
            "n6='10'",
            "n7='15'"
    };

    private static final String fixedSmallCorrectFileName = "fixed_small_correct.txt";
    private static final String fixedSmallCorrectGzipFileName = "fixed_small_correct.txt.gz";
    private static final String fixedSmallCorrectCustomDelimFileName = "fixed_small_correct_custom_delim.txt";
    private static final String fixedSmallCorrectCRDelimFileName = "fixed_small_correct_cr_delim.txt";
    private static final String fixedSmallCorrectCRLFDelimFileName = "fixed_small_correct_crlf_delim.txt";

    private String hdfsPath;
    private String resourcePath;

    /**
     * Prepare all components and all data flow (Hdfs to GPDB)
     */
    @Override
    public void beforeClass() throws Exception {
        // path for storing data on HCFS (for processing by PXF)
        hdfsPath = hdfs.getWorkingDirectory() + "/readableFixed/";

        // location of the data files
        String absolutePath = getClass().getClassLoader().getResource("data").getPath();
        resourcePath = absolutePath + "/fixed/";
        hdfs.copyFromLocal(resourcePath + fixedSmallCorrectFileName, hdfsPath + "lines/nocomp/" + fixedSmallCorrectFileName);
        hdfs.copyFromLocal(resourcePath + fixedSmallCorrectGzipFileName, hdfsPath + "lines/gzip/" + fixedSmallCorrectGzipFileName);
        hdfs.copyFromLocal(resourcePath + fixedSmallCorrectCustomDelimFileName, hdfsPath + fixedSmallCorrectCustomDelimFileName);
        hdfs.copyFromLocal(resourcePath + fixedSmallCorrectCRDelimFileName, hdfsPath + fixedSmallCorrectCRDelimFileName);
        hdfs.copyFromLocal(resourcePath + fixedSmallCorrectCRLFDelimFileName, hdfsPath + fixedSmallCorrectCRLFDelimFileName);
    }

    /**
     * Before every method determine default hdfs data Path, default data, and
     * default external table structure. Each case change it according to it
     * needs.
     */
    @Override
    protected void beforeMethod() throws Exception {
        super.beforeMethod();
    }

    /**
     * Read fixed width formatted file from HCFS using *:fixed profile and fixedwidth_in format.
     */
    @Test(groups = {"features", "gpdb", "hcfs", "security"})
    public void readFixedFile_NoCompression() throws Exception {
        prepareReadableTable("fixed_in_small_correct", SMALL_DATA_FIELDS, SMALL_DATA_FORMATTER_OPTIONS,
                hdfsPath + "lines/nocomp/" + fixedSmallCorrectFileName);
        gpdb.createTableAndVerify(exTable);
        runTincTest("pxf.features.hcfs.fixed.read.small_data_correct.runTest");
    }

    @Test(groups = {"features", "gpdb", "hcfs", "security"})
    public void readFixedFile_GzipCompression() throws Exception {
        prepareReadableTable("fixed_in_small_correct_gzip", SMALL_DATA_FIELDS, SMALL_DATA_FORMATTER_OPTIONS,
                hdfsPath + "lines/gzip/" + fixedSmallCorrectGzipFileName);
        gpdb.createTableAndVerify(exTable);
        runTincTest("pxf.features.hcfs.fixed.read.small_data_correct_gzip.runTest");
    }

    @Test(groups = {"features", "gpdb", "hcfs", "security"})
    public void readFixedFiles_WithAndWithoutCompression() throws Exception {
        prepareReadableTable("fixed_in_small_correct_mixed", SMALL_DATA_FIELDS, SMALL_DATA_FORMATTER_OPTIONS,
                hdfsPath + "lines");
        gpdb.createTableAndVerify(exTable);
        runTincTest("pxf.features.hcfs.fixed.read.small_data_correct_mixed.runTest");
    }

    // ========== Delimiter Tests ==========

    @Test(groups = {"features", "gpdb", "hcfs", "security"})
    public void readFixedFile_CustomLineDelimiter() throws Exception {
        // table without NEWLINE option header - reads the whole file as a single line, might be a problem
        prepareReadableTable("fixed_in_small_correct_custom_delim", SMALL_DATA_FIELDS, SMALL_DATA_FORMATTER_OPTIONS,
                hdfsPath + fixedSmallCorrectCustomDelimFileName);
        exTable.addFormatterOption("line_delim='@#$'");
        gpdb.createTableAndVerify(exTable);

        // table with NEWLINE option header -- correct way - streams lines one by one
        prepareReadableTable("fixed_in_small_correct_custom_delim_header", SMALL_DATA_FIELDS, SMALL_DATA_FORMATTER_OPTIONS,
                hdfsPath + fixedSmallCorrectCustomDelimFileName);
        exTable.addFormatterOption("line_delim='@#$'");
        exTable.addUserParameter("NEWLINE=@#$");
        gpdb.createTableAndVerify(exTable);

        runTincTest("pxf.features.hcfs.fixed.read.small_data_correct_custom_delim.runTest");
    }

    @Test(groups = {"features", "gpdb", "hcfs", "security"})
    public void readFixedFile_CRLineDelimiter() throws Exception {
        // table without NEWLINE option header - returns data but issues a warning
        prepareReadableTable("fixed_in_small_correct_cr_delim", SMALL_DATA_FIELDS, SMALL_DATA_FORMATTER_OPTIONS,
                hdfsPath + fixedSmallCorrectCRDelimFileName);
        exTable.addFormatterOption("line_delim=E'\\r'");
        gpdb.createTableAndVerify(exTable);

        // table with NEWLINE option header
        prepareReadableTable("fixed_in_small_correct_cr_delim_header", SMALL_DATA_FIELDS, SMALL_DATA_FORMATTER_OPTIONS,
                hdfsPath + fixedSmallCorrectCRDelimFileName);
        exTable.addFormatterOption("line_delim=E'\\r'");
        exTable.addUserParameter("NEWLINE=cr");
        gpdb.createTableAndVerify(exTable);

        runTincTest("pxf.features.hcfs.fixed.read.small_data_correct_cr_delim.runTest");
    }

    @Test(groups = {"features", "gpdb", "hcfs", "security"})
    public void readFixedFile_CRLFLineDelimiter() throws Exception {
        // table without NEWLINE option header
        // there is no need to add exTable.addUserParameter("NEWLINE=crlf"); since CR character will be preserved
        // and LF will be added back by PXF (by default) and both of them will be removed by the formatter
        prepareReadableTable("fixed_in_small_correct_crlf_delim", SMALL_DATA_FIELDS, SMALL_DATA_FORMATTER_OPTIONS,
                hdfsPath + fixedSmallCorrectCRLFDelimFileName);
        exTable.addFormatterOption("line_delim=E'\\r\\n'");
        gpdb.createTableAndVerify(exTable);

        // table with NEWLINE option header
        prepareReadableTable("fixed_in_small_correct_crlf_delim_header", SMALL_DATA_FIELDS, SMALL_DATA_FORMATTER_OPTIONS,
                hdfsPath + fixedSmallCorrectCRLFDelimFileName);
        exTable.addFormatterOption("line_delim=E'\\r\\n'");
        exTable.addUserParameter("NEWLINE=crlf");
        gpdb.createTableAndVerify(exTable);

        runTincTest("pxf.features.hcfs.fixed.read.small_data_correct_crlf_delim.runTest");
    }

    private void prepareReadableTable(String name, String[] fields, String[] formatterOptions, String path) {
        // default external table with common settings
        exTable = TableFactory.getPxfHcfsReadableTable(name, fields, path, hdfs.getBasePath(), "fixed");
        exTable.setFormatter("fixedwidth_in");
        exTable.setFormatterOptions(formatterOptions);
    }

}
