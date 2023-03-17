package org.greenplum.pxf.automation.features.hcfs.fixed;

import org.greenplum.pxf.automation.features.BaseWritableFeature;
import org.greenplum.pxf.automation.structures.tables.basic.Table;
import org.greenplum.pxf.automation.structures.tables.utils.TableFactory;
import org.greenplum.pxf.automation.utils.system.ProtocolEnum;
import org.greenplum.pxf.automation.utils.system.ProtocolUtils;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static java.lang.Thread.sleep;

/**
 * Functional Test for writing fixed data format text files to HCFS
 * The dataset is based on a set of tests available in Greenplum
 * https://github.com/greenplum-db/gpdb/blob/main/contrib/formatter_fixedwidth/data/fixedwidth_small_correct.tbl
 */
public class HcfsFixedWriteTest extends BaseWritableFeature {

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
            "n7='19'" // double precision required width of 19
    };

    private static final String[] ROW_VALUES_TEMPLATE = new String[]{
            "%1$s%1$s%1$s", "two%s", "shpits", "2011-06-01 12:30:30",
            "23", "732", "834567", "45.67", "789.123", "7.12345", "123.456789"
    };

    private Table dataTable;
    private String hdfsPath;
    private ProtocolEnum protocol;


    /**
     * Prepare all components and all data flow (Hdfs to GPDB)
     */
    @Override
    public void beforeClass() throws Exception {
        super.beforeClass();
        protocol = ProtocolUtils.getProtocol();

        // create and populate internal GP data with the sample dataset
        dataTable = new Table("fixed_small_data_table", SMALL_DATA_FIELDS);
        dataTable.setDistributionFields(new String[]{"s1"});
        prepareData();
        gpdb.createTableAndVerify(dataTable);
        gpdb.insertData(dataTable, dataTable);

        // path for storing data on HCFS (for processing by PXF)
        hdfsPath = hdfs.getWorkingDirectory() + "/writableFixed/";
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
     * Write fixed width formatted file to HCFS using *:fixed profile and fixedwidth_in format
     * and then read it back using PXF readable external table for verification.
     */
    @Test(groups = {"features", "gpdb", "hcfs", "security"})
    public void writeFixedFile_NoCompression() throws Exception {
        String targetDataDir = hdfsPath + "lines/nocomp/";
        prepareWritableTable("fixed_out_small_correct_write", SMALL_DATA_FIELDS, SMALL_DATA_FORMATTER_OPTIONS, targetDataDir);
        gpdb.createTableAndVerify(writableExTable);

        insertDataIntoWritableTable();

        prepareReadableTable("fixed_out_small_correct_read", SMALL_DATA_FIELDS, SMALL_DATA_FORMATTER_OPTIONS, targetDataDir);
        gpdb.createTableAndVerify(readableExTable);

        runTincTest("pxf.features.hcfs.fixed.write.small_data_correct.runTest");
    }

/*
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
        // table without NEWLINE option header - reads the whole file as a single line
        prepareReadableTable("fixed_in_small_correct_custom_delim", SMALL_DATA_FIELDS, SMALL_DATA_FORMATTER_OPTIONS,
                hdfsPath + fixedSmallCorrectCustomDelimFileName);
        exTable.addFormatterOption("line_delim='@#$'");
        gpdb.createTableAndVerify(exTable);

        // table with NEWLINE option header -- returns an ERROR as PXF only allows CR/LF/CRLF as new line separator values
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
*/

    /**
     * Prepares a set of data with 9 rows from a row template to correspond to "fixed_small_correct.txt" dataset
     */
    private void prepareData() {
        for (int i = 0; i < 10; i++) {
            char letter = (char) ('a' + i);
            List<String> row = Arrays
                    .stream(ROW_VALUES_TEMPLATE)
                    .map(column -> String.format(column, letter))
                    .collect(Collectors.toList());
            dataTable.addRow(row);
        }
    }

    /**
     * Instructs GPDB to insert data from internal table into PXF external writable table.
     * @throws Exception if the operation fails
     */
    private void insertDataIntoWritableTable() throws Exception {
        gpdb.runQuery("INSERT INTO " + writableExTable.getName() + " SELECT * FROM " + dataTable.getName());

        // for HCFS on Cloud, wait a bit for async write in the previous step to finish
        if (protocol != ProtocolEnum.HDFS && protocol != ProtocolEnum.FILE) {
            sleep(10000);
        }
    }


    private void prepareReadableTable(String name, String[] fields, String[] formatterOptions, String path) {
        // default external table with common settings
        readableExTable = TableFactory.getPxfHcfsReadableTable(name, fields, path, hdfs.getBasePath(), "fixed");
        readableExTable.setFormatter("fixedwidth_in");
        readableExTable.setFormatterOptions(formatterOptions);
    }

    private void prepareWritableTable(String name, String[] fields, String[] formatterOptions, String path) {
        // default external table with common settings
        writableExTable = TableFactory.getPxfHcfsWritableTable(name, fields, path, hdfs.getBasePath(), "fixed");
        writableExTable.setFormatter("fixedwidth_out");
        writableExTable.setFormatterOptions(formatterOptions);
    }

}
