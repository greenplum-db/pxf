package org.greenplum.pxf.automation.features.hcfs.fixedwidth;

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
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

/**
 * Functional Test for writing fixedwidth data format text files to HCFS
 * The dataset is based on a set of tests available in Greenplum
 * https://github.com/greenplum-db/gpdb/blob/main/contrib/formatter_fixedwidth/data/fixedwidth_small_correct.tbl
 */
public class HcfsFixedwidthWriteTest extends BaseWritableFeature {

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
        dataTable = new Table("fixedwidth_small_data_table", SMALL_DATA_FIELDS);
        dataTable.setDistributionFields(new String[]{"s1"});
        prepareData();
        gpdb.createTableAndVerify(dataTable);
        gpdb.insertData(dataTable, dataTable);

        // path for storing data on HCFS (for processing by PXF)
        hdfsPath = hdfs.getWorkingDirectory() + "/writableFixedwidth/";
    }

    /**
     * Write fixed width formatted file to HCFS using *:fixedwidth profile and fixedwidth_out format
     * and then read it back using PXF readable external table for verification.
     */
    @Test(groups = {"features", "gpdb", "hcfs", "security"})
    public void writeFixedwidthFile_NoCompression() throws Exception {
        runScenario("default", null, null, false);
    }

    @Test(groups = {"features", "gpdb", "hcfs", "security"})
    public void writeFixedwidthFile_GzipCompression() throws Exception {
        runScenario("gzip", null, null, true);
    }

    // ========== Delimiter Tests ==========

    @Test(groups = {"features", "gpdb", "hcfs", "security"})
    public void writeFixedwidthFile_CustomLineDelimiter() throws Exception {
        runScenario("custom_delim", "@#$", "@#$", false);
    }

    @Test(groups = {"features", "gpdb", "hcfs", "security"})
    public void writeFixedwidthFile_CRLineDelimiter() throws Exception {
        runScenario("cr_delim", "\\r", "cr", false);
    }

    @Test(groups = {"features", "gpdb", "hcfs", "security"})
    public void writeFixedwidthFile_CRLFLineDelimiter() throws Exception {
        runScenario("crlf_delim", "\\r\\n", "crlf", false);
    }

    @Test(groups = {"features", "gpdb", "hcfs", "security"})
    public void writeFixedwidthFile_LFLineDelimiter() throws Exception {
        // this is also a default case, but let's check if delimiters are declared explicitly
        runScenario("lf_delim", "\\n", "lf", false);
    }

    /**
     * Runs a test scenario of inserting data from Greenplum internal table into a PXF writable external table
     * and then reading it back using PXF readable external table in the Tinc test
     * @param name name of the scenario, used by convention in names of external tables and tinc tests
     * @param formatterDelimiter delimiter value to specify for fixedwidth formatter
     * @param pxfDelimiter delimiter value to specify for PXF external table NEWLINE option
     * @param compression true if compression should be use for writable table, false otherwise
     * @throws Exception if any operation fails
     */
    private void runScenario(String name, String formatterDelimiter, String pxfDelimiter, boolean compression) throws Exception {
        String targetDataDir = hdfsPath + name + "/";
        String formatterDelimiterOption = (formatterDelimiter == null) ? null : String.format("line_delim=E'%s'", formatterDelimiter);

        prepareWritableTable("fixedwidth_out_small_correct_" + name + "_write", SMALL_DATA_FIELDS, SMALL_DATA_FORMATTER_OPTIONS, targetDataDir);
        if (formatterDelimiter != null) {
            writableExTable.addFormatterOption(formatterDelimiterOption);
        }
        if (compression) {
            writableExTable.setCompressionCodec("org.apache.hadoop.io.compress.GzipCodec");
        }
        gpdb.createTableAndVerify(writableExTable);

        insertDataIntoWritableTable();

        // verify all written file names end with ".gz" that would indicate that compression worked
        if (compression) {
            List<String> files = hdfs.list(targetDataDir);
            assertNotNull(files);
            assertTrue(files.size() > 0);
            assertTrue(files.stream().allMatch(file -> file.endsWith(".gz")));
        }

        prepareReadableTable("fixedwidth_out_small_correct_" + name + "_read", SMALL_DATA_FIELDS, SMALL_DATA_FORMATTER_OPTIONS, targetDataDir);
        if (formatterDelimiter != null) {
            readableExTable.addFormatterOption(formatterDelimiterOption);
        }
        if (pxfDelimiter != null) {
            readableExTable.addUserParameter("NEWLINE=" + pxfDelimiter);
        }
        gpdb.createTableAndVerify(readableExTable);

        runTincTest("pxf.features.hcfs.fixedwidth.write.small_data_correct_" + name + ".runTest");
    }

    /**
     * Prepares a set of data with 9 rows from a row template to correspond to "fixedwidth_small_correct.txt" dataset
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
        readableExTable = TableFactory.getPxfHcfsReadableTable(name, fields, path, hdfs.getBasePath(), "fixedwidth");
        readableExTable.setFormatter("fixedwidth_in");
        readableExTable.setFormatterOptions(formatterOptions);
    }

    private void prepareWritableTable(String name, String[] fields, String[] formatterOptions, String path) {
        // default external table with common settings
        writableExTable = TableFactory.getPxfHcfsWritableTable(name, fields, path, hdfs.getBasePath(), "fixedwidth");
        writableExTable.setFormatter("fixedwidth_out");
        writableExTable.setFormatterOptions(formatterOptions);
    }

}
