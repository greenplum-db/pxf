package org.greenplum.pxf.automation.features.writable;

import org.greenplum.pxf.automation.components.cluster.SingleCluster;
import org.greenplum.pxf.automation.datapreparer.CustomSequencePreparer;
import org.greenplum.pxf.automation.features.BaseWritableFeature;
import org.greenplum.pxf.automation.structures.tables.basic.Table;
import org.greenplum.pxf.automation.structures.tables.utils.TableFactory;
import org.greenplum.pxf.automation.utils.exception.ExceptionUtils;
import org.greenplum.pxf.automation.utils.fileformats.FileFormatsUtils;
import org.apache.commons.lang.StringUtils;
import org.greenplum.pxf.automation.components.cluster.PhdCluster;
import org.junit.Assert;
import org.junit.Ignore;
import org.postgresql.util.PSQLException;
import org.testng.annotations.Test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

/**
 * Collection of Test cases for PXF ability to write SequenceFile.
 */
public class HdfsWritableSequenceTest extends BaseWritableFeature {

    private String hdfsPath;

    private String schemaPackage = "org.greenplum.pxf.automation.dataschema.";
    private String customSchemaFileName = "CustomWritable";
    private String customSchemaWithCharFileName = "CustomWritableWithChar";
    private String customSchemaWithCircleFileName = "CustomWritableWithCircle";

    private String[] customWritableFields = {
            "tmp1   TIMESTAMP",
            "num1   INTEGER",
            "num2   INTEGER",
            "num3   INTEGER",
            "num4   INTEGER",
            "t1     TEXT",
            "t2     TEXT",
            "t3     TEXT",
            "t4     TEXT",
            "t5     TEXT",
            "t6     TEXT",
            "dub1   DOUBLE PRECISION",
            "dub2   DOUBLE PRECISION",
            "dub3   DOUBLE PRECISION",
            "ft1    REAL",
            "ft2    REAL",
            "ft3    REAL",
            "ln1    BIGINT",
            "ln2    BIGINT",
            "ln3    BIGINT",
            "bool1  BOOLEAN",
            "bool2  BOOLEAN",
            "bool3  BOOLEAN",
            "short1 SMALLINT",
            "short2 SMALLINT",
            "short3 SMALLINT",
            "short4 SMALLINT",
            "short5 SMALLINT",
            "bt     BYTEA",
    };

    @Override
    protected void beforeClass() throws Exception {
        super.beforeClass();

        // path for storing data on HDFS (for processing by PXF)
        hdfsPath = hdfs.getWorkingDirectory() + "/sequence/";

        // location of schema and data files
        String schemaPackageLocation = "/org/greenplum/pxf/automation/dataschema/";
        String resourcePath = "target/classes" + schemaPackageLocation;
        String SUFFIX_CLASS = ".class";

        // copy schema file to all nodes
        String newPath = "/tmp/publicstage/pxf";
        String target = newPath + schemaPackageLocation;
        // copy schema file to cluster nodes, used for avro in sequence cases
        cluster.copyFileToNodes(new File(resourcePath + customSchemaFileName +
                SUFFIX_CLASS).getAbsolutePath(), target, true, false);
        cluster.copyFileToNodes(new File(resourcePath + customSchemaWithCharFileName +
                SUFFIX_CLASS).getAbsolutePath(), target, true, false);
        cluster.copyFileToNodes(new File(resourcePath + customSchemaWithCircleFileName +
                SUFFIX_CLASS).getAbsolutePath(), target, true, false);
        // add new path to classpath file and restart PXF service
        cluster.addPathToPxfClassPath(newPath);
        cluster.restart(PhdCluster.EnumClusterServices.pxf);

        // create and copy data to hdfs
        prepareData();
    }

    private void prepareData() throws Exception {

        String writableInsideSequenceFileName = "writable_inside_sequence.tbl";
        Table dataTable = new Table("dataTable", null);
        Object[] data = FileFormatsUtils.prepareData(new CustomSequencePreparer(), 100, dataTable);
        hdfs.writeSequenceFile(data, hdfsPath + writableInsideSequenceFileName);
    }

    @Override
    protected void afterMethod() throws Exception {
        super.afterMethod();
    }

    @Override
    protected void beforeMethod() throws Exception {

        writableExTable = TableFactory.getPxfWritableSequenceTable(writableTableName,
                null, hdfsWritePath + writableTableName, null);
        writableExTable.setHost(pxfHost);
        writableExTable.setPort(pxfPort);

        readableExTable = TableFactory.getPxfReadableSequenceTable(readableTableName,
                null, hdfsWritePath + writableTableName, null);
        readableExTable.setHost(pxfHost);
        readableExTable.setPort(pxfPort);
    }

    /**
     * Sequence file write and read - all compression combinations
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = { "features", "gpdb" })
    @Ignore("flaky ssh connection")
    public void writeAndRead() throws Exception {

        String[] codecs = { null,
                "org.apache.hadoop.io.compress.DefaultCodec",
                "org.apache.hadoop.io.compress.BZip2Codec" };
        String[][] userParams = new String[][] { null,
                { "COMPRESSION_TYPE=RECORD" },
                { "COMPRESSION_TYPE=BLOCK" } };

        Table dataTable = new Table("dataTable", null);
        FileFormatsUtils.prepareData(new CustomSequencePreparer(), 100, dataTable);
        File path = new File(dataTempFolder + "/customwritable_data.txt");
        createCustomWritableDataFile(dataTable, path, false);

        writableExTable.setFields(customWritableFields);
        writableExTable.setDataSchema(schemaPackage + customSchemaFileName);
        // change name to match existing tinc test
        readableExTable.setName("writable_in_sequence");
        readableExTable.setFields(customWritableFields);
        readableExTable.setDataSchema(schemaPackage + customSchemaFileName);

        int testNum = 1;
        for (String codec : codecs) {
            for (String[] userParam : userParams) {

                String hdfsDir = hdfsWritePath + writableTableName + testNum;
                writableExTable.setPath(hdfsDir);
                writableExTable.setCompressionCodec(codec);
                writableExTable.setUserParameters(userParam);
                gpdb.createTableAndVerify(writableExTable);

                readableExTable.setPath(hdfsDir);
                gpdb.createTableAndVerify(readableExTable);

                gpdb.copyFromFile(writableExTable, path, null, false);
                Assert.assertNotEquals(hdfs.listSize(hdfsDir), 0);

                runTincTest("pxf.features.hdfs.readable.sequence.custom_writable.runTest");
                ++testNum;
            }
        }
    }

    /**
     * Test circle type converted to text and back
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = { "features", "gpdb" })
    public void circleType() throws Exception {

        String[] fields = { "a1 INTEGER", "c1 CIRCLE" };
        String hdfsDir = hdfsWritePath + writableTableName + "circle";

        writableExTable.setName("wr_circle");
        writableExTable.setFields(fields);
        writableExTable.setPath(hdfsDir);
        writableExTable.setDataSchema(schemaPackage + customSchemaWithCircleFileName);
        gpdb.createTableAndVerify(writableExTable);

        Table dataTable = new Table("circle", null);
        dataTable.addRow(new String[] { "1", "<(3,3),9>" });
        dataTable.addRow(new String[] { "2", "<(4,4),16>" });
        gpdb.insertData(dataTable, writableExTable);

        readableExTable.setName("read_circle");
        readableExTable.setFields(fields);
        readableExTable.setPath(hdfsDir);
        readableExTable.setDataSchema(schemaPackage + customSchemaWithCircleFileName);
        gpdb.createTableAndVerify(readableExTable);

        runTincTest("pxf.features.hdfs.writable.sequence.circle.runTest");
    }

    /**
     * Test unsupported type in writable resolver -- negative
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = { "features", "gpdb" })
    public void negativeCharType() throws Exception {

        String[] fields = { "a1 INTEGER", "c1 CHAR" };
        String hdfsDir = hdfsWritePath + writableTableName + "char";

        writableExTable.setName("wr_char");
        writableExTable.setFields(fields);
        writableExTable.setPath(hdfsDir);
        writableExTable.setDataSchema(schemaPackage + customSchemaWithCharFileName);
        gpdb.createTableAndVerify(writableExTable);

        Table dataTable = new Table("data", null);
        dataTable.addRow(new String[] { "100", "a" });
        dataTable.addRow(new String[] { "1000", "b" });

        try {
            gpdb.insertData(dataTable, writableExTable);
            Assert.fail("Insert data should fail because of unsupported type");
        } catch (PSQLException e) {
            ExceptionUtils.validate(null, e, new PSQLException("ERROR.*Type char is not supported " +
                    "by GPDBWritable.*?", null), true);
        }
    }

    /**
     * Test COMPRESSION_TYPE = NONE -- negative
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = { "features", "gpdb" })
    public void negativeCompressionTypeNone() throws Exception {

        String[] fields = { "a1 INTEGER", "c1 CHAR" };
        String hdfsDir = hdfsWritePath + writableTableName + "none";

        writableExTable.setName("compress_type_none");
        writableExTable.setFields(fields);
        writableExTable.setPath(hdfsDir);
        writableExTable.setDataSchema(schemaPackage + customSchemaWithCharFileName);
        writableExTable.setUserParameters(new String[] { "COMPRESSION_TYPE=NONE" });
        gpdb.createTableAndVerify(writableExTable);

        Table dataTable = new Table("data", null);
        dataTable.addRow(new String[] { "100", "a" });
        dataTable.addRow(new String[] { "1000", "b" });

        try {
            gpdb.insertData(dataTable, writableExTable);
            Assert.fail("Insert data should fail because of illegal compression type");
        } catch (PSQLException e) {
            ExceptionUtils.validate(null, e,
                    new PSQLException("ERROR.*Illegal compression type 'NONE'\\. For disabling compression " +
                            "remove COMPRESSION_CODEC parameter\\..*?", null), true);
        }
    }

    /**
     * Test recordkey for sequence file - recordkey of type text
     * The same schema file will work for two tables - one without recordkey field
     * as a first field, and one with.
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = { "features", "gpdb" })
    public void recordkeyTextType() throws Exception {

        String[] fields = new String[customWritableFields.length + 1];
        fields[0] = "recordkey TEXT";
        System.arraycopy(customWritableFields, 0, fields, 1, customWritableFields.length);

        String hdfsDir = hdfsWritePath + writableTableName + "recordkey";
        writableExTable.setName("writable_recordkey_text");
        writableExTable.setPath(hdfsDir);
        writableExTable.setFields(fields);
        writableExTable.setDataSchema(schemaPackage + customSchemaFileName);
        gpdb.createTableAndVerify(writableExTable);

        Table dataTable = new Table("dataTable", null);
        FileFormatsUtils.prepareData(new CustomSequencePreparer(), 50, dataTable);
        File path = new File(dataTempFolder + "/customwritable_recordkey_data.txt");
        createCustomWritableDataFile(dataTable, path, true);
        gpdb.copyFromFile(writableExTable, path, null, false);

        readableExTable.setName("readable_recordkey_text");
        readableExTable.setPath(hdfsDir);
        readableExTable.setFields(fields);
        readableExTable.setDataSchema(schemaPackage + customSchemaFileName);
        gpdb.createTableAndVerify(readableExTable);

        runTincTest("pxf.features.hdfs.writable.sequence.recordkey_text.runTest");
    }

    /**
     * Test recordkey for sequence file - recordkey of type int.
     * One row will fail - it has a text value
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = { "features", "gpdb" })
    public void recordkeyIntType() throws Exception {

        // Skip this test for tests that are running against a remote cluster
        if (!(cluster instanceof SingleCluster)) {
            return;
        }
        String[] fields = new String[customWritableFields.length + 1];
        fields[0] = "recordkey INT";
        System.arraycopy(customWritableFields, 0, fields, 1, customWritableFields.length);

        String hdfsDir = hdfsWritePath + writableTableName + "recordkey_int";
        writableExTable.setName("writable_recordkey_int");
        writableExTable.setPath(hdfsDir);
        writableExTable.setFields(fields);
        writableExTable.setDataSchema(schemaPackage + customSchemaFileName);
        gpdb.createTableAndVerify(writableExTable);

        Table dataTable = new Table("dataTable", null);
        FileFormatsUtils.prepareData(new CustomSequencePreparer(), 50, dataTable);

        File path = new File(dataTempFolder + "/customwritable_recordkey_data.txt");
        createCustomWritableDataFile(dataTable, path, true);

        String copyCmd = "COPY " + writableExTable.getName() + " FROM '" + path.getAbsolutePath() +
                "'" + "SEGMENT REJECT LIMIT 5 ROWS;";
        String noticeMsg = "Found 1 data formatting errors (1 or more input rows). Rejected related input data.";
        gpdb.runQueryWithExpectedWarning(copyCmd, noticeMsg, false);

        readableExTable.setName("readable_recordkey_int");
        readableExTable.setPath(hdfsDir);
        readableExTable.setFields(fields);
        readableExTable.setDataSchema(schemaPackage + customSchemaFileName);
        gpdb.createTableAndVerify(readableExTable);

        runTincTest("pxf.features.hdfs.writable.sequence.recordkey_int.runTest");
    }

    /**
     * Create data file based on CustomWritable schema. data is written from dataTable and to path file.
     *
     * @param dataTable Data Table
     * @param path File path
     * @param recordkey add recordkey data to the beginning of each row
     *
     * @throws IOException if test fails to run
     */
    private void createCustomWritableDataFile(Table dataTable, File path, boolean recordkey)
            throws IOException {

        path.delete();
        Assert.assertTrue(path.createNewFile());
        BufferedWriter out = new BufferedWriter(new FileWriter(path));

        int i = 0;
        for (List<String> row : dataTable.getData()) {
            if (recordkey) {
                String record = "0000" + ++i;
                if (i == 2) {
                    record = "NotANumber";
                }
                row.add(0, record);
            }
            String formatted = StringUtils.join(row, "\t") + "\n";
            out.write(formatted);
        }

        out.close();
    }
}
