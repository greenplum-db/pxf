package org.greenplum.pxf.automation.features.writable;

import org.apache.commons.lang.StringUtils;
import org.greenplum.pxf.automation.components.cluster.PhdCluster;
import org.greenplum.pxf.automation.components.cluster.SingleCluster;
import org.greenplum.pxf.automation.datapreparer.CustomSequencePreparer;
import org.greenplum.pxf.automation.features.BaseWritableFeature;
import org.greenplum.pxf.automation.structures.tables.basic.Table;
import org.greenplum.pxf.automation.structures.tables.pxf.ReadableExternalTable;
import org.greenplum.pxf.automation.structures.tables.pxf.WritableExternalTable;
import org.greenplum.pxf.automation.structures.tables.utils.TableFactory;
import org.greenplum.pxf.automation.utils.exception.ExceptionUtils;
import org.greenplum.pxf.automation.utils.fileformats.FileFormatsUtils;
import org.greenplum.pxf.automation.utils.system.ProtocolEnum;
import org.greenplum.pxf.automation.utils.system.ProtocolUtils;
import org.junit.Assert;
import org.junit.Ignore;
import org.postgresql.util.PSQLException;
import org.testng.annotations.Test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import static java.lang.Thread.sleep;

/**
 * Collection of Test cases for PXF ability to write SequenceFile.
 */
public class HdfsWritableSequenceTest extends BaseWritableFeature {

    private String hdfsPath;

    private final String schemaPackage = "org.greenplum.pxf.automation.dataschema.";
    private final String customSchemaFileName = "CustomWritable";
    private final String customSchemaWithCharFileName = "CustomWritableWithChar";
    private final String customSchemaWithCircleFileName = "CustomWritableWithCircle";

    private ProtocolEnum protocol;

    private final String[] customWritableFields = {
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

        // protocol being used for this test
        protocol = ProtocolUtils.getProtocol();

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

    /**
     * Sequence file write and read - all compression combinations
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"features", "gpdb", "hcfs", "security"})
    @Ignore("flaky ssh connection")
    public void writeAndRead() throws Exception {

        String[] codecs = {null,
                "org.apache.hadoop.io.compress.DefaultCodec",
                "org.apache.hadoop.io.compress.BZip2Codec"};
        String[][] userParams = new String[][]{null,
                {"COMPRESSION_TYPE=RECORD"},
                {"COMPRESSION_TYPE=BLOCK"}};

        Table dataTable = new Table("dataTable", null);
        FileFormatsUtils.prepareData(new CustomSequencePreparer(), 100, dataTable);
        File path = new File(dataTempFolder + "/customwritable_data.txt");
        createCustomWritableDataFile(dataTable, path, false);

        int testNum = 1;
        for (String codec : codecs) {
            for (String[] userParam : userParams) {

                String hdfsDir = hdfsPath + "/write_and_read_" + testNum;
                writableExTable = prepareWritableSequenceTable("pxf_write_and_read_" + testNum + "_w",
                        customWritableFields, hdfsDir,
                        schemaPackage + customSchemaFileName,
                        userParam, codec);
                gpdb.copyFromFile(writableExTable, path, null, false);

                readableExTable = prepareReadableSequenceTable("writable_in_sequence",
                        customWritableFields, hdfsDir, schemaPackage + customSchemaFileName);

                // for HCFS on Cloud, wait a bit for async write in previous steps to finish
                if (protocol != ProtocolEnum.HDFS && protocol != ProtocolEnum.FILE) {
                    sleep(10000);
                }

                Assert.assertNotEquals(hdfs.listSize(hdfsDir), 0);

                runSqlTest("features/hdfs/readable/sequence/custom_writable");
                ++testNum;
            }
        }
    }

    /**
     * Test circle type converted to text and back
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"features", "gpdb", "hcfs", "security"})
    public void circleType() throws Exception {

        String[] fields = {"a1 INTEGER", "c1 CIRCLE"};
        String hdfsDir = hdfsPath + "/circle_type";
        writableExTable = prepareWritableSequenceTable("pxf_sequence_circle_type_w",
                fields, hdfsDir, schemaPackage + customSchemaWithCircleFileName);

        Table dataTable = new Table("circle", null);
        dataTable.addRow(new String[]{"1", "<(3,3),9>"});
        dataTable.addRow(new String[]{"2", "<(4,4),16>"});
        gpdb.insertData(dataTable, writableExTable);

        readableExTable = prepareReadableSequenceTable("pxf_sequence_circle_type_r",
                fields, hdfsDir, schemaPackage + customSchemaWithCircleFileName);
        runSqlTest("features/hdfs/writable/sequence/circle");
    }

    /**
     * Test unsupported type in writable resolver -- negative
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"features", "gpdb", "hcfs", "security"})
    public void negativeCharType() throws Exception {

        String[] fields = {"a1 INTEGER", "c1 CHAR"};
        String hdfsDir = hdfsPath + "/negative_char_type";
        writableExTable = prepareWritableSequenceTable("pxf_negative_char_type_w",
                fields, hdfsDir, schemaPackage + customSchemaWithCharFileName);

        Table dataTable = new Table("data", null);
        dataTable.addRow(new String[]{"100", "a"});
        dataTable.addRow(new String[]{"1000", "b"});

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
    @Test(groups = {"features", "gpdb", "hcfs", "security"})
    public void negativeCompressionTypeNone() throws Exception {

        String[] fields = {"a1 INTEGER", "c1 CHAR"};
        String hdfsDir = hdfsPath + "/negative_compression_type_none";

        writableExTable = prepareWritableSequenceTable("pxf_negative_compression_type_none",
                fields, hdfsDir, schemaPackage + customSchemaWithCharFileName,
                new String[]{"COMPRESSION_TYPE=NONE"}, null);

        Table dataTable = new Table("data", null);
        dataTable.addRow(new String[]{"100", "a"});
        dataTable.addRow(new String[]{"1000", "b"});

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
    @Test(groups = {"features", "gpdb", "hcfs", "security"})
    public void recordkeyTextType() throws Exception {

        String[] fields = new String[customWritableFields.length + 1];
        fields[0] = "recordkey TEXT";
        System.arraycopy(customWritableFields, 0, fields, 1, customWritableFields.length);

        String hdfsDir = hdfsPath + "/recordkey_text_type";
        writableExTable = prepareWritableSequenceTable("pxf_recordkey_text_type_w",
                fields, hdfsDir, schemaPackage + customSchemaFileName);

        Table dataTable = new Table("dataTable", null);
        FileFormatsUtils.prepareData(new CustomSequencePreparer(), 50, dataTable);
        File path = new File(dataTempFolder + "/customwritable_recordkey_data.txt");
        createCustomWritableDataFile(dataTable, path, true);
        gpdb.copyFromFile(writableExTable, path, null, false);

        readableExTable = prepareReadableSequenceTable("pxf_recordkey_text_type_r",
                fields, hdfsDir, schemaPackage + customSchemaFileName);
        runSqlTest("features/hdfs/writable/sequence/recordkey_text");
    }

    /**
     * Test recordkey for sequence file - recordkey of type int.
     * One row will fail - it has a text value
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"features", "gpdb", "hcfs", "security"})
    public void recordkeyIntType() throws Exception {

        // Skip this test for tests that are running against a remote cluster
        if (!(cluster instanceof SingleCluster)) {
            return;
        }
        String[] fields = new String[customWritableFields.length + 1];
        fields[0] = "recordkey INT";
        System.arraycopy(customWritableFields, 0, fields, 1, customWritableFields.length);

        String hdfsDir = hdfsPath + "/recordkey_int_type";
        writableExTable = prepareWritableSequenceTable("pxf_recordkey_int_type_w",
                fields, hdfsDir, schemaPackage + customSchemaFileName);

        Table dataTable = new Table("dataTable", null);
        FileFormatsUtils.prepareData(new CustomSequencePreparer(), 50, dataTable);

        File path = new File(dataTempFolder + "/customwritable_recordkey_data.txt");
        createCustomWritableDataFile(dataTable, path, true);

        String copyCmd = "COPY " + writableExTable.getName() + " FROM '" + path.getAbsolutePath() +
                "'" + "SEGMENT REJECT LIMIT 5 ROWS;";
        String noticeMsg = ".?ound 1 data formatting errors \\(1 or more input rows\\).? .?ejected related input data.*";
        gpdb.runQueryWithExpectedWarning(copyCmd, noticeMsg, true);

        readableExTable = prepareReadableSequenceTable("pxf_recordkey_int_type_r",
                fields, hdfsDir, schemaPackage + customSchemaFileName);
        runSqlTest("features/hdfs/writable/sequence/recordkey_int");
    }

    /**
     * Create data file based on CustomWritable schema. data is written from dataTable and to path file.
     *
     * @param dataTable Data Table
     * @param path      File path
     * @param recordkey add recordkey data to the beginning of each row
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

    private void prepareData() throws Exception {

        String writableInsideSequenceFileName = "writable_inside_sequence.tbl";
        Table dataTable = new Table("dataTable", null);
        Object[] data = FileFormatsUtils.prepareData(new CustomSequencePreparer(), 100, dataTable);
        hdfs.writeSequenceFile(data, hdfsPath + writableInsideSequenceFileName);
    }

    private WritableExternalTable prepareWritableSequenceTable(String name, String[] fields, String path, String schema) throws Exception {
        return prepareWritableSequenceTable(name, fields, path, schema, null, null);
    }

    private WritableExternalTable prepareWritableSequenceTable(String name, String[] fields, String path, String schema, String[] userParameters, String codec) throws Exception {
        WritableExternalTable table = TableFactory.getPxfWritableSequenceTable(
                name, fields, protocol.getExternalTablePath(hdfs.getBasePath(), path), schema);
        if (userParameters != null) {
            table.setUserParameters(userParameters);
        }
        if (codec != null) {
            table.setCompressionCodec(codec);
        }
        createTable(table);
        return table;
    }

    private ReadableExternalTable prepareReadableSequenceTable(String name, String[] fields, String path, String schema) throws Exception {
        ReadableExternalTable table = TableFactory.getPxfReadableSequenceTable(
                name, fields, protocol.getExternalTablePath(hdfs.getBasePath(), path), schema);
        createTable(table);
        return table;
    }
}
