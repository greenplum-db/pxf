package org.greenplum.pxf.automation.features.hdfs;

import java.io.File;

import org.greenplum.pxf.automation.components.cluster.PhdCluster;
import org.greenplum.pxf.automation.structures.tables.basic.Table;
import org.greenplum.pxf.automation.structures.tables.pxf.ReadableExternalTable;
import org.greenplum.pxf.automation.utils.fileformats.FileFormatsUtils;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.greenplum.pxf.automation.datapreparer.CustomSequencePreparer;
import org.greenplum.pxf.automation.features.BaseFeature;

/**
 * Collection of Test cases for PXF ability to read SequenceFile.
 */
public class HdfsReadableSequenceTest extends BaseFeature {

    private String hdfsPath;
    private String resourcePath;

    private final String SUFFIX_CLASS = ".class";

    String schemaPackageLocation = "/org/greenplum/pxf/automation/dataschema/";
    String schemaPackage = "org.greenplum.pxf.automation.dataschema.";

    String customSchemaFileName = "CustomWritable";
    String customSchemaWithCharFileName = "CustomWritableWithChar";
    String customSchemaWithCircleFileName = "CustomWritableWithCircle";

    String writableInsideSequenceFileName = "writable_inside_sequence.tbl";

    String[] customWritableFields = {
            "tmp1  timestamp",
            "num1  integer",
            "num2  integer",
            "num3  integer",
            "num4  integer",
            "t1    text",
            "t2    text",
            "t3    text",
            "t4    text",
            "t5    text",
            "t6    text",
            "dub1  double precision",
            "dub2  double precision",
            "dub3  double precision",
            "ft1   real",
            "ft2   real",
            "ft3   real",
            "ln1   bigint",
            "ln2   bigint",
            "ln3   bigint",
            "bool1 boolean",
            "bool2 boolean",
            "bool3 boolean",
            "short1 smallint",
            "short2 smallint",
            "short3 smallint",
            "short4 smallint",
            "short5 smallint",
            "bt    bytea" };

    @Override
    protected void beforeClass() throws Exception {
        super.beforeClass();

        // path for storing data on HDFS (for processing by PXF)
        hdfsPath = hdfs.getWorkingDirectory() + "/sequence/";

        // location of schema and data files
        resourcePath = "target/classes" + schemaPackageLocation;

        // copy schema file to all nodes
        String newPath = "/tmp/publicstage/pxf";
        // copy schema file to cluster nodes, used for writable in sequence cases
        cluster.copyFileToNodes(new File(resourcePath + customSchemaFileName
                + SUFFIX_CLASS).getAbsolutePath(), newPath
                + schemaPackageLocation, true, false);

        // add new path to classpath file and restart PXF service
        cluster.addPathToPxfClassPath(newPath);
        cluster.restart(PhdCluster.EnumClusterServices.pxf);

        // create and copy data to hdfs
        prepareData();
    }

    private void prepareData() throws Exception {

        Table dataTable = new Table("dataTable", null);
        Object[] data = FileFormatsUtils.prepareData(
                new CustomSequencePreparer(), 100, dataTable);
        hdfs.writeSequenceFile(data,
                (hdfsPath + writableInsideSequenceFileName));
    }

    @BeforeMethod(alwaysRun = true)
    public void setUp() throws Exception {

        // default external table with common settings
        exTable = new ReadableExternalTable("writable_in_sequence", null, "",
                "custom");
        exTable.setHost(pxfHost);
        exTable.setPort(pxfPort);
        exTable.setProfile("SequenceWritable");
        exTable.setFormatter("pxfwritable_import");
    }

    /**
     *
     * Test Writable data inside a SequenceFile (read only).
     *
     * @throws Exception
     */
    @Test(groups = { "features", "gpdb", "hcfs" })
    public void readSequenceFile() throws Exception {

        exTable.setFields(customWritableFields);
        exTable.setDataSchema(schemaPackage + customSchemaFileName);
        exTable.setPath(hdfsPath + writableInsideSequenceFileName);

        gpdb.createTableAndVerify(exTable);

        runTincTest("pxf.features.hdfs.readable.sequence.custom_writable.runTest");
    }
}
