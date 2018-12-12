package org.greenplum.pxf.automation.features.hdfs;

import java.io.File;

import org.greenplum.pxf.automation.components.cluster.PhdCluster;
import org.greenplum.pxf.automation.structures.tables.basic.Table;
import org.greenplum.pxf.automation.structures.tables.pxf.ReadableExternalTable;
import org.greenplum.pxf.automation.utils.fileformats.FileFormatsUtils;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.greenplum.pxf.automation.fileformats.IAvroSchema;
import org.greenplum.pxf.automation.features.BaseFeature;
import org.greenplum.pxf.automation.datapreparer.CustomAvroPreparer;

/**
 * Collection of Test cases for PXF ability to read AvroFile or Avro serialized
 * files from HDFS. Relates to cases located in "PXF Test Suite" in testrail.
 * See <a href="https://testrail.greenplum.com/index.php?/suites/view/1099">HDFS
 * Readable - Avro</a>
 */
public class HdfsReadableAvroTest extends BaseFeature {

    private String hdfsPath;
    private String resourcePath;

    private final String SUFFIX_AVRO = ".avro";
    private final String SUFFIX_AVSC = ".avsc";
    private final String SUFFIX_JSON = ".json";

    String avroSimpleFileName = "simple";
    String avroTypesFileName = "supported_primitive_types";
    String avroArrayFileName = "array";
    String avroComplexFileName = "complex";
    String avroComplexNullFileName = "complex_null";
    String avroInSequenceArraysFileName = "avro_in_sequence_arrays.tbl";
    /** To be used for Avro Complex types */
    String avroInSequenceComplexFileName = "avro_in_sequence_complex.tbl";
    String avroMultiBlockDir = "avro_multi/";
    String avroInSequenceArraysSchemaFile = "PXFCustomAvro.avsc";
    String avroInSequenceArraysSchemaFileWithSpaces = "PXF Custom Avro1.avsc";
    String avroInSequenceComplexSchemaFile = "PXFComplexAvro.avsc";

    @Override
    public void beforeClass() throws Exception {
        // path for storing data on HDFS (for processing by PXF)
        hdfsPath = hdfs.getWorkingDirectory() + "/avro/";

        // location of schema and data files
        resourcePath = localDataResourcesFolder + "/avro/";

        // create and copy data to hdfs
        prepareData();

        // copy schema file to all nodes
        String newPath = "/tmp/publicstage/pxf";
        // copy schema file to cluster nodes, used for avro in sequence cases
        cluster.copyFileToNodes(new File(resourcePath
                + avroInSequenceArraysSchemaFile).getAbsolutePath(), newPath,
                true, false);
        cluster.copyFileToNodes(new File(resourcePath
                + avroInSequenceArraysSchemaFileWithSpaces).getAbsolutePath(), newPath,
                true, false);
        cluster.copyFileToNodes(new File(resourcePath
                + avroInSequenceComplexSchemaFile).getAbsolutePath(), newPath,
                true, false);
        // add new path to classpath file and restart PXF service
        cluster.addPathToPxfClassPath(newPath);
        cluster.restart(PhdCluster.EnumClusterServices.pxf);
    }

    private void prepareData() throws Exception {

        // Create Avro files from schema and json files
        hdfs.writeAvroFileFromJson(hdfsPath + avroSimpleFileName + SUFFIX_AVRO,
                resourcePath + avroSimpleFileName + SUFFIX_AVSC,
                resourcePath + avroSimpleFileName + SUFFIX_JSON, null);

        hdfs.writeAvroFileFromJson(hdfsPath + avroTypesFileName + SUFFIX_AVRO,
                resourcePath + avroTypesFileName + SUFFIX_AVSC,
                resourcePath + avroTypesFileName + SUFFIX_JSON, null);

        hdfs.writeAvroFileFromJson(hdfsPath + avroArrayFileName + SUFFIX_AVRO,
                resourcePath + avroArrayFileName + SUFFIX_AVSC,
                resourcePath + avroArrayFileName + SUFFIX_JSON, null);

        hdfs.writeAvroFileFromJson(hdfsPath + avroComplexFileName + SUFFIX_AVRO,
                resourcePath + avroComplexFileName + SUFFIX_AVSC,
                resourcePath + avroComplexFileName + SUFFIX_JSON, null);

        hdfs.writeAvroFileFromJson(hdfsPath + avroComplexNullFileName + SUFFIX_AVRO,
                resourcePath + avroComplexNullFileName + SUFFIX_AVSC,
                resourcePath + avroComplexNullFileName + SUFFIX_JSON, null);

        String schemaName1 = resourcePath + avroInSequenceArraysSchemaFile;
        Table dataTable1 = new Table("dataTable1", null);
        Object[] data1 = FileFormatsUtils.prepareData(new CustomAvroPreparer(
                schemaName1), 20, dataTable1);
        hdfs.writeAvroInSequenceFile(hdfsPath + avroInSequenceArraysFileName,
                schemaName1, (IAvroSchema[]) data1);
    }

    @BeforeMethod(alwaysRun = true)
    public void setUp() throws Exception {

        // default external table with common settings
        exTable = new ReadableExternalTable("avroSimple", null, "", "custom");
        exTable.setHost(pxfHost);
        exTable.setPort(pxfPort);
        exTable.setFormatter("pxfwritable_import");
    }

    /**
     * TestRail: C120828 Read simple avro file using profile.
     *
     * @throws Exception
     */
    @Test(groups = { "features", "gpdb", "hcfs" })
    public void avroSimple() throws Exception {

        exTable.setName("avrotest_simple");
        exTable.setProfile("Avro");
        exTable.setPath(hdfsPath + avroSimpleFileName + SUFFIX_AVRO);
        exTable.setFields(new String[] { "name text", "age int" });

        gpdb.createTableAndVerify(exTable);

        // Verify results
        runTincTest("pxf.features.hdfs.readable.avro.simple.runTest");
    }

    /**
     * TestRail: C120829 Read an Avro file that includes all supported primitive
     * data types.
     *
     * @throws Exception
     */
    @Test(groups = { "features", "gpdb", "hcfs" })
    public void avroSupportedPrimitives() throws Exception {

        exTable.setName("avrotest_supported_primitive_types");
        exTable.setProfile("Avro");
        exTable.setPath(hdfsPath + avroTypesFileName + SUFFIX_AVRO);
        exTable.setFields(new String[] {
                "type_int int",
                "type_double float8",
                "type_string text",
                "type_float real",
                "type_long bigint",
                "type_bytes bytea",
                "type_boolean bool" });

        gpdb.createTableAndVerify(exTable);

        // Verify results
        runTincTest("pxf.features.hdfs.readable.avro.supported_primitive_types.runTest");
    }

    /**
     * Read an Avro file that includes all supported array types.
     *
     * @throws Exception
     */
    @Test(groups = { "features", "gpdb", "hcfs" })
    public void avroArrays() throws Exception {

        exTable.setName("avrotest_arrays");
        exTable.setProfile("Avro");
        exTable.setPath(hdfsPath + avroArrayFileName + SUFFIX_AVRO);
        exTable.setFields(new String[] {
                "type_int int",
                "type_int_array text",
                "type_double float8",
                "type_double_array text",
                "type_string text",
                "type_string_array text",
                "type_float real",
                "type_float_array text",
                "type_long bigint",
                "type_long_array text",
                "type_bytes bytea",
                "type_bytes_array text",
                "type_boolean bool",
                "type_boolean_array text", });

        gpdb.createTableAndVerify(exTable);

        // Verify results
        runTincTest("pxf.features.hdfs.readable.avro.array_types.runTest");
    }

    /**
     * Read an Avro file that includes all supported complex types.
     *
     * @throws Exception
     */
    @Test(groups = { "features", "gpdb", "hcfs" })
    public void avroComplex() throws Exception {

        exTable.setName("avrotest_complex");
        exTable.setProfile("Avro");
        exTable.setPath(hdfsPath + avroComplexFileName + SUFFIX_AVRO);
        exTable.setFields(new String[] {
                "type_long bigint",
                "type_string text",
                "type_array text",
                "type_union text",
                "type_map text",
                "type_record text",
                "type_enum text",
                "type_fixed bytea" });

        gpdb.createTableAndVerify(exTable);

        // Verify results
        runTincTest("pxf.features.hdfs.readable.avro.complex_types.runTest");
    }

    /**
     * Read an Avro file that includes null values
     *
     * @throws Exception
     */
    @Test(groups = { "features", "gpdb", "hcfs" })
    public void avroNull() throws Exception {

        exTable.setName("avrotest_null");
        exTable.setProfile("Avro");
        exTable.setPath(hdfsPath + avroComplexFileName + SUFFIX_AVRO);
        exTable.setFields(new String[] {
                "type_long bigint",
                "type_string text",
                "type_array text",
                "type_union text",
                "type_map text",
                "type_record text",
                "type_enum text",
                "type_fixed bytea" });

        gpdb.createTableAndVerify(exTable);

        // Verify results
        runTincTest("pxf.features.hdfs.readable.avro.null_values.runTest");
    }

    /**
     * Read an Avro file that includes unions with complex and null default values
     *
     * @throws Exception
     */
    @Test(groups = { "features", "gpdb", "hcfs" })
    public void avroComplexNull() throws Exception {
        exTable.setName("avrotest_complex_null");
        exTable.setProfile("Avro");
        exTable.setPath(hdfsPath + avroComplexNullFileName + SUFFIX_AVRO);
        exTable.setFields(new String[] {
            "sourcetimestamp bigint",
            "sourceagent varchar(200)",
            "sourceip_address varchar(20)",
            "meetmeuser_id bigint",
            "meetmeinterested_uid bigint",
            "meetmevote varchar(4)",
            "meetmeis_match int",
            "meetmenetwork_score bigint",
            "meetmeresponsiveness_score bigint",
            "meetmesession_id varchar(32)",
            "meetmefriends int",
            "meetmeprevious_view varchar(32)",
            "meetmemodel bigint",
            "meetmescore float8",
            "meetmemethod int",
            "meetmecontributions varchar(120)",
            "meetmeclicksource varchar(20)",
            "meetmeclickaction varchar(20)",
            "meetmeprofileview_ts bigint",
            "meetmeplatform varchar(20)",
            "metatopic_name varchar(100)",
            "metarequest_user_agent varchar(200)",
            "metarequest_session_id varchar(50)",
            "metarequest_id text",
            "metakvpairs varchar(1000)",
            "meta_handlers varchar(1000)"});

        gpdb.createTableAndVerify(exTable);

        // Verify results
        runTincTest("pxf.features.hdfs.readable.avro.complex_null_values.runTest");
    }

    /**
     * Read an Avro format stored in sequence file, with separated schema file
     *
     * @throws Exception
     */
    @Test(groups = { "features", "gpdb", "hcfs" })
    public void avroInSequenceFileArrays() throws Exception {

        exTable.setName("avro_in_seq_arrays");
        exTable.setFields(new String[] {
                "num  text",
                "int1  integer",
                "int2  integer",
                "strings  text",
                "st1    text",
                "dubs    text",
                "dub    double precision",
                "fts   text",
                "ft   real",
                "lngs  text",
                "lng   bigint",
                "bls   text",
                "bl   boolean",
                "bts   bytea", });
        exTable.setPath(hdfsPath + avroInSequenceArraysFileName);

        exTable.setFragmenter("org.greenplum.pxf.plugins.hdfs.HdfsDataFragmenter");
        exTable.setAccessor("org.greenplum.pxf.plugins.hdfs.SequenceFileAccessor");
        exTable.setResolver("org.greenplum.pxf.plugins.hdfs.AvroResolver");
        exTable.setDataSchema(avroInSequenceArraysSchemaFile);
        exTable.setFormatter("pxfwritable_import");

        gpdb.createTableAndVerify(exTable);

        // Verify results
        runTincTest("pxf.features.hdfs.readable.avro.avro_in_sequence_arrays.runTest");
    }

    /**
     * Read an Avro format stored in sequence file, with separated schema file
     *
     * @throws Exception
     */
    @Test(groups = { "features", "gpdb", "hcfs" })
    public void avroFileNameWithSpaces() throws Exception {

        exTable.setName("avro_in_seq_arrays");
        exTable.setFields(new String[] {
                "num  text",
                "int1  integer",
                "int2  integer",
                "strings  text",
                "st1    text",
                "dubs    text",
                "dub    double precision",
                "fts   text",
                "ft   real",
                "lngs  text",
                "lng   bigint",
                "bls   text",
                "bl   boolean",
                "bts   bytea", });
        exTable.setPath(hdfsPath + avroInSequenceArraysFileName);

        exTable.setFragmenter("org.greenplum.pxf.plugins.hdfs.HdfsDataFragmenter");
        exTable.setAccessor("org.greenplum.pxf.plugins.hdfs.SequenceFileAccessor");
        exTable.setResolver("org.greenplum.pxf.plugins.hdfs.AvroResolver");
        exTable.setDataSchema(avroInSequenceArraysSchemaFileWithSpaces);
        exTable.setFormatter("pxfwritable_import");

        gpdb.createTableAndVerify(exTable);

        // Verify results
        runTincTest("pxf.features.hdfs.readable.avro.avro_in_sequence_arrays.runTest");
    }

    /**
     * Read multiple Avro files.
     *
     * @throws Exception
     */
    @Test(groups = { "features", "gpdb", "hcfs" })
    public void avroMultiFiles() throws Exception {
        String schemaName = resourcePath + avroInSequenceArraysSchemaFile;

        Table dataTable = new Table("dataTable", null);

        Object[] data = FileFormatsUtils.prepareData(new CustomAvroPreparer(
                schemaName), 10000, dataTable);

        for (int i = 0; i < 5; i++) {
            hdfs.writeAvroFile(hdfsPath + avroMultiBlockDir + "file" + i
                    + SUFFIX_AVRO, schemaName, null, (IAvroSchema[]) data);
        }

        exTable.setName("avro_multi");
        exTable.setFields(new String[] {
                "num  text",
                "int1  integer",
                "int2  integer",
                "strings  text",
                "st1    text",
                "dubs    text",
                "dub    double precision",
                "fts   text",
                "ft   real",
                "lngs  text",
                "lng   bigint",
                "bls   text",
                "bl   boolean",
                "bts   bytea", });
        exTable.setPath(hdfsPath + avroMultiBlockDir);

        exTable.setProfile("Avro");
        exTable.setFormatter("pxfwritable_import");

        gpdb.createTableAndVerify(exTable);

        // Verify results
        runTincTest("pxf.features.hdfs.readable.avro.multi_files.runTest");
    }

    /**
     * Read simple Avro file with Snappy and deflate compressions.
     *
     * @throws Exception
     */
    @Test(groups = { "features", "gpdb", "hcfs" })
    public void avroCodecs() throws Exception {
        String schemaName = resourcePath + avroInSequenceArraysSchemaFile;
        Table dataTable = new Table("dataTable", null);

        String[] codecs = { "snappy", "deflate" };
        for (String codec : codecs) {
            String fileName = hdfsPath + codec + SUFFIX_AVRO;

            // TODO: nhorn - 18 SEP 15 - snappy files cannot be generated on centos5,
            // so we use a pre-generated file for this test.
            if (codec.equals("snappy")) {
                hdfs.copyFromLocal(resourcePath + codec + SUFFIX_AVRO, fileName);
            } else {
                Object[] data = FileFormatsUtils.prepareData(
                        new CustomAvroPreparer(schemaName), 50, dataTable);
                hdfs.writeAvroFile(fileName, schemaName, codec,
                        (IAvroSchema[]) data);
            }
            exTable.setName("avro_codec");
            exTable.setFields(new String[] {
                    "num  text",
                    "int1  integer",
                    "int2  integer",
                    "strings  text",
                    "st1    text",
                    "dubs    text",
                    "dub    double precision",
                    "fts   text",
                    "ft   real",
                    "lngs  text",
                    "lng   bigint",
                    "bls   text",
                    "bl   boolean",
                    "bts   bytea", });
            exTable.setPath(fileName);

            exTable.setProfile("Avro");
            exTable.setFormatter("pxfwritable_import");

            gpdb.createTableAndVerify(exTable);

            // Verify results
            runTincTest("pxf.features.hdfs.readable.avro.codec.runTest");
        }
    }

    /**
     * Try to read Avro file with extra field in the table definition. The
     * expected behaviour is to error out.
     *
     * @throws Exception
     */
    @Test(groups = { "features", "gpdb", "hcfs" })
    public void extraField() throws Exception {
        exTable.setName("avro_extra_field");
        exTable.setProfile("Avro");
        exTable.setPath(hdfsPath + avroSimpleFileName + SUFFIX_AVRO);
        exTable.setFields(new String[] {
                "name text",
                "age int",
                "alive boolean" });

        gpdb.createTableAndVerify(exTable);

        // Verify results
        runTincTest("pxf.features.hdfs.readable.avro.errors.extra_field.runTest");
    }

    /**
     * Try to read Avro file with missing field in the table definition. The
     * expected behaviour is to error out.
     *
     * @throws Exception
     */
    @Test(groups = { "features", "gpdb", "hcfs" })
    public void missingField() throws Exception {
        exTable.setName("avro_missing_field");
        exTable.setProfile("Avro");
        exTable.setPath(hdfsPath + avroSimpleFileName + SUFFIX_AVRO);
        exTable.setFields(new String[] { "name text" });

        gpdb.createTableAndVerify(exTable);

        // Verify results
        runTincTest("pxf.features.hdfs.readable.avro.errors.missing_field.runTest");
    }

    /**
     * Try to read Avro file with wrong schema file.
     *
     * @throws Exception
     */
    @Test(groups = { "features", "gpdb", "hcfs" })
    public void noSchemaFile() throws Exception {
        exTable.setName("avro_in_seq_no_schema");
        exTable.setFields(new String[] {
                "num  text",
                "int1  integer",
                "int2  integer",
                "strings  text",
                "st1    text",
                "dubs    text",
                "dub    double precision",
                "fts   text",
                "ft   real",
                "lngs  text",
                "lng   bigint",
                "bls   text",
                "bl   boolean",
                "bts   bytea", });
        exTable.setPath(hdfsPath + avroInSequenceArraysFileName);

        exTable.setFragmenter("org.greenplum.pxf.plugins.hdfs.HdfsDataFragmenter");
        exTable.setAccessor("org.greenplum.pxf.plugins.hdfs.SequenceFileAccessor");
        exTable.setResolver("org.greenplum.pxf.plugins.hdfs.AvroResolver");
        exTable.setDataSchema("i_do_not_exist");
        exTable.setFormatter("pxfwritable_import");

        gpdb.createTableAndVerify(exTable);

        // Verify results
        runTincTest("pxf.features.hdfs.readable.avro.errors.no_schema_file.runTest");
    }
}
