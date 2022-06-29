package org.greenplum.pxf.automation.features.hive;

import jsystem.utils.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.greenplum.pxf.automation.structures.tables.basic.Table;
import org.greenplum.pxf.automation.structures.tables.hive.HiveTable;
import org.greenplum.pxf.automation.structures.tables.utils.TableFactory;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class HiveOrcTest extends HiveBaseTest {
    private static final String ORC_LARGE_DATA_TYPE = "struct<col1:int,col2:string>";
    private static final String[] HIVE_ORC_LARGE_DATA_COLS = {
            "col1 int",
            "col2 string",
    };

    private static final String[] PXF_ORC_LARGE_DATA_COLS = {
            "col1 int",
            "col2 text",
    };

    private HiveTable hiveOrcSmallDataTable;
    private HiveTable hiveOrcTypesTable;
    private HiveTable hiveOrcPartitionedTable;
    private HiveTable hiveOrcCollectionTable;
    private HiveTable hiveBinaryOrcDataTable;
    private HiveTable hiveOrcLargeDataTable;

    private String largeOrcDataFile;

    @Override
    protected void createExternalTable(String tableName, String[] fields, HiveTable hiveTable) throws Exception {

        exTable = TableFactory.getPxfHiveOrcReadableTable(tableName, fields, hiveTable, true);
        createTable(exTable);
    }

    protected void createExternalVectorizedTable(String tableName, String[] fields, HiveTable hiveTable) throws Exception {

        exTable = TableFactory.getPxfHiveVectorizedOrcReadableTable(tableName, fields, hiveTable, true);
        createTable(exTable);
    }

    @Override
    void prepareData() throws Exception {

        prepareSmallData();
        prepareTypesData();
        prepareOrcData();
        prepareNonDefaultSchemaData();
        preparePxfHiveOrcTypes();
        preparePxfHiveSmallData();
    }

    @Override
    void prepareSmallData() throws Exception {

        super.prepareSmallData();
        // Create a copy of small data in ORC format
        hiveOrcSmallDataTable = new HiveTable(HIVE_SMALL_DATA_TABLE + "_orc", HIVE_SMALLDATA_COLS);
        hiveOrcSmallDataTable.setStoredAs(ORC);
        hive.createTableAndVerify(hiveOrcSmallDataTable);
        hive.insertData(hiveSmallDataTable, hiveOrcSmallDataTable);
    }

    @Override
    void prepareBinaryData() throws Exception{

        super.prepareBinaryData();
        // Create a copy of binary data in ORC format
        hiveBinaryOrcDataTable = new HiveTable(HIVE_BINARY_TABLE + "_orc", HIVE_TYPES_BINARY);
        hiveBinaryOrcDataTable.setStoredAs(ORC);
        hive.createTableAndVerify(hiveBinaryOrcDataTable);
        hive.insertData(hiveBinaryTable, hiveBinaryOrcDataTable);
    }

    @Override
    void prepareHiveCollection() throws Exception {

        super.prepareHiveCollection();
        // Create a copy of collection data in ORC format
        hiveOrcCollectionTable = new HiveTable(HIVE_COLLECTIONS_TABLE + "_orc", HIVE_COLLECTION_COLS);
        hiveOrcCollectionTable.setStoredAs(ORC);
        hive.createTableAndVerify(hiveOrcCollectionTable);
        hive.insertData(hiveCollectionTable, hiveOrcCollectionTable);
    }

    private void preparePartitionedData() throws Exception {

        hiveOrcPartitionedTable = new HiveTable(HIVE_PARTITIONED_TABLE, HIVE_RC_COLS);
        hiveOrcPartitionedTable.setPartitionedBy(HIVE_PARTITION_COLUMN);
        hiveOrcPartitionedTable.setStoredAs(ORC);
        hive.createTableAndVerify(hiveOrcPartitionedTable);

        hive.runQuery("SET hive.exec.dynamic.partition = true");
        hive.runQuery("SET hive.exec.dynamic.partition.mode = nonstrict");
        // Insert into table using dynamic partitioning.
        // Some of the fields are NULL so they will be inserted into the default partition.
        hive.insertDataToPartition(hiveTypesTable, hiveOrcPartitionedTable,
                new String[] { "fmt" }, new String[] { "t1", "t2", "num1", "t1", "vc1" });
    }

    private void preparePxfHiveOrcTypes() throws Exception {

        createExternalTable(PXF_HIVE_ORC_TABLE,
                PXF_HIVE_TYPES_COLS, hiveOrcAllTypes);
    }

    private void preparePxfHiveSmallData() throws Exception {

        createExternalTable(PXF_HIVE_SMALL_DATA_TABLE,
                PXF_HIVE_SMALLDATA_COLS, hiveOrcTable);

        Table gpdbNativeTable = new Table(GPDB_SMALL_DATA_TABLE, PXF_HIVE_SMALLDATA_COLS);
        gpdbNativeTable.setDistributionFields(new String[] { "t1" });
        gpdb.createTableAndVerify(gpdbNativeTable);
        gpdb.copyData(exTable, gpdbNativeTable);
    }

   private void prepareOrcLargeData(int numRows) throws Exception {
        hiveOrcLargeDataTable = new HiveTable("hive_orc_large_data", HIVE_ORC_LARGE_DATA_COLS);
        hiveOrcLargeDataTable.setStoredAs(ORC);
        hive.createTableAndVerify(hiveOrcLargeDataTable);
        Configuration configuration = new Configuration();
        configuration.set("orc.overwrite.output.file", "true");
        TypeDescription schema = TypeDescription.fromString(ORC_LARGE_DATA_TYPE);
        String fileName = String.format("large_data_%d_rows.orc", numRows);
        largeOrcDataFile = dataTempFolder + "/" + fileName;
        Writer writer = OrcFile.createWriter(new Path(largeOrcDataFile),
                OrcFile.writerOptions(configuration).setSchema(schema));

        VectorizedRowBatch batch = schema.createRowBatch();
        LongColumnVector col1 = (LongColumnVector) batch.cols[0];
        BytesColumnVector col2 = (BytesColumnVector) batch.cols[1];

        for (int i = 0; i < numRows; i++) {
            int row = batch.size++;

            byte[] bytes = String.format("row%d", i).getBytes(StandardCharsets.UTF_8);
            col1.vector[row] = i;
            col2.setRef(row, bytes, 0, bytes.length);

            if (batch.size == batch.getMaxSize()) {
                writer.addRowBatch(batch);
                batch.reset();
            }
        }

        if (batch.size != 0) {
            writer.addRowBatch(batch);
            batch.reset();
        }
        writer.close();

        String destPath = hdfs.getWorkingDirectory() + "/" + fileName;
        hdfs.copyFromLocal(largeOrcDataFile, destPath);
        hive.loadData(hiveOrcLargeDataTable, destPath, false);
    }

    /**
     * Query for small data hive table
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = { "hive", "features", "gpdb", "security" })
    public void sanity() throws Exception {

        createExternalTable(PXF_HIVE_SMALL_DATA_TABLE + "_orc",
                PXF_HIVE_SMALLDATA_COLS, hiveOrcSmallDataTable);

        runTincTest("pxf.features.hive.small_data_orc.runTest");
    }

    /**
     * Create a Greenplum table with a subset of columns from the original
     * Hive table
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"hive", "features", "gpdb", "security"})
    public void columnSubsetOfHiveSchema() throws Exception {

        createExternalTable(PXF_HIVE_SMALL_DATA_TABLE,
                PXF_HIVE_SUBSET_COLS, hiveOrcSmallDataTable);

        runTincTest("pxf.features.hive.column_subset.runTest");
    }

    /**
     * Create a Greenplum table with a subset of columns from the original
     * partitioned Hive table
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = { "hive", "features", "gpdb", "security" })
    public void columnSubsetOfPartitionedHiveSchema() throws Exception {

        preparePartitionedData();
        // Create PXF Table using HiveOrc profile
        createExternalTable(PXF_HIVE_SMALL_DATA_TABLE,
                PXF_HIVE_SUBSET_FMT_COLS, hiveOrcPartitionedTable);

        runTincTest("pxf.features.hive.column_subset_partitioned_table_orc.runTest");
    }

    /**
     * Query for binary hive table
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = { "hive", "features", "gpdb", "security" })
    public void hiveLongBinaryType() throws Exception {

        prepareBinaryData();
        createExternalTable(PXF_HIVE_BINARY_TABLE + "_orc",
                new String[] { "b1 BYTEA" }, hiveBinaryOrcDataTable);

        runTincTest("pxf.features.hive.binary_orc_data.runTest");
    }

    /**
     * PXF on Hive ORC format table
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = { "hive", "features", "gpdb", "security" })
    public void storeAsOrc() throws Exception {

        createExternalTable(PXF_HIVE_SMALL_DATA_TABLE,
                PXF_HIVE_SMALLDATA_COLS, hiveOrcTable);

        runTincTest("pxf.features.hive.small_data.runTest");
    }

    /**
     * PXF on Hive ORC format all hive primitive types (all types
     * supported on both Hive 1 & 2)
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = { "hive", "features", "gpdb", "security" })
    public void storeAsOrcAllTypesHive1AndHive2() throws Exception {

        runTincTest("pxf.features.hive.orc_primitive_types.runTest");
    }

    /**
     * PXF on Hive ORC format all hive primitive types (with timestamps;
     * only supported on Hive 1)
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"hive", "features", "gpdb", "security" })
    public void storeAsOrcAllTypesHive1Only() throws Exception {

        runTincTest("pxf.features.hive.orc_primitive_types_hive1_only.runTest");
    }

    /**
     * PXF on Hive ORC Queries with ANY, SOME, ALL, and EXISTS
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = { "hive", "features", "gpdb", "security" })
    public void queryWithNotPushedDownOperators() throws Exception {

        runTincTest("pxf.features.hive.orc_operators_no_ppd.runTest");
    }

    /**
     * PXF on Hive ORC format with Snappy compression table
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = { "hive", "features", "gpdb", "security" })
    public void storeAsOrcSnappy() throws Exception {

        prepareOrcSnappyData();
        createExternalTable("pxf_hive_orc_snappy",
                PXF_HIVE_SMALLDATA_COLS, hiveOrcSnappyTable);

        runTincTest("pxf.features.hive.orc_snappy.runTest");
    }

    /**
     * PXF on Hive ORC format with Zlib compression table
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = { "hive", "features", "gpdb", "security" })
    public void storeAsOrcZlib() throws Exception {

        prepareOrcZlibData();
        createExternalTable(PXF_HIVE_ORC_ZLIB_TABLE,
                PXF_HIVE_SMALLDATA_COLS, hiveOrcZlibTable);

        runTincTest("pxf.features.hive.orc_zlib.runTest");
    }

    /**
     * PXF on Hive ORC format with multiple ORC backing files
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = { "hive", "features", "gpdb", "security" })
    public void storeAsOrcMultiFile() throws Exception {

        prepareOrcMultiFileData();
        createExternalTable(PXF_HIVE_ORC_MULTIFILE_TABLE, PXF_HIVE_SMALLDATA_COLS, hiveOrcMultiFileTable);

        runTincTest("pxf.features.hive.orc_multifile.runTest");
    }

    /**
     * PXF with HiveVectorizedORC deprecated profile on Hive ORC format with multiple ORC backing files
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = { "hive", "features", "gpdb", "security" })
    public void storeAsOrcMultiFileGetVectorized() throws Exception {

        prepareOrcMultiFileData();
        createExternalVectorizedTable(PXF_HIVE_ORC_MULTIFILE_VECTORIZED_TABLE, PXF_HIVE_SMALLDATA_COLS, hiveOrcMultiFileTable);

        runTincTest("pxf.features.hive.orc_multifile_vectorized.runTest");
    }

    /**
     * check default analyze results for pxf external table is as required
     * (pages=1000 tuples=1000000)
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = { "features" })
    public void defaultAnalyze() throws Exception {

        createExternalTable(PXF_HIVE_SMALL_DATA_TABLE,
                PXF_HIVE_SMALLDATA_COLS, hiveSmallDataTable);

        // Perform Analyze on external table and check suitable Warnings.
        gpdb.runQueryWithExpectedWarning("ANALYZE " + exTable.getName(),
                "ANALYZE for HiveRc, HiveText, and HiveOrc plugins is not supported", true);

        runTincTest("pxf.features.hive.default_analyze.runTest");
    }

    /**
     * PXF on Hive table partitioned to one field
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = { "hive", "features", "gpdb", "security" })
    public void hivePartitionedTable() throws Exception {

        preparePartitionedData();
        // Create PXF Table using HiveOrc profile
        createExternalTable(PXF_HIVE_PARTITIONED_TABLE,
                PXF_HIVE_SMALLDATA_FMT_COLS, hiveOrcPartitionedTable);

        runTincTest("pxf.features.hive.hive_partitioned_table_one_format.runTest");
    }

    /**
     * Query pxf external table directed to hive orc table contains 3 types of
     * collections (Array, Map and Struct)
     * TODO: Currently the test returns a failure due to HAWQ-1215.
     *       Update the expected response once HAWQ-1215 is fixed.
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = { "hive", "features", "gpdb", "security" })
    public void hiveCollectionTypes() throws Exception {

        prepareHiveCollection();
        createExternalTable(PXF_HIVE_COLLECTIONS_TABLE,
                PXF_HIVE_COLLECTION_COLS, hiveOrcCollectionTable);

        runTincTest("pxf.features.hive.hive_collection_types.runTest");
    }

    /**
     * Make sure that PXF works with aggregate queries (including null columns)
     * TODO: enable test when issue is resolved.
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = { "features"})
    public void aggregateQueries() throws Exception {

        createExternalTable(PXF_HIVE_SMALL_DATA_TABLE,
                PXF_HIVE_SMALLDATA_COLS, hiveOrcTypesTable);

        exTable = TableFactory.getPxfHiveOrcReadableTable(PXF_HIVE_SMALL_DATA_TABLE +
                        "_multiple_fragments_per_file", PXF_HIVE_TYPES_COLS, hiveOrcAllTypes, true);

        exTable.setAccessor("org.greenplum.pxf.plugins.hive.HiveORCAccessor");
        exTable.setFragmenter("org.greenplum.pxf.automation.testplugin.MultipleHiveFragmentsPerFileFragmenter");
        exTable.setResolver("org.greenplum.pxf.plugins.hive.HiveORCSerdeResolver");
        exTable.setProfile(null);
        exTable.setUserParameters(new String[] { "TEST-FRAGMENTS-NUM=10" });
        createTable(exTable);

        runTincTest("pxf.features.hive.aggregate_queries.runTest");
        runTincTest("pxf.features.hive.aggregate_queries_multiple_fragments_per_file.runTest");
    }

    /**
     * Query a small Hive table, skipping the first few rows with skip.header.line.count.
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = { "hive", "features", "gpdb", "security" })
    public void hiveTableWithSkipHeader() throws Exception {

        HiveTable hiveOrcSkipHeaderTable = new HiveTable("hive_table_with_skipheader_orc", HIVE_RC_COLS);
        hiveOrcSkipHeaderTable.setStoredAs(ORC);
        List<List<String>> tableProperties = new ArrayList<>();
        tableProperties.add(Arrays.asList("skip.header.line.count", "3"));
        hiveOrcSkipHeaderTable.setTableProperties(tableProperties);

        hive.createTableAndVerify(hiveOrcSkipHeaderTable);
        hive.insertData(hiveSmallDataTable, hiveOrcSkipHeaderTable);

        createExternalTable("pxf_hive_table_with_skipheader_orc", PXF_HIVE_SMALLDATA_COLS, hiveOrcSkipHeaderTable);

        runTincTest("pxf.features.hive.orc_skip_header_rows.runTest");
    }

    @Test(groups = {"hive", "features", "gpdb"})
    public void hiveOrcLargeData() throws Exception {
        prepareOrcLargeData(8192);
        createExternalTable("pxf_hive_orc_large_data", PXF_ORC_LARGE_DATA_COLS, hiveOrcLargeDataTable);
        runTincTest("pxf.features.hive.orc_large_data.runTest");
    }

}
