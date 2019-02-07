package org.greenplum.pxf.automation.features.hive;

import org.greenplum.pxf.automation.components.common.ShellSystemObject;
import org.greenplum.pxf.automation.enums.EnumPxfDefaultProfiles;
import org.greenplum.pxf.automation.structures.tables.hive.HiveExternalTable;
import org.greenplum.pxf.automation.structures.tables.hive.HiveTable;
import org.greenplum.pxf.automation.structures.tables.utils.TableFactory;
import org.greenplum.pxf.automation.utils.exception.ExceptionUtils;
import org.greenplum.pxf.automation.utils.tables.ComparisonUtils;
import jsystem.utils.FileUtils;
import org.junit.Assert;
import org.postgresql.util.PSQLException;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class HiveTest extends HiveBaseTest {

    private static final String HIVE_PARTITIONED_SKEWED_TABLE = "hive_partitioned_skewed_table";
    private static final String HIVE_PARTITIONED_SKEWED_STORED_TABLE = "hive_partitioned_skewed_stored_table";
    private static final String HIVE_PARTITIONED_CLUSTERED_TABLE = "hive_partitioned_clustered_table";
    private static final String HIVE_PARTITIONED_CLUSTERED_SORTED_TABLE = "hive_partitioned_clustered_sorted_table";
    private static final String HIVE_MANY_PARTITIONS = "many_partitions";
    private static final String HIVE_MANY_PARTITIONED_TABLE = "hive_many_partitioned_table";
    private static final String HIVE_PARTITIONS_ALL_TYPES = "hive_partitions_all_types";
    private static final String HIVE_PARTITION_FILTER_PUSHDOWN = "hive_partition_filter_pushdown";

    private static final String PXF_HIVE_PARTITIONED_CLUSTERED_TABLE = "pxf_hive_partitioned_clustered_table";
    private static final String PXF_HIVE_PARTITIONED_CLUSTERED_SORTED_TABLE = "pxf_hive_partitioned_clustered_sorted_table";
    private static final String PXF_HIVE_PARTITIONED_SKEWED_TABLE = "pxf_hive_partitioned_skewed_table";
    private static final String PXF_HIVE_PARTITIONED_SKEWED_STORED_TABLE = "pxf_hive_partitioned_skewed_stored_table";

    private HiveExternalTable hivePartitionedTable;
    private HiveTable hiveManyPartitionsTable;
    private HiveTable hivePartitionedClusteredTable;
    private HiveTable hivePartitionedClusteredSortedTable;
    private HiveTable hivePartitionedSkewedTable;
    private HiveTable hivePartitionedSkewedStoredAsDirsTable;

    private void createExternalTable(String tableName, String[] fields,
                                     HiveTable hiveTable, boolean useProfile) throws Exception {

        exTable = TableFactory.getPxfHiveReadableTable(tableName, fields, hiveTable, useProfile);
        createTable(exTable);
    }

    private void createExternalTable(String tableName, String[] fields,
                                     HiveTable hiveTable) throws Exception {

        createExternalTable(tableName, fields, hiveTable, true);
    }

    private void preparePartitionedClusteredSortedData() throws Exception {

        if (hivePartitionedClusteredSortedTable != null)
            return;
        hivePartitionedClusteredSortedTable = TableFactory.getHiveByRowCommaExternalTable(
                HIVE_PARTITIONED_CLUSTERED_SORTED_TABLE, HIVE_RC_COLS);
        hivePartitionedClusteredSortedTable.setPartitionedBy(HIVE_PARTITION_COLUMN);
        hivePartitionedClusteredSortedTable.setSortedBy(new String[] { "num1" });
        hivePartitionedClusteredSortedTable.setClusteredBy(new String[] { "t0" });
        hivePartitionedClusteredSortedTable.setClusterBucketCount(10);
        hive.createTableAndVerify(hivePartitionedClusteredSortedTable);
        hiveAlterPartitionedFileFormats(hivePartitionedClusteredSortedTable);
    }

    private void prepareSkewedData() throws Exception {

        if (hivePartitionedSkewedTable != null)
            return;
        hivePartitionedSkewedTable = TableFactory.getHiveByRowCommaExternalTable(
                HIVE_PARTITIONED_SKEWED_TABLE, HIVE_RC_COLS);
        hivePartitionedSkewedTable.setPartitionedBy(HIVE_PARTITION_COLUMN);
        hivePartitionedSkewedTable.setSkewedBy(new String[] { "num1" });
        hivePartitionedSkewedTable.setSkewedOn(new String[] { "10" });
        hive.createTableAndVerify(hivePartitionedSkewedTable);
        hiveAlterPartitionedFileFormats(hivePartitionedSkewedTable);
    }

    private void prepareSkewedStoredAsDirsData() throws Exception {

        if (hivePartitionedSkewedStoredAsDirsTable != null)
            return;
        hivePartitionedSkewedStoredAsDirsTable = TableFactory.getHiveByRowCommaExternalTable(
                HIVE_PARTITIONED_SKEWED_STORED_TABLE, HIVE_RC_COLS);
        hivePartitionedSkewedStoredAsDirsTable.setPartitionedBy(HIVE_PARTITION_COLUMN);
        hivePartitionedSkewedStoredAsDirsTable.setSkewedBy(new String[] { "num1" });
        hivePartitionedSkewedStoredAsDirsTable.setSkewedOn(new String[] { "10" });
        hive.createTableAndVerify(hivePartitionedSkewedStoredAsDirsTable);
        hiveAlterPartitionedFileFormats(hivePartitionedSkewedStoredAsDirsTable);
    }

    private void preparePartitionedData() throws Exception {

        if (hivePartitionedTable != null)
            return;
        hivePartitionedTable = TableFactory.getHiveByRowCommaExternalTable(HIVE_PARTITIONED_TABLE, HIVE_RC_COLS);
        hivePartitionedTable.setPartitionedBy(HIVE_PARTITION_COLUMN);
        hive.createTableAndVerify(hivePartitionedTable);
        hiveAlterPartitionedFileFormats(hivePartitionedTable);
    }

    private void prepareManyPartitionedData() throws Exception {

        if (hiveManyPartitionsTable != null)
            return;
        hiveManyPartitionsTable = new HiveTable(HIVE_MANY_PARTITIONED_TABLE, new String[] { "s1 STRING" });
        String[] partitionedColumns = Arrays.copyOfRange(HIVE_TYPES_COLS, 1, HIVE_TYPES_COLS.length);
        hiveManyPartitionsTable.setPartitionedBy(partitionedColumns);
        hive.createTableAndVerify(hiveManyPartitionsTable);
        hive.runQuery("SET hive.exec.dynamic.partition = true");
        hive.runQuery("SET hive.exec.dynamic.partition.mode = nonstrict");
        // Insert into table using dynamic partitioning.
        // Some of the fields are NULL so they will be inserted into the default partition.
        hive.insertDataToPartition(hiveTypesTable, hiveManyPartitionsTable,
                new String[] { "s2", "n1", "d1", "dc1", "tm", "f", "bg", "b", "tn", "sml", "dt", "vc1", "c1", "bin" },
                new String[] { "*" });
    }

    private void preparePartitionedClusteredData() throws Exception {

        if (hivePartitionedClusteredTable != null)
            return;
        hivePartitionedClusteredTable = TableFactory.getHiveByRowCommaExternalTable(
                HIVE_PARTITIONED_CLUSTERED_TABLE, HIVE_RC_COLS);
        hivePartitionedClusteredTable.setPartitionedBy(HIVE_PARTITION_COLUMN);
        hivePartitionedClusteredTable.setClusteredBy(new String[] { "t0" });
        hivePartitionedClusteredTable.setClusterBucketCount(10);
        hive.createTableAndVerify(hivePartitionedClusteredTable);
        hiveAlterPartitionedFileFormats(hivePartitionedClusteredTable);
    }

    private void hiveAlterPartitionedFileFormats(HiveTable table) throws Exception {

        prepareSmallData();
        prepareRCData();
        prepareSequenceData();
        prepareParquetData();
        prepareAvroData();

        String tableName = table.getName();
        String location = configuredNameNodeAddress + hdfsBaseDir;
        addHivePartition(tableName, "fmt = 'txt'", "'" + location + hiveSmallDataTable.getName() + "'");
        addHivePartition(tableName, "fmt = 'rc'", "'" + location + hiveRcTable.getName() + "'");
        addHivePartition(tableName, "fmt = 'seq'", "'" + location + hiveSequenceTable.getName() + "'");
        addHivePartition(tableName, "fmt = 'prq'", "'" + location + hiveParquetTable.getName() + "'");
        addHivePartition(tableName, "fmt = 'avro'", "'" + location + hiveAvroTable.getName() + "'");

        setHivePartitionFormat(tableName, "fmt = 'rc'", RCFILE);
        setHivePartitionFormat(tableName, "fmt = 'seq'", SEQUENCEFILE);
        setHivePartitionFormat(tableName, "fmt = 'prq'", PARQUET);
        setHivePartitionFormat(tableName, "fmt = 'avro'", AVRO);
    }

    /**
     * query for small data hive table
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = { "hive", "sanity" })
    public void sanity() throws Exception {

        createExternalTable(PXF_HIVE_SMALL_DATA_TABLE, PXF_HIVE_SMALLDATA_COLS, hiveSmallDataTable);

        runTincTest("pxf.features.hive.small_data.runTest");
        runTincTest("pxf.features.hcatalog.small_data.runTest");
    }

    /**
     * Query external table directed to hive table using hive primitive types
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = { "hive" })
    public void hivePrimitiveTypes() throws Exception {

        createExternalTable(GPDB_HIVE_TYPES_TABLE,
                PXF_HIVE_TYPES_COLS, hiveTypesTable);

        runTincTest("pxf.features.hive.primitive_types.runTest");
        runTincTest("pxf.features.hcatalog.primitive_types.runTest");
    }

    /**
     * PXF on Hive binary data table
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = { "hive" })
    public void hiveBinaryData() throws Exception {

        prepareBinaryData();
        createExternalTable(PXF_HIVE_BINARY_TABLE,
                new String[] { "b1 BYTEA" }, hiveBinaryTable);

        runTincTest("pxf.features.hive.binary_data.runTest");
    }

    /**
     * PXF on Hive ORC format table
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = { "hive" })
    public void storeAsOrc() throws Exception {

        prepareOrcData();
        createExternalTable(PXF_HIVE_SMALL_DATA_TABLE,
                PXF_HIVE_SMALLDATA_COLS, hiveOrcTable);

        runTincTest("pxf.features.hive.small_data.runTest");
        runTincTest("pxf.features.hcatalog.small_data_orc.runTest");
    }

    /**
     * PXF on Hive RC format table, without using profile
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = { "hive" })
    public void storeAsRc() throws Exception {

        prepareRCData();
        createExternalTable(PXF_HIVE_SMALL_DATA_TABLE,
                PXF_HIVE_SMALLDATA_COLS, hiveRcTable, false);

        runTincTest("pxf.features.hive.small_data.runTest");
        runTincTest("pxf.features.hcatalog.small_data_rc.runTest");
    }

    /**
     * PXF on Hive Sequence file format table
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = { "hive" })
    public void storeAsSequenceFile() throws Exception {

        prepareSequenceData();
        createExternalTable(PXF_HIVE_SMALL_DATA_TABLE,
                PXF_HIVE_SMALLDATA_COLS, hiveSequenceTable);

        runTincTest("pxf.features.hive.small_data.runTest");
        runTincTest("pxf.features.hcatalog.small_data_seq.runTest");
    }

    /**
     * PXF on Hive Parquet format table
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = { "hive" })
    public void storeAsParquet() throws Exception {

        prepareParquetData();
        createExternalTable(PXF_HIVE_SMALL_DATA_TABLE,
                PXF_HIVE_SMALLDATA_COLS, hiveParquetTable);

        runTincTest("pxf.features.hive.small_data.runTest");
        runTincTest("pxf.features.hcatalog.small_data_parquet.runTest");
    }

    /**
     * PXF on Hive Avro format table
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = { "hive" })
    public void storeAsAvro() throws Exception {

        prepareAvroData();
        createExternalTable(PXF_HIVE_SMALL_DATA_TABLE,
                PXF_HIVE_SMALLDATA_COLS, hiveAvroTable);

        runTincTest("pxf.features.hive.small_data.runTest");
        runTincTest("pxf.features.hcatalog.small_data_avro.runTest");
    }

    /**
     * PXF Negative test on Hive Views table. (not supported by PXF)
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = { "features" })
    public void viewNegative() throws Exception {

        HiveTable hiveTable = new HiveTable(hiveSmallDataTable.getName() + "_view", null);
        hive.runQuery("DROP VIEW " + hiveTable.getName());
        hive.runQuery("CREATE VIEW " + hiveTable.getName()
                + " AS SELECT s1 FROM " + hiveSmallDataTable.getName());

        createExternalTable("pxf_hive_view_table", new String[] { "t1 TEXT" }, hiveTable);

        runTincTest("pxf.features.hive.errors.hiveViews.runTest");
        runTincTest("pxf.features.hcatalog.errors.hiveViews.runTest");
    }

    /**
     * PXF Negative test using none exists Hive Table
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = { "features" })
    public void notExistingHiveTable() throws Exception {

        HiveTable hiveTable = new HiveTable("no_such_hive_table", null);
        createExternalTable("pxf_none_hive_table",
                new String[] { "t1    TEXT", "num1  INTEGER" }, hiveTable);

        runTincTest("pxf.features.hive.errors.notExistingHiveTable.runTest");
        runTincTest("pxf.features.hcatalog.errors.notExistingHiveTable.runTest");
    }

    /**
     * PXF on Hive table partitioned to text, rc and sequence data.
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = { "hive" })
    public void hivePartitionedTable() throws Exception {

        preparePartitionedData();
        // Create PXF Table using Hive profile
        createExternalTable(PXF_HIVE_PARTITIONED_TABLE,
                PXF_HIVE_SMALLDATA_FMT_COLS, hivePartitionedTable);

        runTincTest("pxf.features.hive.hive_partitioned_table.runTest");
        runTincTest("pxf.features.hcatalog.hive_partitioned_table.runTest");
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
                "ANALYZE for Hive plugin is not supported", true);

        runTincTest("pxf.features.hive.default_analyze.runTest");
    }

    /**
     * query pxf external table directed to hive table contains 3 types of
     * collections (Array, Map and Struct)
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = { "hive", "hcatalog" })
    public void hiveCollectionTypes() throws Exception {

        prepareHiveCollection();
        createExternalTable(PXF_HIVE_COLLECTIONS_TABLE,
                PXF_HIVE_COLLECTION_COLS, hiveCollectionTable);

        runTincTest("pxf.features.hive.collection_types.runTest");
        runTincTest("pxf.features.hcatalog.collection_types.runTest");
    }

    /**
     * Negative test case C119056 Negative - column types mismatch
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = { "hive" })
    public void columnDataTypeMisMatch() throws Exception {

        /* Here t1 column data type is passed as integer where as expected as
         * per hive table is string/text so data type mismatch will occur */
        String[] tableFieldTypes = PXF_HIVE_SMALLDATA_COLS.clone();
        tableFieldTypes[0] = "t1  INTEGER";
        createExternalTable(PXF_HIVE_SMALL_DATA_TABLE,
                tableFieldTypes, hiveSmallDataTable);

        runTincTest("pxf.features.hive.errors.columnDataTypeMisMatch.runTest");
    }

    /**
     * Negative - wrong profile : using HBase profile instead of Hive
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = { "features" })
    public void incorrectProfile() throws Exception {

        exTable = TableFactory.getPxfHiveReadableTable(PXF_HIVE_SMALL_DATA_TABLE,
                PXF_HIVE_SMALLDATA_COLS, hiveSmallDataTable, true);
        exTable.setProfile(EnumPxfDefaultProfiles.HBase.toString());
        createTable(exTable);

        runTincTest("pxf.features.hive.errors.incorrectProfile.runTest");
    }

    /**
     * If pxf table puts more columns than expected one in hive table,
     * an error should raise
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = { "hive" })
    public void columnCountMisMatch() throws Exception {

        // In pxf table creation a dummy extra column is added so that columns
        // count from pxf will not match with hive table
        createExternalTable(PXF_HIVE_SMALL_DATA_TABLE,
                PXF_HIVE_SMALLDATA_FMT_COLS, hiveSmallDataTable);

        runTincTest("pxf.features.hive.errors.columnCountMisMatch.runTest");
    }

    /**
     * Check no results when querying empty hive table
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = { "hive", "hcatalog" })
    public void noDataFilePresentForHive() throws Exception {
        /*
         * In this test case , we want a hive table which is not pointed to any data file or not having
         * any data loaded. so created a new table just for this test case and used it
         */
        HiveTable hiveTable = TableFactory.getHiveByRowCommaTable(
                HIVE_SMALL_DATA_TABLE + "_no_data_file", HIVE_SMALLDATA_COLS);
        hive.createTableAndVerify(hiveTable);

        createExternalTable(PXF_HIVE_SMALL_DATA_TABLE,
                PXF_HIVE_SMALLDATA_COLS, hiveTable);

        runTincTest("pxf.features.hive.noDataFilePresentForHive.runTest");
        runTincTest("pxf.features.hcatalog.noDataFilePresentForHive.runTest");
    }

    /**
     * Create Hive table separated to different partitions (text, RC and
     * Sequence) and do PXF filter push down on it.
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = { "hive" })
    public void partitionFilterPushDown() throws Exception {

        // Create Hive table with partitions
        hiveTable = TableFactory.getHiveByRowCommaExternalTable(
                HIVE_REG_HETEROGEN_TABLE, HIVE_RC_COLS);
        hiveTable.setPartitionedBy(HIVE_PARTITION_COLUMN);
        hive.createTableAndVerify(hiveTable);

        createHivePartitionTable(hiveTable.getName());

        // Create GPDB table without using profiles
        String extTableName = "hive_partition_filter_pushdown";
        exTable = TableFactory.getPxfHiveReadableTable(HIVE_PARTITION_FILTER_PUSHDOWN,
                PXF_HIVE_SMALLDATA_FMT_COLS, hiveTable, false);

        exTable.setFragmenter(TEST_PACKAGE + "HiveDataFragmenterWithFilter");
        exTable.setHost(pxfHost);
        exTable.setPort(pxfPort);

        // Set filter fragmenter for partition rc, mimic SELECT * WHERE fmt = 'rc'
        String filterString = "a4c25s2drco5";
        exTable.setUserParameters(hiveTestFilter(filterString));
        exTable.setName(extTableName + "_rc");
        gpdb.createTableAndVerify(exTable);

        // Set Filter fragmenter for partition seq, mimic SELECT * WHERE fmt = 'seq'
        filterString = "a4c25s3dseqo5";
        exTable.setUserParameters(hiveTestFilter(filterString));
        exTable.setName(extTableName + "_seq");
        gpdb.createTableAndVerify(exTable);

        // Set Filter fragmenter for partition txt, mimic SELECT * WHERE fmt = 'txt'
        filterString = "a4c25s3dtxto5";
        exTable.setUserParameters(hiveTestFilter(filterString));
        exTable.setName(extTableName + "_txt");
        gpdb.createTableAndVerify(exTable);

        // Set Filter fragmenter for partition orc, mimic SELECT * WHERE fmt = 'orc'
        filterString = "a4c25s3dorco5";
        exTable.setUserParameters(hiveTestFilter(filterString));
        exTable.setName(extTableName + "_orc");
        gpdb.createTableAndVerify(exTable);

        // Set Filter fragmenter for NO partition matching, mimic SELECT * WHERE fmt = 'none'
        filterString = "a4c25s4dnoneo5";
        exTable.setUserParameters(hiveTestFilter(filterString));
        exTable.setName(extTableName + "_none");
        gpdb.createTableAndVerify(exTable);

        /*
         * Set Filter fragmenter for partition and non partition filters, mimic
         * SELECT * WHERE t1 = 'row1' AND t2 < 'S_6' AND fmt = 'seq'
         *
         * but the t1 = 'row1' and t2 < 'S_6' will be ignored in PXF (and gpdb
         * as it's not known at all)
         */
        filterString = "a0c25s4drow1o5a1c25s3dS_6o1l0a4c25s3dseqo5l0";
        exTable.setUserParameters(hiveTestFilter(filterString));
        exTable.setName(extTableName + "_complex");
        createTable(exTable);

        runTincTest("pxf.features.hive.partitionFilterPushDown.runTest");
    }

    /**
     * Create Hive table separated to different partitions (text, RC and
     * Sequence) and set invalid filter string for PXF (without EQ sign), check
     * that exception is thrown.
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = { "hive" })
    public void invalidFilterPushDown() throws Exception {

        // Create Hive table with partitions
        hiveTable = TableFactory.getHiveByRowCommaExternalTable(
                HIVE_REG_HETEROGEN_TABLE, HIVE_RC_COLS);
        hiveTable.setPartitionedBy(HIVE_PARTITION_COLUMN);
        hive.createTableAndVerify(hiveTable);

        createHivePartitionTable(hiveTable.getName());

        // Create GPDB table without using profiles
        exTable = TableFactory.getPxfHiveReadableTable("hive_invalid_filter",
                PXF_HIVE_SMALLDATA_FMT_COLS, hiveTable, false);

        /* Set Filter fragmenter with NE instead of AND , mimic SELECT * WHERE
         * fmt = 'txt' [Invalid Operator] fmt = 'seq' */
        String filterString = "a4c25s3dtxto5a4c25s3dseqo5o6";
        exTable.setUserParameters(hiveTestFilter(filterString));
        exTable.setFragmenter(TEST_PACKAGE + "HiveDataFragmenterWithFilter");
        createTable(exTable);

        runTincTest("pxf.features.hive.errors.invalidFilterPushDown.runTest");
    }

    /**
     * Test queries on a Hive table with partitions of all supported types,
     * where some some values are always in the default hive partition. The test
     * checks that the values from the default hive partition are converted to
     * NULL values in GPDB.
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = { "hive", "hcatalog" })
    public void partitionsAllTypes() throws Exception {

        prepareManyPartitionedData();
        createExternalTable(HIVE_PARTITIONS_ALL_TYPES,
                PXF_HIVE_TYPES_COLS, hiveManyPartitionsTable);

        runTincTest("pxf.features.hive.partitions_all_types.runTest");
        runTincTest("pxf.features.hcatalog.partitions_all_types.runTest");
    }

    /**
     * Tries to query a complex type using a wrong field (non Text)
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = { "hive" })
    public void negativeCollectionTypes() throws Exception {

        HiveTable hiveTable = new HiveTable(HIVE_COLLECTIONS_TABLE,
                new String[] {
                        "s1 STRING",
                        "f1 FLOAT",
                        "a1 ARRAY<STRING>",
                        "m1 MAP<STRING, FLOAT>",
                        "sr1 STRUCT<street:STRING, city:STRING, state:ARRAY<STRING>, zip:INT>" });

        hiveTable.setFormat(FORMAT_ROW);
        hiveTable.setDelimiterFieldsBy("\\001");
        hiveTable.setDelimiterCollectionItemsBy("\\002");
        hiveTable.setDelimiterMapKeysBy("\\003");
        hiveTable.setDelimiterLinesBy("\\n");
        hiveTable.setStoredAs(TEXTFILE);

        hive.createTableAndVerify(hiveTable);
        loadDataIntoHive(HIVE_COLLECTIONS_FILE_NAME, hiveTable);

        createExternalTable(PXF_HIVE_COLLECTIONS_TABLE, new String[] {
                        "t1    TEXT",
                        "f1    REAL",
                        "t2    TEXT",
                        "t3    TEXT",
                        "wrong_field_type    INT" }, hiveTable);
        try {
            gpdb.queryResults(exTable, "SELECT * FROM " + exTable.getName() + " ORDER BY t1");
            Assert.fail("Querying a complex type using a wrong field type (non Text) should have throw Exception");
        } catch (Exception e) {
            ExceptionUtils.validate(null, e,
                    new PSQLException(
                            "schema requires type INTEGER but input record has type TEXT", null), true);
        }
    }

    /**
     * Queries a hive table with 30k files under one partition. It used to
     * result in OOM error while serializing the metadata (GPSQL-3143,
     * 92548154). The original test was with 100k which takes about an hour to
     * run, 30k files also resulted in OOM error but takes about 10 minutes including setup.
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = "load")
    public void thirtyKPartitions() throws Exception {

        HiveTable hiveTable = new HiveTable(HIVE_MANY_PARTITIONS, new String[] { "i INT" });
        hiveTable.setPartitionedBy(new String[] { "date_i STRING" });
        hive.createTableAndVerify(hiveTable);
        hive.alterTableAddPartition(hiveTable, new String[] { "date_i=\"2015-01-01\"" });
        hive.alterTableAddPartition(hiveTable, new String[] { "date_i=\"2015-01-02\"" });

        // Cleanup of temp directory
        File localDataTempFolder = new File(dataTempFolder);
        FileUtils.deleteDirectory(localDataTempFolder);
        // Create local Data folder
        localDataTempFolder.mkdirs();

        // Create non empty file to load into first partition:
        File fullPartitionFile = new File(dataTempFolder + "/many_parts_with_data");
        fullPartitionFile.createNewFile();
        FileWriter fw = new FileWriter(fullPartitionFile, true);
        fw.write("5\n6");
        fw.flush();
        fw.close();

        // copy data to hdfs
        hdfs.copyFromLocal(fullPartitionFile.getPath(),
                hdfs.getWorkingDirectory() + "/many_parts_with_data");

        hive.loadDataToPartition(hiveTable, hdfs.getWorkingDirectory()
                + "/many_parts_with_data", false, new String[] { "date_i=\"2015-01-01\"" });

        // Create empty files to load into second partition:
        for (int i = 0; i < 30000; i++) {
            File emptyPartitionFile = new File(dataTempFolder + "/many_parts_" + i + ".txt");
            emptyPartitionFile.createNewFile();
            // copy data to hdfs
            hdfs.copyFromLocal(emptyPartitionFile.getPath(),
                    hdfs.getWorkingDirectory() + "/many_parts_" + i + ".txt");
        }

        hive.loadDataToPartition(hiveTable, hdfs.getWorkingDirectory()
                + "/many_parts_*.txt", false, new String[] { "date_i=\"2015-01-02\"" });

        // Create pxf table
        createExternalTable("hive_30k_parts", new String[] { "i INT", "date_i TEXT" }, hiveTable);

        runTincTest("pxf.features.hive.partitions_30k.runTest");
    }

    /**
     * Test hcatalog queries within the same transaction
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = { "features", "hcatalog" })
    public void hcatalogInTransaction() throws Exception {

        // start transaction, query tables, stop transaction. then query different tables in the same session.
        preparePartitionedData();
        runTincTest("pxf.features.hcatalog.transaction.runTest");
    }

    /**
     * Test that a Hive Partitioned Clustered Table with all supported storage formats
     * (text, rc, sequence, avro) can be queried through HCatalog
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = { "hive", "hcatalog" })
    public void hivePartitionedClusteredTable() throws Exception {

        preparePartitionedClusteredData();
        createExternalTable(PXF_HIVE_PARTITIONED_CLUSTERED_TABLE,
                PXF_HIVE_SMALLDATA_FMT_COLS, hivePartitionedClusteredTable);

        runTincTest("pxf.features.hcatalog.hive_partitioned_clustered_table.runTest");
    }

    /**
     * Test that a Hive Partitioned Clustered and Sorted Table with all supported storage formats
     * (text, rc, sequence, avro) can be queried through Hcatalog
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = { "hive", "hcatalog" })
    public void hivePartitionedClusteredSortedTable() throws Exception {

        preparePartitionedClusteredSortedData();
        createExternalTable(PXF_HIVE_PARTITIONED_CLUSTERED_SORTED_TABLE,
                PXF_HIVE_SMALLDATA_FMT_COLS, hivePartitionedClusteredSortedTable);

        runTincTest("pxf.features.hcatalog.hive_partitioned_clustered_sorted_table.runTest");
    }

    /**
     * Test that a Hive Skewed Table with Stored As Directories options
     * with all supported storage formats
     * (text, rc, sequence, avro) can be queried through Hcatalog
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = { "hive", "hcatalog" })
    public void hivePartitionedSkewedTable() throws Exception {

        prepareSkewedData();
        createExternalTable(PXF_HIVE_PARTITIONED_SKEWED_TABLE,
                PXF_HIVE_SMALLDATA_FMT_COLS, hivePartitionedSkewedTable);

        runTincTest("pxf.features.hcatalog.hive_partitioned_skewed_table.runTest");
    }
    /**
     * Test that a Hive Skewed Table with Stored As Directories options with all supported storage formats
     * (text, rc, sequence, avro) can be queried through Hcatalog
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = { "hive", "hcatalog" })
    public void hivePartitionedSkewedStoredAsDirsTable() throws Exception {

        prepareSkewedStoredAsDirsData();
        createExternalTable(PXF_HIVE_PARTITIONED_SKEWED_STORED_TABLE,
                PXF_HIVE_SMALLDATA_FMT_COLS, hivePartitionedSkewedStoredAsDirsTable);

        runTincTest("pxf.features.hcatalog.hive_partitioned_skewed_stored_as_dirs_table.runTest");
    }

    /**
     * Test behavior of \d command on Hive tables
     *
     * @throws Exception if test fails to run
     */
    @Test(groups  = { "features", "hcatalog", "sanity" })
    public void describeHiveTable() throws Exception {

        prepareNonDefaultSchemaData();
        ShellSystemObject sso = gpdb.openPsql();

        // two tables with same name in different Hive schemas
        String psqlOutput = gpdb.runSqlCmd(sso, "\\d hcatalog.*.hive_s*m*_data", true);
        List<HiveTable> hiveTables = new ArrayList<>();
        hiveTables.add(hiveSmallDataTable);
        hiveTables.add(hiveNonDefaultSchemaTable);

        Assert.assertTrue(ComparisonUtils.comparePsqlDescribeHive(psqlOutput, hiveTables));

        // pattern which describes table and view
        // \d should not include view in response because it's not supported
        String hiveViewName = HIVE_SCHEMA + ".some_" + hiveNonDefaultSchemaTable.getName() + "_view";
        hive.runQuery("DROP VIEW " + hiveViewName);
        hive.runQuery("CREATE VIEW " + hiveViewName
                + " AS SELECT name FROM " + HIVE_SCHEMA + "." + hiveNonDefaultSchemaTable.getName());
        psqlOutput = gpdb.runSqlCmd(sso, "\\d hcatalog."+ HIVE_SCHEMA + "." + "*" +
                hiveNonDefaultSchemaTable.getName() + "*", true);
        hiveTables.remove(0);

        Assert.assertTrue(ComparisonUtils.comparePsqlDescribeHive(psqlOutput, hiveTables));

        // pattern is a name of a view ( \d should fail )
        psqlOutput = gpdb.runSqlCmd(sso, "\\d hcatalog."+ hiveViewName, false);
        Assert.assertTrue(psqlOutput.contains("Hive views are not supported by GPDB"));

        // pattern which describes table with complex types ( \d shouldn't fail )
        HiveTable hiveTable = TableFactory.getHiveByRowCommaTable(HIVE_COLLECTIONS_TABLE, HIVE_COLLECTION_COLS);
        psqlOutput = gpdb.runSqlCmd(sso, "\\d hcatalog."+ HIVE_COLLECTIONS_TABLE, false);
        hiveTables.clear();
        hiveTables.add(hiveTable);
        Assert.assertTrue(ComparisonUtils.comparePsqlDescribeHive(psqlOutput, hiveTables));

        // pattern which describes non existent table ( \d shouldn't fail )
        gpdb.runSqlCmd(sso, "\\d hcatalog."+ "abc*xyz", true);

        // pattern which describes one non existent table ( \d should fail )
        psqlOutput = gpdb.runSqlCmd(sso, "\\d hcatalog."+ "abcxyz", false);
        Assert.assertTrue(psqlOutput.contains("table not found"));

        // describe all Hive tables ( shouldn't fail )
        gpdb.runSqlCmd(sso, "\\d hcatalog.*", true);

        // describe all Hive tables in verbose mode ( shouldn't fail )
        gpdb.runSqlCmd(sso, "\\d+ hcatalog.*", true);

        String HIVE_ORDERING_TABLE1 = "hive_abc";
        String HIVE_ORDERING_TABLE2 = "hive_abc222";
        String HIVE_ORDERING_TABLE3 = "hive_abc";

        hiveTable = new HiveTable(HIVE_ORDERING_TABLE1, HIVE_RC_COLS);
        hive.createTableAndVerify(hiveTable);

        hiveTable = new HiveTable(HIVE_ORDERING_TABLE2, HIVE_RC_COLS);
        hive.createTableAndVerify(hiveTable);

        hiveTable = new HiveTable(HIVE_ORDERING_TABLE3, HIVE_SCHEMA, HIVE_RC_COLS);
        hive.createTableAndVerify(hiveTable);

        // describe three tables with same prefix
        // two tables in default schema, one table is in other schema
        // shouldn't combine them into one
        psqlOutput = gpdb.runSqlCmd(sso, "\\d hcatalog.*." + HIVE_ORDERING_TABLE1 + "*", true);
        Assert.assertTrue(psqlOutput.contains("PXF Hive Table \"default." + HIVE_ORDERING_TABLE1 + "\""));
        Assert.assertTrue(psqlOutput.contains("PXF Hive Table \"default." + HIVE_ORDERING_TABLE2 + "\""));
        Assert.assertTrue(psqlOutput.contains("PXF Hive Table \"" + HIVE_SCHEMA + "." + HIVE_ORDERING_TABLE3 + "\""));

        sso.close();
    }

    /**
     * Make sure GPDB is functional optimized profile for heterogeneous tables.
     * Each partition has different data.
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = { "hive", "hcatalog" })
    public void hiveHeterogenTableOptimizedProfile() throws Exception {

        // Create Hive table with partitions, when each partition has different data
        createGenerateHivePartitionTable("reg_heterogen_diff_data_partitions");

        runTincTest("pxf.features.hcatalog.heterogeneous_table.runTest");
    }

    /**
     * Make sure that PXF works with aggregate queries (including null columns)
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = { "hive", "hcatalog" })
    public void aggregateQueries() throws Exception {

        // hive table with nulls
        HiveTable hiveTable = new HiveTable("hive_table_with_nulls", HIVE_RC_COLS);
        hive.createTableAndVerify(hiveTable);
        hive.runQuery("INSERT INTO TABLE " + hiveTable.getName() +
                " SELECT s1, s2, n1, d1 FROM " + hiveTypesTable.getName());

        createExternalTable(PXF_HIVE_SMALL_DATA_TABLE, PXF_HIVE_SMALLDATA_COLS, hiveTable);

        runTincTest("pxf.features.hcatalog.aggregate_queries.runTest");
        runTincTest("pxf.features.hive.aggregate_queries.runTest");
    }

    /**
     * Query a small Hive table, skipping the first few rows with skip.header.line.count.
     * This table uses text storage.
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = { "hive" })
    public void hiveTableWithSkipHeader() throws Exception {
        List<List<String>> tableProperties = new ArrayList<>();
        tableProperties.add(Arrays.asList("skip.header.line.count", "3"));

        HiveTable hiveTableWithSkipHeader = new HiveTable("hive_table_with_skipHeader", HIVE_SMALLDATA_COLS);
        hiveTableWithSkipHeader.setTableProperties(tableProperties);
        hive.createTableAndVerify(hiveTableWithSkipHeader);
        hive.insertData(hiveSmallDataTable, hiveTableWithSkipHeader);

        createExternalTable("pxf_hive_table_with_skipheader", PXF_HIVE_SMALLDATA_COLS, hiveTableWithSkipHeader);

        runTincTest("pxf.features.hive.skip_header_rows.runTest");
    }
}
