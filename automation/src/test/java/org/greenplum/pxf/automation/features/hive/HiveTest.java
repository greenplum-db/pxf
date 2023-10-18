package org.greenplum.pxf.automation.features.hive;

import jsystem.framework.system.SystemManagerImpl;
import org.greenplum.pxf.automation.components.common.ShellSystemObject;
import org.greenplum.pxf.automation.components.hive.Hive;
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

    private static final String HIVE_DATA_FILE_NAME_3 = "hive_small_data_third.txt";

    private static final String PXF_HIVE_SMALL_DATA_TABLE_NON_SECURE = "pxf_hive_small_data_hive_non_secure";

    private HiveExternalTable hivePartitionedTable;
    private HiveTable hiveManyPartitionsTable;
    private HiveTable hivePartitionedClusteredTable;
    private HiveTable hivePartitionedClusteredSortedTable;
    private HiveTable hivePartitionedSkewedTable;
    private HiveTable hivePartitionedSkewedStoredAsDirsTable;

    private void preparePartitionedClusteredSortedData() throws Exception {

        if (hivePartitionedClusteredSortedTable != null)
            return;
        hivePartitionedClusteredSortedTable = TableFactory.getHiveByRowCommaExternalTable(
                HIVE_PARTITIONED_CLUSTERED_SORTED_TABLE, HIVE_RC_COLS);
        hivePartitionedClusteredSortedTable.setPartitionedBy(HIVE_PARTITION_COLUMN);
        hivePartitionedClusteredSortedTable.setSortedBy(new String[]{"num1"});
        hivePartitionedClusteredSortedTable.setClusteredBy(new String[]{"t1"});
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
        hivePartitionedSkewedTable.setSkewedBy(new String[]{"num1"});
        hivePartitionedSkewedTable.setSkewedOn(new String[]{"10"});
        hive.createTableAndVerify(hivePartitionedSkewedTable);
        hiveAlterPartitionedFileFormats(hivePartitionedSkewedTable);
    }

    private void prepareSkewedStoredAsDirsData() throws Exception {

        if (hivePartitionedSkewedStoredAsDirsTable != null)
            return;
        hivePartitionedSkewedStoredAsDirsTable = TableFactory.getHiveByRowCommaExternalTable(
                HIVE_PARTITIONED_SKEWED_STORED_TABLE, HIVE_RC_COLS);
        hivePartitionedSkewedStoredAsDirsTable.setPartitionedBy(HIVE_PARTITION_COLUMN);
        hivePartitionedSkewedStoredAsDirsTable.setSkewedBy(new String[]{"num1"});
        hivePartitionedSkewedStoredAsDirsTable.setSkewedOn(new String[]{"10"});
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
        hiveManyPartitionsTable = new HiveTable(HIVE_MANY_PARTITIONED_TABLE, new String[]{"t1 STRING"});
        String[] partitionedColumns = Arrays.copyOfRange(HIVE_TYPES_COLS, 1, HIVE_TYPES_COLS.length);
        hiveManyPartitionsTable.setPartitionedBy(partitionedColumns);
        hive.createTableAndVerify(hiveManyPartitionsTable);
        hive.runQuery("SET hive.exec.dynamic.partition = true");
        hive.runQuery("SET hive.exec.dynamic.partition.mode = nonstrict");
        hive.runQuery("SET hive.stats.autogather = false");
        // Insert into table using dynamic partitioning.
        // Some of the fields are NULL so they will be inserted into the default partition.
        hive.insertDataToPartition(hiveTypesTable, hiveManyPartitionsTable,
                new String[]{"t2", "num1", "dub1", "dec1", "tm", "r", "bg", "b", "tn", "sml", "dt", "vc1", "c1", "bin"},
                new String[]{"*"});
    }

    private void preparePartitionedClusteredData() throws Exception {

        if (hivePartitionedClusteredTable != null)
            return;
        hivePartitionedClusteredTable = TableFactory.getHiveByRowCommaExternalTable(
                HIVE_PARTITIONED_CLUSTERED_TABLE, HIVE_RC_COLS);
        hivePartitionedClusteredTable.setPartitionedBy(HIVE_PARTITION_COLUMN);
        hivePartitionedClusteredTable.setClusteredBy(new String[]{"t1"});
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
    @Test(groups = {"hive", "sanity", "features", "gpdb", "security"})
    public void sanity() throws Exception {

        createExternalTable(PXF_HIVE_SMALL_DATA_TABLE, PXF_HIVE_SMALLDATA_COLS, hiveSmallDataTable);

        runSqlTest("features/hive/small_data");
        runSqlTest("features/hcatalog/small_data");
    }

    @Test(groups = {"features", "security"})
    public void testSecureServerAndNonSecuredServer() throws Exception {
        if (hdfsNonSecure == null) return;

        Hive hiveNonSecure = (Hive) SystemManagerImpl.getInstance().getSystemObject("hiveNonSecure");

        HiveTable hiveSmallDataTable3 =
                prepareTableData(hdfsNonSecure, hiveNonSecure, null, HIVE_SMALL_DATA_TABLE, HIVE_SMALLDATA_COLS, HIVE_DATA_FILE_NAME_3);
        createExternalTable(PXF_HIVE_SMALL_DATA_TABLE_NON_SECURE, PXF_HIVE_SMALLDATA_COLS, hiveSmallDataTable3, true, "SERVER=hdfs-non-secure");

        runSqlTest("features/hive/secured_and_non_secured_hive");
    }

    /**
     * Query external table directed to hive table using hive primitive types
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"hive", "features", "gpdb", "security"})
    public void hivePrimitiveTypes() throws Exception {

        createExternalTable(GPDB_HIVE_TYPES_TABLE,
                PXF_HIVE_TYPES_COLS, hiveTypesTable);

        runSqlTest("features/hive/primitive_types");
        runSqlTest("features/hcatalog/primitive_types");
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
                PXF_HIVE_SUBSET_COLS, hiveSmallDataTable);

        runSqlTest("features/hive/column_subset");
    }

    /**
     * Create a Greenplum table with a subset of columns from the original
     * partitioned Hive table
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"hive", "features", "gpdb", "security"})
    public void columnSubsetOfPartitionedHiveSchema() throws Exception {

        preparePartitionedData();
        // Create PXF Table using HiveOrc profile
        createExternalTable(PXF_HIVE_SMALL_DATA_TABLE,
                PXF_HIVE_SUBSET_FMT_COLS, hivePartitionedTable);

        runSqlTest("features/hive/column_subset_partitioned_table");
    }

    /**
     * PXF on Hive Parquet format table, the table is altered after data is
     * inserted, and the query is still successful after more columns are
     * added to the Hive schema
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"hive", "features", "gpdb", "security"})
    public void readHiveTableAfterColumnsAddedToTable() throws Exception {

        prepareParquetForAlterData();
        createExternalTable(PXF_HIVE_SMALL_DATA_TABLE,
                PXF_HIVE_SMALLDATA_COLS, hiveParquetForAlterTable);

        runSqlTest("features/hive/small_data");

        hive.runQuery("ALTER TABLE " + hiveParquetForAlterTable.getName() + " ADD COLUMNS (new1 int)");
        hive.runQuery("INSERT INTO TABLE " + hiveParquetForAlterTable.getName() +
                " VALUES ('row11', 's_16', 11, 16, 100)");

        // Re-run the test to make sure we can still read the hive table
        runSqlTest("features/hive/small_data");

        gpdb.runQuery("ALTER EXTERNAL TABLE " + PXF_HIVE_SMALL_DATA_TABLE + " ADD COLUMN new1 INT");
        runSqlTest("features/hive/small_data_alter");
    }

    /**
     * PXF on Hive binary data table
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"hive", "features", "gpdb", "security"})
    public void hiveBinaryData() throws Exception {

        prepareBinaryData();
        createExternalTable(PXF_HIVE_BINARY_TABLE,
                new String[]{"b1 BYTEA"}, hiveBinaryTable);

        runSqlTest("features/hive/binary_data");
    }

    /**
     * PXF on Hive ORC format table
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"hive", "features", "gpdb", "security"})
    public void storeAsOrc() throws Exception {

        prepareOrcData();
        createExternalTable(PXF_HIVE_SMALL_DATA_TABLE,
                PXF_HIVE_SMALLDATA_COLS, hiveOrcTable);

        runSqlTest("features/hive/small_data");
        runSqlTest("features/hcatalog/small_data_orc");
    }

    /**
     * PXF on Hive RC format table, without using profile
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"hive", "features", "gpdb", "security"})
    public void storeAsRc() throws Exception {

        prepareRCData();
        createExternalTable(PXF_HIVE_SMALL_DATA_TABLE,
                PXF_HIVE_SMALLDATA_COLS, hiveRcTable, false);

        runSqlTest("features/hive/small_data");
        runSqlTest("features/hcatalog/small_data_rc");
    }

    /**
     * PXF on Hive Sequence file format table
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"hive", "features", "gpdb", "security"})
    public void storeAsSequenceFile() throws Exception {

        prepareSequenceData();
        createExternalTable(PXF_HIVE_SMALL_DATA_TABLE,
                PXF_HIVE_SMALLDATA_COLS, hiveSequenceTable);

        runSqlTest("features/hive/small_data");
        runSqlTest("features/hcatalog/small_data_seq");
    }

    /**
     * PXF on Hive Parquet format table
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"hive", "features", "gpdb", "security"})
    public void storeAsParquet() throws Exception {

        prepareParquetData();
        createExternalTable(PXF_HIVE_SMALL_DATA_TABLE,
                PXF_HIVE_SMALLDATA_COLS, hiveParquetTable);

        runSqlTest("features/hive/small_data");
        runSqlTest("features/hcatalog/small_data_parquet");
    }

    /**
     * PXF on Hive OpenCSV format table
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"hive", "features", "gpdb", "security"})
    public void storeAsHiveOpenCsv() throws Exception {

        prepareOpenCsvData();
        createExternalTable(PXF_HIVE_SMALL_DATA_TABLE,
                PXF_HIVE_SMALLDATA_AS_TEXT_COLS, hiveOpenCsvTable);

        runSqlTest("features/hive/small_data_as_text");
    }

    /**
     * PXF on Hive Avro format table
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"hive", "features", "gpdb", "security"})
    public void storeAsAvro() throws Exception {

        prepareAvroData();
        createExternalTable(PXF_HIVE_SMALL_DATA_TABLE,
                PXF_HIVE_SMALLDATA_COLS, hiveAvroTable);

        runSqlTest("features/hive/small_data");
        runSqlTest("features/hcatalog/small_data_avro");
    }

    /**
     * PXF Negative test on Hive Views table. (not supported by PXF)
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"features"})
    public void viewNegative() throws Exception {

        HiveTable hiveTable = new HiveTable(hiveSmallDataTable.getName() + "_view", null);
        hive.runQuery("DROP VIEW " + hiveTable.getName());
        hive.runQuery("CREATE VIEW " + hiveTable.getName()
                + " AS SELECT s1 FROM " + hiveSmallDataTable.getName());

        createExternalTable("pxf_hive_view_table", new String[]{"t1 TEXT"}, hiveTable);

        runSqlTest("features/hive/errors/hiveViews");
        runSqlTest("features/hcatalog/errors/hiveViews");
    }

    /**
     * PXF Negative test using none exists Hive Table
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"hive", "features", "gpdb"})
    public void notExistingHiveTable() throws Exception {

        HiveTable hiveTable = new HiveTable("no_such_hive_table", null);
        createExternalTable("pxf_none_hive_table",
                new String[]{"t1    TEXT", "num1  INTEGER"}, hiveTable);

        runSqlTest("features/hive/errors/notExistingHiveTable");
        runSqlTest("features/hcatalog/errors/notExistingHiveTable");
    }

    /**
     * PXF on Hive table partitioned to text, rc and sequence data.
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"hive", "features", "gpdb", "security"})
    public void hivePartitionedTable() throws Exception {

        preparePartitionedData();
        // Create PXF Table using Hive profile
        createExternalTable(PXF_HIVE_PARTITIONED_TABLE,
                PXF_HIVE_SMALLDATA_FMT_COLS, hivePartitionedTable);

        runSqlTest("features/hive/hive_partitioned_table");
        runSqlTest("features/hcatalog/hive_partitioned_table");
    }

    /**
     * Test hive tables stored as parquet with timestamps issue #328
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"hive", "features", "gpdb", "security"})
    public void hiveStoredAsParquetWithTimestamps() throws Exception {

        HiveTable parquetTimestampTable = new HiveTable(HIVE_PARQUET_TIMESTAMP_TABLE, PARQUET_TIMESTAMP_COLS);
        parquetTimestampTable.setStoredAs("PARQUET");
        hive.createTableAndVerify(parquetTimestampTable);
        hive.runQuery("INSERT INTO TABLE " + parquetTimestampTable.getName() +
                " VALUES ('2020-04-03 06:50:52.188'), ('2020-04-03 06:55:19.028'), ('2020-04-03 06:55:26.078'), ('2020-04-03 06:55:38.948')");

        createExternalTable(PXF_HIVE_PARQUET_TIMESTAMP_TABLE, PARQUET_TIMESTAMP_COLS, parquetTimestampTable);

        runSqlTest("features/hive/parquet_timestamp");
    }

    /**
     * Test predicate pushdown(ppd) functionality on Hive table partitioned by string and integer columns
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"hive", "features", "gpdb", "security"})
    public void hivePartitionedPPDTable() throws Exception {

        HiveExternalTable hivePartitionedPPDTable = TableFactory.getHiveByRowCommaExternalTable(HIVE_PARTITIONED_PPD_TABLE, HIVE_SMALLDATA_PPD_COLS);
        hivePartitionedPPDTable.setPartitionedBy(HIVE_PARTITION_PPD_COLS);
        hive.createTableAndVerify(hivePartitionedPPDTable);

        hive.runQuery("SET hive.exec.dynamic.partition = true");
        hive.runQuery("SET hive.exec.dynamic.partition.mode = nonstrict");
        hive.insertDataToPartition(hiveSmallDataTable, hivePartitionedPPDTable,
                new String[]{"t2, num1"}, new String[]{"t1", "dub1", "t2", "num1"});

        // Create PXF Table using Hive profile
        createExternalTable(PXF_HIVE_PARTITIONED_PPD_TABLE,
                PXF_HIVE_SMALLDATA_PPD_COLS, hivePartitionedPPDTable);

        runSqlTest("features/hive/hive_partitioned_ppd_table");
    }

    /**
     * Test predicate pushdown(ppd) functionality on partitioned Hive table with custom filter
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"hive", "features", "gpdb", "security"})
    public void hivePartitionedPPDTableCustomFilters() throws Exception {

        // Hive talbe with partition columns s2, n1.
        // 10 partition with one row in each partition
        HiveExternalTable hivePartitionedPPDTable = TableFactory.getHiveByRowCommaExternalTable(HIVE_PARTITIONED_PPD_TABLE, HIVE_SMALLDATA_PPD_COLS);
        hivePartitionedPPDTable.setPartitionedBy(HIVE_PARTITION_PPD_COLS);
        hive.createTableAndVerify(hivePartitionedPPDTable);

        hive.runQuery("SET hive.exec.dynamic.partition = true");
        hive.runQuery("SET hive.exec.dynamic.partition.mode = nonstrict");
        hive.insertDataToPartition(hiveSmallDataTable, hivePartitionedPPDTable,
                new String[]{"t2, num1"}, new String[]{"t1", "dub1", "t2", "num1"});

        String extTableName = PXF_HIVE_PARTITIONED_PPD_TABLE + "_customfilter";
        String filterString;

        // Filter with NP1 AND NP2 AND P3 -> P3
        // Test for (t1 LIKE 'row1') AND (num1 < 999) AND (t2 = 's_14) return one partitions
        exTable = TableFactory.getPxfHiveReadableTable(extTableName,
                PXF_HIVE_SMALLDATA_PPD_COLS, hivePartitionedPPDTable, false);
        filterString = "a0c25s4drow1o7a3c23s3d999o1l0a2c25s4ds_14o5l0";
        exTable.setName(extTableName + "_1");
        exTable.setUserParameters(hiveTestFilter(filterString));
        exTable.setFragmenter(TEST_PACKAGE + "HiveDataFragmenterWithFilter");
        createTable(exTable);

        // Filter with P1 AND (NOT P2 OR NOT P3) -> P1
        // (t2 = 's_9') AND (NOT(t2 = 's_7') OR NOT(num1 = 4)) should return one partition
        filterString = "a2c25s3ds_7o5a3c23s1d4o5l2a2c25s3ds_9o5l2l1l0";
        exTable.setName(extTableName + "_2");
        exTable.setUserParameters(hiveTestFilter(filterString));
        exTable.setFragmenter(TEST_PACKAGE + "HiveDataFragmenterWithFilter");
        createTable(exTable);

        // Filter with P1 OR (NOT P2 OR NOT P3) -> null
        // (t2 = 's_9') OR (NOT(t2 = 's_7') OR NOT(num1 = 4)) should return all partitions
        filterString = "a2c25s3ds_7o5a3c23s1d4o5l2a2c25s3ds_9o5l2l1l1";
        exTable.setName(extTableName + "_3");
        exTable.setUserParameters(hiveTestFilter(filterString));
        exTable.setFragmenter(TEST_PACKAGE + "HiveDataFragmenterWithFilter");
        createTable(exTable);

        // Test for != operators
        // (t2 != 's_14') should return 9 rows
        filterString = "a2c25s4ds_14o6";
        exTable.setName(extTableName + "_4");
        exTable.setUserParameters(hiveTestFilter(filterString));
        exTable.setFragmenter(TEST_PACKAGE + "HiveDataFragmenterWithFilter");
        createTable(exTable);

        runSqlTest("features/hive/hive_partitioned_ppd_table_customfilters");
    }

    /**
     * PXF on Hive table partitioned to text, rc and sequence data.
     * Test union all
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"hive", "features", "gpdb", "security"})
    public void hivePartitionedTableUnionAll() throws Exception {

        preparePartitionedData();
        // Create PXF Table using Hive profile
        createExternalTable(PXF_HIVE_PARTITIONED_TABLE,
                PXF_HIVE_SMALLDATA_FMT_COLS, hivePartitionedTable);

        runSqlTest("features/hive/hive_partitioned_table_union_all");
    }

    /**
     * check default analyze results for pxf external table is as required
     * (pages=1000 tuples=1000000)
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"features"})
    public void defaultAnalyze() throws Exception {

        createExternalTable(PXF_HIVE_SMALL_DATA_TABLE,
                PXF_HIVE_SMALLDATA_COLS, hiveSmallDataTable);

        // Perform Analyze on external table and check suitable Warnings.
        gpdb.runQueryWithExpectedWarning("ANALYZE " + exTable.getName(),
                "ANALYZE for Hive plugin is not supported", true);

        runSqlTest("features/hive/default_analyze");
    }

    /**
     * query pxf external table directed to hive table contains 3 types of
     * collections (Array, Map and Struct)
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"hive", "hcatalog", "features", "gpdb", "security"})
    public void hiveCollectionTypes() throws Exception {

        prepareHiveCollection();
        createExternalTable(PXF_HIVE_COLLECTIONS_TABLE,
                PXF_HIVE_COLLECTION_COLS, hiveCollectionTable);

        runSqlTest("features/hive/collection_types");
        runSqlTest("features/hcatalog/collection_types");
    }

    /**
     * Negative test case C119056 Negative - column types mismatch
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"hive", "features", "gpdb", "security"})
    public void columnDataTypeMisMatch() throws Exception {

        /* Here t1 column data type is passed as integer where as expected as
         * per hive table is string/text so data type mismatch will occur */
        String[] tableFieldTypes = PXF_HIVE_SMALLDATA_COLS.clone();
        tableFieldTypes[0] = "t1  INTEGER";
        createExternalTable(PXF_HIVE_SMALL_DATA_TABLE,
                tableFieldTypes, hiveSmallDataTable);

        runSqlTest("features/hive/errors/columnDataTypeMisMatch");
    }

    /**
     * Negative - wrong profile : using HBase profile instead of Hive
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"features"})
    public void incorrectProfile() throws Exception {

        exTable = TableFactory.getPxfHiveReadableTable(PXF_HIVE_SMALL_DATA_TABLE,
                PXF_HIVE_SMALLDATA_COLS, hiveSmallDataTable, true);
        exTable.setProfile(EnumPxfDefaultProfiles.HBase.toString());
        createTable(exTable);

        runSqlTest("features/hive/errors/incorrectProfile");
    }

    /**
     * Tests when the column name doesn't match any of the columns defined at
     * the Greenplum table definition
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"hive", "features", "gpdb", "security"})
    public void columnNameMismatch() throws Exception {

        String[] nonMatchingColumnNames = PXF_HIVE_SMALLDATA_FMT_COLS.clone();

        nonMatchingColumnNames[1] = "s2    TEXT";

        // In pxf table creation a dummy extra column is added so that columns
        // count from pxf will not match with hive table
        createExternalTable(PXF_HIVE_SMALL_DATA_TABLE,
                nonMatchingColumnNames, hiveSmallDataTable);

        runSqlTest("features/hive/errors/columnNameMismatch");
    }

    /**
     * Check no results when querying empty hive table
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"hive", "hcatalog", "features", "gpdb", "security"})
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

        runSqlTest("features/hive/noDataFilePresentForHive");
        runSqlTest("features/hcatalog/noDataFilePresentForHive");
    }

    /**
     * Create Hive table separated to different partitions (text, RC and
     * Sequence) and do PXF filter push down on it.
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"hive", "features", "gpdb", "security"})
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

        runSqlTest("features/hive/partitionFilterPushDown");
    }

    /**
     * Create Hive table separated to different partitions (text, RC and
     * Sequence) and set invalid filter string for PXF (without EQ sign), check
     * that exception is thrown.
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"hive", "features", "gpdb", "security"})
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

        runSqlTest("features/hive/errors/invalidFilterPushDown");
    }

    /**
     * Test queries on a Hive table with partitions of all supported types,
     * where some some values are always in the default hive partition. The test
     * checks that the values from the default hive partition are converted to
     * NULL values in GPDB.
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"hive", "hcatalog", "features", "gpdb", "security"})
    public void partitionsAllTypes() throws Exception {

        prepareManyPartitionedData();
        createExternalTable(HIVE_PARTITIONS_ALL_TYPES,
                PXF_HIVE_TYPES_COLS, hiveManyPartitionsTable);

        runSqlTest("features/hive/partitions_all_types");
        runSqlTest("features/hcatalog/partitions_all_types");
    }

    /**
     * Tries to query a complex type using a wrong field (non Text)
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"hive", "features", "gpdb", "security"})
    public void negativeCollectionTypes() throws Exception {

        HiveTable hiveTable = new HiveTable(HIVE_COLLECTIONS_TABLE,
                new String[]{
                        "t1 STRING",
                        "f1 FLOAT",
                        "t2 ARRAY<STRING>",
                        "t3 MAP<STRING, FLOAT>",
                        "wrong_field_type STRUCT<street:STRING, city:STRING, state:ARRAY<STRING>, zip:INT>"});

        hiveTable.setFormat(FORMAT_ROW);
        hiveTable.setDelimiterFieldsBy("\\001");
        hiveTable.setDelimiterCollectionItemsBy("\\002");
        hiveTable.setDelimiterMapKeysBy("\\003");
        hiveTable.setDelimiterLinesBy("\\n");
        hiveTable.setStoredAs(TEXTFILE);

        hive.createTableAndVerify(hiveTable);
        loadDataIntoHive(HIVE_COLLECTIONS_FILE_NAME, hiveTable);

        createExternalTable(PXF_HIVE_COLLECTIONS_TABLE, new String[]{
                "t1    TEXT",
                "f1    REAL",
                "t2    TEXT",
                "t3    TEXT",
                "wrong_field_type    INT"}, hiveTable);
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

        HiveTable hiveTable = new HiveTable(HIVE_MANY_PARTITIONS, new String[]{"i INT"});
        hiveTable.setPartitionedBy(new String[]{"date_i STRING"});
        hive.createTableAndVerify(hiveTable);
        hive.alterTableAddPartition(hiveTable, new String[]{"date_i=\"2015-01-01\""});
        hive.alterTableAddPartition(hiveTable, new String[]{"date_i=\"2015-01-02\""});

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
                + "/many_parts_with_data", false, new String[]{"date_i=\"2015-01-01\""});

        // Create empty files to load into second partition:
        for (int i = 0; i < 30000; i++) {
            File emptyPartitionFile = new File(dataTempFolder + "/many_parts_" + i + ".txt");
            emptyPartitionFile.createNewFile();
            // copy data to hdfs
            hdfs.copyFromLocal(emptyPartitionFile.getPath(),
                    hdfs.getWorkingDirectory() + "/many_parts_" + i + ".txt");
        }

        hive.loadDataToPartition(hiveTable, hdfs.getWorkingDirectory()
                + "/many_parts_*.txt", false, new String[]{"date_i=\"2015-01-02\""});

        // Create pxf table
        createExternalTable("hive_30k_parts", new String[]{"i INT", "date_i TEXT"}, hiveTable);

        runSqlTest("features/hive/partitions_30k");
    }

    /**
     * Test hcatalog queries within the same transaction
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"features", "hcatalog"})
    public void hcatalogInTransaction() throws Exception {

        // start transaction, query tables, stop transaction. then query different tables in the same session.
        preparePartitionedData();
        runSqlTest("features/hcatalog/transaction");
    }

    /**
     * Test that a Hive Partitioned Clustered Table with all supported storage formats
     * (text, rc, sequence, avro) can be queried through HCatalog
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"hive", "hcatalog", "features", "gpdb", "security"})
    public void hivePartitionedClusteredTable() throws Exception {

        preparePartitionedClusteredData();
        createExternalTable(PXF_HIVE_PARTITIONED_CLUSTERED_TABLE,
                PXF_HIVE_SMALLDATA_FMT_COLS, hivePartitionedClusteredTable);

        runSqlTest("features/hcatalog/hive_partitioned_clustered_table");
    }

    /**
     * Test that a Hive Partitioned Clustered and Sorted Table with all supported storage formats
     * (text, rc, sequence, avro) can be queried through Hcatalog
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"hive", "hcatalog", "features", "gpdb", "security"})
    public void hivePartitionedClusteredSortedTable() throws Exception {

        preparePartitionedClusteredSortedData();
        createExternalTable(PXF_HIVE_PARTITIONED_CLUSTERED_SORTED_TABLE,
                PXF_HIVE_SMALLDATA_FMT_COLS, hivePartitionedClusteredSortedTable);

        runSqlTest("features/hcatalog/hive_partitioned_clustered_sorted_table");
    }

    /**
     * Test that a Hive Skewed Table with Stored As Directories options
     * with all supported storage formats
     * (text, rc, sequence, avro) can be queried through Hcatalog
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"hive", "hcatalog", "features", "gpdb", "security"})
    public void hivePartitionedSkewedTable() throws Exception {

        prepareSkewedData();
        createExternalTable(PXF_HIVE_PARTITIONED_SKEWED_TABLE,
                PXF_HIVE_SMALLDATA_FMT_COLS, hivePartitionedSkewedTable);

        runSqlTest("features/hcatalog/hive_partitioned_skewed_table");
    }

    /**
     * Test that a Hive Skewed Table with Stored As Directories options with all supported storage formats
     * (text, rc, sequence, avro) can be queried through Hcatalog
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"hive", "hcatalog", "features", "gpdb", "security"})
    public void hivePartitionedSkewedStoredAsDirsTable() throws Exception {

        prepareSkewedStoredAsDirsData();
        createExternalTable(PXF_HIVE_PARTITIONED_SKEWED_STORED_TABLE,
                PXF_HIVE_SMALLDATA_FMT_COLS, hivePartitionedSkewedStoredAsDirsTable);

        runSqlTest("features/hcatalog/hive_partitioned_skewed_stored_as_dirs_table");
    }

    /**
     * Test behavior of \d command on Hive tables
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"features", "hcatalog", "sanity"})
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
        psqlOutput = gpdb.runSqlCmd(sso, "\\d hcatalog." + HIVE_SCHEMA + "." + "*" +
                hiveNonDefaultSchemaTable.getName() + "*", true);
        hiveTables.remove(0);

        Assert.assertTrue(ComparisonUtils.comparePsqlDescribeHive(psqlOutput, hiveTables));

        // pattern is a name of a view ( \d should fail )
        psqlOutput = gpdb.runSqlCmd(sso, "\\d hcatalog." + hiveViewName, false);
        Assert.assertTrue(psqlOutput.contains("Hive views are not supported by GPDB"));

        // pattern which describes table with complex types ( \d shouldn't fail )
        HiveTable hiveTable = TableFactory.getHiveByRowCommaTable(HIVE_COLLECTIONS_TABLE, HIVE_COLLECTION_COLS);
        psqlOutput = gpdb.runSqlCmd(sso, "\\d hcatalog." + HIVE_COLLECTIONS_TABLE, false);
        hiveTables.clear();
        hiveTables.add(hiveTable);
        Assert.assertTrue(ComparisonUtils.comparePsqlDescribeHive(psqlOutput, hiveTables));

        // pattern which describes non existent table ( \d shouldn't fail )
        gpdb.runSqlCmd(sso, "\\d hcatalog." + "abc*xyz", true);

        // pattern which describes one non existent table ( \d should fail )
        psqlOutput = gpdb.runSqlCmd(sso, "\\d hcatalog." + "abcxyz", false);
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
    @Test(groups = {"hive", "hcatalog", "features", "gpdb", "security"})
    public void hiveHeterogenTableOptimizedProfile() throws Exception {

        // Create Hive table with partitions, when each partition has different data
        createGenerateHivePartitionTable("reg_heterogen_diff_data_partitions");

        runSqlTest("features/hcatalog/heterogeneous_table");
    }

    /**
     * Make sure that PXF works with aggregate queries (including null columns)
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"hive", "hcatalog", "features", "gpdb", "security"})
    public void aggregateQueries() throws Exception {

        // hive table with nulls
        HiveTable hiveTable = new HiveTable("hive_table_with_nulls", HIVE_RC_COLS);
        hive.createTableAndVerify(hiveTable);
        hive.runQuery("INSERT INTO TABLE " + hiveTable.getName() +
                " SELECT t1, t2, num1, dub1 FROM " + hiveTypesTable.getName());

        createExternalTable(PXF_HIVE_SMALL_DATA_TABLE, PXF_HIVE_SMALLDATA_COLS, hiveTable);

        runSqlTest("features/hcatalog/aggregate_queries");
        runSqlTest("features/hive/aggregate_queries");
    }

    /**
     * Query a small Hive table, skipping the first few rows with skip.header.line.count.
     * This table uses text storage.
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"hive", "features", "gpdb", "security"})
    public void hiveTableWithSkipHeader() throws Exception {
        List<List<String>> tableProperties = new ArrayList<>();
        tableProperties.add(Arrays.asList("skip.header.line.count", "3"));

        HiveTable hiveTableWithSkipHeader = new HiveTable("hive_table_with_skipHeader", HIVE_SMALLDATA_COLS);
        hiveTableWithSkipHeader.setTableProperties(tableProperties);
        hive.createTableAndVerify(hiveTableWithSkipHeader);
        hive.insertData(hiveSmallDataTable, hiveTableWithSkipHeader);

        createExternalTable("pxf_hive_table_with_skipheader", PXF_HIVE_SMALLDATA_COLS, hiveTableWithSkipHeader);

        runSqlTest("features/hive/skip_header_rows");
    }

    /**
     * Queries a Hive Parquet table consisting of several partitions with backing Parquet files having
     * column order mismatch and several extra / missing columns.
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"hive", "features", "gpdb", "security"})
    public void hiveTablePartitionedWithParquetColumnMismatch() throws Exception {
        // create the source table with the superset of all columns and data for all partitions
        prepareParquetMismatchSourceTable();

        // create Hive readable table with a subset of source columns (no 'ext_par' column)
        createHiveParquetColumnMismatchPartitionTable();

        // create PXF external table with a narrower subset of columns (no 'ext_par' and 'ext_hive_par' columns)
        createExternalTable("pxf_hive_parquet_mismatch", PXF_HIVE_PARQUET_MISMATCH_COLS, hiveParquetMismatchTable);

        runSqlTest("features/hive/parquet_mismatch");
    }

    /**
     * Queries a Hive table consisting of several partitions with backing Parquet or text files having
     * a nested struct type column. It tests both null cases as well as special characters.
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"hive", "features", "gpdb", "security"})
    public void hiveNestedStruct() throws Exception {
        // create Hive readable table with data stored as PARQUET
        hiveNestedStructTable = new HiveTable(HIVE_NESTED_STRUCT_TABLE, HIVE_NESTED_STRUCT_COLS);
        hiveNestedStructTable.setStoredAs(PARQUET);
        hive.createTableAndVerify(hiveNestedStructTable);

        hive.runQuery("INSERT INTO TABLE " + hiveNestedStructTable.getName() +
                " SELECT NAMED_STRUCT('field1', NAMED_STRUCT('subfield1', cast(NULL AS STRING), 'subfield2', 'test string'), 'field2', 1002) as t1" +
                " FROM " + HIVE_SMALL_DATA_TABLE + " LIMIT 1");
        hive.runQuery("INSERT INTO TABLE " + hiveNestedStructTable.getName() +
                " SELECT NAMED_STRUCT('field1', NAMED_STRUCT('subfield1', 'a really \"fancy\" string', 'subfield2', 'test string'), 'field2', 1002) as t1" +
                " FROM " + HIVE_SMALL_DATA_TABLE + " LIMIT 1");

        createExternalTable(PXF_HIVE_NESTED_STRUCT_TABLE, PXF_HIVE_NESTED_STRUCT_COLS, hiveNestedStructTable);

        runSqlTest("features/hive/nested_struct");

        // recreate table stored as TEXTFILE
        hiveNestedStructTable = new HiveTable(HIVE_NESTED_STRUCT_TABLE, HIVE_NESTED_STRUCT_COLS);
        hiveNestedStructTable.setStoredAs(TEXTFILE);
        hive.createTableAndVerify(hiveNestedStructTable);

        hive.runQuery("INSERT INTO TABLE " + hiveNestedStructTable.getName() +
                " SELECT NAMED_STRUCT('field1', NAMED_STRUCT('subfield1', cast(NULL AS STRING), 'subfield2', 'test string'), 'field2', 1002) as t1" +
                " FROM " + HIVE_SMALL_DATA_TABLE + " LIMIT 1");
        hive.runQuery("INSERT INTO TABLE " + hiveNestedStructTable.getName() +
                " SELECT NAMED_STRUCT('field1', NAMED_STRUCT('subfield1', 'a really \"fancy\" string', 'subfield2', 'test string'), 'field2', 1002) as t1" +
                " FROM " + HIVE_SMALL_DATA_TABLE + " LIMIT 1");

        runSqlTest("features/hive/nested_struct");
    }

}
