package org.greenplum.pxf.automation.features.hive;

import org.greenplum.pxf.automation.structures.tables.basic.Table;
import org.greenplum.pxf.automation.structures.tables.hive.HiveExternalTable;
import org.greenplum.pxf.automation.structures.tables.hive.HiveTable;
import org.greenplum.pxf.automation.structures.tables.utils.TableFactory;
import org.greenplum.pxf.automation.utils.tables.ComparisonUtils;
import org.greenplum.pxf.automation.datapreparer.hive.HiveRcBinaryPreparer;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class HiveRcTest extends HiveBaseTest {

    private HiveTable hiveRcTypes = null;
    private HiveTable hiveRcTableNoSerde = null;


    @Override
    protected void createExternalTable(String tableName, String[] fields,
                                       HiveTable hiveTable, boolean useProfile) throws Exception {

        exTable = TableFactory.getPxfHiveRcReadableTable(tableName, fields, hiveTable, useProfile);
        createTable(exTable);
    }

    void prepareRCData() throws Exception {

        // hive types data
        HiveTable hiveTable = TableFactory.getHiveByRowCommaTable(
                "hive_types_limited", HIVE_TYPES_LIMITED_COLS);
        hive.createTableAndVerify(hiveTable);
        loadDataIntoHive(HIVE_TYPES_LIMITED_FILE_NAME, hiveTable);

        // create hive RC table and load data from "hive_types" table to it
        hiveRcTypes = new HiveTable(HIVE_TYPES_TABLE + "_rc", HIVE_TYPES_LIMITED_COLS);

        hiveRcTypes.setFormat(FORMAT_ROW);
        hiveRcTypes.setSerde(COLUMNAR_SERDE);
        hiveRcTypes.setStoredAs(RCFILE);

        hive.createTableAndVerify(hiveRcTypes);
        hive.runQuery("INSERT INTO TABLE " + hiveRcTypes.getName() +
                " SELECT * FROM " + hiveTable.getName());
    }

    @Override
    void prepareData() throws Exception {

        prepareSmallData();
        prepareRCData();

        // create RC tables with and without serde mentioned
        hiveRcTable = createRcTableAndLoadData(HIVE_RC_TABLE, true);
        hiveRcForAlterTable = createRcTableAndLoadData(HIVE_RC_FOR_ALTER_TABLE, true);
        hiveRcTableNoSerde = createRcTableAndLoadData(HIVE_RC_TABLE + "_no_serde", false);
        comparisonDataTable = new Table("comparisonData", null);
    }

    /**
     * Create Hive RC table and load small data to it
     *
     * @param tableName for new Hive RC table
     * @param setSerde  use "ColumnarSerDe" in hive table creation or not.
     * @return {@link HiveTable} object
     * @throws Exception if test fails to run
     */
    private HiveTable createRcTableAndLoadData(String tableName, boolean setSerde)
            throws Exception {

        HiveTable hiveRcTable = new HiveTable(tableName, HIVE_RC_COLS);

        // if setSerde is true use ROW foramt ans ColumnarSerDe serde
        if (setSerde) {
            hiveRcTable.setFormat(FORMAT_ROW);
            hiveRcTable.setSerde(COLUMNAR_SERDE);
        }

        // set RCFILE format for storage
        hiveRcTable.setStoredAs(RCFILE);
        hive.createTableAndVerify(hiveRcTable);

        // load data from small data text file
        hive.runQuery("INSERT INTO TABLE " + hiveRcTable.getName() +
                " SELECT * FROM " + hiveSmallDataTable.getName());

        return hiveRcTable;
    }


    private void addPartitionsOne(HiveTable hiveTable, HiveTable partitionTable) throws Exception {

        addTableAsPartition(hiveTable, "fmt = 'rc1'", partitionTable);
        addTableAsPartition(hiveTable, "fmt = 'rc2'", partitionTable);
        addTableAsPartition(hiveTable, "fmt = 'rc3'", partitionTable);
    }

    private void addPartitionsTwo(HiveTable hiveTable, HiveTable partitionTable) throws Exception {

        addTableAsPartition(hiveTable, "fmt = 'rc1', prt = 'a'", partitionTable);
        addTableAsPartition(hiveTable, "fmt = 'rc1', prt = 'b'", partitionTable);
        addTableAsPartition(hiveTable, "fmt = 'rc2', prt = 'c'", partitionTable);
        addTableAsPartition(hiveTable, "fmt = 'rc2', prt = 'a'", partitionTable);
        addTableAsPartition(hiveTable, "fmt = 'rc3', prt = 'b'", partitionTable);
        addTableAsPartition(hiveTable, "fmt = 'rc3', prt = 'c'", partitionTable);
    }

    /**
     * Create Hive table with all supported types:
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"hive", "features", "gpdb", "security"})
    public void supportedTypesRc() throws Exception {

        createExternalTable(GPDB_HIVE_TYPES_TABLE,
                PXF_HIVE_TYPES_LIMITED_COLS, hiveRcTypes, true);

        runSqlTest("features/hive/rc_types");
    }

    /**
     * Load RC file contains 10 rows of string,int,binary,binary data to Hive Rc table.<br>
     * Read using PXF HiveRc profile and compare results.<br>
     * The data content can be seen in the {@link HiveRcBinaryPreparer} class.
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"hive", "features", "gpdb", "security"})
    public void binaryData() throws Exception {

        // create "hiveBinaryRc" Hive RC table
        HiveTable hiveTable = new HiveTable(HIVE_BINARY_TABLE + "_rc", new String[]{
                "t1 STRING", "num1 INT", "data1 BINARY", "data2 BINARY"});
        hiveTable.setFormat(FORMAT_ROW);
        hiveTable.setSerde(COLUMNAR_SERDE);
        hiveTable.setStoredAs(RCFILE);
        hive.createTableAndVerify(hiveTable);
        loadDataIntoHive("hiveBinaryRcFormatData", hiveTable);

        // create external table with BYTEA field
        createExternalTable("gpdb_hive_binary", new String[]{
                "t1 TEXT", "num1 INTEGER", "data1 BYTEA", "data2 BYTEA"}, hiveTable, true);

        runSqlTest("features/hive/rc_binary_data");
    }

    /**
     * Use unsupported types for RC connector and check for error
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"hive", "features", "gpdb", "security"})
    public void mismatchedTypes() throws Exception {

        String[] mismatchedFields = PXF_HIVE_TYPES_LIMITED_COLS.clone();
        mismatchedFields[8] = "sml INTEGER";
        createExternalTable(GPDB_HIVE_TYPES_TABLE,
                mismatchedFields, hiveRcTypes, false);

        runSqlTest("features/hive/errors/rc_mismatchedTypes");
    }

    /**
     * use PXF RC connectors to get data from Hive RC table with mentioned ColumnarSerDe serde.
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"hive", "features", "gpdb", "security"})
    public void hiveRcTable() throws Exception {

        createExternalTable(PXF_HIVE_SMALL_DATA_TABLE,
                PXF_HIVE_SMALLDATA_COLS, hiveRcTable, true);

        runSqlTest("features/hive/small_data");
    }

    /**
     * Tests when the column name or partition name doesn't match any of the
     * column names on the Greenplum definition
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"hive", "features", "gpdb", "security"})
    public void columnNameMismatch() throws Exception {

        String[] nonMatchingColumnNames = PXF_HIVE_SMALLDATA_COLS.clone();

        nonMatchingColumnNames[1] = "s2    TEXT";

        createExternalTable(PXF_HIVE_SMALL_DATA_TABLE,
                nonMatchingColumnNames, hiveRcTable, true);

        runSqlTest("features/hive/errors/partitionNameMismatch");
    }

    /**
     * Create a Greenplum table with a subset of columns from the original
     * Hive table
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"hive", "features", "gpdb", "security"})
    public void columnSubsetOfHiveSchema() throws Exception {

        // Create PXF Table using Hive RC profile
        createExternalTable(PXF_HIVE_SMALL_DATA_TABLE,
                PXF_HIVE_SUBSET_COLS, hiveRcTable, true);

        runSqlTest("features/hive/column_subset");
    }

    /**
     * Create a Greenplum table with a subset of columns from the original
     * partitioned Hive table.
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"hive", "features", "gpdb", "security"})
    public void columnSubsetOfPartitionedHiveSchema() throws Exception {

        hiveTable = new HiveExternalTable(HIVE_REG_HETEROGEN_TABLE, HIVE_RC_COLS);
        hiveTable.setPartitionedBy(HIVE_PARTITION_COLUMN);
        hiveTable.setFormat(FORMAT_ROW);
        hiveTable.setSerde(COLUMNAR_SERDE);
        hiveTable.setStoredAs(RCFILE);

        hive.createTableAndVerify(hiveTable);
        addPartitionsOne(hiveTable, hiveRcTable);

        // Create PXF Table for Hive partitioned table
        createExternalTable(PXF_HIVE_SMALL_DATA_TABLE,
                PXF_HIVE_SUBSET_FMT_COLS, hiveTable, true);

        runSqlTest("features/hive/column_subset_partitioned_table_rc");
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

        // Create PXF Table using Hive RC profile
        createExternalTable(PXF_HIVE_SMALL_DATA_TABLE,
                PXF_HIVE_SMALLDATA_COLS, hiveRcForAlterTable, true);

        runSqlTest("features/hive/small_data");

        hive.runQuery("ALTER TABLE " + hiveRcForAlterTable.getName() + " ADD COLUMNS (new1 int)");
        hive.runQuery("INSERT INTO TABLE " + hiveRcForAlterTable.getName() +
                " VALUES ('row11', 's_16', 11, 16, 100)");

        // Re-run the test to make sure we can still read the hive table
        runSqlTest("features/hive/small_data");

        gpdb.runQuery("ALTER EXTERNAL TABLE " + PXF_HIVE_SMALL_DATA_TABLE + " ADD COLUMN new1 INT");
        runSqlTest("features/hive/small_data_alter");
    }

    /**
     * use PXF RC connectors to get data from Hive RC table without mentioned serde.
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"hive", "features", "gpdb", "security"})
    public void hiveRcTableDefaultSerde() throws Exception {

        createExternalTable(PXF_HIVE_SMALL_DATA_TABLE,
                PXF_HIVE_SMALLDATA_COLS, hiveRcTableNoSerde, false);

        runSqlTest("features/hive/small_data");
    }

    /**
     * use PXF RC connectors to get data from Hive partitioned table using
     * specific ColumnarSerDe serde.
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"hive", "features", "gpdb", "security"})
    public void severalRcPartitions() throws Exception {

        hiveTable = new HiveExternalTable(HIVE_REG_HETEROGEN_TABLE, HIVE_RC_COLS);
        hiveTable.setPartitionedBy(HIVE_PARTITION_COLUMN);
        hiveTable.setFormat(FORMAT_ROW);
        hiveTable.setSerde(COLUMNAR_SERDE);
        hiveTable.setStoredAs(RCFILE);

        hive.createTableAndVerify(hiveTable);
        addPartitionsOne(hiveTable, hiveRcTable);

        // Create PXF Table for Hive partitioned table
        createExternalTable(PXF_HIVE_HETEROGEN_TABLE,
                PXF_HIVE_SMALLDATA_FMT_COLS, hiveTable, true);

        runSqlTest("features/hive/rc_partitions");
    }

    /**
     * use PXF RC connectors to get data from Hive partitioned table using default serde.
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"hive", "features", "gpdb", "security"})
    public void severalPartitionsDefaultSerde() throws Exception {

        hiveTable = new HiveExternalTable(HIVE_REG_HETEROGEN_TABLE, HIVE_RC_COLS);
        hiveTable.setPartitionedBy(HIVE_PARTITION_COLUMN);
        hiveTable.setStoredAs(RCFILE);

        hive.createTableAndVerify(hiveTable);
        addPartitionsOne(hiveTable, hiveRcTableNoSerde);

        // Create PXF Table for Hive partitioned table
        createExternalTable(PXF_HIVE_HETEROGEN_TABLE,
                PXF_HIVE_SMALLDATA_FMT_COLS, hiveTable, false);

        runSqlTest("features/hive/rc_partitions");
    }

    /**
     * Filter partitions columns on external table directed to hive partitioned
     * table The filtering will be done in PXF fragmenter level.
     *
     * @throws Exception if test fails to run
     */
    @Test(enabled = false, groups = {"hive", "features", "gpdb", "security"})
    public void filterBetweenPartitionsInFragmenter() throws Exception {

        gpdb.runQuery("SET optimizer = on");
        filterBetweenPartitions();
    }

    /**
     * Filter partitions columns on external table directed to hive partitioned
     * table The filtering shell be done in PXF accessor level, by disabling ORCA
     * so filter string will not be forwarded to PXF fragmenter, only to PXF accessor.
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"hive", "features", "gpdb", "security"})
    public void filterBetweenPartitionsInAccessor() throws Exception {

        gpdb.runQuery("SET optimizer = off");
        filterBetweenPartitions();
    }

    private void createHiveExternalTable(String name) throws Exception {

        hiveTable = new HiveExternalTable(name, HIVE_RC_COLS);
        hiveTable.setPartitionedBy(HIVE_PARTITION_COLS);
        hiveTable.setFormat(FORMAT_ROW);
        hiveTable.setSerde(COLUMNAR_SERDE);
        hiveTable.setStoredAs(RCFILE);

        hive.createTableAndVerify(hiveTable);
    }

    /**
     * Filter partitions columns on external table directed to hive partitioned table
     *
     * @throws Exception if test fails to run
     */
    private void filterBetweenPartitions() throws Exception {

        createHiveExternalTable(HIVE_REG_HETEROGEN_TABLE);
        addPartitionsTwo(hiveTable, hiveRcTable);

        // Create PXF Table using Hive RC profile
        createExternalTable(PXF_HIVE_HETEROGEN_TABLE + "_using_filter",
                PXF_HIVE_SMALLDATA_PRT_COLS, hiveTable, false);

        runSqlTest("features/hive/rc_filter_partitions");
    }

    /**
     * Filter on non partitions columns on external table directed to hive partitioned table
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"hive", "features", "gpdb", "security"})
    public void filterNoPartitions() throws Exception {

        createHiveExternalTable(HIVE_REG_HETEROGEN_TABLE);
        addPartitionsTwo(hiveTable, hiveRcTable);

        // Create PXF Table using Hive profile
        createExternalTable(PXF_HIVE_HETEROGEN_TABLE + "_using_filter",
                PXF_HIVE_SMALLDATA_PRT_COLS, hiveTable, true);

        runSqlTest("features/hive/rc_filter_no_partitions");
    }

    /**
     * Filter pushdown on partitions columns on external table
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"hive", "features", "gpdb", "security"})
    public void partitionFilterPushDown() throws Exception {

        createHiveExternalTable(HIVE_REG_HETEROGEN_TABLE);
        addTableAsPartition(hiveTable, "fmt = 'rc1', prt = 'a'", hiveRcTable);
        addTableAsPartition(hiveTable, "fmt = 'rc1', prt = 'e'", hiveRcTable);
        addTableAsPartition(hiveTable, "fmt = 'rc2', prt = 'b'", hiveRcTable);
        addTableAsPartition(hiveTable, "fmt = 'rc2', prt = 'd'", hiveRcTable);
        addTableAsPartition(hiveTable, "fmt = 'rc3', prt = 'a'", hiveRcTable);
        addTableAsPartition(hiveTable, "fmt = 'rc3', prt = 'c'", hiveRcTable);
        addTableAsPartition(hiveTable, "fmt = 'rc3', prt = 'f'", hiveRcTable);

        /* Create PXF Table using Hive profile */
        exTable = TableFactory.getPxfHiveRcReadableTable(PXF_HIVE_HETEROGEN_TABLE + "_using_filter",
                PXF_HIVE_SMALLDATA_PRT_COLS, hiveTable, false);
        exTable.setHost(pxfHost);
        exTable.setPort(pxfPort);
        exTable.setFragmenter(TEST_PACKAGE + "HiveInputFormatFragmenterWithFilter");

        // Filter 'rc1' & 'a' partitions, mimic WHERE fmt = 'rc1' AND prt='a'
        String filterString = "a4c25s3drc1o5a5c25s1dao5l0";
        compareFilteredHiveData(filterString, Arrays.asList("rc1", "a"));

        // Filter 'rc2' & 'b' partitions, mimic WHERE fmt = 'rc2' AND prt='b'
        filterString = "a4c25s3drc2o5a5c25s1dbo5l0";
        compareFilteredHiveData(filterString, Arrays.asList("rc2", "b"));

        // Filter 'rc3' & 'c' partitions, mimic WHERE fmt = 'rc3' AND prt='c'
        filterString = "a4c25s3drc3o5a5c25s1dco5l0";
        compareFilteredHiveData(filterString, Arrays.asList("rc3", "c"));

        // Filter for non existent partition, mimic WHERE fmt = 'rc4' AND prt='d'
        filterString = "a4c25s3drc4o5a5c25s1ddo6l0";
        exTable.setUserParameters(hiveTestFilter(filterString));

        // Query all data, non matched filter will return null data.
        gpdb.createTableAndVerify(exTable);
        gpdb.queryResults(exTable, "SELECT * FROM " + exTable.getName() + " ORDER BY fmt, t1");
        ComparisonUtils.compareTables(exTable, new Table("comparisonData", null), null);

        // Mixed filter with partition and non partition fields, partition filtering: fmt = 'rc3' AND part = 'c'
        filterString = "a4c25s3drc3o5a5c25s1dco5l0";
        exTable.setUserParameters(hiveTestFilter(filterString));

        // Query all data
        gpdb.createTableAndVerify(exTable);
        gpdb.queryResults(exTable, "SELECT * FROM " + exTable.getName()
                + " WHERE t1='row6' AND t2='s_11' AND num1='6' AND dub1='11' ORDER BY fmt, t1");

        Table dataCompareTable = new Table("dataTable", null);

        // Prepare expected data
        List<String> expectedData = Arrays.asList("row6", "s_11", "6", "11", "rc3", "c");
        dataCompareTable.addRow(expectedData);
        ComparisonUtils.compareTables(exTable, dataCompareTable, null);

        // Mixed filter with partition and non partition fields, partition filtering: fmt ='rc3'
        String query = "SELECT * FROM " + exTable.getName()
                + " WHERE t1='row6' AND t2='s_11' AND num1='6' AND dub1='11' ORDER BY fmt, t1, prt";
        filterString = "a4c25s3drc3o5";
        exTable.setUserParameters(hiveTestFilter(filterString));

        // Query all data, non matched filter is done in PXF
        gpdb.createTableAndVerify(exTable);
        gpdb.queryResults(exTable, query);

        // Prepare expected data
        dataCompareTable = new Table("dataTable", null);
        dataCompareTable.addRow(new String[]{"row6", "s_11", "6", "11", "rc3", "a"});
        dataCompareTable.addRow(expectedData);
        dataCompareTable.addRow(new String[]{"row6", "s_11", "6", "11", "rc3", "f"});
        ComparisonUtils.compareTables(exTable, dataCompareTable, null);

        // Mixed filter with partition and non partition fields, partition filtering: prt='a'
        filterString = "a5c25s1dao5";
        exTable.setUserParameters(hiveTestFilter(filterString));

        // Query all data, non matched filter is done in PXF
        gpdb.createTableAndVerify(exTable);
        gpdb.queryResults(exTable, "SELECT * FROM " + exTable.getName()
                + " WHERE t1='row5' AND t2='s_10' AND num1='5' AND dub1='10' ORDER BY fmt, t1, prt");

        // Prepare expected data
        dataCompareTable = new Table("dataTable", null);
        dataCompareTable.addRow(new String[]{"row5", "s_10", "5", "10", "rc1", "a"});
        dataCompareTable.addRow(new String[]{"row5", "s_10", "5", "10", "rc3", "a"});
        ComparisonUtils.compareTables(exTable, dataCompareTable, null);
    }

    private void compareFilteredHiveData(String filterString, List<String> pumpUpColumns)
            throws Exception {

        exTable.setUserParameters(hiveTestFilter(filterString));

        // Query matched partition name
        gpdb.createTableAndVerify(exTable);
        gpdb.queryResults(exTable, "SELECT * FROM " + exTable.getName() + " ORDER BY fmt, t1");

        // Pump up the small data to fit the unified data
        if (pumpUpColumns != null)
            appendToEachRowOfComparisonTable(pumpUpColumns);
        ComparisonUtils.compareTables(exTable, comparisonDataTable, null);
    }

    /**
     * Use PXF RC connectors to get data from Hive partitioned table using default serde.
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"hive", "features", "gpdb", "security"})
    public void partitionFilterPushDownWithDefaultSerde() throws Exception {

        hiveTable = new HiveExternalTable(HIVE_REG_HETEROGEN_TABLE, HIVE_RC_COLS);
        hiveTable.setPartitionedBy(HIVE_PARTITION_COLUMN);
        hiveTable.setStoredAs(RCFILE);

        hive.createTableAndVerify(hiveTable);
        addPartitionsOne(hiveTable, hiveRcTableNoSerde);

        // Create PXF Table for Hive partitioned table
        exTable = TableFactory.getPxfHiveRcReadableTable(PXF_HIVE_HETEROGEN_TABLE + "_using_profile",
                PXF_HIVE_SMALLDATA_FMT_COLS, hiveTable, false);
        exTable.setFragmenter(TEST_PACKAGE + "HiveInputFormatFragmenterWithFilter");

        // Filter rc1 partition, mimic WHERE fmt = 'rc1'
        String filterString = "a4c25s3drc1o5";
        compareFilteredHiveData(filterString, Collections.singletonList("rc1"));

        // Filter rc2 partition, mimic WHERE fmt = 'rc2'
        filterString = "a4c25s3drc2o5";
        compareFilteredHiveData(filterString, Collections.singletonList("rc2"));

        // Filter rc3 partition, mimic WHERE fmt = 'rc3'
        filterString = "a4c25s3drc3o5";
        compareFilteredHiveData(filterString, Collections.singletonList("rc3"));

        // Filter Non Exist partition, mimic WHERE fmt = 'rc4'
        filterString = "a4c25s3drc4o5";
        exTable.setUserParameters(hiveTestFilter(filterString));

        gpdb.createTableAndVerify(exTable);
        gpdb.queryResults(exTable, "SELECT * FROM " + exTable.getName() + " ORDER BY fmt, t1");

        ComparisonUtils.compareTables(exTable, new Table("comparisonData", null), null);
    }

    /**
     * Make sure that PXF works with aggregate queries (including null columns)
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"hive", "hcatalog", "features", "gpdb", "security"})
    public void aggregateQueries() throws Exception {

        prepareTypesData();

        //hive text table with nulls
        HiveTable hiveTable = new HiveTable("hive_rc_table_with_nulls", HIVE_RC_COLS);
        hiveTable.setFormat(FORMAT_ROW);
        hiveTable.setSerde(COLUMNAR_SERDE);
        hiveTable.setStoredAs(RCFILE);

        hive.createTableAndVerify(hiveTable);
        hive.runQuery("INSERT INTO TABLE " + hiveTable.getName()
                + " SELECT t1, t2, num1, dub1 FROM " + hiveTypesTable.getName());

        createExternalTable(PXF_HIVE_SMALL_DATA_TABLE,
                PXF_HIVE_SMALLDATA_COLS, hiveTable, true);

        runSqlTest("features/hcatalog/aggregate_queries");
        runSqlTest("features/hive/aggregate_queries");
    }
}
