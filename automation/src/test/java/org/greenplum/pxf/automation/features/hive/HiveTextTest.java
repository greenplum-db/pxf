package org.greenplum.pxf.automation.features.hive;

import org.greenplum.pxf.automation.structures.tables.basic.Table;
import org.greenplum.pxf.automation.structures.tables.hive.HiveExternalTable;
import org.greenplum.pxf.automation.structures.tables.hive.HiveTable;
import org.greenplum.pxf.automation.structures.tables.utils.TableFactory;
import org.greenplum.pxf.automation.utils.exception.ExceptionUtils;
import org.greenplum.pxf.automation.utils.tables.ComparisonUtils;
import org.junit.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class HiveTextTest extends HiveBaseTest {

    private HiveExternalTable hiveHeteroTable;
    private HiveExternalTable hiveTextPartitionTable;
    private HiveTable hiveTextTable;

    @Override
    protected void createExternalTable(String tableName, String[] fields,
                                     HiveTable hiveTable, boolean useProfile, String delimiter) throws Exception {

        exTable = TableFactory.getPxfHiveTextReadableTable(tableName, fields, hiveTable, useProfile);
        exTable.setDelimiter(delimiter);
        createTable(exTable);
    }

    @Override
    protected void createExternalTable(String tableName, String[] fields,
                                     HiveTable hiveTable, boolean useProfile) throws Exception {

        exTable = TableFactory.getPxfHiveTextReadableTable(tableName, fields, hiveTable, useProfile);
        createTable(exTable);
    }

    @Override
    protected void createExternalTable(String tableName, String[] fields,
                                     HiveTable hiveTable) throws Exception {

        createExternalTable(tableName, fields, hiveTable, true);
    }

    private void prepareHeteroData() throws Exception {

        if (hiveHeteroTable != null)
            return;
        hiveHeteroTable = new HiveExternalTable(HIVE_REG_HETEROGEN_TABLE, HIVE_RC_COLS);
        hiveHeteroTable.setPartitionedBy(HIVE_PARTITION_COLS);
        hive.createTableAndVerify(hiveHeteroTable);

        String tableName = hiveHeteroTable.getName();
        String location = "'hdfs:" + hdfsBaseDir + hiveTextTable.getName() + "'";
        addHivePartition(tableName, "fmt = 'rc1', prt = 'a'", location);
        addHivePartition(tableName, "fmt = 'rc2', prt = 'b'", location);
        addHivePartition(tableName, "fmt = 'rc3', prt = 'c'", location);
    }

    @Override
    void prepareData() throws Exception {

        prepareSmallData();
        prepareTypesData();
        prepareRCData();

        // comparison table
        comparisonDataTable = new Table("comparisonData", null);
        comparisonDataTable.loadDataFromFile(localDataResourcesFolder + "/hive/" + HIVE_DATA_FILE_NAME,
                ",", 0, false);

        //hive text table
        hiveTextTable = new HiveTable(HIVE_TEXT_TABLE, HIVE_RC_COLS);
        hive.createTableAndVerify(hiveTextTable);
        hive.runQuery("INSERT INTO TABLE " + hiveTextTable.getName() +
                " SELECT * FROM " + hiveSmallDataTable.getName());

        //hive text table with three partitions
        hiveTextPartitionTable = new HiveExternalTable(
                HIVE_REG_HETEROGEN_TABLE + "_three_partitions", HIVE_RC_COLS);
        hiveTextPartitionTable.setPartitionedBy(HIVE_PARTITION_COLUMN);
        hive.createTableAndVerify(hiveTextPartitionTable);

        String tableName = hiveTextPartitionTable.getName();
        String location = String.format("'%s%s'", hdfsBaseDir, hiveTextTable.getName());
        addHivePartition(tableName, "fmt = 'rc1'", location);
        addHivePartition(tableName, "fmt = 'rc2'", location);
        addHivePartition(tableName, "fmt = 'rc3'", location);
    }

    /**
     * Create Hive table with all supported types:
     * TODO: enable once [#92489334] is merged
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = "features")
    public void supportedTypesText() throws Exception {

        createExternalTable(GPDB_HIVE_TYPES_TABLE,
                PXF_HIVE_TYPES_COLS, hiveTypesTable, true, ",");

        gpdb.queryResults(exTable, "SELECT * FROM " + exTable.getName() + " ORDER BY t1");
        comparisonDataTable.loadDataFromFile(localDataResourcesFolder + "/hive/" + HIVE_TYPES_DATA_FILE_NAME,
                ",", 0, false);

        ComparisonUtils.compareTables(exTable, comparisonDataTable, null);
    }

    /**
     * Use unsupported types for Text connector and check for error
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"hive", "features", "gpdb", "security"})
    public void mismatchedTypes() throws Exception {

        // Hive column is SMALLINT, expected GPDB type is SMALLINT(int2), but actual is INTEGER(int4)
        String unsupported = "Invalid definition for column ";
        String[] tableFieldTypes = PXF_HIVE_TYPES_COLS.clone();
        tableFieldTypes[9] = "tn INTEGER";
        createExternalTable(GPDB_HIVE_TYPES_TABLE,
                tableFieldTypes, hiveTypesTable, false, ",");

        String queryStr = "SELECT * FROM " + exTable.getName() + " ORDER BY t1";
        try {
            gpdb.queryResults(exTable, queryStr);
            Assert.fail("Query should fail with schema mismatch error");
        } catch (Exception e) {
            ExceptionUtils.validate(null, e,
                    new Exception(unsupported + "tn: expected GPDB type SMALLINT, actual GPDB type INTEGER"),
                    true, true);
        }

        // Hive column is DECIMAL(38,18), expected GPDB type is NUMERIC(38,18) or NUMERIC, but actual is NUMERIC(30,18)
        tableFieldTypes = PXF_HIVE_TYPES_COLS.clone();
        tableFieldTypes[4] = "dec1 NUMERIC(30,18)";
        createExternalTable(GPDB_HIVE_TYPES_TABLE,
                tableFieldTypes, hiveTypesTable, false, ",");

        try {
            gpdb.queryResults(exTable, queryStr);
            Assert.fail("Query should fail with schema mismatch error");
        } catch (Exception e) {
            ExceptionUtils.validate(null, e,
                    new Exception(unsupported + "dec1: modifiers are not compatible, \\[38, 18\\], \\[30, 18\\]"),
                    true, true);
        }

        // Hive column is DECIMAL(38,18), expected GPDB type is NUMERIC(38,18) or NUMERIC, but actual is NUMERIC(38)
        tableFieldTypes = PXF_HIVE_TYPES_COLS.clone();
        tableFieldTypes[4] = "dec1 NUMERIC(38)";
        createExternalTable(GPDB_HIVE_TYPES_TABLE,
                tableFieldTypes, hiveTypesTable, false, ",");

        try {
            gpdb.queryResults(exTable, queryStr);
            Assert.fail("Query should fail with schema mismatch error");
        } catch (Exception e) {
            ExceptionUtils.validate(null, e,
                    new Exception(unsupported + "dec1: modifiers are not compatible, \\[38, 18\\], \\[38, 0\\]"),
                    true, true);
        }
    }

    /**
     * Use PXF Text connectors to get data from Hive Text table
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"hive", "features", "gpdb", "security"})
    public void hiveTextTable() throws Exception {

        exTable = TableFactory.getPxfHiveTextReadableTable(HIVE_TEXT_TABLE,
                PXF_HIVE_SMALLDATA_COLS, hiveTextTable, true);
        exTable.setHost(pxfHost);
        exTable.setPort(pxfPort);

        // check that without escaping the LOCATION part, a warning is shown
        String createCmd = exTable.constructCreateStmt();
        createCmd = createCmd.replace("LOCATION (E'", "LOCATION ('");
        try {
            gpdb.dropTable(exTable, false);
            gpdb.runQuery(createCmd);
        } catch (Exception e) {
            ExceptionUtils.validate(null, e, new Exception(
                    "nonstandard use of escape in a string literal"), true, true);
        }
        Assert.assertTrue("Table " + exTable.getName() + " was not created", gpdb.checkTableExists(exTable));

        gpdb.queryResults(exTable, "SELECT * FROM " + exTable.getName() + " ORDER BY t1");
        ComparisonUtils.compareTables(exTable, comparisonDataTable, null);
    }

    /**
     * Use PXF Text connectors to get data from Hive partitioned table
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"hive", "features", "gpdb", "security"})
    public void severalTextPartitions() throws Exception {

        createExternalTable(PXF_HIVE_HETEROGEN_TABLE,
                PXF_HIVE_SMALLDATA_FMT_COLS, hiveTextPartitionTable);
        gpdb.queryResults(exTable, "SELECT * FROM " + exTable.getName() + " ORDER BY fmt, t1");

        // pump up the small data to fit the unified data
        comparisonDataTable.loadDataFromFile(localDataResourcesFolder + "/hive/" + HIVE_DATA_FILE_NAME,
                ",", 0, false);
        pumpUpComparisonTableData(3, false);

        ComparisonUtils.compareTables(exTable, comparisonDataTable, null);
    }

    /**
     * Create GPDB external table without the partition column. check error.
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"hive", "features", "gpdb", "security"})
    public void severalTextPartitionsNoPartitonColumInGpdb() throws Exception {

        hiveTable = new HiveExternalTable(HIVE_REG_HETEROGEN_TABLE, HIVE_RC_COLS);
        hiveTable.setPartitionedBy(HIVE_PARTITION_COLUMN);
        hive.createTableAndVerify(hiveTable);

        String tableName = hiveTable.getName();
        String location = "'hdfs:" + hdfsBaseDir + hiveTextTable.getName() + "'";
        addHivePartition(tableName, "fmt = 'rc1'", location);
        addHivePartition(tableName, "fmt = 'rc2'", location);
        addHivePartition(tableName, "fmt = 'rc3'", location);

        // Create PXF Table using Hive profile
        createExternalTable(PXF_HIVE_HETEROGEN_TABLE + "_using_profile",
                PXF_HIVE_SMALLDATA_COLS, hiveTable);

        try {
            gpdb.queryResults(exTable, "SELECT * FROM " + exTable.getName() + " ORDER BY fmt, t1");
            Assert.fail("Query should fail with nonexistent column error");
        } catch (Exception e) {
            ExceptionUtils.validate(null, e, new Exception(
                    "ERROR: column \"fmt\" does not exist"), true, true);
        }
    }

    /**
     * Filter partitions columns on external table directed to hive partitioned table
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"hive", "features", "gpdb", "security"})
    public void partitionFilterPushDown() throws Exception {

        hiveTable = new HiveExternalTable(HIVE_REG_HETEROGEN_TABLE, HIVE_RC_COLS);
        hiveTable.setPartitionedBy(HIVE_PARTITION_COLS);
        hive.createTableAndVerify(hiveTable);

        String tableName = hiveTable.getName();
        String location = "'hdfs:" + hdfsBaseDir + hiveTextTable.getName() + "'";
        addHivePartition(tableName, "fmt = 'rc1', prt = 'a'", location);
        addHivePartition(tableName, "fmt = 'rc1', prt = 'e'", location);
        addHivePartition(tableName, "fmt = 'rc2', prt = 'b'", location);
        addHivePartition(tableName, "fmt = 'rc2', prt = 'd'", location);
        addHivePartition(tableName, "fmt = 'rc3', prt = 'a'", location);
        addHivePartition(tableName, "fmt = 'rc3', prt = 'c'", location);
        addHivePartition(tableName, "fmt = 'rc3', prt = 'f'", location);

        // Create PXF Table using Hive profile
        exTable = TableFactory.getPxfHiveTextReadableTable(PXF_HIVE_HETEROGEN_TABLE + "_using_filter",
                PXF_HIVE_SMALLDATA_PRT_COLS, hiveTable, false);
        exTable.setFragmenter("org.greenplum.pxf.automation.testplugin.HiveDataFragmenterWithFilter");
        exTable.setDelimiter("E'\\x01'");

        // Filter rc1 & a partition, mimic WHERE fmt = 'rc1' AND prt = 'a'
        String filterString = "a4c25s3drc1o5a5c25s1dao5l0";
        String queryStr = "SELECT * FROM " + exTable.getName() + " ORDER BY fmt, t1";
        exTable.setUserParameters(hiveTestFilter(filterString));
        createTable(exTable);
        gpdb.queryResults(exTable, queryStr);

        // Pump up the small data to fit the unified data
        appendToEachRowOfComparisonTable(Arrays.asList("rc1", "a"));
        ComparisonUtils.compareTables(exTable, comparisonDataTable, null);

        // Filter rc2 & b partition, mimic WHERE fmt = 'rc2' AND prt = 'b'
        filterString = "a4c25s3drc2o5a5c25s1dbo5l0";
        exTable.setUserParameters(hiveTestFilter(filterString));
        createTable(exTable);
        gpdb.queryResults(exTable, queryStr);

        // Pump up the small data to fit the unified data
        appendToEachRowOfComparisonTable(Arrays.asList("rc2", "b"));
        ComparisonUtils.compareTables(exTable, comparisonDataTable, null);

        // Filter rc3 & c partition, mimic WHERE fmt = 'rc3' AND prt = 'c'
        filterString = "a4c25s3drc3o5a5c25s1dco5l0";
        exTable.setDelimiter("E'\\x01'");
        exTable.setUserParameters(hiveTestFilter(filterString));
        createTable(exTable);
        gpdb.queryResults(exTable, queryStr);

        // Pump up the small data to fit the unified data
        appendToEachRowOfComparisonTable(Arrays.asList("rc3", "c"));
        ComparisonUtils.compareTables(exTable, comparisonDataTable, null);

        // Filter Non Existing partitions, WHERE fmt = 'rc4' AND prt = 'd'
        filterString = "a4c25s3drc4o5a5c25s1ddo5l0";
        exTable.setDelimiter("E'\\x01'");
        exTable.setUserParameters(hiveTestFilter(filterString));
        createTable(exTable);
        // Query all data, non matched filter is done in PXF
        gpdb.queryResults(exTable, queryStr);
        ComparisonUtils.compareTables(exTable, new Table("comparisonData", null), null);

        // Disable back gpdb filter
        gpdb.runQuery("SET gp_external_enable_filter_pushdown = false;");

        // Mixed filter with partition and non partition fields, partition filtering: fmt = 'rc3' AND prt = 'c'
        filterString = "a4c25s3drc3o5a5c25s1dco5l0";
        exTable.setDelimiter("E'\\x01'");
        exTable.setUserParameters(hiveTestFilter(filterString));
        createTable(exTable);
        gpdb.queryResults(exTable, "SELECT * FROM " + exTable.getName()
                + " WHERE t1='row6' AND t2='s_11' AND num1='6' AND dub1='11' ORDER BY fmt, t1");

        // Prepare expected data
        Table dataCompareTable = new Table("dataTable", null);
        dataCompareTable.addRow(new String[]{"row6", "s_11", "6", "11", "rc3", "c"});
        ComparisonUtils.compareTables(exTable, dataCompareTable, null);

        // Mixed filter with partition and non partition fields, partition filtering: fmt ='rc3'
        filterString = "a4c25s3drc3o5";
        exTable.setDelimiter("E'\\x01'");
        exTable.setUserParameters(hiveTestFilter(filterString));
        createTable(exTable);
        gpdb.queryResults(exTable, "SELECT * FROM " + exTable.getName()
                + " WHERE t1='row6' AND t2='s_11' AND num1='6' AND dub1='11' ORDER BY fmt, t1, prt");

        // Prepare expected data
        dataCompareTable = new Table("dataTable", null);
        dataCompareTable.addRow(new String[]{"row6", "s_11", "6", "11", "rc3", "a"});
        dataCompareTable.addRow(new String[]{"row6", "s_11", "6", "11", "rc3", "c"});
        dataCompareTable.addRow(new String[]{"row6", "s_11", "6", "11", "rc3", "f"});
        ComparisonUtils.compareTables(exTable, dataCompareTable, null);

        // Mixed filter with partition and non partition fields, partition filtering: part='a'
        filterString = "a5c25s1dao5";
        exTable.setDelimiter("E'\\x01'");
        exTable.setUserParameters(hiveTestFilter(filterString));
        createTable(exTable);
        gpdb.queryResults(exTable, "SELECT * FROM " + exTable.getName()
                + " WHERE t1='row5' AND t2='s_10' AND num1='5' AND dub1='10' ORDER BY fmt, t1, prt");

        // prepare expected data
        dataCompareTable = new Table("dataTable", null);
        dataCompareTable.addRow(new String[]{"row5", "s_10", "5", "10", "rc1", "a"});
        dataCompareTable.addRow(new String[]{"row5", "s_10", "5", "10", "rc3", "a"});
        ComparisonUtils.compareTables(exTable, dataCompareTable, null);
    }

    /**
     * Filter none partitions columns on external table directed to hive partitioned table
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"hive", "features", "gpdb", "security"})
    public void filterNonePartitions() throws Exception {

        // Create PXF Table using Hive profile
        prepareHeteroData();
        createExternalTable(PXF_HIVE_HETEROGEN_TABLE + "_using_filter",
                PXF_HIVE_SMALLDATA_PRT_COLS, hiveHeteroTable);
        gpdb.queryResults(exTable, "SELECT * FROM " + exTable.getName()
                + " WHERE num1 > 5 AND dub1 < 12 ORDER BY fmt, t1");

        // prepare expected data
        Table dataCompareTable = new Table("dataTable", null);
        dataCompareTable.addRow(new String[]{"row6", "s_11", "6", "11", "rc1", "a"});
        dataCompareTable.addRow(new String[]{"row6", "s_11", "6", "11", "rc2", "b"});
        dataCompareTable.addRow(new String[]{"row6", "s_11", "6", "11", "rc3", "c"});

        ComparisonUtils.compareTables(exTable, dataCompareTable, null);
    }

    /**
     * Filter partitions columns on external table directed to hive partitioned table
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"hive", "features", "gpdb", "security"})
    public void filterBetweenPartitions() throws Exception {

        // Create PXF Table using Hive profile
        prepareHeteroData();
        createExternalTable(PXF_HIVE_HETEROGEN_TABLE + "_using_filter",
                PXF_HIVE_SMALLDATA_PRT_COLS, hiveHeteroTable, false);
        gpdb.queryResults(exTable, "SELECT * FROM " + exTable.getName()
                + " WHERE fmt = 'rc1' AND prt = 'a' ORDER BY fmt, t1");

        // pump up the small data to fit the unified data
        comparisonDataTable.loadDataFromFile(localDataResourcesFolder + "/hive/" + HIVE_DATA_FILE_NAME,
                ",", 0, false);
        pumpUpComparisonTableData(1, true);

        ComparisonUtils.compareTables(exTable, comparisonDataTable, null);
    }

    /**
     * use PXF Text connectors to get data from Hive text table with custom delimiter using HCatalog
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"hive", "hcatalog", "features", "gpdb", "security"})
    public void hiveTextTableCustomDelimiter() throws Exception {

        //hive text table with custom delimiter
        HiveTable hiveTable = new HiveTable(HIVE_TEXT_TABLE + "_custom_delimiter", HIVE_RC_COLS);
        hiveTable.setFormat(FORMAT_ROW);
        hiveTable.setDelimiterFieldsBy("%");

        hive.createTableAndVerify(hiveTable);
        hive.runQuery("INSERT INTO TABLE " + hiveTable.getName() +
                " SELECT * FROM " + hiveSmallDataTable.getName());

        runSqlTest("features/hcatalog/hive_text_custom_delimiter");
    }

    /**
     * Make sure GPDB is functional for heterogeneous table with three text partitions
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"hive", "hcatalog", "features", "gpdb", "security"})
    public void hiveTextTableOptimizedProfile() throws Exception {

        runSqlTest("features/hcatalog/heterogeneous_table_three_text_partitions");
    }

    /**
     * Make sure that PXF works with aggregate queries (including null columns)
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"hive", "hcatalog", "features", "gpdb", "security"})
    public void aggregateQueries() throws Exception {

        //hive text table with nulls
        HiveTable hiveTable = new HiveTable(HIVE_TEXT_TABLE + "_with_nulls", HIVE_RC_COLS);
        hive.createTableAndVerify(hiveTable);
        hive.runQuery("INSERT INTO TABLE " + hiveTable.getName() +
                " SELECT t1, t2, num1, dub1 FROM " + hiveTypesTable.getName());

        createExternalTable(PXF_HIVE_SMALL_DATA_TABLE,
                PXF_HIVE_SMALLDATA_COLS, hiveTable);

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

        HiveTable hiveTableWithSkipHeader = new HiveTable("hive_table_with_skipHeader_text", HIVE_SMALLDATA_COLS);
        hiveTableWithSkipHeader.setTableProperties(tableProperties);
        hive.createTableAndVerify(hiveTableWithSkipHeader);
        hive.insertData(hiveSmallDataTable, hiveTableWithSkipHeader);

        createExternalTable("pxf_hive_table_with_skipheader_text", PXF_HIVE_SMALLDATA_COLS, hiveTableWithSkipHeader);

        runSqlTest("features/hive/text_skip_header_rows");
    }

    /**
     * Pump up the comparison table data for partitions test case
     */
    private void pumpUpComparisonTableData(int pumpAmount, boolean useSecondPartition)
            throws IOException {

        // get original number of line before pump
        int originalNumberOfLines = comparisonDataTable.getData().size();

        // duplicate data in factor of 3
        comparisonDataTable.pumpUpTableData(pumpAmount, true);

        // extra field to add
        String[] arr1 = {"rc1", "rc2", "rc3"};
        String[] arr2 = {"a", "b", "c"};

        int lastIndex = 0;

        // run over fields to add and add it in batches of "originalNumberOfLines"
        for (int i = 0; i < pumpAmount; i++) {
            for (int j = lastIndex; j < (lastIndex + originalNumberOfLines); j++) {
                comparisonDataTable.getData().get(j).add(arr1[i]);
                if (useSecondPartition) {
                    comparisonDataTable.getData().get(j).add(arr2[i]);
                }
            }
            lastIndex += originalNumberOfLines;
        }
    }
}
