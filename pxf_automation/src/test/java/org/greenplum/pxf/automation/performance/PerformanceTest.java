package org.greenplum.pxf.automation.performance;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.commons.lang.StringUtils;
import org.greenplum.pxf.automation.structures.data.DataPattern;
import org.greenplum.pxf.automation.structures.tables.basic.Table;
import org.greenplum.pxf.automation.structures.tables.hive.HiveTable;
import org.greenplum.pxf.automation.structures.tables.pxf.ReadableExternalTable;
import org.greenplum.pxf.automation.structures.tables.utils.TableFactory;
import org.testng.annotations.Test;

import jsystem.framework.system.SystemManagerImpl;

import org.greenplum.pxf.automation.components.common.DbSystemObject;
import org.greenplum.pxf.automation.components.hive.Hive;
import org.greenplum.pxf.automation.enums.EnumPxfDefaultProfiles;
import org.greenplum.pxf.automation.utils.data.DataUtils;
import org.greenplum.pxf.automation.utils.jsystem.report.ReportUtils;
import org.greenplum.pxf.automation.features.BaseFeature;

@Test(groups = "performance")
public class PerformanceTest extends BaseFeature {

    private static final String GENERATE_TEXT_DATA_COL_DELIMITER = ",";
    private static final long GENERATE_TEXT_DATA_SIZE_MB = 500;
    private static final int GENERATE_COLUMN_MAX_WIDTH = 50;
    private static final int GENERATE_INT_COLUMNS_NUMBER = 5;
    private static final int GENERATE_TEXT_COLUMNS_NUMBER = 5;

    private static final int SAMPLES_NUMBER = 50;

    //Values for filters
    private static final String FILTER_50_PERCENT_RANGE = StringUtils.repeat(
            "Z", GENERATE_COLUMN_MAX_WIDTH); // Select 50% range
    private static final String FILTER_10_PERCENT_RANGE = StringUtils.repeat(
            "F", GENERATE_COLUMN_MAX_WIDTH); // Select 10% range
    private static final String FILTER_2_PERCENT_RANGE = StringUtils.repeat(
            "B", GENERATE_COLUMN_MAX_WIDTH); // Select 2% range

    //Use cases
    private static final String COUNT_WITHOUT_FILTER = "Count total number of rows in table";
    private static final String COUNT_50_PERCENT = "Count number of rows in table 50% range";
    private static final String COUNT_10_PERCENT = "Count number of rows in table 10% range";
    private static final String COUNT_2_PERCENT = "Count number of rows in table 2% range";
    private static final String SELECT_WITHOUT_FILTER_ALL_COLUMNS = "Select all rows, all columns";
    private static final String SELECT_50_PERCENT_ALL_COLUMNS = "Select 50% rows, all columns";
    private static final String SELECT_10_PERCENT_ALL_COLUMNS = "Select 10% rows, all columns";
    private static final String SELECT_2_PERCENT_ALL_COLUMNS = "Select 2% rows, all columns";
    private static final String SELECT_WITHOUT_FILTER_ONE_COLUMN = "Select all rows, one column";
    private static final String SELECT_50_PERCENT_ONE_COLUMN = "Select 50% rows, one column";
    private static final String SELECT_10_PERCENT_ONE_COLUMN = "Select 10% rows, one column";
    private static final String SELECT_2_PERCENT_ONE_COLUMN = "Select 2% rows, one column";

    Hive hive;

    HiveTable hiveTextPerfTable = null;
    HiveTable hiveOrcPerfTable = null;
    HiveTable hiveRcPerfTable = null;

    ReadableExternalTable gpdbTextHiveProfile = null;
    ReadableExternalTable gpdbTextHiveTextProfile = null;
    ReadableExternalTable gpdbOrcHiveProfile = null;
    ReadableExternalTable gpdbRcHiveProfile = null;

    Table gpdbNativeTable = null;

    List<Table> allTables = null;

    protected void prepareData() throws Exception {
        hive = (Hive) SystemManagerImpl.getInstance().getSystemObject("hive");
        // setup ownership only if we don't have a kerberized cluster
        if (StringUtils.isEmpty(cluster.getTestKerberosPrincipal())) {
            hdfs.setOwner("/" + hdfs.getWorkingDirectory(), hive.getUserName(),
                    hive.getUserName());
        }

        prepareTextData();
        prepareOrcData();
        prepareRcData();
        prepareNativeGpdbData();

    }

    private void prepareTextData() throws Exception {
        hiveTextPerfTable = TableFactory.getHiveByRowCommaTable(
                "perf_test_text", getColumnTypeHive());
        DataPattern dp = new DataPattern();
        ArrayList<String> columnsTypeList = new ArrayList<String>();

        columnsTypeList.addAll(getColumnTypeDataPattern());

        dp.setCoulmnsTypeList(columnsTypeList);
        dp.setColumnDelimiter(GENERATE_TEXT_DATA_COL_DELIMITER);
        dp.setDataSizeInMegaBytes(GENERATE_TEXT_DATA_SIZE_MB);
        dp.setRandomValues(true);
        dp.setColumnMaxSize(GENERATE_COLUMN_MAX_WIDTH);
        hiveTextPerfTable.setDataPattern(dp);
        hiveTextPerfTable.setStoredAs("TEXTFILE");
        hive.createTableAndVerify(hiveTextPerfTable);
        long linesNumGenerated = DataUtils.generateAndLoadData(
                hiveTextPerfTable, hdfs);

        String filePath = hdfs.getWorkingDirectory() + "/"
                + hiveTextPerfTable.getName();

        hive.loadData(hiveTextPerfTable, filePath, false);

        gpdbTextHiveProfile = TableFactory.getPxfHiveReadableTable(
                "perf_text_hive_profile", getColumnTypeGpdb(),
                hiveTextPerfTable, true);
        gpdbTextHiveProfile.setProfile(EnumPxfDefaultProfiles.Hive.toString());
        gpdbTextHiveProfile.setHost(/* pxfHost */"127.0.0.1");
        gpdbTextHiveProfile.setPort(pxfPort);
        gpdb.createTableAndVerify(gpdbTextHiveProfile);

        gpdbTextHiveTextProfile = TableFactory.getPxfHiveTextReadableTable(
                "perf_text_hive_text_profile", getColumnTypeGpdb(),
                hiveTextPerfTable, true);
        gpdbTextHiveTextProfile.setProfile(EnumPxfDefaultProfiles.HiveText
                .toString());
        gpdbTextHiveTextProfile.setHost(/* pxfHost */"127.0.0.1");
        gpdbTextHiveTextProfile.setPort(pxfPort);
        gpdbTextHiveTextProfile.setDelimiter(",");
        gpdb.createTableAndVerify(gpdbTextHiveTextProfile);

    }

    private void prepareOrcData() throws Exception {

        hiveOrcPerfTable = TableFactory.getHiveByRowCommaTable("perf_test_orc",
                getColumnTypeHive());
        hiveOrcPerfTable.setStoredAs("ORC");
        hive.createTableAndVerify(hiveOrcPerfTable);

        hive.insertData(hiveTextPerfTable, hiveOrcPerfTable);

        gpdbOrcHiveProfile = TableFactory.getPxfHiveReadableTable(
                "perf_orc_hive_profile", getColumnTypeGpdb(), hiveOrcPerfTable,
                true);

        gpdbOrcHiveProfile.setProfile(EnumPxfDefaultProfiles.Hive.toString());
        gpdbOrcHiveProfile.setHost(/* pxfHost */"127.0.0.1");
        gpdbOrcHiveProfile.setPort(pxfPort);
        gpdb.createTableAndVerify(gpdbOrcHiveProfile);

    }

    private void prepareRcData() throws Exception {
        hiveRcPerfTable = TableFactory.getHiveByRowCommaTable("perf_test_rc",
                getColumnTypeHive());
        hiveRcPerfTable.setStoredAs("RCFILE");
        hive.createTableAndVerify(hiveRcPerfTable);

        hive.insertData(hiveTextPerfTable, hiveRcPerfTable);

        gpdbRcHiveProfile = TableFactory.getPxfHiveReadableTable(
                "perf_rc_hive_profile", getColumnTypeGpdb(), hiveRcPerfTable,
                true);

        gpdbRcHiveProfile.setProfile(EnumPxfDefaultProfiles.Hive.toString());
        gpdbRcHiveProfile.setHost(/* pxfHost */"127.0.0.1");
        gpdbRcHiveProfile.setPort(pxfPort);
        gpdb.createTableAndVerify(gpdbRcHiveProfile);
    }

    private void prepareNativeGpdbData() throws Exception {
        gpdbNativeTable = new Table("perf_test", getColumnTypeGpdb());
        gpdb.createTableAndVerify(gpdbNativeTable);

        gpdb.insertData(gpdbTextHiveProfile, gpdbNativeTable);
    }

    @Override
    protected void beforeClass() throws Exception {
        prepareData();
        allTables = new ArrayList<Table>();

        allTables.add(hiveTextPerfTable);
        allTables.add(hiveOrcPerfTable);
        allTables.add(hiveRcPerfTable);
        allTables.add(gpdbTextHiveProfile);
        allTables.add(gpdbTextHiveTextProfile);
        allTables.add(gpdbOrcHiveProfile);
        allTables.add(gpdbRcHiveProfile);
        allTables.add(gpdbNativeTable);
    }

    @Test(groups = "performance")
    public void testCountWithoutFilter() throws Exception {

        runAndReportQueries("SELECT COUNT(*) FROM %s", COUNT_WITHOUT_FILTER,
                allTables);
    }

    @Test(groups = "performance")
    public void testCount50PercentRange() throws Exception {

        runAndReportQueries("SELECT COUNT(*) FROM %s WHERE str0 < '"
                + FILTER_50_PERCENT_RANGE + "'", COUNT_50_PERCENT, allTables);
    }

    @Test(groups = "performance")
    public void testCount10PercentRange() throws Exception {

        runAndReportQueries("SELECT COUNT(*) FROM %s WHERE str0 < '"
                + FILTER_10_PERCENT_RANGE + "'", COUNT_10_PERCENT, allTables);
    }

    @Test(groups = "performance")
    public void testCount2PercentRange() throws Exception {

        runAndReportQueries("SELECT COUNT(*) FROM %s WHERE str0 < '"
                + FILTER_2_PERCENT_RANGE + "'", COUNT_2_PERCENT, allTables);
    }

    @Test(groups = "performance")
    public void testSelectAllRowsAllColumns() throws Exception {

        runAndReportQueries("SELECT * FROM %s",
                SELECT_WITHOUT_FILTER_ALL_COLUMNS, allTables);
    }

    @Test(groups = "performance")
    public void testSelect50PercentRowsAllColumns() throws Exception {

        runAndReportQueries("SELECT * FROM %s WHERE str0 < '"
                + FILTER_50_PERCENT_RANGE + "'", SELECT_50_PERCENT_ALL_COLUMNS,
                allTables);
    }

    @Test(groups = "performance")
    public void testSelect10PercentRowsAllColumns() throws Exception {

        runAndReportQueries("SELECT * FROM %s WHERE str0 < '"
                + FILTER_10_PERCENT_RANGE + "'", SELECT_10_PERCENT_ALL_COLUMNS,
                allTables);
    }

    @Test(groups = "performance")
    public void testSelect2PercentRowsAllColumns() throws Exception {

        runAndReportQueries("SELECT * FROM %s WHERE str0 < '"
                + FILTER_2_PERCENT_RANGE + "'", SELECT_2_PERCENT_ALL_COLUMNS,
                allTables);
    }

    @Test(groups = "performance")
    public void testSelectAllRowsOneColumn() throws Exception {

        runAndReportQueries("SELECT str0 FROM %s",
                SELECT_WITHOUT_FILTER_ONE_COLUMN, allTables);
    }

    @Test(groups = "performance")
    public void testSelect50PercentRowsOneColumn() throws Exception {

        runAndReportQueries("SELECT str0 FROM %s WHERE str0 < '"
                + FILTER_50_PERCENT_RANGE + "'", SELECT_50_PERCENT_ONE_COLUMN,
                allTables);
    }

    @Test(groups = "performance")
    public void testSelect10PercentRowsOneColumn() throws Exception {

        runAndReportQueries("SELECT str0 FROM %s WHERE str0 < '"
                + FILTER_10_PERCENT_RANGE + "'", SELECT_10_PERCENT_ONE_COLUMN,
                allTables);
    }

    @Test(groups = "performance")
    public void testSelect2PercentRowsOneColumn() throws Exception {

        runAndReportQueries("SELECT str0 FROM %s WHERE str0 < '"
                + FILTER_2_PERCENT_RANGE + "'", SELECT_2_PERCENT_ONE_COLUMN,
                allTables);
    }

    private void runAndReportQueries(String queryTemplate, String queryType,
            List<Table> tables) throws Exception {

        SortedMap<Long, Table> results = new TreeMap<Long, Table>();

        for (Table table : tables) {
            String query = String.format(queryTemplate, table.getName());
            Long avgTime = measureAverageQueryTime(query, getDbForTable(table));
            results.put(avgTime, table);
        }

        printPerformanceReport();

        for (Entry<Long, Table> entry : results.entrySet()) {
            String query = String.format(queryTemplate, entry.getValue()
                    .getName());
            printPerformanceReportPerTable(queryType, query, entry.getValue(),
                    entry.getKey());
        }

    }

    private List<String> getColumnTypeDataPattern() {
        List<String> result = new ArrayList<String>();
        for (int i = 0; i < GENERATE_INT_COLUMNS_NUMBER; i++)
            result.add("INTEGER");

        for (int i = 0; i < GENERATE_TEXT_COLUMNS_NUMBER; i++)
            result.add("TEXT");

        return result;
    }

    private String[] getColumnTypeGpdb() {
        String[] result = new String[GENERATE_INT_COLUMNS_NUMBER
                + GENERATE_TEXT_COLUMNS_NUMBER];

        for (int i = 0; i < GENERATE_INT_COLUMNS_NUMBER; i++)
            result[i] = "int" + i + " int4";

        for (int i = 0; i < GENERATE_TEXT_COLUMNS_NUMBER; i++)
            result[i + GENERATE_INT_COLUMNS_NUMBER] = "str" + i + " text";

        return result;

    }

    private String[] getColumnTypeHive() {
        String[] result = new String[GENERATE_INT_COLUMNS_NUMBER
                + GENERATE_TEXT_COLUMNS_NUMBER];

        for (int i = 0; i < GENERATE_INT_COLUMNS_NUMBER; i++)
            result[i] = "int" + i + " int";

        for (int i = 0; i < GENERATE_TEXT_COLUMNS_NUMBER; i++)
            result[i + GENERATE_INT_COLUMNS_NUMBER] = "str" + i + " string";

        return result;

    }

    private DbSystemObject getDbForTable(Table table) throws Exception {
        if (table instanceof HiveTable)
            return hive;
        else if (table instanceof ReadableExternalTable
                || table instanceof Table)
            return gpdb;
        else
            throw new Exception("Unable to get db engine for table: "
                    + table.getClass());
    }

    private String getTableInfo(Table table) throws Exception {
        if (table instanceof HiveTable)
            return "Hive table stored as " + ((HiveTable) table).getStoredAs();
        else if (table instanceof ReadableExternalTable)
            return "External Gpdb table, using PXF with profile "
                    + ((ReadableExternalTable) table).getProfile();
        else if (table instanceof Table)
            return "Native Gpdb table";
        else
            throw new Exception(
                    "Unable to print table details, unknown table: "
                            + table.getClass());
    }

    private void printPerformanceReportPerTable(String queryType, String query,
            Table table, long avgTime) throws Exception {

        DbSystemObject db = getDbForTable(table);

        String tableInfo = getTableInfo(table);
        ReportUtils.startLevel(null, getClass(), "PERFORMANCE RESULTS FOR: "
                + tableInfo);

        ReportUtils.report(null, getClass(), "Query type: " + queryType);
        ReportUtils.report(null, getClass(), "Query: " + query);
        ReportUtils.report(null, getClass(), "Db engine: " + db.getClass());
        ReportUtils.report(null, getClass(), "AVERAGE TIME: " + avgTime
                + " MILLISECONDS");
        ReportUtils.report(null, getClass(), "");

        ReportUtils.stopLevel(null);
    }

    private void printPerformanceReport() throws Exception {
        ReportUtils.startLevel(null, getClass(), "PERFORMANCE RESULTS");
        ReportUtils.report(null, getClass(),
                "Initial data size in text format: "
                        + GENERATE_TEXT_DATA_SIZE_MB + " Mb");
        ReportUtils.report(null, getClass(), "String column width: "
                + GENERATE_COLUMN_MAX_WIDTH);
        ReportUtils.report(null, getClass(),
                "Number of samples per each query: " + SAMPLES_NUMBER);
        ReportUtils.report(null, getClass(), "");
        ReportUtils.stopLevel(null);
    }

    private long measureAverageQueryTime(String query, DbSystemObject db)
            throws Exception {
        long avgQueryTime = 0;
        for (int i = 0; i < SAMPLES_NUMBER; i++)
            avgQueryTime += db.runQueryTiming(query);

        avgQueryTime /= SAMPLES_NUMBER;
        return avgQueryTime;
    }
}
