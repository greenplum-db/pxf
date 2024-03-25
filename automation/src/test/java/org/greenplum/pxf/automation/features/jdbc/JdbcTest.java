package org.greenplum.pxf.automation.features.jdbc;

import java.io.File;

import annotations.FailsWithFDW;
import annotations.WorksWithFDW;
import org.greenplum.pxf.automation.structures.tables.basic.Table;
import org.greenplum.pxf.automation.structures.tables.pxf.ExternalTable;
import org.greenplum.pxf.automation.structures.tables.utils.TableFactory;
import org.testng.annotations.Test;

import org.greenplum.pxf.automation.enums.EnumPartitionType;

import org.greenplum.pxf.automation.features.BaseFeature;

@WorksWithFDW
public class JdbcTest extends BaseFeature {

    private static final String POSTGRES_DRIVER_CLASS = "org.postgresql.Driver";
    private static final String GPDB_PXF_AUTOMATION_DB_JDBC = "jdbc:postgresql://";
    private static final String[] TYPES_TABLE_FIELDS = new String[]{
            "t1    text",
            "t2    text",
            "num1  int",
            "dub1  double precision",
            "dec1  numeric",
            "tm    timestamp",
            "r     real",
            "bg    bigint",
            "b     boolean",
            "tn    smallint",
            "sml   smallint",
            "dt    date",
            "vc1   varchar(5)",
            "c1    char(3)",
            "bin   bytea",
            "u     uuid",
            "tmz   timestamp with time zone"
    };
    private static final String[] PGSETTINGS_VIEW_FIELDS = new String[]{
            "name    text",
            "setting text"};
    private static final String[] TYPES_TABLE_FIELDS_SMALL = new String[]{
            "t1    text",
            "t2    text",
            "num1  int"};
    private static final String[] COLUMNS_TABLE_FIELDS = new String[]{
            "t text",
            "\"num 1\" int",
            "\"n@m2\" int"};
    private static final String[] COLUMNS_TABLE_FIELDS_IN_DIFFERENT_ORDER_SUBSET = new String[]{
            "\"n@m2\" int",
            "\"num 1\" int"};
    private static final String[] COLUMNS_TABLE_FIELDS_SUPERSET = new String[]{
            "t text",
            "\"does_not_exist_on_source\" text",
            "\"num 1\" int",
            "\"n@m2\" int"};
    private static final String[] NAMED_QUERY_FIELDS = new String[]{
            "name  text",
            "count int",
            "max  int"};

    private ExternalTable pxfJdbcSingleFragment;
    private ExternalTable pxfJdbcDateWideRangeOn;
    private ExternalTable pxfJdbcDateWideRangeOff;
    private ExternalTable pxfJdbcMultipleFragmentsByInt;
    private ExternalTable pxfJdbcMultipleFragmentsByDate;
    private ExternalTable pxfJdbcMultipleFragmentsByEnum;
    private ExternalTable pxfJdbcReadServerConfigAll; // all server-based props coming from there, not DDL
    private ExternalTable pxfJdbcReadViewNoParams, pxfJdbcReadViewSessionParams;
    private ExternalTable pxfJdbcWritable;
    private ExternalTable pxfJdbcWritableWithDateWideRange;
    private ExternalTable pxfJdbcWritableNoBatch;
    private ExternalTable pxfJdbcWritablePool;
    private ExternalTable pxfJdbcColumns;
    private ExternalTable pxfJdbcColumnProjectionSubset;
    private ExternalTable pxfJdbcColumnProjectionSuperset;
    private ExternalTable pxfJdbcNamedQuery;

    private static final String gpdbTypesWithDateWideRangeDataFileName = "gpdb_types_with_date_wide_range.txt";
    private static final String gpdbTypesDataFileName = "gpdb_types.txt";
    private static final String gpdbColumnsDataFileName = "gpdb_columns.txt";
    private Table gpdbNativeTableTypes, gpdbNativeTableTypesWithDateWideRange, gpdbNativeTableColumns, gpdbWritableTargetTable, gpdbWritableTargetTableWithDateWideRange;
    private Table gpdbWritableTargetTableNoBatch, gpdbWritableTargetTablePool;
    private Table gpdbDeptTable, gpdbEmpTable;

    @Override
    protected void beforeClass() throws Exception {
        prepareData();
    }

    protected void prepareData() throws Exception {
        prepareTypesData();
        prepareSingleFragment();
        prepareMultipleFragmentsByInt();
        prepareMultipleFragmentsByDate();
        prepareMultipleFragmentsByEnum();
        prepareServerBasedMultipleFragmentsByInt();
        prepareViewBasedForTestingSessionParams();
        prepareWritable();
        prepareColumns();
        prepareColumnProjectionSubsetInDifferentOrder();
        prepareColumnProjectionSuperset();
        prepareFetchSizeZero();
        prepareDateWideRange();
        prepareNamedQuery();
    }

    private void prepareTypesData() throws Exception {
        // create a table prepared for partitioning
        gpdbNativeTableTypes = new Table("gpdb_types", TYPES_TABLE_FIELDS);
        gpdbNativeTableTypes.setDistributionFields(new String[]{"t1"});
        gpdb.createTableAndVerify(gpdbNativeTableTypes);
        gpdb.copyFromFile(gpdbNativeTableTypes, new File(localDataResourcesFolder
                + "/gpdb/" + gpdbTypesDataFileName), "E'\\t'", "E'\\\\N'", true);

        // create a table that is the same as above but with timestamp with time zone
        gpdbNativeTableTypesWithDateWideRange = new Table("gpdb_types_with_date_wide_range", TYPES_TABLE_FIELDS);
        gpdbNativeTableTypesWithDateWideRange.setDistributionFields(new String[]{"t1"});
        gpdb.createTableAndVerify(gpdbNativeTableTypesWithDateWideRange);
        gpdb.copyFromFile(gpdbNativeTableTypesWithDateWideRange, new File(localDataResourcesFolder
                + "/gpdb/" + gpdbTypesWithDateWideRangeDataFileName), "E'\\t'", "E'\\\\N'", true);

        // create a table to be filled by the writable test case
        gpdbWritableTargetTable = new Table("gpdb_types_target", TYPES_TABLE_FIELDS);
        gpdbWritableTargetTable.setDistributionFields(new String[]{"t1"});
        gpdb.createTableAndVerify(gpdbWritableTargetTable);

        // create a table to be filled by the writable test case
        gpdbWritableTargetTableWithDateWideRange = new Table("gpdb_types_target_with_date_wide_range", TYPES_TABLE_FIELDS);
        gpdbWritableTargetTableWithDateWideRange.setDistributionFields(new String[]{"t1"});
        gpdb.createTableAndVerify(gpdbWritableTargetTableWithDateWideRange);

        // create a table to be filled by the writable test case with no batch
        gpdbWritableTargetTableNoBatch = new Table("gpdb_types_nobatch_target", TYPES_TABLE_FIELDS_SMALL);
        gpdbWritableTargetTableNoBatch.setDistributionFields(new String[]{"t1"});
        gpdb.createTableAndVerify(gpdbWritableTargetTableNoBatch);

        // create a table to be filled by the writable test case with pool size > 1
        gpdbWritableTargetTablePool = new Table("gpdb_types_pool_target", TYPES_TABLE_FIELDS_SMALL);
        gpdbWritableTargetTablePool.setDistributionFields(new String[]{"t1"});
        gpdb.createTableAndVerify(gpdbWritableTargetTablePool);

        // create a table with special column names
        gpdbNativeTableColumns = new Table("gpdb_columns", COLUMNS_TABLE_FIELDS);
        gpdbNativeTableColumns.setDistributionFields(new String[]{"t"});
        gpdb.createTableAndVerify(gpdbNativeTableColumns);
        gpdb.copyFromFile(gpdbNativeTableColumns, new File(localDataResourcesFolder
                + "/gpdb/" + gpdbColumnsDataFileName), "E'\\t'", "E'\\\\N'", true);

        // create emp and dept tables for named query test
        String[] deptTableFields = new String[]{"name text", "id int"};
        gpdbDeptTable = new Table("gpdb_dept", deptTableFields);
        gpdbDeptTable.setDistributionFields(new String[]{"name"});
        gpdb.createTableAndVerify(gpdbDeptTable);
        String[][] deptRows = new String[][] {
                { "sales", "1"},
                { "finance", "2"},
                { "it", "3"}};
        Table dataTable = new Table("data", deptTableFields);
        dataTable.addRows(deptRows);
        gpdb.insertData(dataTable, gpdbDeptTable);

        String[] empTableFields = new String[]{"name text", "dept_id int", "salary int"};
        gpdbEmpTable = new Table("gpdb_emp", empTableFields);
        gpdbEmpTable.setDistributionFields(new String[]{"name"});
        gpdb.createTableAndVerify(gpdbEmpTable);
        final String[][] empRows = new String[][] {
                { "alice", "1", "115" },
                { "bob", "1", "120" },
                { "charli", "1", "93" },
                { "daniel", "2", "87" },
                { "emma", "2", "100" },
                { "frank", "2", "103" },
                { "george", "2", "90" },
                { "henry", "3", "96" },
                { "ivanka", "3", "70" }};
        dataTable = new Table("data", empTableFields);
        dataTable.addRows(empRows);
        gpdb.insertData(dataTable, gpdbEmpTable);
    }

    private void prepareSingleFragment() throws Exception {
        pxfJdbcSingleFragment = TableFactory.getPxfJdbcReadableTable(
                "pxf_jdbc_single_fragment",
                TYPES_TABLE_FIELDS,
                gpdbNativeTableTypes.getName(),
                POSTGRES_DRIVER_CLASS,
                GPDB_PXF_AUTOMATION_DB_JDBC + gpdb.getMasterHost() + ":" + gpdb.getPort() + "/pxfautomation",
                gpdb.getUserName());
        pxfJdbcSingleFragment.setHost(pxfHost);
        pxfJdbcSingleFragment.setPort(pxfPort);
        gpdb.createTableAndVerify(pxfJdbcSingleFragment);
    }

    private void prepareMultipleFragmentsByEnum() throws Exception {
        pxfJdbcMultipleFragmentsByEnum = TableFactory
                .getPxfJdbcReadablePartitionedTable(
                        "pxf_jdbc_multiple_fragments_by_enum",
                        TYPES_TABLE_FIELDS,
                        gpdbNativeTableTypes.getName(),
                        POSTGRES_DRIVER_CLASS,
                        GPDB_PXF_AUTOMATION_DB_JDBC + gpdb.getMasterHost() + ":" + gpdb.getPort() + "/pxfautomation",
                        13,
                        "USD:UAH",
                        "1",
                        gpdb.getUserName(),
                        EnumPartitionType.ENUM,
                        null);
        pxfJdbcMultipleFragmentsByEnum.setHost(pxfHost);
        pxfJdbcMultipleFragmentsByEnum.setPort(pxfPort);
        gpdb.createTableAndVerify(pxfJdbcMultipleFragmentsByEnum);
    }

    private void prepareMultipleFragmentsByInt() throws Exception {
        pxfJdbcMultipleFragmentsByInt = TableFactory
                .getPxfJdbcReadablePartitionedTable(
                        "pxf_jdbc_multiple_fragments_by_int",
                        TYPES_TABLE_FIELDS,
                        gpdbNativeTableTypes.getName(),
                        POSTGRES_DRIVER_CLASS,
                        GPDB_PXF_AUTOMATION_DB_JDBC + gpdb.getMasterHost() + ":" + gpdb.getPort() + "/pxfautomation",
                        2,
                        "1:6",
                        "1",
                        gpdb.getUserName(),
                        EnumPartitionType.INT,
                        null);
        pxfJdbcMultipleFragmentsByInt.setHost(pxfHost);
        pxfJdbcMultipleFragmentsByInt.setPort(pxfPort);
        gpdb.createTableAndVerify(pxfJdbcMultipleFragmentsByInt);
    }

    private void prepareMultipleFragmentsByDate() throws Exception {
        pxfJdbcMultipleFragmentsByDate = TableFactory
                .getPxfJdbcReadablePartitionedTable(
                        "pxf_jdbc_multiple_fragments_by_date",
                        TYPES_TABLE_FIELDS,
                        gpdbNativeTableTypes.getName(),
                        POSTGRES_DRIVER_CLASS,
                        GPDB_PXF_AUTOMATION_DB_JDBC + gpdb.getMasterHost() + ":" + gpdb.getPort() + "/pxfautomation",
                        11,
                        "2015-03-06:2015-03-20",
                        "1:DAY",
                        gpdb.getUserName(),
                        EnumPartitionType.DATE,
                        null);
        pxfJdbcMultipleFragmentsByDate.setHost(pxfHost);
        pxfJdbcMultipleFragmentsByDate.setPort(pxfPort);
        gpdb.createTableAndVerify(pxfJdbcMultipleFragmentsByDate);
    }

    private void prepareServerBasedMultipleFragmentsByInt() throws Exception {
        pxfJdbcReadServerConfigAll = TableFactory
                .getPxfJdbcReadablePartitionedTable(
                        "pxf_jdbc_read_server_config_all",
                        TYPES_TABLE_FIELDS,
                        gpdbNativeTableTypes.getName(),
                        null,
                        null,
                        2,
                        "1:6",
                        "1",
                        null,
                        EnumPartitionType.INT,
                        "database");
        pxfJdbcReadServerConfigAll.setHost(pxfHost);
        pxfJdbcReadServerConfigAll.setPort(pxfPort);
        gpdb.createTableAndVerify(pxfJdbcReadServerConfigAll);
    }

    private void prepareViewBasedForTestingSessionParams() throws Exception {
        pxfJdbcReadViewNoParams = TableFactory.getPxfJdbcReadableTable(
                "pxf_jdbc_read_view_no_params",
                PGSETTINGS_VIEW_FIELDS,
                "pg_settings",
                "database");
        pxfJdbcReadViewNoParams.setHost(pxfHost);
        pxfJdbcReadViewNoParams.setPort(pxfPort);
        gpdb.createTableAndVerify(pxfJdbcReadViewNoParams);

        pxfJdbcReadViewSessionParams = TableFactory.getPxfJdbcReadableTable(
                "pxf_jdbc_read_view_session_params",
                PGSETTINGS_VIEW_FIELDS,
                "pg_settings",
                "db-session-params");
        pxfJdbcReadViewSessionParams.setHost(pxfHost);
        pxfJdbcReadViewSessionParams.setPort(pxfPort);
        gpdb.createTableAndVerify(pxfJdbcReadViewSessionParams);
    }

    private void prepareWritable() throws Exception {
        pxfJdbcWritable = TableFactory.getPxfJdbcWritableTable(
                "pxf_jdbc_writable",
                TYPES_TABLE_FIELDS,
                gpdbWritableTargetTable.getName(),
                POSTGRES_DRIVER_CLASS,
                GPDB_PXF_AUTOMATION_DB_JDBC + gpdb.getMasterHost() + ":" + gpdb.getPort() + "/pxfautomation",
                gpdb.getUserName(), null);
        pxfJdbcWritable.setHost(pxfHost);
        pxfJdbcWritable.setPort(pxfPort);
        pxfJdbcWritable.addUserParameter("date_wide_range=false");
        gpdb.createTableAndVerify(pxfJdbcWritable);

        pxfJdbcWritableWithDateWideRange = TableFactory.getPxfJdbcWritableTable(
                "pxf_jdbc_writable_date_wide_range_on",
                TYPES_TABLE_FIELDS,
                gpdbWritableTargetTableWithDateWideRange.getName(),
                POSTGRES_DRIVER_CLASS,
                GPDB_PXF_AUTOMATION_DB_JDBC + gpdb.getMasterHost() + ":" + gpdb.getPort() + "/pxfautomation",
                gpdb.getUserName(), null);
        pxfJdbcWritableWithDateWideRange.setHost(pxfHost);
        pxfJdbcWritableWithDateWideRange.setPort(pxfPort);
        pxfJdbcWritableWithDateWideRange.addUserParameter("date_wide_range=true");
        gpdb.createTableAndVerify(pxfJdbcWritableWithDateWideRange);

        pxfJdbcWritableNoBatch = TableFactory.getPxfJdbcWritableTable(
                "pxf_jdbc_writable_nobatch",
                TYPES_TABLE_FIELDS_SMALL,
                gpdbWritableTargetTableNoBatch.getName(),
                POSTGRES_DRIVER_CLASS,
                GPDB_PXF_AUTOMATION_DB_JDBC + gpdb.getMasterHost() + ":" + gpdb.getPort() + "/pxfautomation",
                gpdb.getUserName(), "BATCH_SIZE=1");
        pxfJdbcWritableNoBatch.setHost(pxfHost);
        pxfJdbcWritableNoBatch.setPort(pxfPort);
        gpdb.createTableAndVerify(pxfJdbcWritableNoBatch);

        pxfJdbcWritablePool = TableFactory.getPxfJdbcWritableTable(
                "pxf_jdbc_writable_pool",
                TYPES_TABLE_FIELDS_SMALL,
                gpdbWritableTargetTablePool.getName(),
                POSTGRES_DRIVER_CLASS,
                GPDB_PXF_AUTOMATION_DB_JDBC + gpdb.getMasterHost() + ":" + gpdb.getPort() + "/pxfautomation",
                gpdb.getUserName(), "POOL_SIZE=2");
        pxfJdbcWritablePool.setHost(pxfHost);
        pxfJdbcWritablePool.setPort(pxfPort);
        gpdb.createTableAndVerify(pxfJdbcWritablePool);
    }

    private void prepareColumns() throws Exception {
        pxfJdbcColumns = TableFactory.getPxfJdbcReadableTable(
                "pxf_jdbc_columns",
                COLUMNS_TABLE_FIELDS,
                gpdbNativeTableColumns.getName(),
                POSTGRES_DRIVER_CLASS,
                GPDB_PXF_AUTOMATION_DB_JDBC + gpdb.getMasterHost() + ":" + gpdb.getPort() + "/pxfautomation",
                gpdb.getUserName());
        pxfJdbcColumns.setHost(pxfHost);
        pxfJdbcColumns.setPort(pxfPort);
        gpdb.createTableAndVerify(pxfJdbcColumns);
    }

    private void prepareColumnProjectionSubsetInDifferentOrder() throws Exception {
        pxfJdbcColumnProjectionSubset = TableFactory.getPxfJdbcReadableTable(
                "pxf_jdbc_subset_of_fields_diff_order",
                COLUMNS_TABLE_FIELDS_IN_DIFFERENT_ORDER_SUBSET,
                gpdbNativeTableColumns.getName(),
                POSTGRES_DRIVER_CLASS,
                GPDB_PXF_AUTOMATION_DB_JDBC + gpdb.getMasterHost() + ":" + gpdb.getPort() + "/pxfautomation",
                gpdb.getUserName());
        pxfJdbcColumnProjectionSubset.setHost(pxfHost);
        pxfJdbcColumnProjectionSubset.setPort(pxfPort);
        gpdb.createTableAndVerify(pxfJdbcColumnProjectionSubset);
    }

    private void prepareColumnProjectionSuperset() throws Exception {
        pxfJdbcColumnProjectionSuperset = TableFactory.getPxfJdbcReadableTable(
                "pxf_jdbc_superset_of_fields",
                COLUMNS_TABLE_FIELDS_SUPERSET,
                gpdbNativeTableColumns.getName(),
                POSTGRES_DRIVER_CLASS,
                GPDB_PXF_AUTOMATION_DB_JDBC + gpdb.getMasterHost() + ":" + gpdb.getPort() + "/pxfautomation",
                gpdb.getUserName());
        pxfJdbcColumnProjectionSuperset.setHost(pxfHost);
        pxfJdbcColumnProjectionSuperset.setPort(pxfPort);
        gpdb.createTableAndVerify(pxfJdbcColumnProjectionSuperset);
    }

    private void prepareFetchSizeZero() throws Exception {
        pxfJdbcSingleFragment = TableFactory.getPxfJdbcReadableTable(
                "pxf_jdbc_readable_nobatch",
                TYPES_TABLE_FIELDS,
                gpdbNativeTableTypes.getName(),
                POSTGRES_DRIVER_CLASS,
                GPDB_PXF_AUTOMATION_DB_JDBC + gpdb.getMasterHost() + ":" + gpdb.getPort() + "/pxfautomation",
                gpdb.getUserName(), "FETCH_SIZE=0");
        pxfJdbcSingleFragment.setHost(pxfHost);
        pxfJdbcSingleFragment.setPort(pxfPort);
        gpdb.createTableAndVerify(pxfJdbcSingleFragment);
    }

    private void prepareDateWideRange() throws Exception {
        pxfJdbcDateWideRangeOn = TableFactory.getPxfJdbcReadableTable(
                "pxf_jdbc_readable_date_wide_range_on",
                TYPES_TABLE_FIELDS,
                gpdbNativeTableTypesWithDateWideRange.getName(),
                POSTGRES_DRIVER_CLASS,
                GPDB_PXF_AUTOMATION_DB_JDBC + gpdb.getMasterHost() + ":" + gpdb.getPort() + "/pxfautomation",
                gpdb.getUserName());
        pxfJdbcDateWideRangeOn.setHost(pxfHost);
        pxfJdbcDateWideRangeOn.setPort(pxfPort);
        pxfJdbcDateWideRangeOn.addUserParameter("date_wide_range=true");
        gpdb.createTableAndVerify(pxfJdbcDateWideRangeOn);

        pxfJdbcDateWideRangeOff = TableFactory.getPxfJdbcReadableTable(
                "pxf_jdbc_readable_date_wide_range_off",
                TYPES_TABLE_FIELDS,
                gpdbNativeTableTypesWithDateWideRange.getName(),
                POSTGRES_DRIVER_CLASS,
                GPDB_PXF_AUTOMATION_DB_JDBC + gpdb.getMasterHost() + ":" + gpdb.getPort() + "/pxfautomation",
                gpdb.getUserName());
        pxfJdbcDateWideRangeOff.setHost(pxfHost);
        pxfJdbcDateWideRangeOff.setPort(pxfPort);
        pxfJdbcDateWideRangeOff.addUserParameter("date_wide_range=false");
        gpdb.createTableAndVerify(pxfJdbcDateWideRangeOff);
    }

    private void prepareNamedQuery() throws Exception {
        pxfJdbcNamedQuery = TableFactory.getPxfJdbcReadableTable(
                "pxf_jdbc_read_named_query",
                NAMED_QUERY_FIELDS,
                "query:report",
                "database");
        pxfJdbcNamedQuery.setHost(pxfHost);
        pxfJdbcNamedQuery.setPort(pxfPort);
        gpdb.createTableAndVerify(pxfJdbcNamedQuery);

        pxfJdbcNamedQuery = TableFactory.getPxfJdbcReadablePartitionedTable(
                "pxf_jdbc_read_named_query_partitioned",
                NAMED_QUERY_FIELDS,
                "query:report",
                null,
                null,
                1,
                "1:5",
                "1",
                null,
                EnumPartitionType.INT,
                "database");
        pxfJdbcNamedQuery.setHost(pxfHost);
        pxfJdbcNamedQuery.setPort(pxfPort);
        gpdb.createTableAndVerify(pxfJdbcNamedQuery);
    }

    @Test(groups = {"features", "gpdb", "security", "jdbc"})
    public void singleFragmentTable() throws Exception {
        runSqlTest("features/jdbc/single_fragment");
    }

    @Test(groups = {"features", "gpdb", "security", "jdbc"})
    public void multipleFragmentsTables() throws Exception {
        runSqlTest("features/jdbc/multiple_fragments");
    }

    @Test(groups = {"features", "gpdb", "security", "jdbc"})
    public void readServerConfig() throws Exception {
        runSqlTest("features/jdbc/server_config");
    }

    @Test(groups = {"features", "gpdb", "security", "jdbc"})
    public void readViewSessionParams() throws Exception {
        runSqlTest("features/jdbc/session_params");
    }

    @FailsWithFDW
    // All the Writable Tests are failing with this Error:
    // ERROR:  PXF server error : class java.io.DataInputStream cannot be cast to class
    // [B (java.io.DataInputStream and [B are in module java.base of loader 'bootstrap')
    @Test(groups = {"features", "gpdb", "security", "jdbc"})
    public void jdbcWritableTable() throws Exception {
        runSqlTest("features/jdbc/writable");
    }

    @FailsWithFDW
    // All the Writable Tests are failing with this Error:
    // ERROR:  PXF server error : class java.io.DataInputStream cannot be cast to class
    // [B (java.io.DataInputStream and [B are in module java.base of loader 'bootstrap')
    @Test(groups = {"features", "gpdb", "security", "jdbc"})
    public void jdbcWritableTableWithDateWideRange() throws Exception {
        runSqlTest("features/jdbc/writable_date_wide_range");
    }

    @FailsWithFDW
    @Test(groups = {"features", "gpdb", "security", "jdbc"})
    public void jdbcWritableTableNoBatch() throws Exception {
        runSqlTest("features/jdbc/writable_nobatch");
    }

    @FailsWithFDW
    @Test(groups = {"features", "gpdb", "security", "jdbc"})
    public void jdbcWritableTablePool() throws Exception {
        runSqlTest("features/jdbc/writable_pool");
    }

    @Test(groups = {"features", "gpdb", "security", "jdbc"})
    public void jdbcColumns() throws Exception {
        runSqlTest("features/jdbc/columns");
    }

    @Test(groups = {"features", "gpdb", "security", "jdbc"})
    public void jdbcColumnProjection() throws Exception {
        runSqlTest("features/jdbc/column_projection");
    }

    @Test(groups = {"features", "gpdb", "security", "jdbc"})
    public void jdbcReadableTableNoBatch() throws Exception {
        runSqlTest("features/jdbc/readable_nobatch");
    }

    @Test(groups = {"features", "gpdb", "security", "jdbc"})
    public void jdbcReadableTableWithDateWideRange() throws Exception {
        runSqlTest("features/jdbc/readable_date_wide_range");
    }

    @Test(groups = {"features", "gpdb", "security", "jdbc"})
    public void jdbcNamedQuery() throws Exception {
        runSqlTest("features/jdbc/named_query");
    }
}
