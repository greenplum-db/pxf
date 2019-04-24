package org.greenplum.pxf.automation.features.jdbc;

import java.io.File;

import org.greenplum.pxf.automation.structures.tables.basic.Table;
import org.greenplum.pxf.automation.structures.tables.pxf.ExternalTable;
import org.greenplum.pxf.automation.structures.tables.utils.TableFactory;
import org.testng.annotations.Test;

import org.greenplum.pxf.automation.enums.EnumPartitionType;

import org.greenplum.pxf.automation.features.BaseFeature;

public class JdbcTest extends BaseFeature {

    private static final String POSTGRES_DRIVER_CLASS = "org.postgresql.Driver";
    private static final String GPDB_PXF_AUTOMATION_DB_JDBC = "jdbc:postgresql://";
    private static final String[] TYPES_TABLE_FIELDS = new String[]{
            "t1    text",
            "t2    text",
            "num1  int",
            "dub1  double precision",
            "dec1  numeric",
            "tm timestamp",
            "r real",
            "bg bigint",
            "b boolean",
            "tn smallint",
            "sml smallint",
            "dt date",
            "vc1 varchar(5)",
            "c1 char(3)",
            "bin bytea"};
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
    private ExternalTable pxfJdbcSingleFragment;
    private ExternalTable pxfJdbcMultipleFragmentsByInt;
    private ExternalTable pxfJdbcMultipleFragmentsByDate;
    private ExternalTable pxfJdbcMultipleFragmentsByEnum;
    private ExternalTable pxfJdbcReadServerConfigAll; // all server-based props coming from there, not DDL
    private ExternalTable pxfJdbcReadViewNoParams, pxfJdbcReadViewSessionParams;
    private ExternalTable pxfJdbcWritable;
    private ExternalTable pxfJdbcWritableNoBatch;
    private ExternalTable pxfJdbcWritablePool;
    private ExternalTable pxfJdbcColumns;
    private ExternalTable pxfJdbcColumnProjectionSubset;
    private ExternalTable pxfJdbcColumnProjectionSuperset;

    private static final String gpdbTypesDataFileName = "gpdb_types.txt";
    private static final String gpdbColumnsDataFileName = "gpdb_columns.txt";
    private Table gpdbNativeTableTypes, gpdbNativeTableColumns, gpdbWritableTargetTable;
    private Table gpdbWritableTargetTableNoBatch, gpdbWritableTargetTablePool;

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
    }

    private void prepareTypesData() throws Exception {
        // create a table prepared for partitioning
        gpdbNativeTableTypes = new Table("gpdb_types", TYPES_TABLE_FIELDS);
        gpdbNativeTableTypes.setDistributionFields(new String[]{"t1"});
        gpdb.createTableAndVerify(gpdbNativeTableTypes);
        gpdb.copyFromFile(gpdbNativeTableTypes, new File(localDataResourcesFolder
                + "/gpdb/" + gpdbTypesDataFileName), "E'\\t'", "E'\\\\N'", true);

        // create a table to be filled by the writable test case
        gpdbWritableTargetTable = new Table("gpdb_types_target", TYPES_TABLE_FIELDS);
        gpdbWritableTargetTable.setDistributionFields(new String[]{"t1"});
        gpdb.createTableAndVerify(gpdbWritableTargetTable);

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
        gpdb.createTableAndVerify(pxfJdbcWritable);

        pxfJdbcWritableNoBatch = TableFactory.getPxfJdbcWritableTable(
                "pxf_jdbc_writable_nobatch",
                TYPES_TABLE_FIELDS_SMALL,
                gpdbWritableTargetTableNoBatch.getName(),
                POSTGRES_DRIVER_CLASS,
                GPDB_PXF_AUTOMATION_DB_JDBC + gpdb.getMasterHost() + ":" + gpdb.getPort() + "/pxfautomation",
                gpdb.getUserName(), "BATCH_SIZE=1");
        pxfJdbcWritable.setHost(pxfHost);
        pxfJdbcWritable.setPort(pxfPort);
        gpdb.createTableAndVerify(pxfJdbcWritableNoBatch);

        pxfJdbcWritablePool = TableFactory.getPxfJdbcWritableTable(
                "pxf_jdbc_writable_pool",
                TYPES_TABLE_FIELDS_SMALL,
                gpdbWritableTargetTablePool.getName(),
                POSTGRES_DRIVER_CLASS,
                GPDB_PXF_AUTOMATION_DB_JDBC + gpdb.getMasterHost() + ":" + gpdb.getPort() + "/pxfautomation",
                gpdb.getUserName(), "POOL_SIZE=2");
        pxfJdbcWritable.setHost(pxfHost);
        pxfJdbcWritable.setPort(pxfPort);
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

    @Test(groups = {"features", "gpdb"})
    public void singleFragmentTable() throws Exception {
        runTincTest("pxf.features.jdbc.single_fragment.runTest");
    }

    @Test(groups = {"features", "gpdb"})
    public void multipleFragmentsTables() throws Exception {
        runTincTest("pxf.features.jdbc.multiple_fragments.runTest");
    }

    @Test(groups = {"features", "gpdb"})
    public void readServerConfig() throws Exception {
        runTincTest("pxf.features.jdbc.server_config.runTest");
    }

    @Test(groups = {"features", "gpdb"})
    public void readViewSessionParams() throws Exception {
        runTincTest("pxf.features.jdbc.session_params.runTest");
    }

    @Test(groups = {"features", "gpdb"})
    public void jdbcWritableTable() throws Exception {
        runTincTest("pxf.features.jdbc.writable.runTest");
    }

    @Test(groups = {"features", "gpdb"})
    public void jdbcWritableTableNoBatch() throws Exception {
        runTincTest("pxf.features.jdbc.writable_nobatch.runTest");
    }

    @Test(groups = {"features", "gpdb"})
    public void jdbcWritableTablePool() throws Exception {
        runTincTest("pxf.features.jdbc.writable_pool.runTest");
    }

    @Test(groups = {"features", "gpdb"})
    public void jdbcColumns() throws Exception {
        runTincTest("pxf.features.jdbc.columns.runTest");
    }

    @Test(groups = {"features", "gpdb"})
    public void jdbcColumnProjection() throws Exception {
        runTincTest("pxf.features.jdbc.column_projection.runTest");
    }


}
