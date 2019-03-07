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
    private static final String[] TYPES_TABLE_FIELDS = new String[] {
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
        "bin bytea" };
    private static final String[] COLUMNS_TABLE_FIELDS = new String[] {
        "t text",
        "\"num 1\" int",
        "\"n@m2\" int" };
    private ExternalTable pxfJdbcSingleFragment;
    private ExternalTable pxfJdbcMultipleFragmentsByInt;
    private ExternalTable pxfJdbcMultipleFragmentsByDate;
    private ExternalTable pxfJdbcMultipleFragmentsByEnum;
    private ExternalTable pxfJdbcWritable;
    private ExternalTable pxfJdbcColumns;

    private static final String gpdbTypesDataFileName = "gpdb_types.txt";
    private static final String gpdbColumnsDataFileName = "gpdb_columns.txt";
    private Table gpdbNativeTableTypes, gpdbNativeTableColumns, gpdbWritableTargetTable;

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
        prepareWritable();
        prepareColumns();
    }

    private void prepareTypesData() throws Exception {
        // create a table prepared for partitioning
        gpdbNativeTableTypes = new Table("gpdb_types", TYPES_TABLE_FIELDS);
        gpdbNativeTableTypes.setDistributionFields(new String[] { "t1" });
        gpdb.createTableAndVerify(gpdbNativeTableTypes);
        gpdb.copyFromFile(gpdbNativeTableTypes, new File(localDataResourcesFolder
                + "/gpdb/" + gpdbTypesDataFileName), "E'\\t'", "E'\\\\N'", true);

        // create a table to be filled by the writable test case
        gpdbWritableTargetTable = new Table("gpdb_types_target", TYPES_TABLE_FIELDS);
        gpdbWritableTargetTable.setDistributionFields(new String[] { "t1" });
        gpdb.createTableAndVerify(gpdbWritableTargetTable);

        // create a table with special column names
        gpdbNativeTableColumns = new Table("gpdb_columns", COLUMNS_TABLE_FIELDS);
        gpdbNativeTableColumns.setDistributionFields(new String[] { "t" });
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
                EnumPartitionType.ENUM);
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
                EnumPartitionType.INT);
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
                EnumPartitionType.DATE);
        pxfJdbcMultipleFragmentsByDate.setHost(pxfHost);
        pxfJdbcMultipleFragmentsByDate.setPort(pxfPort);
        gpdb.createTableAndVerify(pxfJdbcMultipleFragmentsByDate);
    }

    private void prepareWritable() throws Exception {
        pxfJdbcWritable = TableFactory.getPxfJdbcWritableTable(
                "pxf_jdbc_writable",
                TYPES_TABLE_FIELDS,
                gpdbNativeTableTypes.getName() + "_target",
                POSTGRES_DRIVER_CLASS,
                GPDB_PXF_AUTOMATION_DB_JDBC + gpdb.getMasterHost() + ":" + gpdb.getPort() + "/pxfautomation",
                gpdb.getUserName());
        pxfJdbcWritable.setHost(pxfHost);
        pxfJdbcWritable.setPort(pxfPort);
        gpdb.createTableAndVerify(pxfJdbcWritable);
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

    @Test(groups = {"features", "gpdb"})
    public void singleFragmentTable() throws Exception {
        runTincTest("pxf.features.jdbc.single_fragment.runTest");
    }

    @Test(groups = {"features", "gpdb"})
    public void multipleFragmentsTables() throws Exception {
        runTincTest("pxf.features.jdbc.multiple_fragments.runTest");
    }

    @Test(groups = {"features", "gpdb"})
    public void jdbcWritableTable() throws Exception {
        runTincTest("pxf.features.jdbc.writable.runTest");
    }

    @Test(groups = {"features", "gpdb"})
    public void jdbcColumns() throws Exception {
        runTincTest("pxf.features.jdbc.columns.runTest");
    }
}
