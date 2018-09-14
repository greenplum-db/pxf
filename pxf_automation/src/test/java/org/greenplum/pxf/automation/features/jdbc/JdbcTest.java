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
    private ExternalTable pxfJdbcSingleFragment;
    private ExternalTable pxfJdbcMultipleFragmentsByInt;
    private ExternalTable pxfJdbcMultipleFragmentsByDate;
    private ExternalTable pxfJdbcMultipleFragmentsByEnum;
    private ExternalTable pxfJdbcWritable;


    final String gpdbTypesDataFileName = "gpdb_types.txt";
    private Table gpdbNativeTable, gpdbWritableTargetTable;

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
    }

    private void prepareTypesData() throws Exception {
        gpdbNativeTable = new Table("gpdb_types", TYPES_TABLE_FIELDS);
        gpdbNativeTable.setDistributionFields(new String[] { "t1" });
        gpdb.createTableAndVerify(gpdbNativeTable);
        gpdb.copyFromFile(gpdbNativeTable, new File(localDataResourcesFolder
                + "/gpdb/" + gpdbTypesDataFileName), "E'\\t'", "E'\\\\N'", true);

        // create a table to be filled by the writable test case
        gpdbWritableTargetTable = new Table("gpdb_types_target", TYPES_TABLE_FIELDS);
        gpdbWritableTargetTable.setDistributionFields(new String[] { "t1" });
        gpdb.createTableAndVerify(gpdbWritableTargetTable);

    }

    private void prepareSingleFragment() throws Exception {
        pxfJdbcSingleFragment = TableFactory.getPxfJdbcReadableTable(
                "pxf_jdbc_single_fragment",
                TYPES_TABLE_FIELDS,
                gpdbNativeTable.getName(),
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
                gpdbNativeTable.getName(),
                POSTGRES_DRIVER_CLASS,
                GPDB_PXF_AUTOMATION_DB_JDBC + gpdb.getMasterHost() + ":" + gpdb.getPort() + "/pxfautomation",
                13,
                "USD:UAH",
                "1",
                gpdb.getUserName(),
                EnumPartitionType.ENUM);
        pxfJdbcSingleFragment.setHost(pxfHost);
        pxfJdbcSingleFragment.setPort(pxfPort);
        gpdb.createTableAndVerify(pxfJdbcMultipleFragmentsByEnum);
    }

    private void prepareMultipleFragmentsByInt() throws Exception {
        pxfJdbcMultipleFragmentsByInt = TableFactory
                .getPxfJdbcReadablePartitionedTable(
                "pxf_jdbc_multiple_fragments_by_int",
                TYPES_TABLE_FIELDS,
                gpdbNativeTable.getName(),
                POSTGRES_DRIVER_CLASS,
                GPDB_PXF_AUTOMATION_DB_JDBC + gpdb.getMasterHost() + ":" + gpdb.getPort() + "/pxfautomation",
                2,
                "1:6",
                "1",
                gpdb.getUserName(),
                EnumPartitionType.INT);
        pxfJdbcSingleFragment.setHost(pxfHost);
        pxfJdbcSingleFragment.setPort(pxfPort);
        gpdb.createTableAndVerify(pxfJdbcMultipleFragmentsByInt);
    }

    private void prepareMultipleFragmentsByDate() throws Exception {
        pxfJdbcMultipleFragmentsByDate = TableFactory
                .getPxfJdbcReadablePartitionedTable(
                "pxf_jdbc_multiple_fragments_by_date",
                TYPES_TABLE_FIELDS,
                gpdbNativeTable.getName(),
                POSTGRES_DRIVER_CLASS,
                GPDB_PXF_AUTOMATION_DB_JDBC + gpdb.getMasterHost() + ":" + gpdb.getPort() + "/pxfautomation",
                11,
                "2015-03-06:2015-03-20",
                "1:DAY",
                gpdb.getUserName(),
                EnumPartitionType.DATE);
        pxfJdbcSingleFragment.setHost(pxfHost);
        pxfJdbcSingleFragment.setPort(pxfPort);
        gpdb.createTableAndVerify(pxfJdbcMultipleFragmentsByDate);
    }

    private void prepareWritable() throws Exception {
        pxfJdbcWritable = TableFactory.getPxfJdbcWritableTable(
                "pxf_jdbc_writable",
                TYPES_TABLE_FIELDS,
                gpdbNativeTable.getName() + "_target",
                POSTGRES_DRIVER_CLASS,
                GPDB_PXF_AUTOMATION_DB_JDBC + gpdb.getMasterHost() + ":" + gpdb.getPort() + "/pxfautomation",
                gpdb.getUserName());
        pxfJdbcSingleFragment.setHost(pxfHost);
        pxfJdbcSingleFragment.setPort(pxfPort);
        gpdb.createTableAndVerify(pxfJdbcWritable);
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
}
