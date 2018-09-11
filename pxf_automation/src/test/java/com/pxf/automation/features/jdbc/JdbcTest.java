package com.pxf.automation.features.jdbc;

import java.io.File;

import org.testng.annotations.Test;

import com.pxf.automation.enums.EnumPartitionType;

import com.pxf.automation.structures.tables.basic.Table;
import com.pxf.automation.structures.tables.pxf.ExternalTable;
import com.pxf.automation.structures.tables.utils.TableFactory;
import com.pxf.automation.features.BaseFeature;

public class JdbcTest extends BaseFeature {

    private static final String POSTGRES_DRIVER_CLASS = "org.postgresql.Driver";
    private static final String HAWQ_PXF_AUTOMATION_DB_JDBC = "jdbc:postgresql://";
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


    final String hawqTypesDataFileName = "hawq_types.txt";
    private Table hawqNativeTable, hawqWritableTargetTable;

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
        hawqNativeTable = new Table("hawq_types", TYPES_TABLE_FIELDS);
        hawqNativeTable.setDistributionFields(new String[] { "t1" });
        hawq.createTableAndVerify(hawqNativeTable);
        hawq.copyFromFile(hawqNativeTable, new File(localDataResourcesFolder
                + "/hawq/" + hawqTypesDataFileName), "E'\\t'", "E'\\\\N'", true);

        // create a table to be filled by the writable test case
        hawqWritableTargetTable = new Table("hawq_types_target", TYPES_TABLE_FIELDS);
        hawqWritableTargetTable.setDistributionFields(new String[] { "t1" });
        hawq.createTableAndVerify(hawqWritableTargetTable);

    }

    private void prepareSingleFragment() throws Exception {
        pxfJdbcSingleFragment = TableFactory.getPxfJdbcReadableTable(
                "pxf_jdbc_single_fragment",
                TYPES_TABLE_FIELDS,
                hawqNativeTable.getName(),
                POSTGRES_DRIVER_CLASS,
                HAWQ_PXF_AUTOMATION_DB_JDBC + hawq.getMasterHost() + ":" + hawq.getPort() + "/pxfautomation",
                hawq.getUserName());
        pxfJdbcSingleFragment.setHost(pxfHost);
        pxfJdbcSingleFragment.setPort(pxfPort);
        hawq.createTableAndVerify(pxfJdbcSingleFragment);
    }

    private void prepareMultipleFragmentsByEnum() throws Exception {
        pxfJdbcMultipleFragmentsByEnum = TableFactory
                .getPxfJdbcReadablePartitionedTable(
                "pxf_jdbc_multiple_fragments_by_enum",
                TYPES_TABLE_FIELDS,
                hawqNativeTable.getName(),
                POSTGRES_DRIVER_CLASS,
                HAWQ_PXF_AUTOMATION_DB_JDBC + hawq.getMasterHost() + ":" + hawq.getPort() + "/pxfautomation",
                13,
                "USD:UAH",
                "1",
                hawq.getUserName(),
                EnumPartitionType.ENUM);
        pxfJdbcSingleFragment.setHost(pxfHost);
        pxfJdbcSingleFragment.setPort(pxfPort);
        hawq.createTableAndVerify(pxfJdbcMultipleFragmentsByEnum);
    }

    private void prepareMultipleFragmentsByInt() throws Exception {
        pxfJdbcMultipleFragmentsByInt = TableFactory
                .getPxfJdbcReadablePartitionedTable(
                "pxf_jdbc_multiple_fragments_by_int",
                TYPES_TABLE_FIELDS,
                hawqNativeTable.getName(),
                POSTGRES_DRIVER_CLASS,
                HAWQ_PXF_AUTOMATION_DB_JDBC + hawq.getMasterHost() + ":" + hawq.getPort() + "/pxfautomation",
                2,
                "1:6",
                "1",
                hawq.getUserName(),
                EnumPartitionType.INT);
        pxfJdbcSingleFragment.setHost(pxfHost);
        pxfJdbcSingleFragment.setPort(pxfPort);
        hawq.createTableAndVerify(pxfJdbcMultipleFragmentsByInt);
    }

    private void prepareMultipleFragmentsByDate() throws Exception {
        pxfJdbcMultipleFragmentsByDate = TableFactory
                .getPxfJdbcReadablePartitionedTable(
                "pxf_jdbc_multiple_fragments_by_date",
                TYPES_TABLE_FIELDS,
                hawqNativeTable.getName(),
                POSTGRES_DRIVER_CLASS,
                HAWQ_PXF_AUTOMATION_DB_JDBC + hawq.getMasterHost() + ":" + hawq.getPort() + "/pxfautomation",
                11,
                "2015-03-06:2015-03-20",
                "1:DAY",
                hawq.getUserName(),
                EnumPartitionType.DATE);
        pxfJdbcSingleFragment.setHost(pxfHost);
        pxfJdbcSingleFragment.setPort(pxfPort);
        hawq.createTableAndVerify(pxfJdbcMultipleFragmentsByDate);
    }

    private void prepareWritable() throws Exception {
        pxfJdbcWritable = TableFactory.getPxfJdbcWritableTable(
                "pxf_jdbc_writable",
                TYPES_TABLE_FIELDS,
                hawqNativeTable.getName() + "_target",
                POSTGRES_DRIVER_CLASS,
                HAWQ_PXF_AUTOMATION_DB_JDBC + hawq.getMasterHost() + ":" + hawq.getPort() + "/pxfautomation",
                hawq.getUserName());
        pxfJdbcSingleFragment.setHost(pxfHost);
        pxfJdbcSingleFragment.setPort(pxfPort);
        hawq.createTableAndVerify(pxfJdbcWritable);
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
