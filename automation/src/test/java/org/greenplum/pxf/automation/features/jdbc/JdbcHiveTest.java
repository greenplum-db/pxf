package org.greenplum.pxf.automation.features.jdbc;

import jsystem.framework.system.SystemManagerImpl;
import org.greenplum.pxf.automation.components.hive.Hive;
import org.greenplum.pxf.automation.features.BaseFeature;
import org.greenplum.pxf.automation.structures.tables.basic.Table;
import org.greenplum.pxf.automation.structures.tables.hive.HiveTable;
import org.greenplum.pxf.automation.structures.tables.pxf.ExternalTable;
import org.greenplum.pxf.automation.structures.tables.utils.TableFactory;
import org.testng.annotations.Test;

public class JdbcHiveTest extends BaseFeature {

    private static final String HIVE_JDBC_DRIVER_CLASS = "org.apache.hive.jdbc.HiveDriver";
    private static final String HIVE_JDBC_URL_PREFIX = "jdbc:hive2://";
    private static final String[] HIVE_SMALL_DATA_TABLE_FIELDS = new String[]{
            "s1 string",
            "n1 int",
            "d1 double",
            "bg bigint",
            "b boolean"};

    private static final String[] GPDB_SMALL_DATA_TABLE_FIELDS = new String[]{
            "s1 text",
            "n1 int",
            "d1 float",
            "bg bigint",
            "b bool"};

    private static final String FILENAME = "jdbcHiveSmallData.txt";

    private Hive hive;
    private ExternalTable pxfJdbcHiveTable;

    @Override
    public void beforeClass() throws Exception {
        // Initialize Hive system object
        hive = (Hive) SystemManagerImpl.getInstance().getSystemObject("hive");
        prepareData();
        createTables();
    }

    @Override
    public void afterClass() throws Exception {
        // close hive connection
        if (hive != null)
            hive.close();
    }

    protected void prepareData() throws Exception {
        // Create Hive table
        HiveTable hiveTable = TableFactory.getHiveByRowCommaTable("jdbc_hive_table", HIVE_SMALL_DATA_TABLE_FIELDS);
        // hive.dropTable(hiveTable, false);
        hive.createTableAndVerify(hiveTable);
        // Generate Small data, write to HDFS and load to Hive
        Table dataTable = getSmallData();
        hdfs.writeTableToFile((hdfs.getWorkingDirectory() + "/" + FILENAME), dataTable, ",");

        // load data from HDFS file
        hive.loadData(hiveTable, (hdfs.getWorkingDirectory() + "/" + FILENAME), false);
    }

    protected void createTables() throws Exception {
        // Create GPDB external table
        String jdbcUrl = HIVE_JDBC_URL_PREFIX + hive.getHost() + ":10000/default";
        pxfJdbcHiveTable = TableFactory.getPxfJdbcReadableTable(
                "pxf_jdbc_hive_small_data", GPDB_SMALL_DATA_TABLE_FIELDS, "jdbc_hive_table", HIVE_JDBC_DRIVER_CLASS, jdbcUrl, null);
        pxfJdbcHiveTable.setHost(pxfHost);
        pxfJdbcHiveTable.setPort(pxfPort);
        gpdb.createTableAndVerify(pxfJdbcHiveTable);
    }

    @Test(groups = {"features", "gpdb"})
    public void jdbcHiveRead() throws Exception {
        runTincTest("pxf.features.jdbc.hive.runTest");
    }
}
