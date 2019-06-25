package org.greenplum.pxf.automation.features.jdbc;

import jsystem.framework.system.SystemManagerImpl;
import org.greenplum.pxf.automation.components.hive.Hive;
import org.greenplum.pxf.automation.features.BaseFeature;
import org.greenplum.pxf.automation.structures.tables.hive.HiveTable;
import org.greenplum.pxf.automation.structures.tables.pxf.ExternalTable;
import org.greenplum.pxf.automation.structures.tables.utils.TableFactory;
import org.testng.annotations.Test;

public class JdbcHiveTest extends BaseFeature {

    private static final String HIVE_JDBC_DRIVER_CLASS = "org.apache.hive.jdbc.HiveDriver";
    private static final String HIVE_JDBC_URL_PREFIX = "jdbc:hive2://";

    private static final String GPDB_TYPES_TABLE_NAME = "pxf_jdbc_hive_types_table";
    private static final String GPDB_QUERY_TABLE_NAME = "pxf_jdbc_hive_types_server_table";
    private static final String HIVE_TYPES_TABLE_NAME = "jdbc_hive_types_table";
    private static final String HIVE_TYPES_FILE_NAME = "hive_types_no_binary.txt";

    private static final String[] GPDB_TYPES_TABLE_FIELDS = {
            "s1    TEXT",
            "s2    TEXT",
            "n1    INTEGER",
            "d1    DOUBLE PRECISION",
            "dc1   NUMERIC",
            "tm    TIMESTAMP",
            "f     REAL",
            "bg    BIGINT",
            "b     BOOLEAN",
            "tn    SMALLINT",
            "sml   SMALLINT",
            "dt    DATE",
            "vc1   VARCHAR(5)",
            "c1    CHAR(3)"
    };
    private static final String[] GPDB_QUERY_FIELDS = {
            "n1    INTEGER",
            "c     INTEGER",
            "s     INTEGER"
    };
    static final String[] HIVE_TYPES_TABLE_FIELDS = {
            "s1    STRING",
            "s2    STRING",
            "n1    INT",
            "d1    DOUBLE",
            "dc1   DECIMAL(38,18)",
            "tm    TIMESTAMP",
            "f     FLOAT",
            "bg    BIGINT",
            "b     BOOLEAN",
            "tn    TINYINT",
            "sml   SMALLINT",
            "dt    DATE",
            "vc1   VARCHAR(5)",
            "c1    CHAR(3)"
    };

    private Hive hive;
    private ExternalTable pxfJdbcHiveTypesTable, pxfJdbcHiveTypesServerTable;

    @Override
    public void beforeClass() throws Exception {
        // Initialize Hive system object
        hive = (Hive) SystemManagerImpl.getInstance().getSystemObject("hive");
        prepareData();
        createTables();
    }

    @Override
    public void afterClass() {
        // close hive connection
        if (hive != null)
            hive.close();
    }

    protected void prepareData() throws Exception {
        // Create Hive table
        HiveTable hiveTypesTable = TableFactory.getHiveByRowCommaTable(HIVE_TYPES_TABLE_NAME, HIVE_TYPES_TABLE_FIELDS);
        hive.dropTable(hiveTypesTable, false);
        hive.createTableAndVerify(hiveTypesTable);
        // copy file with types data to hdfs
        hdfs.copyFromLocal(localDataResourcesFolder + "/hive/" + HIVE_TYPES_FILE_NAME,hdfs.getWorkingDirectory() + "/" + HIVE_TYPES_FILE_NAME);
        // load to hive table
        hive.loadData(hiveTypesTable, hdfs.getWorkingDirectory() + "/" + HIVE_TYPES_FILE_NAME, false);
    }

    protected void createTables() throws Exception {
        String jdbcUrl = HIVE_JDBC_URL_PREFIX + hive.getHost() + ":10000/default";
        // Create GPDB external table pointing to Hive table using JDBC profile
        pxfJdbcHiveTypesTable = TableFactory.getPxfJdbcReadableTable(
                GPDB_TYPES_TABLE_NAME, GPDB_TYPES_TABLE_FIELDS, HIVE_TYPES_TABLE_NAME, HIVE_JDBC_DRIVER_CLASS, jdbcUrl, null);
        pxfJdbcHiveTypesTable.setHost(pxfHost);
        pxfJdbcHiveTypesTable.setPort(pxfPort);
        gpdb.createTableAndVerify(pxfJdbcHiveTypesTable);

        pxfJdbcHiveTypesServerTable = TableFactory.getPxfJdbcReadableTable(
                GPDB_QUERY_TABLE_NAME, GPDB_QUERY_FIELDS, "query:hive-report", "db-hive");
        pxfJdbcHiveTypesServerTable.setHost(pxfHost);
        pxfJdbcHiveTypesServerTable.setPort(pxfPort);
        gpdb.createTableAndVerify(pxfJdbcHiveTypesServerTable);
    }

    @Test(groups = {"features", "gpdb"})
    public void jdbcHiveRead() throws Exception {
        runTincTest("pxf.features.jdbc.hive.runTest");
    }
}
