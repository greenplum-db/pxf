package org.greenplum.pxf.automation.features.jdbc;

import jsystem.framework.sut.SutFactory;
import jsystem.framework.system.SystemManagerImpl;
import jsystem.framework.system.SystemObject;
import org.apache.commons.lang.StringUtils;
import org.greenplum.pxf.automation.components.hdfs.Hdfs;
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

    private static final String GPDB_TYPES_TABLE_NAME_2 = "pxf_jdbc_hive_2_types_table";
    private static final String GPDB_QUERY_TABLE_NAME_2 = "pxf_jdbc_hive_2_types_server_table";

    private static final String GPDB_TYPES_TABLE_NAME_3 = "pxf_jdbc_hive_non_secure_types_table";
    private static final String GPDB_QUERY_TABLE_NAME_3 = "pxf_jdbc_hive_non_secure_types_server_table";

    private static final String HIVE_TYPES_TABLE_NAME = "jdbc_hive_types_table";
    private static final String HIVE_TYPES_FILE_NAME_1 = "hive_types_no_binary.txt";
    private static final String HIVE_TYPES_FILE_NAME_2 = "hive_types_no_binary_second.txt";
    private static final String HIVE_TYPES_FILE_NAME_3 = "hive_types_no_binary_third.txt";

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
    private Hive hive2;
    private Hive hiveNonSecure;
    private ExternalTable pxfJdbcHiveTypesTable, pxfJdbcHiveTypesServerTable;

    // and another kerberized hadoop environment
    private Hdfs hdfs2;

    @Override
    public void beforeClass() throws Exception {
        // Initialize Hive system object
        hive = (Hive) SystemManagerImpl.getInstance().getSystemObject("hive");
        prepareData(hive, hdfs, HIVE_TYPES_FILE_NAME_1);
        createTables(hive, "db-hive", GPDB_TYPES_TABLE_NAME, GPDB_QUERY_TABLE_NAME);
    }

    @Override
    public void afterClass() {
        // close hive connection
        if (hive != null)
            hive.close();

        if (hive2 != null)
            hive2.close();

        if (hdfs2 != null)
            removeWorkingDirectory(hdfs2);
    }

    protected void prepareData(Hive hive, Hdfs hdfs, String hiveTypesFileName) throws Exception {
        // Create Hive table
        HiveTable hiveTypesTable = TableFactory.getHiveByRowCommaTable(HIVE_TYPES_TABLE_NAME, HIVE_TYPES_TABLE_FIELDS);
        hive.dropTable(hiveTypesTable, false);
        hive.createTableAndVerify(hiveTypesTable);
        // copy file with types data to hdfs
        hdfs.copyFromLocal(localDataResourcesFolder + "/hive/" + hiveTypesFileName, hdfs.getWorkingDirectory() + "/" + hiveTypesFileName);
        // load to hive table
        hive.loadData(hiveTypesTable, hdfs.getWorkingDirectory() + "/" + hiveTypesFileName, false);
    }

    protected void createTables(Hive hive, String serverName, String gpdbTypesTableName, String gpdbQueryTableName) throws Exception {
        String jdbcUrl = HIVE_JDBC_URL_PREFIX + hive.getHost() + ":10000/default";
        String user = null;

        // On kerberized cluster, enabled then we need the hive/hiveserver2_hostname principal in the connection string.
        // Assuming here that somewhere upstream ugi has a valid login context
        if (!StringUtils.isEmpty(hive.getKerberosPrincipal())) {
            // When using Hive with Kerberos, JDBC properties must be defined in a server configuration
            pxfJdbcHiveTypesTable = TableFactory.getPxfJdbcReadableTable(
                    gpdbTypesTableName, GPDB_TYPES_TABLE_FIELDS, HIVE_TYPES_TABLE_NAME, serverName);
        } else {
            // Create GPDB external table pointing to Hive table using JDBC profile
            pxfJdbcHiveTypesTable = TableFactory.getPxfJdbcReadableTable(
                    gpdbTypesTableName, GPDB_TYPES_TABLE_FIELDS, HIVE_TYPES_TABLE_NAME, HIVE_JDBC_DRIVER_CLASS, jdbcUrl, user);
        }
        pxfJdbcHiveTypesTable.setHost(pxfHost);
        pxfJdbcHiveTypesTable.setPort(pxfPort);
        gpdb.createTableAndVerify(pxfJdbcHiveTypesTable);

        pxfJdbcHiveTypesServerTable = TableFactory.getPxfJdbcReadableTable(
                gpdbQueryTableName, GPDB_QUERY_FIELDS, "query:hive-report", serverName);
        pxfJdbcHiveTypesServerTable.setHost(pxfHost);
        pxfJdbcHiveTypesServerTable.setPort(pxfPort);
        gpdb.createTableAndVerify(pxfJdbcHiveTypesServerTable);
    }

    @Test(groups = {"features", "gpdb", "security"})
    public void jdbcHiveRead() throws Exception {
        runTincTest("pxf.features.jdbc.hive.runTest");
    }

    @Test(groups = {"features", "multiClusterSecurity"})
    public void jdbcHiveReadFromTwoSecuredServers() throws Exception {
        // Initialize an additional HDFS system object (optional system object)
        hdfs2 = (Hdfs) systemManager.
                getSystemObject("/sut", "hdfs2", -1, (SystemObject) null, false, (String) null, SutFactory.getInstance().getSutInstance());

        if (hdfs2 == null) return;

        trySecureLogin(hdfs2, hdfs2.getTestKerberosPrincipal());
        initializeWorkingDirectory(gpdb, hdfs2);

        hive2 = (Hive) SystemManagerImpl.getInstance().getSystemObject("hive2");
        prepareData(hive2, hdfs2, HIVE_TYPES_FILE_NAME_2);
        createTables(hive2, "db-hive-kerberos", GPDB_TYPES_TABLE_NAME_2, GPDB_QUERY_TABLE_NAME_2);

        runTincTest("pxf.features.jdbc.two_secured_hive.runTest");
    }

    @Test(groups = {"features", "multiClusterSecurity"})
    public void jdbcHiveReadFromSecureServerAndNonSecuredServer() throws Exception {
        if (hdfsNonSecure == null) return;

        hiveNonSecure = (Hive) SystemManagerImpl.getInstance().getSystemObject("hiveNonSecure");
        prepareData(hiveNonSecure, hdfsNonSecure, HIVE_TYPES_FILE_NAME_3);
        createTables(hiveNonSecure, "db-hive-non-secure", GPDB_TYPES_TABLE_NAME_3, GPDB_QUERY_TABLE_NAME_3);

        runTincTest("pxf.features.jdbc.secured_and_non_secured_hive.runTest");
    }
}
