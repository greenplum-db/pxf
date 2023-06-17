package org.greenplum.pxf.automation.features.jdbc;

import annotations.FailsWithFDW;
import annotations.WorksWithFDW;
import jsystem.framework.sut.SutFactory;
import jsystem.framework.system.SystemManagerImpl;
import jsystem.framework.system.SystemObject;
import org.apache.commons.lang.StringUtils;
import org.greenplum.pxf.automation.components.hdfs.Hdfs;
import org.greenplum.pxf.automation.components.hive.Hive;
import org.greenplum.pxf.automation.features.BaseFeature;
import org.greenplum.pxf.automation.structures.tables.basic.Table;
import org.greenplum.pxf.automation.structures.tables.hive.HiveTable;
import org.greenplum.pxf.automation.structures.tables.pxf.ExternalTable;
import org.greenplum.pxf.automation.structures.tables.utils.TableFactory;
import org.testng.annotations.Test;

import java.io.File;

@WorksWithFDW
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

    private static final String HIVE_WRITE_TYPES_TABLE_NAME = "hive_pxf_jdbc_target";
    private static final String GPDB_TABLE_HIVE_WRITE_SUPPORTED_TYPES_NAME = "jdbc_write_hive_supported_types";

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

    /*
     * GPDB columns when writing from GPDB to Hive with the JDBC profile
     */
    private static final String[] GPDB_WRITE_TYPES_TABLE_FIELDS = new String[] {
            "t1    text",
            "t2    text",
            "num1  int",
            "dub1  double precision",
            // Hive JDBC driver 1.1.0 does not support BigDecimal
            // https://issues.apache.org/jira/browse/HIVE-13614
            // fixed in 2.3.0
            // "dec1   numeric",
            // Hive JDBC driver does not quote value as required
            // https://issues.apache.org/jira/browse/HIVE-11748
            // fixed in 2.0.0
            // "tm    timestamp",
            "r     real",
            "bg    bigint",
            "b     boolean",
            "tn    smallint",
            "sml   smallint",
            // Hive JDBC driver does not quote value as required
            // https://issues.apache.org/jira/browse/HIVE-11024
            // fixed in 1.3.0, 2.0.0
            // "dt    date",
            "vc1   varchar(5)",
            "c1    char(3)",
            // Hive JDBC driver does not support setBytes()
            // https://github.com/apache/hive/blob/dc8891ec9459d2eff5a23154383ec3bd19481fd2/jdbc/src/java/org/apache/hive/jdbc/HivePreparedStatement.java#L251-L254
            // not yet fixed
            //"bin   bytea"
    };

    private static final String[] HIVE_WRITE_TYPES_TABLE_FIELDS = new String[] {
            "t1    string",
            "t2    string",
            "num1  int",
            "dub1  double",
            "r     float",
            "bg    bigint",
            "b     boolean",
            "tn    tinyint",
            "sml   smallint",
            "vc1   varchar(5)",
            "c1    char(3)"
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

    protected void prepareDataForWriteTest() throws Exception {
        // create GPDB table with data for inserting into writable external table
        Table gpdbDataTable = new Table(GPDB_TABLE_HIVE_WRITE_SUPPORTED_TYPES_NAME, GPDB_WRITE_TYPES_TABLE_FIELDS);
        gpdbDataTable.setDistributionFields(new String[]{"t1"});
        gpdb.createTableAndVerify(gpdbDataTable);
        gpdb.copyFromFile(gpdbDataTable, new File(localDataResourcesFolder + "/gpdb/jdbc_write_hive_supported_types.txt"), "E'\\t'", "E'\\\\N'", true);
    }

    protected void createTablesForWriteTest(Hive hive, String hiverServerName, String serverName) throws Exception {
        // create Hive table to write to via JDBC profile
        HiveTable targetHiveTable = TableFactory.getHiveByRowCommaTable(HIVE_WRITE_TYPES_TABLE_NAME, HIVE_WRITE_TYPES_TABLE_FIELDS);
        hive.createTableAndVerify(targetHiveTable);

        String hiveWritableName = String.format("pxf_jdbc_%s_writable", hiverServerName);
        String hiveReadableName = String.format("pxf_jdbc_%s_readable", hiverServerName);

        ExternalTable hiveWritable;
        ExternalTable hiveReadable;

        // On kerberized cluster, enabled then we need the hive/hiveserver2_hostname principal in the connection string.
        // Assuming here that somewhere upstream ugi has a valid login context
        if (!StringUtils.isEmpty(hive.getKerberosPrincipal())) {
            // When using Hive with Kerberos, JDBC properties must be defined in a server configuration
            hiveWritable = TableFactory.getPxfJdbcWritableTable(
                    hiveWritableName, GPDB_WRITE_TYPES_TABLE_FIELDS, targetHiveTable.getFullName(), serverName);
            hiveReadable = TableFactory.getPxfJdbcReadableTable(
                    hiveReadableName, GPDB_WRITE_TYPES_TABLE_FIELDS, targetHiveTable.getFullName(), serverName);
        } else {
            String jdbcUrl = String.format("%s%s:10000/default", HIVE_JDBC_URL_PREFIX, hive.getHost());
            // create GPDB external table for writing data from GPDB to Hive with JDBC profile
            hiveWritable = TableFactory.getPxfJdbcWritableTable(
                    hiveWritableName, GPDB_WRITE_TYPES_TABLE_FIELDS, targetHiveTable.getFullName(),
                    HIVE_JDBC_DRIVER_CLASS, jdbcUrl, null, null);
            // create GPDB external table to read data back from Hive
            hiveReadable = TableFactory.getPxfJdbcReadableTable(
                    hiveReadableName, GPDB_WRITE_TYPES_TABLE_FIELDS, targetHiveTable.getFullName(),
                    HIVE_JDBC_DRIVER_CLASS, jdbcUrl, null);
        }

        hiveWritable.setHost(pxfHost);
        hiveWritable.setPort(pxfPort);
        gpdb.createTableAndVerify(hiveWritable);

        hiveReadable.setHost(pxfHost);
        hiveReadable.setPort(pxfPort);
        gpdb.createTableAndVerify(hiveReadable);
    }

    @Test(groups = {"features", "gpdb", "security"})
    public void jdbcHiveRead() throws Exception {
        runTincTest("pxf.features.jdbc.hive.runTest");
    }

    // Fails with the error: ERROR:  PXF server error : java.io.DataInputStream cannot be cast to [B
    @FailsWithFDW
    @Test(groups = {"features", "gpdb", "security"})
    public void jdbcHiveWrite() throws Exception {
        prepareDataForWriteTest();
        createTablesForWriteTest(hive, "hive", "db-hive");
        runTincTest("pxf.features.jdbc.hive_writable.runTest");
    }

    @Test(groups = {"features", "multiClusterSecurity"})
    public void jdbcHiveReadFromTwoSecuredServers() throws Exception {
        // Initialize an additional HDFS system object (optional system object)
        hdfs2 = (Hdfs) systemManager.
                getSystemObject("/sut", "hdfs2", -1, (SystemObject) null, false, (String) null, SutFactory.getInstance().getSutInstance());

        if (hdfs2 == null) return;

        trySecureLogin(hdfs2, hdfs2.getTestKerberosPrincipal());
        initializeWorkingDirectory(hdfs2, gpdb.getUserName());

        hive2 = (Hive) SystemManagerImpl.getInstance().getSystemObject("hive2");
        prepareData(hive2, hdfs2, HIVE_TYPES_FILE_NAME_2);
        createTables(hive2, "db-hive-kerberos", GPDB_TYPES_TABLE_NAME_2, GPDB_QUERY_TABLE_NAME_2);

        runTincTest("pxf.features.jdbc.two_secured_hive.runTest");
    }

    @FailsWithFDW
    @Test(groups = {"features", "multiClusterSecurity"})
    public void jdbcHiveWriteToTwoSecuredServers() throws Exception {
        prepareDataForWriteTest();
        createTablesForWriteTest(hive, "hive", "db-hive");
        hdfs2 = (Hdfs) systemManager.getSystemObject("/sut", "hdfs2", -1, null, false, null, SutFactory.getInstance().getSutInstance());

        if (hdfs2 == null) return;

        trySecureLogin(hdfs2, hdfs2.getTestKerberosPrincipal());
        initializeWorkingDirectory(hdfs2, gpdb.getUserName());

        hive2 = (Hive) SystemManagerImpl.getInstance().getSystemObject("hive2");

        createTablesForWriteTest(hive2, "hive2", "db-hive-kerberos");

        runTincTest("pxf.features.jdbc.write_two_secured_hive.runTest");
    }

    @Test(groups = {"features", "multiClusterSecurity"})
    public void jdbcHiveReadFromSecureServerAndNonSecuredServer() throws Exception {
        if (hdfsNonSecure == null) return;

        hiveNonSecure = (Hive) SystemManagerImpl.getInstance().getSystemObject("hiveNonSecure");
        prepareData(hiveNonSecure, hdfsNonSecure, HIVE_TYPES_FILE_NAME_3);
        createTables(hiveNonSecure, "db-hive-non-secure", GPDB_TYPES_TABLE_NAME_3, GPDB_QUERY_TABLE_NAME_3);

        runTincTest("pxf.features.jdbc.secured_and_non_secured_hive.runTest");
    }

    @FailsWithFDW
    @Test(groups = {"features", "multiClusterSecurity"})
    public void jdbcHiveWriteToSecureServerAndNonSecuredServer() throws Exception {
        if (hdfsNonSecure == null) return;

        hiveNonSecure = (Hive) SystemManagerImpl.getInstance().getSystemObject("hiveNonSecure");
        prepareDataForWriteTest();
        createTablesForWriteTest(hive, "hive", "db-hive");
        createTablesForWriteTest(hiveNonSecure, "hive_non_secure", "db-hive-non-secure");

        runTincTest("pxf.features.jdbc.write_secured_and_non_secured_hive.runTest");
    }
}
