package org.greenplum.pxf.automation.features.multiserver;

import jsystem.framework.sut.SutFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.greenplum.pxf.automation.components.hdfs.Hdfs;
import org.greenplum.pxf.automation.features.BaseFeature;
import org.greenplum.pxf.automation.structures.tables.basic.Table;
import org.greenplum.pxf.automation.structures.tables.pxf.ExternalTable;
import org.greenplum.pxf.automation.structures.tables.utils.TableFactory;
import org.greenplum.pxf.automation.utils.system.ProtocolUtils;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.UUID;

/**
 * MultiServerTest verifies that multiple servers
 * can be accessed via PXF.
 */
public class MultiServerTest extends BaseFeature {

    private static final String PROTOCOL_S3 = "s3a://";

    private static final String[] PXF_MULTISERVER_COLS = {
            "name text",
            "num integer",
            "dub double precision",
            "longNum bigint",
            "bool boolean"
    };
    private static final String SERVER_NON_SECURE_HDFS = "hdfs-non-secure";
    private static final String SERVER_SECURE_HDFS_2 = "hdfs-secure";
    private static final String SERVER_SECURE_HDFS_IPA = "hdfs-ipa";
    private Hdfs s3Server;
    private String s3Path;
    private String defaultPath;

    // and another kerberized hadoop environment
    private Hdfs hdfs2;

    // Hadoop cluster with IPA-based Kerberos KDC, used for testing constrained delegation
    private Hdfs hdfsIpa;

    /**
     * Prepare all server configurations and components
     */
    @Override
    public void beforeClass() throws Exception {
        // Initialize an additional HDFS system object (optional system object)
        hdfs2 = (Hdfs) systemManager.
                getSystemObject("/sut", "hdfs2", -1, null, false, null, SutFactory.getInstance().getSutInstance());

        if (hdfs2 != null) {
            trySecureLogin(hdfs2, hdfs2.getTestKerberosPrincipal());
            initializeWorkingDirectory(hdfs2, gpdb.getUserName());
        }

        hdfsIpa = (Hdfs) systemManager.
                getSystemObject("/sut", "hdfsIpa", -1, null, false, null, SutFactory.getInstance().getSutInstance());

        if (hdfsIpa != null) {
            trySecureLogin(hdfsIpa, hdfsIpa.getTestKerberosPrincipal());
            initializeWorkingDirectory(hdfsIpa, gpdb.getUserName());
        }

        String hdfsWorkingDirectory = hdfs.getWorkingDirectory();
        defaultPath = hdfsWorkingDirectory + "/" + fileName;

        // Initialize server objects
        s3Path = String.format("gpdb-ud-scratch/tmp/pxf_automation_data/%s/", UUID.randomUUID().toString());
        Configuration s3Configuration = new Configuration();
        s3Configuration.set("fs.s3a.access.key", ProtocolUtils.getAccess());
        s3Configuration.set("fs.s3a.secret.key", ProtocolUtils.getSecret());

        FileSystem fs2 = FileSystem.get(URI.create(PROTOCOL_S3 + s3Path + fileName), s3Configuration);
        s3Server = new Hdfs(fs2, s3Configuration, true);
    }

    /**
     * Before every method determine default hdfs data Path, default data, and
     * default external table structure. Each case change it according to it
     * needs.
     */
    @Override
    protected void beforeMethod() throws Exception {
        super.beforeMethod();
        prepareData();
        createTables();
    }

    @Override
    protected void afterMethod() throws Exception {
        super.afterMethod();
        if (s3Server != null) {
            s3Server.removeDirectory(PROTOCOL_S3 + s3Path);
        }
    }

    @Override
    protected void afterClass() throws Exception {
        super.afterClass();
        removeWorkingDirectory(hdfs2);
        removeWorkingDirectory(hdfsIpa);
    }

    protected void prepareData() throws Exception {
        // Prepare data in table
        Table dataTable = getSmallData("hdfs_data");

        // Prepare data for S3 table
        Table s3DataTable = getSmallData("data_on_s3");

        // default w/core-site.xml
        hdfs.writeTableToFile(defaultPath, dataTable, ",");

        // hdfs-non-secure
        if (hdfsNonSecure != null) {
            // Prepare data for non-secure HDFS table
            Table dataTableNonSecure = getSmallData("non_secure_hdfs");
            hdfsNonSecure.writeTableToFile(defaultPath, dataTableNonSecure, ",");
        }

        // second hdfs sever
        if (hdfs2 != null) {
            // Prepare data for second HDFS table
            Table dataTableHdfs2 = getSmallData("second_hdfs");
            hdfs2.writeTableToFile(defaultPath, dataTableHdfs2, ",");
        }

        // IPA Hadoop cluster
        if (hdfsIpa != null) {
            Table dataTableHdfsIpa = getSmallData("ipa_hdfs");
            hdfsIpa.writeTableToFile(defaultPath, dataTableHdfsIpa, ",");
        }

        // Create Data for s3Server
        s3Server.writeTableToFile(PROTOCOL_S3 + s3Path + fileName, s3DataTable, ",");
    }

    protected void createTables() throws Exception {
        // Create GPDB external table directed to the HDFS file
        exTable =
                TableFactory.getPxfReadableTextTable(
                        "pxf_multiserver_default", PXF_MULTISERVER_COLS, defaultPath, ",");
        gpdb.createTableAndVerify(exTable);

        // Create GPDB external table directed to s3Server
        ExternalTable s3Table = TableFactory.getPxfReadableTextTable(
                "pxf_multiserver_s3", PXF_MULTISERVER_COLS, s3Path + fileName, ",");
        s3Table.setServer("server=s3");
        s3Table.setUserParameters(new String[]{"accesskey=" + ProtocolUtils.getAccess(), "secretkey=" + ProtocolUtils.getSecret()});
        s3Table.setProfile("s3:text");
        gpdb.createTableAndVerify(s3Table);

        // hdfs-non-secure
        if (hdfsNonSecure != null) {
            exTable =
                    TableFactory.getPxfReadableTextTable(
                            "pxf_multiserver_non_secure", PXF_MULTISERVER_COLS, defaultPath, ",");
            exTable.setServer("SERVER=" + SERVER_NON_SECURE_HDFS);
            gpdb.createTableAndVerify(exTable);
        }

        // second hdfs sever
        if (hdfs2 != null) {
            exTable =
                    TableFactory.getPxfReadableTextTable(
                            "pxf_multiserver_secure_2", PXF_MULTISERVER_COLS, defaultPath, ",");
            exTable.setServer("SERVER=" + SERVER_SECURE_HDFS_2);
            gpdb.createTableAndVerify(exTable);
        }

        // IPA Hadoop cluster
        if (hdfsIpa != null) {
            exTable = TableFactory.getPxfReadableTextTable("pxf_multiserver_ipa", PXF_MULTISERVER_COLS, defaultPath, ",");
            exTable.setServer("SERVER=" + SERVER_SECURE_HDFS_IPA);
            gpdb.createTableAndVerify(exTable);
        }
    }

    @Test(groups = {"features", "gpdb", "security"})
    public void testHdfsAndCloudServers() throws Exception {
        runSqlTest("features/multi_server/hdfs_and_cloud");
    }

    @Test(groups = {"features", "multiClusterSecurity"})
    public void testTwoSecuredServers() throws Exception {
        runSqlTest("features/multi_server/two_secure_hdfs");
    }

    @Test(groups = {"features", "multiClusterSecurity"})
    public void testSecureServerAndNonSecuredServer() throws Exception {
        runSqlTest("features/multi_server/secure_hdfs_and_non_secure_hdfs");
    }

    @Test(groups = {"features", "multiClusterSecurity"})
    public void testTwoSecuredServersNonSecureServerAndCloudServer() throws Exception {
        if (hdfsIpa != null) {
            // in an environment with an IPA hadoop cluster run the test that also queries that cluster
            runSqlTest("features/multi_server/test_all_ipa");
        } else {
            // in an environment without an IPA hadoop cluster run the test that does not include queries to IPA cluster
            runSqlTest("features/multi_server/test_all");
        }
    }
}
