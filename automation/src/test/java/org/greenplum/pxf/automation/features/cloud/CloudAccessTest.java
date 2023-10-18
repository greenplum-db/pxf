package org.greenplum.pxf.automation.features.cloud;

import annotations.WorksWithFDW;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.greenplum.pxf.automation.components.hdfs.Hdfs;
import org.greenplum.pxf.automation.features.BaseFeature;
import org.greenplum.pxf.automation.structures.tables.basic.Table;
import org.greenplum.pxf.automation.structures.tables.utils.TableFactory;
import org.greenplum.pxf.automation.utils.system.ProtocolUtils;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.UUID;

/**
 * Functional CloudAccess Test
 */
@WorksWithFDW
public class CloudAccessTest extends BaseFeature {

    private static final String PROTOCOL_S3 = "s3a://";

    private static final String[] PXF_MULTISERVER_COLS = {
            "name text",
            "num integer",
            "dub double precision",
            "longNum bigint",
            "bool boolean"
    };

    private static final String[] PXF_WRITE_COLS = {
            "name text",
            "score integer"
    };

    private Hdfs s3Server;
    private String s3PathRead, s3PathWrite;

    /**
     * Prepare all server configurations and components
     */
    @Override
    public void beforeClass() throws Exception {
        // Initialize server objects
        String random = UUID.randomUUID().toString();
        s3PathRead  = String.format("gpdb-ud-scratch/tmp/pxf_automation_data_read/%s/" , random);
        s3PathWrite = String.format("gpdb-ud-scratch/tmp/pxf_automation_data_write/%s/", random);

        Configuration s3Configuration = new Configuration();
        s3Configuration.set("fs.s3a.access.key", ProtocolUtils.getAccess());
        s3Configuration.set("fs.s3a.secret.key", ProtocolUtils.getSecret());

        FileSystem fs2 = FileSystem.get(URI.create(PROTOCOL_S3 + s3PathRead + fileName), s3Configuration);
        s3Server = new Hdfs(fs2, s3Configuration, true);
    }

    @Override
    protected void beforeMethod() throws Exception {
        super.beforeMethod();
        prepareData();
    }

    @Override
    protected void afterMethod() throws Exception {
        super.afterMethod();
        if (s3Server != null) {
            s3Server.removeDirectory(PROTOCOL_S3 + s3PathRead);
            s3Server.removeDirectory(PROTOCOL_S3 + s3PathWrite);
        }
    }

    protected void prepareData() throws Exception {
        // Prepare data in table
        Table dataTable = getSmallData();

        // Create Data for s3Server
        s3Server.writeTableToFile(PROTOCOL_S3 + s3PathRead + fileName, dataTable, ",");
        s3Server.createDirectory(PROTOCOL_S3 + s3PathWrite);
    }

    /*
     * The tests below are for the case where there's NO Hadoop cluster configured under "default" server
     * and assumes the "default" server has not configuration files. They are part of "s3" group and do not
     * make sense in the environment with Kerberized Hadoop, where the tests in the "security" group would run
     */

    @Test(groups = {"s3"})
    public void testCloudAccessFailsWhenNoServerNoCredsSpecified() throws Exception {
        runTestScenario("no_server_no_credentials", null, false);
    }

    @Test(groups = {"s3"})
    public void testCloudAccessFailsWhenServerNoCredsNoConfigFileExists() throws Exception {
        runTestScenario("server_no_credentials_no_config", "s3-non-existent", false);
    }

    @Test(groups = {"s3"})
    public void testCloudAccessOkWhenNoServerCredsNoConfigFileExists() throws Exception {
        runTestScenario("no_server_credentials_no_config", null, true);
    }

    @Test(groups = {"s3"})
    public void testCloudAccessFailsWhenServerNoCredsInvalidConfigFileExists() throws Exception {
        runTestScenario("server_no_credentials_invalid_config", "s3-invalid", false);
    }

    @Test(groups = {"s3"})
    public void testCloudAccessOkWhenServerCredsInvalidConfigFileExists() throws Exception {
        runTestScenario("server_credentials_invalid_config", "s3-invalid", true);
    }

    @Test(groups = {"s3"})
    public void testCloudAccessOkWhenServerCredsNoConfigFileExists() throws Exception {
        runTestScenario("server_credentials_no_config", "s3-non-existent", true);
    }

    /*
     * The tests below are for the case where there's a Hadoop cluster configured under "default" server
     * both without and with Kerberos security, testing that cloud access works in presence of "default" server
     */

    @Test(groups = {"gpdb", "security"})
    public void testCloudAccessWithHdfsFailsWhenNoServerNoCredsSpecified() throws Exception {
        runTestScenario("no_server_no_credentials_with_hdfs", null, false);
    }

    @Test(groups = {"gpdb", "security"})
    public void testCloudAccessWithHdfsOkWhenServerNoCredsValidConfigFileExists() throws Exception {
        runTestScenario("server_no_credentials_valid_config_with_hdfs", "s3", false);
    }

    @Test(groups = {"gpdb", "security"})
    public void testCloudWriteWithHdfsOkWhenServerNoCredsValidConfigFileExists() throws Exception {
        runTestScenarioForWrite("server_no_credentials_valid_config_with_hdfs_write", "s3", false);
    }

    @Test(groups = {"gpdb", "security"})
    public void testCloudAccessWithHdfsFailsWhenServerNoCredsNoConfigFileExists() throws Exception {
        runTestScenario("server_no_credentials_no_config_with_hdfs", "s3-non-existent", false);
    }

    @Test(groups = {"gpdb", "security"})
    public void testCloudAccessWithHdfsFailsWhenNoServerCredsNoConfigFileExists() throws Exception {
        runTestScenario("no_server_credentials_no_config_with_hdfs", null, true);
    }

    @Test(groups = {"gpdb", "security"})
    public void testCloudAccessWithHdfsFailsWhenServerNoCredsInvalidConfigFileExists() throws Exception {
        runTestScenario("server_no_credentials_invalid_config_with_hdfs", "s3-invalid", false);
    }

    @Test(groups = {"gpdb", "security"})
    public void testCloudAccessWithHdfsOkWhenServerCredsInvalidConfigFileExists() throws Exception {
        runTestScenario("server_credentials_invalid_config_with_hdfs", "s3-invalid", true);
    }

    private void runTestScenario(String name, String server, boolean creds) throws Exception {
        String tableName = "cloudaccess_" + name;
        exTable = TableFactory.getPxfReadableTextTable(tableName, PXF_MULTISERVER_COLS, s3PathRead + fileName, ",");
        exTable.setProfile("s3:text");
        String serverParam = (server == null) ? null : "server=" + server;
        exTable.setServer(serverParam);
        if (creds) {
            exTable.setUserParameters(new String[]{"accesskey=" + ProtocolUtils.getAccess(), "secretkey=" + ProtocolUtils.getSecret()});
        }
        gpdb.createTableAndVerify(exTable);

        runSqlTest("features/cloud_access/" + name);
    }

    private void runTestScenarioForWrite(String name, String server, boolean creds) throws Exception {
        // create writable external table to write to S3
        String tableName = "cloudwrite_" + name;
        exTable = TableFactory.getPxfWritableTextTable(tableName, PXF_WRITE_COLS, s3PathWrite, ",");
        exTable.setProfile("s3:text");
        String serverParam = (server == null) ? null : "server=" + server;
        exTable.setServer(serverParam);
        if (creds) {
            exTable.setUserParameters(new String[]{"accesskey=" + ProtocolUtils.getAccess(), "secretkey=" + ProtocolUtils.getSecret()});
        }
        gpdb.createTableAndVerify(exTable);

        // create readable external table to read back from S3, making sure previous insert made it all the way to S3
        tableName = "cloudaccess_" + name;
        exTable = TableFactory.getPxfReadableTextTable(tableName, PXF_WRITE_COLS, s3PathWrite, ",");
        exTable.setProfile("s3:text");
        exTable.setServer(serverParam);
        if (creds) {
            exTable.setUserParameters(new String[]{"accesskey=" + ProtocolUtils.getAccess(), "secretkey=" + ProtocolUtils.getSecret()});
        }
        gpdb.createTableAndVerify(exTable);

        runSqlTest("features/cloud_access/" + name);
    }
}
