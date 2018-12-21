package org.greenplum.pxf.automation.features.cloud;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.greenplum.pxf.automation.components.cluster.PhdCluster;
import org.greenplum.pxf.automation.components.hdfs.Hdfs;
import org.greenplum.pxf.automation.features.BaseFeature;
import org.greenplum.pxf.automation.structures.tables.basic.Table;
import org.greenplum.pxf.automation.structures.tables.utils.TableFactory;
import org.greenplum.pxf.automation.utils.system.ProtocolUtils;
import org.testng.annotations.Test;

import java.net.URI;

/**
 * Functional CloudAccess Test
 */
public class CloudAccessTest extends BaseFeature {

    private static final String PROTOCOL_S3 = "s3a://";

    private static final String[] PXF_MULTISERVER_COLS = {
            "name text",
            "num integer",
            "dub double precision",
            "longNum bigint",
            "bool boolean"
    };

    private Hdfs s3Server;
    private String s3Path;

    /**
     * Prepare all server configurations and components
     */
    @Override
    public void beforeClass() throws Exception {
        // Initialize server objects
        s3Path = hdfs.getWorkingDirectory() + "/" + fileName;
        Configuration s3Configuration = new Configuration();
        s3Configuration.set("fs.s3a.access.key", ProtocolUtils.getAccess());
        s3Configuration.set("fs.s3a.secret.key", ProtocolUtils.getSecret());

        FileSystem fs2 = FileSystem.get(URI.create(PROTOCOL_S3 + s3Path), s3Configuration);
        s3Server = new Hdfs(fs2, s3Configuration, true);
    }

    /**
     * Before every method determine default hdfs data Path, default data, and
     * default external table structure. Each case change it according to it
     * needs.
     *
     * @throws Exception
     */
    @Override
    protected void beforeMethod() throws Exception {
        super.beforeMethod();
        prepareData();
        cluster.restart(PhdCluster.EnumClusterServices.pxf);
    }

    @Override
    protected void afterMethod() throws Exception {
        super.afterMethod();
        s3Server.removeDirectory(PROTOCOL_S3 + s3Path);
    }

    protected void prepareData() throws Exception {
        // Prepare data in table
        Table dataTable = getSmallData();

        // Create Data for s3Server
        s3Server.writeTableToFile(PROTOCOL_S3 + s3Path, dataTable, ",");
    }

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

    private void runTestScenario(String name, String server, boolean creds) throws Exception {
        String tableName = "cloudaccess_" + name;
        exTable = TableFactory.getPxfReadableTextTable(tableName, PXF_MULTISERVER_COLS, s3Path, ",");
        exTable.setProfile(ProtocolUtils.getProtocol().value() + ":text");
        String serverParam = (server == null) ? null : "server=" + server;
        exTable.setServer(serverParam);
        if (creds) {
            exTable.setUserParameters(new String[]{"accesskey=" + ProtocolUtils.getAccess(), "secretkey=" + ProtocolUtils.getSecret()});
        }
        gpdb.createTableAndVerify(exTable);

        runTincTest("pxf.features.cloud_access." + name + ".runTest");
    }
}
