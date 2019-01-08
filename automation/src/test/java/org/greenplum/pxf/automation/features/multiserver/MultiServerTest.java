package org.greenplum.pxf.automation.features.multiserver;

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
    private Hdfs s3Server;
    private ExternalTable s3Table;
    private String s3Path;
    private String defaultPath;

    /**
     * Prepare all server configurations and components
     */
    @Override
    public void beforeClass() throws Exception {
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
     *
     * @throws Exception
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

        s3Server.removeDirectory(PROTOCOL_S3 + s3Path);
    }

    protected void prepareData() throws Exception {
        // Prepare data in table
        Table dataTable = getSmallData();

        // default w/core-site.xml
        hdfs.writeTableToFile(defaultPath, dataTable, ",");

        // Create Data for s3Server
        s3Server.writeTableToFile(PROTOCOL_S3 + s3Path + fileName, dataTable, ",");
    }

    protected void createTables() throws Exception {
        // Create GPDB external table directed to the HDFS file
        exTable =
                TableFactory.getPxfReadableTextTable(
                        "pxf_multiserver_default", PXF_MULTISERVER_COLS, defaultPath, ",");
        gpdb.createTableAndVerify(exTable);

        // Create GPDB external table directed to s3Server
        s3Table =
                TableFactory.getPxfReadableTextTable(
                        "pxf_multiserver_s3", PXF_MULTISERVER_COLS, s3Path + fileName, ",");
        s3Table.setServer("server=s3");
        s3Table.setUserParameters(new String[]{"accesskey=" + ProtocolUtils.getAccess(), "secretkey=" + ProtocolUtils.getSecret()});
        s3Table.setProfile("s3:text");
        gpdb.createTableAndVerify(s3Table);
    }

    @Test(groups = {"features", "gpdb"})
    public void testTwoServers() throws Exception {
        runTincTest("pxf.features.multi_server.runTest");
    }

}
