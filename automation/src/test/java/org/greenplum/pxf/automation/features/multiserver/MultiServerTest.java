package org.greenplum.pxf.automation.features.multiserver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.greenplum.pxf.automation.components.hdfs.Hdfs;
import org.greenplum.pxf.automation.features.BaseFeature;
import org.greenplum.pxf.automation.structures.tables.basic.Table;
import org.greenplum.pxf.automation.structures.tables.pxf.ExternalTable;
import org.greenplum.pxf.automation.structures.tables.utils.TableFactory;
import org.testng.annotations.Test;

import java.net.URI;

/**
 * MultiServerTest verifies that multiple servers
 * can be accessed via PXF.
 */
public class MultiServerTest extends BaseFeature {

    static final String PROTOCOL_S3 = "s3a://";
    static final String PROTOCOL_AZURE = "adl://";

    static final String[] PXF_MULTISERVER_COLS = {
            "name text",
            "num integer",
            "dub double precision",
            "longNum bigint",
            "bool boolean"
    };

    Hdfs hdfsServer;
    Hdfs s3Server;

    ExternalTable hdfsTable;
    ExternalTable s3Table;

    String defaultPath;
    String hdfsPath;
    String s3Path;

    /**
     * Prepare all server configurations and components
     */
    @Override
    public void beforeClass() throws Exception {
        String hdfsWorkingDirectory = hdfs.getWorkingDirectory();
        defaultPath = hdfsWorkingDirectory + "/" + fileName;

        // Initialize server objects
        Configuration config1 = new Configuration();
        hdfsPath = hdfsWorkingDirectory + "/hdfsserver/" + fileName;
        config1.addResource(new Path("/home/gpadmin/pxf/servers/hdfsserver/core-site.xml"));
        FileSystem fs1 = FileSystem.get(URI.create(hdfsPath), config1);
        hdfsServer = new Hdfs(fs1, config1, true);

        String s3Bucket = "gpdb-ud-scratch";
        s3Path = s3Bucket + "/s3Server/" + fileName;
        Configuration config2 = new Configuration();
        config2.addResource(new Path("/home/gpadmin/pxf/servers/s3server/core-site.xml"));
        FileSystem fs2 = FileSystem.get(URI.create(PROTOCOL_S3 + s3Path), config2);
        s3Server = new Hdfs(fs2, config2, true);
    }

    @Override
    protected void afterClass() throws Exception {
        super.afterClass();
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

    protected void prepareData() throws Exception {
        // Prepare data in table
        Table dataTable = getSmallData();

        hdfs.writeTableToFile(defaultPath, dataTable, ",");

        // Create Data for hdfsServer
        hdfsServer.writeTableToFile(hdfsPath, dataTable, ",");

        // Create Data for s3Server
        s3Server.writeTableToFile(s3Path, dataTable, ",");
    }

    protected void createTables() throws Exception {
        // Create GPDB external table directed to the HDFS file
        exTable =
                TableFactory.getPxfReadableTextTable(
                        "pxf_multiserver_default", PXF_MULTISERVER_COLS, defaultPath, ",");
        gpdb.createTableAndVerify(exTable);

        // Create GPDB external table directed to hdfsServer
        hdfsTable =
                TableFactory.getPxfReadableTextTable(
                        "pxf_multiserver_1", PXF_MULTISERVER_COLS, hdfsPath, ",");
        hdfsTable.setUserParameters(new String[]{"server=1"});
        gpdb.createTableAndVerify(hdfsTable);

        // Create GPDB external table directed to s3Server
        s3Table =
                TableFactory.getPxfReadableTextTable(
                        "pxf_multiserver_2", PXF_MULTISERVER_COLS, s3Path, ",");
        s3Table.setUserParameters(new String[]{"server=2"});
        s3Table.setProfile("S3Text");
        gpdb.createTableAndVerify(s3Table);
    }

    @Test(groups = {"features", "gpdb"})
    public void testTwoServers() throws Exception {
        runTincTest("pxf.features.multi_server.runTest");
    }

}
