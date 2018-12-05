package org.greenplum.pxf.automation.smoke;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.greenplum.pxf.automation.components.hdfs.Hdfs;
import org.greenplum.pxf.automation.structures.tables.basic.Table;
import org.greenplum.pxf.automation.structures.tables.pxf.ExternalTable;
import org.greenplum.pxf.automation.structures.tables.utils.TableFactory;
import org.testng.annotations.Test;

import java.net.URI;

/**
 * MultiServerTest verifies that multiple servers
 * can be accessed via PXF.
 */
public class MultiServerTest extends BaseSmoke {

    private static final String PROTOCOL_S3 = "s3a://";
    private static final String PROTOCOL_AZURE = "adl://";

    Hdfs hdfsServer;
    Hdfs s3Server;

    ExternalTable hdfsTable;
    ExternalTable s3Table;

    String defaultPath;
    String hdfsPath;
    String s3Path;

    @Override
    public void beforeClass() throws Exception {

        String hdfsWorkingDirectory = hdfs.getWorkingDirectory();
        defaultPath = hdfs.getWorkingDirectory() + "/" + fileName;

        // Initialize server objects
        Configuration config1 = new Configuration();
        hdfsPath = hdfsWorkingDirectory + "/hdfsserver/" + fileName;
        config1.addResource(new Path("/home/gpadmin/pxf/servers/1/core-site.xml"));
        FileSystem fs1 = FileSystem.get(URI.create(hdfsPath), config1);
        hdfsServer = new Hdfs(fs1, config1, true);

        String s3Bucket = "gpdb-ud-scratch";
        s3Path = s3Bucket + "/s3Server/" + fileName;
        Configuration config2 = new Configuration();
        config2.addResource(new Path("/home/gpadmin/pxf/servers/2/core-site.xml"));
        FileSystem fs2 = FileSystem.get(URI.create(PROTOCOL_S3 + s3Path), config2);
        s3Server = new Hdfs(fs2, config2, true);
    }

    @Override
    protected void prepareData() throws Exception {
        // Create Data and write it to HDFS
        Table dataTable = getSmallData();

        hdfs.writeTableToFile(defaultPath, dataTable, ",");

        // Create Data for hdfsServer
        hdfsServer.writeTableToFile(hdfsPath, dataTable, ",");

        // Create Data for s3Server
        s3Server.writeTableToFile(s3Path, dataTable, ",");
    }

    @Override
    protected void createTables() throws Exception {
        // Create GPDB external table directed to the HDFS file
        exTable =
                TableFactory.getPxfReadableTextTable("pxf_smoke_small_data", new String[]{
                        "name text",
                        "num integer",
                        "dub double precision",
                        "longNum bigint",
                        "bool boolean"
                }, defaultPath, ",");
        exTable.setHost(pxfHost);
        exTable.setPort(pxfPort);
        gpdb.createTableAndVerify(exTable);

        // Create GPDB external table directed to hdfsServer
        hdfsTable =
                TableFactory.getPxfReadableTextTable("pxf_smoke_server1", new String[]{
                        "name text",
                        "num integer",
                        "dub double precision",
                        "longNum bigint",
                        "bool boolean"
                }, hdfsPath, ",");
        hdfsTable.setUserParameters(new String[]{"server=1"});
        gpdb.createTableAndVerify(hdfsTable);

        // Create GPDB external table directed to s3Server
        s3Table =
                TableFactory.getPxfReadableTextTable("pxf_smoke_server2", new String[]{
                        "name text",
                        "num integer",
                        "dub double precision",
                        "longNum bigint",
                        "bool boolean"
                }, s3Path, ",");
        s3Table.setUserParameters(new String[]{"server=2"});
        s3Table.setProfile("S3Text");
        gpdb.createTableAndVerify(s3Table);
    }

    @Override
    protected void queryResults() throws Exception {
        runTincTest("pxf.smoke.multi_server.runTest");
    }

    @Test(groups = {"smoke", "gpdb"})
    public void test() throws Exception {
        runTest();
    }

}
