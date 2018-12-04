package org.greenplum.pxf.automation.smoke;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.greenplum.pxf.automation.components.hdfs.Hdfs;
import org.greenplum.pxf.automation.structures.tables.basic.Table;
import org.greenplum.pxf.automation.structures.tables.pxf.ExternalTable;
import org.greenplum.pxf.automation.structures.tables.utils.TableFactory;
import org.testng.annotations.Test;

/**
 * MultiServerTest verifies that multiple servers
 * can be accessed via PXF.
 */
public class MultiServerTest extends BaseSmoke {

    @Override
    protected void prepareData() throws Exception {
        // Create Data and write it to HDFS
        Table dataTable = getSmallData();
        hdfs.writeTableToFile(hdfs.getWorkingDirectory() + "/" + fileName, dataTable, ",");
    }

    @Override
    protected void createTables() throws Exception {
        // Create GPDB external table directed to the HDFS file
        exTable =
                TableFactory.getPxfReadableTextTable("pxf_smoke_small_data", new String[] {
                        "name text",
                        "num integer",
                        "dub double precision",
                        "longNum bigint",
                        "bool boolean"
                }, hdfs.getWorkingDirectory() + "/" + fileName, ",");
        exTable.setHost(pxfHost);
        exTable.setPort(pxfPort);
        gpdb.createTableAndVerify(exTable);
    }

    @Override
    protected void queryResults() throws Exception {
        Configuration config1 = new Configuration();
        config1.addResource(new Path("/home/gpadmin/pxf/servers/1/core-site.xml"));
        FileSystem fs1 = FileSystem.get(config1);

        Configuration config2 = new Configuration();
        config2.addResource(new Path("/home/gpadmin/pxf/servers/2/core-site.xml"));
        FileSystem fs2 = FileSystem.get(config2);

        Hdfs server1 = new Hdfs(fs1, config1, true);
        Hdfs server2 = new Hdfs(fs2, config2, true);

        // Create GPDB external table directed to server1
        ExternalTable exTable1 =
                TableFactory.getPxfReadableTextTable("pxf_smoke_server2", new String[] {
                        "name text",
                        "num integer",
                        "dub double precision",
                        "longNum bigint",
                        "bool boolean"
                }, hdfs.getWorkingDirectory() + "/" + fileName, ",");
        exTable1.setUserParameters(new String[] { "server=1" });
        gpdb.createTableAndVerify(exTable1);

        // Create GPDB external table directed to server2
        ExternalTable exTable2 =
                TableFactory.getPxfReadableTextTable("pxf_smoke_server1", new String[] {
                        "name text",
                        "num integer",
                        "dub double precision",
                        "longNum bigint",
                        "bool boolean"
                }, "/gpdb-ud-scratch/tmp" + "/" + fileName, ",");
        exTable2.setUserParameters(new String[] { "server=2" });
        exTable2.setProfile("S3Text");
        gpdb.createTableAndVerify(exTable2);

        runTincTest("pxf.smoke.multi_server.runTest");
    }

    @Test(groups = { "smoke", "gpdb" })
    public void test() throws Exception {
        runTest();
    }

}
