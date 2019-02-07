package org.greenplum.pxf.automation.proxy;


import org.greenplum.pxf.automation.smoke.BaseSmoke;
import org.greenplum.pxf.automation.structures.tables.basic.Table;
import org.greenplum.pxf.automation.structures.tables.pxf.ReadableExternalTable;
import org.greenplum.pxf.automation.structures.tables.utils.TableFactory;
import org.testng.annotations.Test;


/** Basic PXF on HDFS small text file using non-gpadmin user */
public class HdfsProxySmokeTest extends BaseSmoke {

    public static final String ADMIN_USER = System.getProperty("user.name");
    public static final String TEST_USER = "testuser";

    private String locationProhibited, locationAllowed;

    @Override
    protected void prepareData() throws Exception {
        // create small data table and write it to HDFS twice to be owned by gapdmin and test user
        Table dataTable = getSmallData();

        locationProhibited = String.format("%s/proxy/%s/%s", hdfs.getWorkingDirectory(), ADMIN_USER, fileName);
        locationAllowed = String.format("%s/proxy/%s/%s", hdfs.getWorkingDirectory(), TEST_USER, fileName);

        hdfs.writeTableToFile(locationProhibited, dataTable, ",");
        hdfs.setMode("/" + locationProhibited, "700");

        hdfs.writeTableToFile(locationAllowed, dataTable, ",");
        hdfs.setOwner("/" + locationAllowed, TEST_USER, TEST_USER);
        hdfs.setMode("/" + locationAllowed, "700");
    }

    @Override
    protected void createTables() throws Exception {
        // Create GPDB external table directed to the HDFS file
        ReadableExternalTable exTableProhibited =
                TableFactory.getPxfReadableTextTable("pxf_proxy_small_data_prohibited", new String[] {
                        "name text",
                        "num integer",
                        "dub double precision",
                        "longNum bigint",
                        "bool boolean"
                }, locationProhibited, ",");
        exTableProhibited.setHost(pxfHost);
        exTableProhibited.setPort(pxfPort);
        gpdb.createTableAndVerify(exTableProhibited);

        ReadableExternalTable exTableAllowed =
                TableFactory.getPxfReadableTextTable("pxf_proxy_small_data_allowed", new String[] {
                        "name text",
                        "num integer",
                        "dub double precision",
                        "longNum bigint",
                        "bool boolean"
                }, locationAllowed, ",");
        exTableAllowed.setHost(pxfHost);
        exTableAllowed.setPort(pxfPort);
        gpdb.createTableAndVerify(exTableAllowed);

    }

    @Override
    protected void queryResults() throws Exception {
        runTincTest("pxf.proxy.small_data.runTest");
    }

    @Test(groups = { "proxy", "gpdb" })
    public void test() throws Exception {
        runTest();
    }
}
