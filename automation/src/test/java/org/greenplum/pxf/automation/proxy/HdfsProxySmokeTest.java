package org.greenplum.pxf.automation.proxy;


import org.greenplum.pxf.automation.smoke.BaseSmoke;
import org.greenplum.pxf.automation.structures.tables.basic.Table;
import org.greenplum.pxf.automation.structures.tables.pxf.ReadableExternalTable;
import org.greenplum.pxf.automation.structures.tables.utils.TableFactory;
import org.testng.annotations.Test;


/**
 * Basic PXF on HDFS small text file using non-gpadmin user
 */
public class HdfsProxySmokeTest extends BaseSmoke {

    public static final String ADMIN_USER = System.getProperty("user.name");
    public static final String TEST_USER = "testuser";
    public static final String[] FIELDS = {
            "name text",
            "num integer",
            "dub double precision",
            "longNum bigint",
            "bool boolean"
    };

    private String locationProhibited, locationAllowed;

    @Override
    protected void prepareData() throws Exception {
        // create small data table and write it to HDFS twice to be owned by gpadmin and test user
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
                TableFactory.getPxfReadableTextTable("pxf_proxy_small_data_prohibited",
                        FIELDS, locationProhibited, ",");
        exTableProhibited.setHost(pxfHost);
        exTableProhibited.setPort(pxfPort);
        gpdb.createTableAndVerify(exTableProhibited);

        ReadableExternalTable exTableProhibitedNoImpersonationServer =
                TableFactory.getPxfReadableTextTable("pxf_proxy_small_data_prohibited_no_impersonation",
                        FIELDS, locationProhibited, ",");
        exTableProhibitedNoImpersonationServer.setHost(pxfHost);
        exTableProhibitedNoImpersonationServer.setPort(pxfPort);
        exTableProhibitedNoImpersonationServer.setServer("SERVER=default-no-impersonation");
        gpdb.createTableAndVerify(exTableProhibitedNoImpersonationServer);

        ReadableExternalTable exTableAllowed =
                TableFactory.getPxfReadableTextTable("pxf_proxy_small_data_allowed",
                        FIELDS, locationAllowed, ",");
        exTableAllowed.setHost(pxfHost);
        exTableAllowed.setPort(pxfPort);
        gpdb.createTableAndVerify(exTableAllowed);

        // Configure a server with the same configuration as the default
        // server, but disable impersonation
        ReadableExternalTable exTableAllowedNoImpersonationServer =
                TableFactory.getPxfReadableTextTable("pxf_proxy_small_data_allowed_no_impersonation",
                        FIELDS, locationAllowed, ",");
        exTableAllowedNoImpersonationServer.setHost(pxfHost);
        exTableAllowedNoImpersonationServer.setPort(pxfPort);
        exTableAllowedNoImpersonationServer.setServer("SERVER=default-no-impersonation");
        gpdb.createTableAndVerify(exTableAllowedNoImpersonationServer);

    }

    @Override
    protected void queryResults() throws Exception {
        runTincTest("pxf.proxy.small_data.runTest");
    }

    @Test(groups = {"proxy", "hdfs", "proxySecurity"})
    public void test() throws Exception {
        runTest();
    }
}
