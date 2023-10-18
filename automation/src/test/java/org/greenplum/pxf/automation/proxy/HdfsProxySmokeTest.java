package org.greenplum.pxf.automation.proxy;


import org.greenplum.pxf.automation.components.hdfs.Hdfs;
import org.greenplum.pxf.automation.smoke.BaseSmoke;
import org.greenplum.pxf.automation.structures.tables.basic.Table;
import org.greenplum.pxf.automation.structures.tables.pxf.ReadableExternalTable;
import org.greenplum.pxf.automation.structures.tables.utils.TableFactory;
import org.testng.annotations.Test;


/**
 * Basic PXF on HDFS small text file using non-gpadmin user
 */
public class HdfsProxySmokeTest extends BaseSmoke {

    public static final String SYSTEM_USER = System.getProperty("user.name");
    public static final String TEST_USER = "testuser";
    public static final String[] FIELDS = {
            "name text",
            "num integer",
            "dub double precision",
            "longNum bigint",
            "bool boolean"
    };

    private String locationProhibited, locationAllowed;

    protected Hdfs getHdfsTarget() throws Exception {
        return hdfs;
    }

    protected String getTableInfix() {
        return "";
    }

    protected String getServerName() {
        return "default";
    }

    protected String getAdminUser() {
        return SYSTEM_USER;
    }

    @Override
    protected void prepareData() throws Exception {
        // get HDFS to work against, it is "hdfs" defined in SUT file by default, but subclasses can choose
        // a different HDFS to use, such as HDFS in IPA-based Hadoop cluster
        Hdfs hdfsTarget = getHdfsTarget();

        // create small data table and write it to HDFS twice to be owned by gpadmin and test user
        Table dataTable = getSmallData();

        locationProhibited = String.format("%s/proxy/%s/%s", hdfsTarget.getWorkingDirectory(), getAdminUser(), fileName);
        locationAllowed = String.format("%s/proxy/%s/%s", hdfsTarget.getWorkingDirectory(), TEST_USER, fileName);

        // "prohibited" location is readable only by the admin user (pxf runtime user or Kerberos principal)
        hdfsTarget.writeTableToFile(locationProhibited, dataTable, ",");
        hdfsTarget.setOwner("/" + locationProhibited, getAdminUser(), getAdminUser());
        hdfsTarget.setMode("/" + locationProhibited, "700");

        // "allowed" location is readable only by the test user
        hdfsTarget.writeTableToFile(locationAllowed, dataTable, ",");
        hdfsTarget.setOwner("/" + locationAllowed, TEST_USER, TEST_USER);
        hdfsTarget.setMode("/" + locationAllowed, "700");
    }

    @Override
    protected void createTables() throws Exception {
        String serverName = getServerName();

        // --- PXF tables pointing to the location prohibited to be read by TEST_USER (allowed for ADMIN_USER only) ---
        // server with impersonation
        createReadablePxfTable(serverName, "_small_data_prohibited", locationProhibited);
        // server without impersonation but with a service user
        createReadablePxfTable(serverName + "-no-impersonation", "_small_data_prohibited_no_impersonation", locationProhibited);
        // server without impersonation and without a service user
        createReadablePxfTable(serverName + "-no-impersonation-no-svcuser", "_small_data_prohibited_no_impersonation_no_svcuser", locationProhibited);

        // --- PXF tables pointing to the location allowed to be read by the TEST_USER only ---
        // server with impersonation
        createReadablePxfTable(serverName, "_small_data_allowed", locationAllowed);
        // server without impersonation but with a service user
        createReadablePxfTable(serverName + "-no-impersonation", "_small_data_allowed_no_impersonation", locationAllowed);
        // server without impersonation and without a service user
        createReadablePxfTable(serverName + "-no-impersonation-no-svcuser", "_small_data_allowed_no_impersonation_no_svcuser", locationAllowed);

    }

    private void createReadablePxfTable(String serverName, String tableSuffix, String location) throws Exception {
        ReadableExternalTable exTable =
                TableFactory.getPxfReadableTextTable("pxf_proxy" + getTableInfix() + tableSuffix,
                        FIELDS, location, ",");
        exTable.setHost(pxfHost);
        exTable.setPort(pxfPort);
        if (!serverName.equalsIgnoreCase("default")) {
            exTable.setServer("SERVER=" + serverName);
        }
        gpdb.createTableAndVerify(exTable);
    }

    @Override
    protected void queryResults() throws Exception {
        runSqlTest("proxy/small_data");
    }

    @Test(groups = {"proxy", "hdfs", "proxySecurity"})
    public void test() throws Exception {
        runTest();
    }
}
