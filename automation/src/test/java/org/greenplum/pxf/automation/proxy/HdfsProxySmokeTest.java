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

    protected Hdfs getHdfsTarget() throws Exception {
        return hdfs;
    }

    protected String getTableSuffix() {
        return "";
    }

    protected String getServerName() {
        return "default";
    }

    @Override
    protected void prepareData() throws Exception {
        // get HDFS to work against, it is "hdfs" defined in SUT file by default, but subclasses can choose
        // a different HDFS to use, such as HDFS in IPA-based Hadoop cluster
        Hdfs hdfsTarget = getHdfsTarget();

        // create small data table and write it to HDFS twice to be owned by gpadmin and test user
        Table dataTable = getSmallData();

        locationProhibited = String.format("%s/proxy/%s/%s", hdfsTarget.getWorkingDirectory(), ADMIN_USER, fileName);
        locationAllowed = String.format("%s/proxy/%s/%s", hdfsTarget.getWorkingDirectory(), TEST_USER, fileName);

        hdfsTarget.writeTableToFile(locationProhibited, dataTable, ",");
        hdfsTarget.setMode("/" + locationProhibited, "700");

        hdfsTarget.writeTableToFile(locationAllowed, dataTable, ",");
        hdfsTarget.setOwner("/" + locationAllowed, TEST_USER, TEST_USER);
        hdfsTarget.setMode("/" + locationAllowed, "700");
    }

    @Override
    protected void createTables() throws Exception {
        String serverName = getServerName();
        // Create GPDB external table directed to the HDFS file
        ReadableExternalTable exTableProhibited =
                TableFactory.getPxfReadableTextTable("pxf_proxy" + getTableSuffix() + "_small_data_prohibited",
                        FIELDS, locationProhibited, ",");
        exTableProhibited.setHost(pxfHost);
        exTableProhibited.setPort(pxfPort);
        if (!serverName.equalsIgnoreCase("default")) {
            exTableProhibited.setServer("SERVER=" + serverName);
        }
        gpdb.createTableAndVerify(exTableProhibited);

        ReadableExternalTable exTableProhibitedNoImpersonationServer =
                TableFactory.getPxfReadableTextTable("pxf_proxy" + getTableSuffix() + "_small_data_prohibited_no_impersonation",
                        FIELDS, locationProhibited, ",");
        exTableProhibitedNoImpersonationServer.setHost(pxfHost);
        exTableProhibitedNoImpersonationServer.setPort(pxfPort);
        exTableProhibitedNoImpersonationServer.setServer("SERVER=" + getServerName() + "-no-impersonation");
        gpdb.createTableAndVerify(exTableProhibitedNoImpersonationServer);

        ReadableExternalTable exTableAllowed =
                TableFactory.getPxfReadableTextTable("pxf_proxy" + getTableSuffix() + "_small_data_allowed",
                        FIELDS, locationAllowed, ",");
        exTableAllowed.setHost(pxfHost);
        exTableAllowed.setPort(pxfPort);
        if (!serverName.equalsIgnoreCase("default")) {
            exTableProhibited.setServer("SERVER=" + serverName);
        }
        gpdb.createTableAndVerify(exTableAllowed);

        // Configure a server with the same configuration as the default
        // server, but disable impersonation
        ReadableExternalTable exTableAllowedNoImpersonationServer =
                TableFactory.getPxfReadableTextTable("pxf_proxy" + getTableSuffix() + "_small_data_allowed_no_impersonation",
                        FIELDS, locationAllowed, ",");
        exTableAllowedNoImpersonationServer.setHost(pxfHost);
        exTableAllowedNoImpersonationServer.setPort(pxfPort);
        exTableAllowedNoImpersonationServer.setServer("SERVER=" + getServerName() + "-no-impersonation");
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
