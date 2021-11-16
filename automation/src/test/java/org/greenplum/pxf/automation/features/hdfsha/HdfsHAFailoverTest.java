package org.greenplum.pxf.automation.features.hdfsha;

import jsystem.framework.sut.SutFactory;
import org.greenplum.pxf.automation.BaseFunctionality;
import org.greenplum.pxf.automation.components.hdfs.Hdfs;
import org.greenplum.pxf.automation.structures.tables.basic.Table;
import org.greenplum.pxf.automation.structures.tables.pxf.ReadableExternalTable;
import org.greenplum.pxf.automation.structures.tables.utils.TableFactory;
import org.testng.annotations.Test;

/**
 * Tests for making sure PXF continues to work when HDFS namenode failover occurs. While the failover is mostly
 * handled by HDFS client library, we are interested whether Kerberos tokens cached by PXF can be re-used
 * or can be re-obtained when connecting to another namenode. This is mostly relevant to Kerberos Constrained
 * Delegation use case, where a TGS on behalf of an end-user is cached by PXF.
 */
public class HdfsHAFailoverTest extends BaseFunctionality {

    public static final String SYSTEM_USER = System.getProperty("user.name");
    public static final String ADMIN_USER = "porter"; // PXF service principal for the IPA cluster

    public static final String TEST_USER = "testuser";
    public static final String[] FIELDS = {
            "name text",
            "num integer",
            "dub double precision",
            "longNum bigint",
            "bool boolean"
    };

    private String locationAdminUser;

    @Test(groups = {"ipa"})
    public void test() throws Exception {
        // prepare small data file in HDFS
        prepareData();

        // create PXF external table for no impersonation, no service user case (normal Kerberos)
        createReadablePxfTable("hdfs-ipa-no-impersonation-no-svcuser", locationAdminUser);

        // run tinc to read PXF data, it will issue 2 queries to cache the token / use it
        runTincTest("pxf.features.hdfsha.admin.step_1_pre_failover.runTest");

        // failover the namenode to standby, wait a little
        // run tinc to read PXF data, it will issue 2 queries to cache the token / use it

        // failover the namenode back, wait a little
        // run tinc to read PXF data, it will issue 2 queries to cache the token / use it

    }

    private void prepareData() throws Exception {
        // obtain HDFS object for the IPA cluster from the SUT file
        hdfs = (Hdfs) systemManager.
                getSystemObject("/sut", "hdfsIpa", -1, null, false,
                        null, SutFactory.getInstance().getSutInstance());
        trySecureLogin(hdfs, hdfs.getTestKerberosPrincipal());
        initializeWorkingDirectory(hdfs, gpdb.getUserName());

        locationAdminUser = String.format("%s/hdfsha/%s/%s", hdfs.getWorkingDirectory(), ADMIN_USER, fileName);

        // Create Data and write it to HDFS
        Table dataTable = getSmallData();
        hdfs.writeTableToFile(locationAdminUser, dataTable, ",");
    }

    private void createReadablePxfTable(String serverName, String location) throws Exception {
        String tableSuffix = serverName.replace("-", "_");
        ReadableExternalTable exTable =
                TableFactory.getPxfReadableTextTable("pxf_hdfsha_" + tableSuffix, FIELDS, location, ",");
        exTable.setHost(pxfHost);
        exTable.setPort(pxfPort);
        exTable.setServer("SERVER=" + serverName);
        gpdb.createTableAndVerify(exTable);
    }
}
