package org.greenplum.pxf.automation.features.hdfsha;

import jsystem.framework.sut.SutFactory;
import org.apache.commons.lang.StringUtils;
import org.greenplum.pxf.automation.BaseFunctionality;
import org.greenplum.pxf.automation.components.hdfs.Hdfs;
import org.greenplum.pxf.automation.structures.tables.basic.Table;
import org.greenplum.pxf.automation.structures.tables.pxf.ReadableExternalTable;
import org.greenplum.pxf.automation.structures.tables.utils.TableFactory;
import org.testng.annotations.Test;

import java.util.List;

/**
 * Tests for making sure PXF continues to work when HDFS namenode failover occurs. While the failover is mostly
 * handled by HDFS client library, we are interested whether Kerberos tokens cached by PXF can be re-used
 * or can be re-obtained when connecting to another namenode. This is mostly relevant to Kerberos Constrained
 * Delegation use case, where a ticket granting ticket (TGT) on behalf of an end-user is cached by PXF.
 */
public class HdfsHAFailoverTest extends BaseFunctionality {

    public static final String ADMIN_USER = "porter"; // PXF service principal for the IPA cluster
    public static final String TEST_USER = "testuser";
    public static final String[] FIELDS = {
            "name text",
            "num integer",
            "dub double precision",
            "longNum bigint",
            "bool boolean"
    };

    @Test(groups = {"proxySecurityIpa"})
    public void testFailoverScenario() throws Exception {
        // prepare small data file in HDFS
        String locationAdminUser = prepareData(ADMIN_USER);
        String locationTestUser = prepareData(TEST_USER);

        // create PXF external table for no impersonation, no service user case (normal Kerberos)
        createReadablePxfTable("hdfs-ipa-no-impersonation-no-svcuser", locationAdminUser);

        // create PXF external table for impersonation based on Constrained Delegation
        createReadablePxfTable("hdfs-ipa", locationTestUser);

        // run SQL test to read PXF data, it will issue 2 queries per PXF server to cache the tokens / use them
        runSqlTest("features/hdfsha/step_1_pre_failover");

        // failover the namenode to standby
        hdfs.failover("nn01", "nn02");

        // run SQL test to read PXF data, it will issue 2 queries per PXF server to cache the tokens / use them
        runSqlTest("features/hdfsha/step_2_after_failover");

        // failover the namenode back
        hdfs.failover("nn02", "nn01");

        // run SQL test to read PXF data, it will issue 2 queries per PXF server to cache the tokens / use them
        runSqlTest("features/hdfsha/step_3_after_failover_back");
    }

    private String prepareData(String hdpUser) throws Exception {
        // obtain HDFS object for the IPA cluster from the SUT file
        hdfs = (Hdfs) systemManager.
                getSystemObject("/sut", "hdfsIpa", -1, null, false,
                        null, SutFactory.getInstance().getSutInstance());
        trySecureLogin(hdfs, hdfs.getTestKerberosPrincipal());
        initializeWorkingDirectory(hdfs, gpdb.getUserName());

        String location = String.format("%s/hdfsha/%s/%s", hdfs.getWorkingDirectory(), hdpUser, fileName);

        // Create Data and write it to HDFS
        Table dataTable = getSmallData();
        // update name column value with the name of the user to make sure the proper data sets are read in the tests
        for (List<String> row : dataTable.getData()) {
            row.set(0, hdpUser + "-" + row.get(0));
        }
        hdfs.writeTableToFile(location, dataTable, ",");

        // operation below require absolute path
        String locationAbsolute = "/" + StringUtils.removeStart(location, "/");
        hdfs.setOwner(locationAbsolute, hdpUser, "gpadmin");
        hdfs.setMode(locationAbsolute, "400"); // read only by specified Hadoop user
        return location;
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
