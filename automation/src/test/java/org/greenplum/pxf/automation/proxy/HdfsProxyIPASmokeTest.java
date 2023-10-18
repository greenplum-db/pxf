package org.greenplum.pxf.automation.proxy;

import jsystem.framework.sut.SutFactory;
import org.greenplum.pxf.automation.components.hdfs.Hdfs;
import org.testng.annotations.Test;

/**
 * This class runs the proxy smoke test against a Hadoop cluster with Kerberos constrained delegation
 * enabled by an IPA server. It is supposed to be used only for MultiServer setup and leverages the setup logic
 * in the parent class. It specifies the correct HDFS target name from the SUT file as well as PXF configuration
 * server name and the suffix for the GPDB PXF external table. It runs its own SQL test that is very similar
 * to the regular proxy test, but the name of the PXF table is different.
 */
public class HdfsProxyIPASmokeTest extends HdfsProxySmokeTest {

    // PXF service principal (short name) for the IPA cluster
    public static final String PXF_IPA_USER = "porter";

    @Override
    protected Hdfs getHdfsTarget() throws Exception {
        Hdfs hdfsIpa = (Hdfs) systemManager.
                getSystemObject("/sut", "hdfsIpa", -1, null, false,
                        null, SutFactory.getInstance().getSutInstance());

        if (hdfsIpa != null) {
            trySecureLogin(hdfsIpa, hdfsIpa.getTestKerberosPrincipal());
            initializeWorkingDirectory(hdfsIpa, gpdb.getUserName());
        }

        return hdfsIpa;
    }

    @Override
    protected String getTableInfix() {
        return "_ipa";
    }

    @Override
    protected String getServerName() {
        return "hdfs-ipa";
    }

    @Override
    protected String getAdminUser() {
        return PXF_IPA_USER;
    }

    @Override
    protected void queryResults() throws Exception {
        runSqlTest("proxy/small_data_ipa");
    }

    @Test(groups = {"proxySecurityIpa"})
    public void test() throws Exception {
        runTest();
    }

}
