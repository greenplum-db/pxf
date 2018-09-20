package org.greenplum.pxf.automation.features.general;

import jsystem.framework.system.SystemManagerImpl;

import org.testng.Assert;
import org.testng.annotations.Test;

import org.greenplum.pxf.automation.components.pxf.Pxf;
import org.greenplum.pxf.automation.utils.jsystem.report.ReportUtils;
import org.greenplum.pxf.automation.features.BaseFeature;

/**
 * Test PXF API
 */
public class ApiTest extends BaseFeature {

    Pxf pxf;

    @Override
    protected void beforeClass() throws Exception {
        pxf = (Pxf) SystemManagerImpl.getInstance().getSystemObject("pxf");
    }

    @Override
    protected void afterClass() throws Exception {
        super.afterClass();
        pxf.close();
    }

    /**
     * Call pxf/ProtocolVersion API via curl and verify response
     *
     * @throws Exception if the test failed to run
     */
    @Test(groups = "features")
    public void protocolVersion() throws Exception {

        ReportUtils.startLevel(null, getClass(), "get protocol version");

        String version = pxf.getProtocolVersion();

        Assert.assertNotNull(version, "version should not be null");
        Assert.assertTrue(version.matches("v[0-9]+"), "version " + version
                + " should be of the format v<number>");

        ReportUtils.stopLevel(null);
    }

    /**
     * Call pxf/v0 API via curl and verify error response
     *
     * @throws Exception if the test failed to run
     */
    @Test(groups = "features")
    public void wrongVersion() throws Exception {

        ReportUtils.startLevel(null, getClass(), "Check wrong version message");

        String result = pxf.curl(pxf.getHost(), pxf.getPort(), "pxf/v0");

        String expected = "Wrong version v0, supported version is v[0-9]+";

        Assert.assertTrue(result.matches(expected), "result " + result
                + " should match regex " + expected);

        ReportUtils.stopLevel(null);
    }

    /**
     * Call pxf/unknownpath API via curl and verify error response
     *
     * @throws Exception if the test failed to run
     */
    @Test(groups = "features")
    public void wrongPath() throws Exception {

        ReportUtils.startLevel(null, getClass(), "Check wrong path message");

        String result = pxf.curl(pxf.getHost(), pxf.getPort(), "pxf/kunilemel");

        String expected = "Unknown path \".*pxf/kunilemel\"";

        Assert.assertTrue(result.matches(expected), "result " + result
                + " should match regex " + expected);

        ReportUtils.stopLevel(null);
    }

    /**
     * Call pxf/version/unknownpath API via curl and verify error response
     *
     * @throws Exception if the test failed to run
     */
    @Test(groups = "features")
    public void wrongPathRightVersion() throws Exception {

        ReportUtils.startLevel(null, getClass(), "Check wrong path message");

        ReportUtils.report(null, getClass(), "Get current version");
        String version = pxf.getProtocolVersion();
        Assert.assertNotNull(version, "version should not be null");
        ReportUtils.report(null, getClass(), "Current version is " + version);

        String path = "pxf/" + version + "/kuni/lemel";
        String result = pxf.curl(pxf.getHost(), pxf.getPort(), path);

        String expected = "Unknown path \".*" + path + "\"";

        Assert.assertTrue(result.matches(expected), "result " + result
                + " should match regex " + expected);

        ReportUtils.stopLevel(null);
    }

    /**
     * Call pxf/retiredpath API via curl and verify error response
     *
     * @throws Exception if the test failed to run
     */
    @Test(groups = "features")
    public void retiredPathNoVersion() throws Exception {

        ReportUtils.startLevel(null, getClass(), "Check wrong path message");

        String result = pxf.curl(pxf.getHost(), pxf.getPort(), "pxf/Analyzer");

        String expected = "Unknown path \".*pxf/Analyzer\"";

        Assert.assertTrue(result.matches(expected), "result " + result
                + " should match regex " + expected);

        ReportUtils.stopLevel(null);
    }

    /**
     * Call pxf/version/retiredpath API via curl and verify error response
     *
     * @throws Exception if the test failed to run
     */
    @Test(groups = "features")
    public void retiredPathWrongVersion() throws Exception {

        ReportUtils.startLevel(null, getClass(), "Check wrong version message");

        String result = pxf.curl(pxf.getHost(), pxf.getPort(),
                "pxf/v0/Analyzer");

        String expected = "Wrong version v0, supported version is v[0-9]+";

        Assert.assertTrue(result.matches(expected), "result " + result
                + " should match regex " + expected);

        ReportUtils.stopLevel(null);
    }

    /**
     * Call pxf/version/retiredpath API via curl and verify error response
     *
     * @throws Exception if the test failed to run
     */
    @Test(groups = "features")
    public void retiredPathRightVersion() throws Exception {

        ReportUtils.startLevel(null, getClass(), "Check wrong path message");

        ReportUtils.report(null, getClass(), "Get current version");
        String version = pxf.getProtocolVersion();
        Assert.assertNotNull(version, "version should not be null");
        ReportUtils.report(null, getClass(), "Current version is " + version);

        String path = "pxf/" + version + "/Analyzer";
        String result = pxf.curl(pxf.getHost(), pxf.getPort(), path);

        String expected = "Analyzer API is retired. Please use /Fragmenter/getFragmentsStats instead";

        Assert.assertEquals(result, expected);

        ReportUtils.stopLevel(null);
    }
}
