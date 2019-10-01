package org.greenplum.pxf.automation.features.security;

import org.greenplum.pxf.automation.features.BaseFeature;
import org.testng.annotations.Test;

/**
 * SecuredServerTest verifies functionality when running queries against
 * a Kerberized Hadoop cluster via PXF.
 */
public class SecuredServerTest extends BaseFeature {

    @Test(groups = {"features", "security"})
    public void testSecuredServerFailsWithInvalidPrincipalName() {

    }

    @Test(groups = {"features", "security"})
    public void testSecuredServerFailsWithInvalidKeytabPath() {

    }
}
