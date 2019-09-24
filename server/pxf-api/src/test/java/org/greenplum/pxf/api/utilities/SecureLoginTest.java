package org.greenplum.pxf.api.utilities;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SecureLoginTest {

    private String PROPERTY_KEY_USER_IMPERSONATION = "pxf.service.user.impersonation.enabled";

    private SecureLogin secureLogin;
    private Configuration configuration;

    @Before
    public void setup() {
        secureLogin = SecureLogin.getInstance();
        configuration = new Configuration();
    }

    @Test
    public void testImpersonationPropertyAbsent() {
        System.clearProperty(PROPERTY_KEY_USER_IMPERSONATION);
        assertFalse(secureLogin.isUserImpersonationEnabled(configuration));
    }

    @Test
    public void testImpersonationPropertyEmpty() {
        System.setProperty(PROPERTY_KEY_USER_IMPERSONATION, "");
        assertFalse(secureLogin.isUserImpersonationEnabled(configuration));
    }

    @Test
    public void testImpersonationPropertyFalse() {
        System.setProperty(PROPERTY_KEY_USER_IMPERSONATION, "foo");
        assertFalse(secureLogin.isUserImpersonationEnabled(configuration));
    }

    @Test
    public void testImpersonationPropertyTRUE() {
        System.setProperty(PROPERTY_KEY_USER_IMPERSONATION, "TRUE");
        assertTrue(secureLogin.isUserImpersonationEnabled(configuration));
    }

    @Test
    public void testImpersonationPropertyTrue() {
        System.setProperty(PROPERTY_KEY_USER_IMPERSONATION, "true");
        assertTrue(secureLogin.isUserImpersonationEnabled(configuration));
    }

    @Test
    public void testConfigurationImpersonationPropertyFalse() {
        configuration.set("pxf.service.user.impersonation", "foo");
        assertFalse(secureLogin.isUserImpersonationEnabled(configuration));
    }

    @Test
    public void testConfigurationImpersonationPropertyTrue() {
        configuration.set("pxf.service.user.impersonation", "true");
        assertTrue(secureLogin.isUserImpersonationEnabled(configuration));
    }

    @Test
    public void testConfigurationImpersonationPropertyTRUE() {
        configuration.set("pxf.service.user.impersonation", "TRUE");
        assertTrue(secureLogin.isUserImpersonationEnabled(configuration));
    }

}