package org.greenplum.pxf.api.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.LoginSession;
import org.apache.hadoop.security.PxfUserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.net.InetAddress;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({PxfUserGroupInformation.class})
public class SecureLoginTest {

    private static final String PROPERTY_KEY_USER_IMPERSONATION = "pxf.service.user.impersonation.enabled";
    private static final String PROPERTY_KEY_SERVICE_PRINCIPAL = "pxf.service.kerberos.principal";
    private static final String PROPERTY_KEY_SERVICE_KEYTAB = "pxf.service.kerberos.keytab";


    private static final String hostname;
    private static final String RESOLVED_PRINCIPAL;
    static {
        try {
            hostname = InetAddress.getLocalHost().getCanonicalHostName();
            RESOLVED_PRINCIPAL = String.format("principal/%s@REALM", hostname);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private SecureLogin secureLogin;
    private Configuration configuration;
    private LoginSession expectedLoginSession;
    private UserGroupInformation expectedUGI;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setup() {
        secureLogin = SecureLogin.getInstance();
        SecureLogin.reset();
        configuration = new Configuration();
        System.clearProperty(PROPERTY_KEY_USER_IMPERSONATION);
        System.clearProperty(PROPERTY_KEY_SERVICE_PRINCIPAL);
        System.clearProperty(PROPERTY_KEY_SERVICE_KEYTAB);

        // simulate presence of krb.conf file
        System.setProperty("java.security.krb5.kdc", "localhost");
        System.setProperty("java.security.krb5.realm", "DEFAULT_REALM");
    }

    @Test
    public void testSingleton() {
        assertSame(secureLogin, SecureLogin.getInstance());
    }

    /* ---------- methods to test login / relogin ---------- */

    @Test
    public void testLoginNoKerberosNoServiceUser() throws IOException {
        PowerMockito.mockStatic(PxfUserGroupInformation.class);
        expectedLoginSession = new LoginSession("config");

        UserGroupInformation loginUGI = secureLogin.getLoginUser("server", "config", configuration);

        LoginSession loginSession = SecureLogin.getCache().get("server");
        assertEquals(1, SecureLogin.getCache().size());
        assertEquals(expectedLoginSession, loginSession);
        assertSame(loginUGI, loginSession.getLoginUser());
        assertEquals(System.getProperty("user.name"), loginUGI.getUserName());
        assertNull(loginSession.getSubject());
        assertNull(loginSession.getUser());

        // Make sure that the cached entry is the same after the second call
        assertSame(loginUGI, secureLogin.getLoginUser("server", "config", configuration));

        PowerMockito.verifyZeroInteractions(PxfUserGroupInformation.class);
    }

    @Test
    public void testLoginNoKerberosNoServiceUserWhenConfigurationValuesAreProvided() throws IOException {
        PowerMockito.mockStatic(PxfUserGroupInformation.class);
        expectedLoginSession = new LoginSession("config");
        // These values in the configuration should not be added to the LoginSession
        configuration.set("pxf.service.kerberos.principal", "foo");
        configuration.set("pxf.service.kerberos.keytab", "bar");
        configuration.set("hadoop.kerberos.min.seconds.before.relogin", "100");

        UserGroupInformation loginUGI = secureLogin.getLoginUser("server", "config", configuration);

        LoginSession loginSession = SecureLogin.getCache().get("server");
        assertEquals(1, SecureLogin.getCache().size());
        assertEquals(expectedLoginSession, loginSession);
        assertSame(loginUGI, loginSession.getLoginUser());
        assertEquals(System.getProperty("user.name"), loginUGI.getUserName());
        assertNull(loginSession.getSubject());
        assertNull(loginSession.getUser());

        // Make sure that the cached entry is the same after the second call
        assertSame(loginUGI, secureLogin.getLoginUser("server", "config", configuration));

        PowerMockito.verifyZeroInteractions(PxfUserGroupInformation.class);
    }

    @Test
    public void testLoginNoKerberosWithServiceUser() throws IOException {
        PowerMockito.mockStatic(PxfUserGroupInformation.class);
        expectedLoginSession = new LoginSession("config", null, null, null, null, 0);
        configuration.set("pxf.service.user.name", "foo");

        UserGroupInformation loginUGI = secureLogin.getLoginUser("server", "config", configuration);

        LoginSession loginSession = SecureLogin.getCache().get("server");
        assertEquals(1, SecureLogin.getCache().size());
        assertEquals(expectedLoginSession, loginSession);
        assertSame(loginUGI, loginSession.getLoginUser());
        assertEquals("foo", loginUGI.getUserName());
        assertNull(loginSession.getSubject());
        assertNull(loginSession.getUser());

        PowerMockito.verifyZeroInteractions(PxfUserGroupInformation.class);
    }

    @Test
    public void testLoginKerberosFailsWhenNoPrincipal() throws IOException {
        expectedException.expect(RuntimeException.class);
        expectedException.expectMessage("PXF service login failed for server test : Kerberos Security for server test requires a valid principal.");

        PowerMockito.mockStatic(PxfUserGroupInformation.class);
        configuration.set("hadoop.security.authentication", "kerberos");

        secureLogin.getLoginUser("test", "config", configuration);
    }

    @Test
    public void testLoginKerberosFailsWhenNoKeytab() throws IOException {
        expectedException.expect(RuntimeException.class);
        expectedException.expectMessage("PXF service login failed for server test : Kerberos Security for server test requires a valid keytab file name.");

        PowerMockito.mockStatic(PxfUserGroupInformation.class);
        configuration.set("hadoop.security.authentication", "kerberos");
        configuration.set(PROPERTY_KEY_SERVICE_PRINCIPAL, "foo");

        secureLogin.getLoginUser("test", "config", configuration);
    }

    @Test
    public void testLoginKerberosFirstTime() throws IOException {
        PowerMockito.mockStatic(PxfUserGroupInformation.class);
        expectedUGI = UserGroupInformation.createUserForTesting("some", new String[]{});
        expectedLoginSession = new LoginSession("config", "principal", "/path/to/keytab", expectedUGI, null, 0);
        configuration.set("hadoop.security.authentication", "kerberos");
        configuration.set(PROPERTY_KEY_SERVICE_PRINCIPAL, "principal");
        configuration.set(PROPERTY_KEY_SERVICE_KEYTAB, "/path/to/keytab");
        configuration.set("hadoop.kerberos.min.seconds.before.relogin", "90");
        when(PxfUserGroupInformation.loginUserFromKeytab(configuration, "server", "config", "principal", "/path/to/keytab")).thenReturn(expectedLoginSession);

        UserGroupInformation loginUGI = secureLogin.getLoginUser("server", "config", configuration);

        LoginSession loginSession = SecureLogin.getCache().get("server");
        assertEquals(1, SecureLogin.getCache().size());
        assertEquals(expectedLoginSession, loginSession);
        assertSame(loginUGI, loginSession.getLoginUser());
        assertSame(expectedUGI, loginUGI); // since actual login was mocked, we should get back whatever we mocked

        // verify static calls issued
        PowerMockito.verifyStatic();
        PxfUserGroupInformation.loginUserFromKeytab(configuration, "server", "config", "principal", "/path/to/keytab");
        PowerMockito.verifyStatic();
        PxfUserGroupInformation.reloginFromKeytab("server", expectedLoginSession);

        PowerMockito.verifyNoMoreInteractions(PxfUserGroupInformation.class);
    }

    @Test
    public void testLoginKerberosSameSession() throws IOException {
        PowerMockito.mockStatic(PxfUserGroupInformation.class);
        expectedUGI = UserGroupInformation.createUserForTesting("some", new String[]{});
        expectedLoginSession = new LoginSession("config", "principal", "/path/to/keytab", expectedUGI, null, 90000);
        configuration.set("hadoop.security.authentication", "kerberos");
        configuration.set(PROPERTY_KEY_SERVICE_PRINCIPAL, "principal");
        configuration.set(PROPERTY_KEY_SERVICE_KEYTAB, "/path/to/keytab");
        configuration.set("hadoop.kerberos.min.seconds.before.relogin", "90");
        when(PxfUserGroupInformation.loginUserFromKeytab(configuration, "server", "config", "principal", "/path/to/keytab")).thenReturn(expectedLoginSession);
        when(PxfUserGroupInformation.getKerberosMinMillisBeforeRelogin("server", configuration)).thenReturn(90000L);

        UserGroupInformation loginUGI = secureLogin.getLoginUser("server", "config", configuration);

        LoginSession loginSession = SecureLogin.getCache().get("server");
        assertEquals(1, SecureLogin.getCache().size());
        assertEquals(expectedLoginSession, loginSession);
        assertSame(loginUGI, loginSession.getLoginUser());
        assertSame(expectedUGI, loginUGI); // since actual login was mocked, we should get back whatever we mocked

        // now login the same user again, should use cache and not login again, but call relogin to renew the tokens
        secureLogin.getLoginUser("server", "config", configuration);

        LoginSession loginAgainSession = SecureLogin.getCache().get("server");
        assertEquals(1, SecureLogin.getCache().size());
        assertEquals(expectedLoginSession, loginAgainSession);
        assertSame(loginUGI, loginAgainSession.getLoginUser());
        assertSame(expectedUGI, loginUGI); // since actual login was mocked, we should get back whatever we mocked
        assertSame(loginSession, loginAgainSession); // got the same from cache

        // verify static calls issued
        PowerMockito.verifyStatic();
        PxfUserGroupInformation.loginUserFromKeytab(configuration, "server", "config", "principal", "/path/to/keytab");
        PowerMockito.verifyStatic();
        PxfUserGroupInformation.getKerberosMinMillisBeforeRelogin("server", configuration);
        PowerMockito.verifyStatic(times(2)); // 1 extra relogin call
        PxfUserGroupInformation.reloginFromKeytab("server", expectedLoginSession);

        PowerMockito.verifyNoMoreInteractions(PxfUserGroupInformation.class);
    }

    @Test
    public void testLoginKerberosDifferentServer() throws IOException {
        PowerMockito.mockStatic(PxfUserGroupInformation.class);
        expectedUGI = UserGroupInformation.createUserForTesting("some", new String[]{});

        expectedLoginSession = new LoginSession("config", "principal", "/path/to/keytab", expectedUGI, null, 0);

        configuration.set("hadoop.security.authentication", "kerberos");
        configuration.set(PROPERTY_KEY_SERVICE_PRINCIPAL, "principal");
        configuration.set(PROPERTY_KEY_SERVICE_KEYTAB, "/path/to/keytab");
        when(PxfUserGroupInformation.loginUserFromKeytab(configuration, "server", "config", "principal", "/path/to/keytab")).thenReturn(expectedLoginSession);

        UserGroupInformation loginUGI = secureLogin.getLoginUser("server", "config", configuration);

        LoginSession loginSession = SecureLogin.getCache().get("server");
        assertEquals(1, SecureLogin.getCache().size());
        assertEquals(expectedLoginSession, loginSession);
        assertSame(loginUGI, loginSession.getLoginUser());
        assertSame(expectedUGI, loginUGI); // since actual login was mocked, we should get back whatever we mocked


        // ------------------- now login for another server -------------------------

        LoginSession expectedDiffLoginSession = new LoginSession("diff-config", "principal", "/path/to/keytab", expectedUGI, null, 0);
        Configuration diffConfiguration = new Configuration();
        diffConfiguration.set("hadoop.security.authentication", "kerberos");
        diffConfiguration.set(PROPERTY_KEY_SERVICE_PRINCIPAL, "principal");
        diffConfiguration.set(PROPERTY_KEY_SERVICE_KEYTAB, "/path/to/keytab");
        when(PxfUserGroupInformation.loginUserFromKeytab(diffConfiguration, "diff-server", "diff-config", "principal", "/path/to/keytab")).thenReturn(expectedDiffLoginSession);

        UserGroupInformation diffLoginUGI = secureLogin.getLoginUser("diff-server", "diff-config", diffConfiguration);

        LoginSession diffLoginSession = SecureLogin.getCache().get("diff-server");
        assertEquals(2, SecureLogin.getCache().size());
        assertEquals(expectedDiffLoginSession, diffLoginSession);
        assertSame(loginUGI, diffLoginSession.getLoginUser());
        assertSame(expectedUGI, diffLoginUGI); // since actual login was mocked, we should get back whatever we mocked
        assertNotSame(loginSession, diffLoginSession); // should be different object

        // verify static calls issued
        PowerMockito.verifyStatic();
        PxfUserGroupInformation.loginUserFromKeytab(configuration, "server", "config", "principal", "/path/to/keytab");
        PowerMockito.verifyStatic();
        PxfUserGroupInformation.reloginFromKeytab("server", expectedLoginSession);

        PowerMockito.verifyStatic();
        PxfUserGroupInformation.loginUserFromKeytab(diffConfiguration, "diff-server", "diff-config", "principal", "/path/to/keytab");
        PowerMockito.verifyStatic();
        PxfUserGroupInformation.reloginFromKeytab("diff-server", expectedDiffLoginSession);

        PowerMockito.verifyNoMoreInteractions(PxfUserGroupInformation.class);
    }

    @Test
    public void testLoginKerberosSameServerDifferentPrincipal() throws IOException {
        PowerMockito.mockStatic(PxfUserGroupInformation.class);
        expectedUGI = UserGroupInformation.createUserForTesting("some", new String[]{});

        expectedLoginSession = new LoginSession("config", "principal", "/path/to/keytab", expectedUGI, null, 0);

        configuration.set("hadoop.security.authentication", "kerberos");
        configuration.set(PROPERTY_KEY_SERVICE_PRINCIPAL, "principal");
        configuration.set(PROPERTY_KEY_SERVICE_KEYTAB, "/path/to/keytab");
        configuration.set("hadoop.kerberos.min.seconds.before.relogin", "90");
        when(PxfUserGroupInformation.loginUserFromKeytab(configuration, "server", "config", "principal", "/path/to/keytab")).thenReturn(expectedLoginSession);

        UserGroupInformation loginUGI = secureLogin.getLoginUser("server", "config", configuration);

        LoginSession loginSession = SecureLogin.getCache().get("server");
        assertEquals(1, SecureLogin.getCache().size());
        assertEquals(expectedLoginSession, loginSession);
        assertSame(loginUGI, loginSession.getLoginUser());
        assertSame(expectedUGI, loginUGI); // since actual login was mocked, we should get back whatever we mocked

        // ------------------- now change the principal in the configuration, login again -------------------------

        LoginSession expectedDiffLoginSession = new LoginSession("config", "diff-principal", "/path/to/keytab", expectedUGI, null, 90000);
        Configuration diffConfiguration = new Configuration();
        diffConfiguration.set("hadoop.security.authentication", "kerberos");
        diffConfiguration.set(PROPERTY_KEY_SERVICE_PRINCIPAL, "diff-principal");
        diffConfiguration.set(PROPERTY_KEY_SERVICE_KEYTAB, "/path/to/keytab");
        diffConfiguration.set("hadoop.kerberos.min.seconds.before.relogin", "180");
        when(PxfUserGroupInformation.loginUserFromKeytab(configuration, "server", "config", "principal", "/path/to/keytab")).thenReturn(expectedLoginSession);
        when(PxfUserGroupInformation.loginUserFromKeytab(diffConfiguration, "server", "config", "diff-principal", "/path/to/keytab")).thenReturn(expectedDiffLoginSession);
        when(PxfUserGroupInformation.getKerberosMinMillisBeforeRelogin("server", diffConfiguration)).thenReturn(180000L);

        UserGroupInformation diffLoginUGI = secureLogin.getLoginUser("server", "config", diffConfiguration);

        LoginSession diffLoginSession = SecureLogin.getCache().get("server");
        assertEquals(1, SecureLogin.getCache().size());
        assertEquals(expectedDiffLoginSession, diffLoginSession);
        assertSame(loginUGI, diffLoginSession.getLoginUser());
        assertSame(expectedUGI, diffLoginUGI); // since actual login was mocked, we should get back whatever we mocked
        assertNotSame(loginSession, diffLoginSession); // should be different object

        // verify static calls issued
        PowerMockito.verifyStatic();
        PxfUserGroupInformation.loginUserFromKeytab(configuration, "server", "config", "principal", "/path/to/keytab");
        PowerMockito.verifyStatic();
        PxfUserGroupInformation.reloginFromKeytab("server", expectedLoginSession);

        PowerMockito.verifyStatic();
        PxfUserGroupInformation.loginUserFromKeytab(diffConfiguration, "server", "config", "diff-principal", "/path/to/keytab");
        PowerMockito.verifyStatic(times(2));
        PxfUserGroupInformation.getKerberosMinMillisBeforeRelogin("server", diffConfiguration);
        PowerMockito.verifyStatic();
        PxfUserGroupInformation.reloginFromKeytab("server", expectedDiffLoginSession);

        PowerMockito.verifyNoMoreInteractions(PxfUserGroupInformation.class);
    }

    @Test
    public void testLoginKerberosReuseExistingLoginSessionWithResolvedHostnameInPrincipal() throws IOException {
        PowerMockito.mockStatic(PxfUserGroupInformation.class);
        when(PxfUserGroupInformation.getKerberosMinMillisBeforeRelogin("server", configuration)).thenReturn(90L);

        expectedUGI = UserGroupInformation.createUserForTesting("some", new String[]{});

        expectedLoginSession = new LoginSession("config", RESOLVED_PRINCIPAL, "/path/to/keytab", expectedUGI, null, 90);
        SecureLogin.addToCache("server", expectedLoginSession);

        configuration.set("hadoop.security.authentication", "kerberos");
        configuration.set(PROPERTY_KEY_SERVICE_PRINCIPAL, "principal/_HOST@REALM");
        configuration.set(PROPERTY_KEY_SERVICE_KEYTAB, "/path/to/keytab");
        configuration.set("hadoop.kerberos.min.seconds.before.relogin", "90");

        UserGroupInformation loginUGI = secureLogin.getLoginUser("server", "config", configuration);

        LoginSession loginSession = SecureLogin.getCache().get("server");
        assertEquals(1, SecureLogin.getCache().size());
        assertSame(expectedLoginSession, loginSession);
        assertSame(loginUGI, loginSession.getLoginUser());
        assertSame(expectedUGI, loginUGI); // since actual login was mocked, we should get back whatever we mocked

        // login should be never called, only re-login
        PowerMockito.verifyStatic();
        PxfUserGroupInformation.reloginFromKeytab("server", loginSession);
        PowerMockito.verifyStatic(never());
        PxfUserGroupInformation.loginUserFromKeytab(any(), any(), any(), any(), any());
    }

    /* ---------- methods to test service principal / keytab properties ---------- */

    @Test
    public void testPrincipalAbsentForServerNoSystemDefault() {
        assertNull(SecureLogin.getInstance().getServicePrincipal("default", configuration));
        assertNull(SecureLogin.getInstance().getServicePrincipal("any", configuration));
    }

    @Test
    public void testPrincipalAbsentForServerWithSystemDefault() {
        System.setProperty(PROPERTY_KEY_SERVICE_PRINCIPAL, "foo");
        assertEquals("foo", SecureLogin.getInstance().getServicePrincipal("default", configuration));
        assertNull(SecureLogin.getInstance().getServicePrincipal("any", configuration));
    }

    @Test
    public void testPrincipalSpecifiedForServer() {
        System.setProperty(PROPERTY_KEY_SERVICE_PRINCIPAL, "foo");
        configuration.set(PROPERTY_KEY_SERVICE_PRINCIPAL, "bar");
        assertEquals("bar", SecureLogin.getInstance().getServicePrincipal("default", configuration));
        assertEquals("bar", SecureLogin.getInstance().getServicePrincipal("any", configuration));
    }

    @Test
    public void testPrincipalGetsResolvedForServer() {
        configuration.set(PROPERTY_KEY_SERVICE_PRINCIPAL, "principal/_HOST@REALM");
        assertEquals(RESOLVED_PRINCIPAL, SecureLogin.getInstance().getServicePrincipal("any", configuration));
    }

    @Test
    public void testKeytabAbsentForServerNoSystemDefault() {
        assertNull(SecureLogin.getInstance().getServiceKeytab("default", configuration));
        assertNull(SecureLogin.getInstance().getServiceKeytab("any", configuration));
    }

    @Test
    public void testKeytabAbsentForServerWithSystemDefault() {
        System.setProperty(PROPERTY_KEY_SERVICE_KEYTAB, "foo");
        assertEquals("foo", SecureLogin.getInstance().getServiceKeytab("default", configuration));
        assertNull(SecureLogin.getInstance().getServiceKeytab("any", configuration));
    }

    @Test
    public void testKeytabSpecifiedForServer() {
        System.setProperty(PROPERTY_KEY_SERVICE_KEYTAB, "foo");
        configuration.set(PROPERTY_KEY_SERVICE_KEYTAB, "bar");
        assertEquals("bar", SecureLogin.getInstance().getServiceKeytab("default", configuration));
        assertEquals("bar", SecureLogin.getInstance().getServiceKeytab("any", configuration));
    }

    /* ---------- methods to test impersonation property ---------- */

    @Test
    public void testGlobalImpersonationPropertyAbsent() {
        assertFalse(secureLogin.isUserImpersonationEnabled(configuration));
    }

    @Test
    public void testGlobalImpersonationPropertyEmpty() {
        System.setProperty(PROPERTY_KEY_USER_IMPERSONATION, "");
        assertFalse(secureLogin.isUserImpersonationEnabled(configuration));
    }

    @Test
    public void testGlobalImpersonationPropertyFalse() {
        System.setProperty(PROPERTY_KEY_USER_IMPERSONATION, "foo");
        assertFalse(secureLogin.isUserImpersonationEnabled(configuration));
    }

    @Test
    public void testGlobalImpersonationPropertyTRUE() {
        System.setProperty(PROPERTY_KEY_USER_IMPERSONATION, "TRUE");
        assertTrue(secureLogin.isUserImpersonationEnabled(configuration));
    }

    @Test
    public void testGlobalImpersonationPropertyTrue() {
        System.setProperty(PROPERTY_KEY_USER_IMPERSONATION, "true");
        assertTrue(secureLogin.isUserImpersonationEnabled(configuration));
    }

    @Test
    public void testServerConfigurationImpersonationPropertyFalse() {
        configuration.set("pxf.service.user.impersonation", "foo");
        assertFalse(secureLogin.isUserImpersonationEnabled(configuration));
    }

    @Test
    public void testServerConfigurationImpersonationPropertyTrue() {
        configuration.set("pxf.service.user.impersonation", "true");
        assertTrue(secureLogin.isUserImpersonationEnabled(configuration));
    }

    @Test
    public void testServerConfigurationImpersonationPropertyTRUE() {
        configuration.set("pxf.service.user.impersonation", "TRUE");
        assertTrue(secureLogin.isUserImpersonationEnabled(configuration));
    }

    @Test
    public void testServerConfigurationImpersonationOverwritesGlobalTrue() {
        System.setProperty(PROPERTY_KEY_USER_IMPERSONATION, "false");
        configuration.set("pxf.service.user.impersonation", "true");
        assertTrue(secureLogin.isUserImpersonationEnabled(configuration));
    }

    @Test
    public void testServerConfigurationImpersonationOverwritesGlobalFalse() {
        System.setProperty(PROPERTY_KEY_USER_IMPERSONATION, "true");
        configuration.set("pxf.service.user.impersonation", "false");
        assertFalse(secureLogin.isUserImpersonationEnabled(configuration));
    }

}