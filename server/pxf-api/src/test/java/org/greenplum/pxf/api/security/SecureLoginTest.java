package org.greenplum.pxf.api.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.LoginSession;
import org.apache.hadoop.security.PxfUserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetAddress;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class SecureLoginTest {

    private static final String PROPERTY_KEY_SERVICE_PRINCIPAL = "pxf.service.kerberos.principal";
    private static final String PROPERTY_KEY_SERVICE_KEYTAB = "pxf.service.kerberos.keytab";
    private static final String PROPERTY_KEY_KERBEROS_KDC = "java.security.krb5.kdc";
    private static final String PROPERTY_KEY_KERBEROS_REALM = "java.security.krb5.realm";
    private static String kerberosPrincipal;
    private static String kerberosKeytab;
    private static String kdcDefault;
    private static String realmDefault;

    @BeforeAll
    public static void getProperties() {
        kerberosPrincipal = System.getProperty(PROPERTY_KEY_SERVICE_PRINCIPAL);
        kerberosKeytab = System.getProperty(PROPERTY_KEY_SERVICE_KEYTAB);
        kdcDefault = System.getProperty(PROPERTY_KEY_KERBEROS_KDC);
        realmDefault = System.getProperty(PROPERTY_KEY_KERBEROS_REALM);
    }

    @AfterAll
    public static void resetProperties() {
        resetProperty(PROPERTY_KEY_SERVICE_PRINCIPAL, kerberosPrincipal);
        resetProperty(PROPERTY_KEY_SERVICE_KEYTAB, kerberosKeytab);
        resetProperty(PROPERTY_KEY_KERBEROS_KDC, kdcDefault);
        resetProperty(PROPERTY_KEY_KERBEROS_REALM, realmDefault);
        UserGroupInformation.reset(); // clean up so that downstream tests in the same JVM are not affected
    }

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
    private PxfUserGroupInformation pxfUserGroupInformationMock;


    @BeforeEach
    public void setup() {
        pxfUserGroupInformationMock = mock(PxfUserGroupInformation.class);
        secureLogin = new SecureLogin(pxfUserGroupInformationMock);
        SecureLogin.reset();
        configuration = new Configuration();
        System.clearProperty(PROPERTY_KEY_SERVICE_PRINCIPAL);
        System.clearProperty(PROPERTY_KEY_SERVICE_KEYTAB);
        // simulate presence of krb.conf file
        System.setProperty(PROPERTY_KEY_KERBEROS_KDC, "localhost");
        System.setProperty(PROPERTY_KEY_KERBEROS_REALM, "DEFAULT_REALM");
    }

    /* ---------- methods to test login / relogin ---------- */

    @Test
    public void testLoginNoKerberosNoServiceUser() throws IOException {
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

        verifyNoInteractions(pxfUserGroupInformationMock);
    }

    @Test
    public void testLoginNoKerberosNoServiceUserWhenConfigurationValuesAreProvided() throws IOException {
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

        verifyNoInteractions(pxfUserGroupInformationMock);
    }

    @Test
    public void testLoginNoKerberosWithServiceUser() throws IOException {
        expectedLoginSession = new LoginSession("config", null, null, null, null, 0, 0);
        configuration.set("pxf.service.user.name", "foo");

        UserGroupInformation loginUGI = secureLogin.getLoginUser("server", "config", configuration);

        LoginSession loginSession = SecureLogin.getCache().get("server");
        assertEquals(1, SecureLogin.getCache().size());
        assertEquals(expectedLoginSession, loginSession);
        assertSame(loginUGI, loginSession.getLoginUser());
        assertEquals("foo", loginUGI.getUserName());
        assertNull(loginSession.getSubject());
        assertNull(loginSession.getUser());

        verifyNoInteractions(pxfUserGroupInformationMock);
    }

    @Test
    public void testLoginKerberosFailsWhenNoPrincipal() {
        configuration.set("hadoop.security.authentication", "kerberos");

        Exception e = assertThrows(RuntimeException.class,
                () -> secureLogin.getLoginUser("test", "config", configuration));
        assertEquals("PXF service login failed for server test : Kerberos Security for server test requires a valid principal.", e.getMessage());
    }

    @Test
    public void testLoginKerberosFailsWhenNoKeytab() {
        configuration.set("hadoop.security.authentication", "kerberos");
        configuration.set(PROPERTY_KEY_SERVICE_PRINCIPAL, "foo");

        Exception e = assertThrows(RuntimeException.class,
                () -> secureLogin.getLoginUser("test", "config", configuration));
        assertEquals("PXF service login failed for server test : Kerberos Security for server test requires a valid keytab file name.", e.getMessage());
    }

    @Test
    public void testLoginKerberosFirstTime() throws IOException {
        expectedUGI = UserGroupInformation.createUserForTesting("some", new String[]{});
        expectedLoginSession = new LoginSession("config", "principal", "/path/to/keytab", expectedUGI, null, 0, 0.8f);
        configuration.set("hadoop.security.authentication", "kerberos");
        configuration.set(PROPERTY_KEY_SERVICE_PRINCIPAL, "principal");
        configuration.set(PROPERTY_KEY_SERVICE_KEYTAB, "/path/to/keytab");
        configuration.set("hadoop.kerberos.min.seconds.before.relogin", "90");
        when(pxfUserGroupInformationMock.loginUserFromKeytab(configuration, "server", "config", "principal", "/path/to/keytab")).thenReturn(expectedLoginSession);

        UserGroupInformation loginUGI = secureLogin.getLoginUser("server", "config", configuration);

        LoginSession loginSession = SecureLogin.getCache().get("server");
        assertEquals(1, SecureLogin.getCache().size());
        assertEquals(expectedLoginSession, loginSession);
        assertSame(loginUGI, loginSession.getLoginUser());
        assertSame(expectedUGI, loginUGI); // since actual login was mocked, we should get back whatever we mocked

        verify(pxfUserGroupInformationMock).loginUserFromKeytab(configuration, "server", "config", "principal", "/path/to/keytab");
        verify(pxfUserGroupInformationMock).reloginFromKeytab("server", expectedLoginSession);

        verifyNoMoreInteractions(pxfUserGroupInformationMock);
    }

    @Test
    public void testLoginKerberosSameSession() throws IOException {
        expectedUGI = UserGroupInformation.createUserForTesting("some", new String[]{});
        expectedLoginSession = new LoginSession("config", "principal", "/path/to/keytab", expectedUGI, null, 90000, 0.8f);
        configuration.set("hadoop.security.authentication", "kerberos");
        configuration.set(PROPERTY_KEY_SERVICE_PRINCIPAL, "principal");
        configuration.set(PROPERTY_KEY_SERVICE_KEYTAB, "/path/to/keytab");
        configuration.set("hadoop.kerberos.min.seconds.before.relogin", "90");
        when(pxfUserGroupInformationMock.loginUserFromKeytab(configuration, "server", "config", "principal", "/path/to/keytab")).thenReturn(expectedLoginSession);
        when(pxfUserGroupInformationMock.getKerberosMinMillisBeforeRelogin("server", configuration)).thenReturn(90000L);

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

        verify(pxfUserGroupInformationMock).loginUserFromKeytab(configuration, "server", "config", "principal", "/path/to/keytab");
        verify(pxfUserGroupInformationMock).getKerberosMinMillisBeforeRelogin("server", configuration);
        // 1 extra relogin call
        verify(pxfUserGroupInformationMock, times(2)).reloginFromKeytab("server", expectedLoginSession);

        verifyNoMoreInteractions(pxfUserGroupInformationMock);
    }

    @Test
    public void testLoginKerberosDifferentServer() throws IOException {
        expectedUGI = UserGroupInformation.createUserForTesting("some", new String[]{});

        expectedLoginSession = new LoginSession("config", "principal", "/path/to/keytab", expectedUGI, null, 0, 0.8f);

        configuration.set("hadoop.security.authentication", "kerberos");
        configuration.set(PROPERTY_KEY_SERVICE_PRINCIPAL, "principal");
        configuration.set(PROPERTY_KEY_SERVICE_KEYTAB, "/path/to/keytab");
        when(pxfUserGroupInformationMock.loginUserFromKeytab(configuration, "server", "config", "principal", "/path/to/keytab")).thenReturn(expectedLoginSession);

        UserGroupInformation loginUGI = secureLogin.getLoginUser("server", "config", configuration);

        LoginSession loginSession = SecureLogin.getCache().get("server");
        assertEquals(1, SecureLogin.getCache().size());
        assertEquals(expectedLoginSession, loginSession);
        assertSame(loginUGI, loginSession.getLoginUser());
        assertSame(expectedUGI, loginUGI); // since actual login was mocked, we should get back whatever we mocked


        // ------------------- now login for another server -------------------------

        LoginSession expectedDiffLoginSession = new LoginSession("diff-config", "principal", "/path/to/keytab", expectedUGI, null, 0, 0.8f);
        Configuration diffConfiguration = new Configuration();
        diffConfiguration.set("hadoop.security.authentication", "kerberos");
        diffConfiguration.set(PROPERTY_KEY_SERVICE_PRINCIPAL, "principal");
        diffConfiguration.set(PROPERTY_KEY_SERVICE_KEYTAB, "/path/to/keytab");
        when(pxfUserGroupInformationMock.loginUserFromKeytab(diffConfiguration, "diff-server", "diff-config", "principal", "/path/to/keytab")).thenReturn(expectedDiffLoginSession);

        UserGroupInformation diffLoginUGI = secureLogin.getLoginUser("diff-server", "diff-config", diffConfiguration);

        LoginSession diffLoginSession = SecureLogin.getCache().get("diff-server");
        assertEquals(2, SecureLogin.getCache().size());
        assertEquals(expectedDiffLoginSession, diffLoginSession);
        assertSame(loginUGI, diffLoginSession.getLoginUser());
        assertSame(expectedUGI, diffLoginUGI); // since actual login was mocked, we should get back whatever we mocked
        assertNotSame(loginSession, diffLoginSession); // should be different object

        verify(pxfUserGroupInformationMock).loginUserFromKeytab(configuration, "server", "config", "principal", "/path/to/keytab");
        verify(pxfUserGroupInformationMock).reloginFromKeytab("server", expectedLoginSession);

        verify(pxfUserGroupInformationMock).loginUserFromKeytab(diffConfiguration, "diff-server", "diff-config", "principal", "/path/to/keytab");
        verify(pxfUserGroupInformationMock).reloginFromKeytab("diff-server", expectedDiffLoginSession);

        verifyNoMoreInteractions(pxfUserGroupInformationMock);
    }

    @Test
    public void testLoginKerberosSameServerDifferentPrincipal() throws IOException {
        expectedUGI = UserGroupInformation.createUserForTesting("some", new String[]{});

        expectedLoginSession = new LoginSession("config", "principal", "/path/to/keytab", expectedUGI, null, 0, 0.8f);

        configuration.set("hadoop.security.authentication", "kerberos");
        configuration.set(PROPERTY_KEY_SERVICE_PRINCIPAL, "principal");
        configuration.set(PROPERTY_KEY_SERVICE_KEYTAB, "/path/to/keytab");
        configuration.set("hadoop.kerberos.min.seconds.before.relogin", "90");
        when(pxfUserGroupInformationMock.loginUserFromKeytab(configuration, "server", "config", "principal", "/path/to/keytab")).thenReturn(expectedLoginSession);

        UserGroupInformation loginUGI = secureLogin.getLoginUser("server", "config", configuration);

        LoginSession loginSession = SecureLogin.getCache().get("server");
        assertEquals(1, SecureLogin.getCache().size());
        assertEquals(expectedLoginSession, loginSession);
        assertSame(loginUGI, loginSession.getLoginUser());
        assertSame(expectedUGI, loginUGI); // since actual login was mocked, we should get back whatever we mocked

        // ------------------- now change the principal in the configuration, login again -------------------------

        LoginSession expectedDiffLoginSession = new LoginSession("config", "diff-principal", "/path/to/keytab", expectedUGI, null, 90000, 0.8f);
        Configuration diffConfiguration = new Configuration();
        diffConfiguration.set("hadoop.security.authentication", "kerberos");
        diffConfiguration.set(PROPERTY_KEY_SERVICE_PRINCIPAL, "diff-principal");
        diffConfiguration.set(PROPERTY_KEY_SERVICE_KEYTAB, "/path/to/keytab");
        diffConfiguration.set("hadoop.kerberos.min.seconds.before.relogin", "180");
        when(pxfUserGroupInformationMock.loginUserFromKeytab(configuration, "server", "config", "principal", "/path/to/keytab")).thenReturn(expectedLoginSession);
        when(pxfUserGroupInformationMock.loginUserFromKeytab(diffConfiguration, "server", "config", "diff-principal", "/path/to/keytab")).thenReturn(expectedDiffLoginSession);
        when(pxfUserGroupInformationMock.getKerberosMinMillisBeforeRelogin("server", diffConfiguration)).thenReturn(180000L);

        UserGroupInformation diffLoginUGI = secureLogin.getLoginUser("server", "config", diffConfiguration);

        LoginSession diffLoginSession = SecureLogin.getCache().get("server");
        assertEquals(1, SecureLogin.getCache().size());
        assertEquals(expectedDiffLoginSession, diffLoginSession);
        assertSame(loginUGI, diffLoginSession.getLoginUser());
        assertSame(expectedUGI, diffLoginUGI); // since actual login was mocked, we should get back whatever we mocked
        assertNotSame(loginSession, diffLoginSession); // should be different object

        verify(pxfUserGroupInformationMock).loginUserFromKeytab(configuration, "server", "config", "principal", "/path/to/keytab");
        verify(pxfUserGroupInformationMock).reloginFromKeytab("server", expectedLoginSession);

        verify(pxfUserGroupInformationMock).loginUserFromKeytab(diffConfiguration, "server", "config", "diff-principal", "/path/to/keytab");
        verify(pxfUserGroupInformationMock, times(2)).getKerberosMinMillisBeforeRelogin("server", diffConfiguration);
        verify(pxfUserGroupInformationMock).reloginFromKeytab("server", expectedDiffLoginSession);

        verifyNoMoreInteractions(pxfUserGroupInformationMock);
    }

    @Test
    public void testLoginKerberosReuseExistingLoginSessionWithResolvedHostnameInPrincipal() throws IOException {
        when(pxfUserGroupInformationMock.getKerberosMinMillisBeforeRelogin("server", configuration)).thenReturn(90L);

        expectedUGI = UserGroupInformation.createUserForTesting("some", new String[]{});

        expectedLoginSession = new LoginSession("config", RESOLVED_PRINCIPAL, "/path/to/keytab", expectedUGI, null, 90, 0.8f);
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
        verify(pxfUserGroupInformationMock).reloginFromKeytab("server", loginSession);
        verify(pxfUserGroupInformationMock, never()).loginUserFromKeytab(any(), any(), any(), any(), any());
    }

    /* ---------- methods to test service principal / keytab properties ---------- */

    @Test
    public void testPrincipalAbsentForServerNoSystemDefault() {
        SecureLogin secureLogin = new SecureLogin(pxfUserGroupInformationMock);
        assertNull(secureLogin.getServicePrincipal("default", configuration));
        assertNull(secureLogin.getServicePrincipal("any", configuration));
    }

    @Test
    public void testPrincipalAbsentForServerWithSystemDefault() {
        System.setProperty(PROPERTY_KEY_SERVICE_PRINCIPAL, "foo");
        SecureLogin secureLogin = new SecureLogin(pxfUserGroupInformationMock);
        assertEquals("foo", secureLogin.getServicePrincipal("default", configuration));
        assertNull(secureLogin.getServicePrincipal("any", configuration));
    }

    @Test
    public void testPrincipalSpecifiedForServer() {
        System.setProperty(PROPERTY_KEY_SERVICE_PRINCIPAL, "foo");
        configuration.set(PROPERTY_KEY_SERVICE_PRINCIPAL, "bar");
        SecureLogin secureLogin = new SecureLogin(pxfUserGroupInformationMock);
        assertEquals("bar", secureLogin.getServicePrincipal("default", configuration));
        assertEquals("bar", secureLogin.getServicePrincipal("any", configuration));
    }

    @Test
    public void testPrincipalGetsResolvedForServer() {
        configuration.set(PROPERTY_KEY_SERVICE_PRINCIPAL, "principal/_HOST@REALM");
        SecureLogin secureLogin = new SecureLogin(pxfUserGroupInformationMock);
        assertEquals(RESOLVED_PRINCIPAL, secureLogin.getServicePrincipal("any", configuration));
    }

    @Test
    public void testKeytabAbsentForServerNoSystemDefault() {
        SecureLogin secureLogin = new SecureLogin(pxfUserGroupInformationMock);
        assertNull(secureLogin.getServiceKeytab("default", configuration));
        assertNull(secureLogin.getServiceKeytab("any", configuration));
    }

    @Test
    public void testKeytabAbsentForServerWithSystemDefault() {
        System.setProperty(PROPERTY_KEY_SERVICE_KEYTAB, "foo");
        SecureLogin secureLogin = new SecureLogin(pxfUserGroupInformationMock);
        assertEquals("foo", secureLogin.getServiceKeytab("default", configuration));
        assertNull(secureLogin.getServiceKeytab("any", configuration));
    }

    @Test
    public void testKeytabSpecifiedForServer() {
        System.setProperty(PROPERTY_KEY_SERVICE_KEYTAB, "foo");
        configuration.set(PROPERTY_KEY_SERVICE_KEYTAB, "bar");
        SecureLogin secureLogin = new SecureLogin(pxfUserGroupInformationMock);
        assertEquals("bar", secureLogin.getServiceKeytab("default", configuration));
        assertEquals("bar", secureLogin.getServiceKeytab("any", configuration));
    }

    /* ---------- methods to test impersonation property ---------- */

    @Test
    public void testGlobalImpersonationPropertyAbsent() {
        assertTrue(secureLogin.isUserImpersonationEnabled(configuration));
    }

    @Test
    public void testServerConfigurationImpersonationPropertyJunk() {
        // true if missing or non-parsable boolean
        configuration.set("pxf.service.user.impersonation", "junk");
        assertTrue(secureLogin.isUserImpersonationEnabled(configuration));
    }

    @Test
    public void testServerConfigurationImpersonationPropertyFalse() {
        configuration.set("pxf.service.user.impersonation", "false");
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

    /* ---------- methods to test constrained delegation property ---------- */

    @Test
    public void testGlobalConstrainedDelegationPropertyAbsent() {
        assertFalse(secureLogin.isConstrainedDelegationEnabled(configuration));
    }

    @Test
    public void testServerConfigurationConstrainedDelegationPropertyJunk() {
        // false if missing or non-parsable boolean
        configuration.set("pxf.service.kerberos.constrained-delegation", "junk");
        assertFalse(secureLogin.isConstrainedDelegationEnabled(configuration));
    }

    @Test
    public void testServerConfigurationConstrainedDelegationPropertyFalse() {
        configuration.set("pxf.service.kerberos.constrained-delegation", "false");
        assertFalse(secureLogin.isConstrainedDelegationEnabled(configuration));
    }

    @Test
    public void testServerConfigurationConstrainedDelegationPropertyTrue() {
        configuration.set("pxf.service.kerberos.constrained-delegation", "true");
        assertTrue(secureLogin.isUserImpersonationEnabled(configuration));
    }

    @Test
    public void testServerConfigurationConstrainedDelegationPropertyTRUE() {
        configuration.set("pxf.service.kerberos.constrained-delegation", "TRUE");
        assertTrue(secureLogin.isUserImpersonationEnabled(configuration));
    }

    private static void resetProperty(String key, String val) {
        if (val != null) {
            System.setProperty(key, val);
            return;
        }
        System.clearProperty(key);
    }
}