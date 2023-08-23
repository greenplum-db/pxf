package org.apache.hadoop.security;

import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.kerberos.KerberosTicket;
import javax.security.auth.kerberos.KeyTab;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.util.Date;

import static org.apache.hadoop.security.PxfUserGroupInformation.PROPERTY_TICKET_RENEW_THRESHOLD;
import static org.greenplum.pxf.api.security.SecureLogin.DEFAULT_TICKET_RENEW_THRESHOLD;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

public class PxfUserGroupInformationTest {

    private String serverName;
    private Configuration configuration;
    private UserGroupInformation ugi;
    private Subject subject;
    private Subject subjectWithKerberosKeyTab;
    private User user;
    private LoginContext mockLoginContext, mockAnotherLoginContext;
    private PxfUserGroupInformation.LoginContextProvider mockLoginContextProvider;
    private KerberosTicket mockTGT;
    private final KerberosPrincipal tgtPrincipal = new KerberosPrincipal("krbtgt/EXAMPLE.COM@EXAMPLE.COM");
    private final KerberosPrincipal nonTgtPrincipal = new KerberosPrincipal("some/somewhere@EXAMPLE.COM");
    private LoginSession session;
    private long nowMs;
    private PxfUserGroupInformation pxfUserGroupInformation;

    private static final String PROPERTY_KEY_JAVA_VENDOR = "java.vendor";
    private static final String PROPERTY_KEY_KERBEROS_KDC = "java.security.krb5.kdc";
    private static final String PROPERTY_KEY_KERBEROS_REALM = "java.security.krb5.realm";
    private static String kdcDefault;
    private static String realmDefault;
    private static String javaVendor;

    @BeforeAll
    public static void setProperties() {
        // simulate presence of krb.conf file, important for prevention of test pollution when creating Users
        kdcDefault = System.setProperty(PROPERTY_KEY_KERBEROS_KDC, "localhost");
        realmDefault = System.setProperty(PROPERTY_KEY_KERBEROS_REALM, "DEFAULT_REALM");

        // Not IBM. Refer to org.apache.hadoop.security.authentication.util.KerberosUtil.getKrb5LoginModuleName
        javaVendor = System.setProperty(PROPERTY_KEY_JAVA_VENDOR, "foobar");
    }

    @AfterAll
    public static void resetProperties() {
        resetProperty(PROPERTY_KEY_JAVA_VENDOR, javaVendor);
        resetProperty(PROPERTY_KEY_KERBEROS_KDC, kdcDefault);
        resetProperty(PROPERTY_KEY_KERBEROS_REALM, realmDefault);
    }

    @BeforeEach
    public void setup() throws Exception {

        // prepare objects
        nowMs = System.currentTimeMillis();
        configuration = new Configuration();
        configuration.setFloat(PROPERTY_TICKET_RENEW_THRESHOLD, DEFAULT_TICKET_RENEW_THRESHOLD);
        user = new User("user");
        serverName = "server";
        pxfUserGroupInformation = new PxfUserGroupInformation();

        // prepare common mocks
        mockTGT = mock(KerberosTicket.class);

        KeyTab mockKeyTab = mock(KeyTab.class);
        // subject will have a known User as principal and mock TGT credential, train it to have appropriate expiration
        subject = new Subject(false, Sets.newHashSet(user), Sets.newHashSet(), Sets.newHashSet(mockTGT));

        // subject with a Kerberos Keytab
        subjectWithKerberosKeyTab = new Subject(false, Sets.newHashSet(user), Sets.newHashSet(), Sets.newHashSet(mockTGT, mockKeyTab));

        // train to return mock Login Context when created with constructor
        mockLoginContext = mock(LoginContext.class);
        mockLoginContextProvider = mock(PxfUserGroupInformation.LoginContextProvider.class);
        when(mockLoginContextProvider.newLoginContext(anyString(), any(), any())).thenReturn(mockLoginContext);

        pxfUserGroupInformation.loginContextProvider = mockLoginContextProvider;

        // setup PUGI to use a known subject instead of creating a brand new one
        pxfUserGroupInformation.subjectProvider = () -> subject;
        doNothing().when(mockLoginContext).login();
    }

    @Test
    public void testLoginFromKeytabMinMillisFromConfig() throws Exception {
        configuration.set("hadoop.kerberos.min.seconds.before.relogin", "33");
        ugi = new UserGroupInformation(subject);

        session = pxfUserGroupInformation.loginUserFromKeytab(configuration, "server", "config-dir", "principal/some.host.com@EXAMPLE.COM", "/path/to/keytab");

        // assert that the login session was created with properly wired up ugi/subject/user/loginContext
        assertEquals(33000, session.getKerberosMinMillisBeforeRelogin()); // will pick from configuration
        assertEquals("/path/to/keytab", session.getKeytabPath());
        assertEquals("principal/some.host.com@EXAMPLE.COM", session.getPrincipalName());
        assertEquals(ugi, session.getLoginUser()); // UGI equality only compares enclosed subjects
        assertNotSame(ugi, session.getLoginUser()); // UGI equality only compares enclosed subjects
        assertSame(subject, session.getSubject());
        assertSame(user, session.getUser());
        assertSame(mockLoginContext, session.getUser().getLogin());
        assertEquals(UserGroupInformation.AuthenticationMethod.KERBEROS, session.getLoginUser().getAuthenticationMethod());

        // verify that login() was called
        verify(mockLoginContext).login();
    }

    @Test
    public void testLoginFromKeytabMinMillisFromDefault() throws Exception {
        ugi = new UserGroupInformation(subject);

        session = pxfUserGroupInformation.loginUserFromKeytab(configuration, "server", "config-dir", "principal/some.host.com@EXAMPLE.COM", "/path/to/keytab");

        // assert that the login session was created with properly wired up ugi/subject/user/loginContext
        assertEquals(60000, session.getKerberosMinMillisBeforeRelogin()); // will pick from default
        assertEquals("/path/to/keytab", session.getKeytabPath());
        assertEquals("principal/some.host.com@EXAMPLE.COM", session.getPrincipalName());
        assertEquals(ugi, session.getLoginUser()); // UGI equality only compares enclosed subjects
        assertNotSame(ugi, session.getLoginUser()); // UGI equality only compares enclosed subjects
        assertSame(subject, session.getSubject());
        assertSame(user, session.getUser());
        assertSame(mockLoginContext, session.getUser().getLogin());
        assertEquals(UserGroupInformation.AuthenticationMethod.KERBEROS, session.getLoginUser().getAuthenticationMethod());

        // verify that login() was called
        verify(mockLoginContext).login();
    }

    /* ---------- Test below follow either cause no re-login (noop) or error out ---------- */

    @Test
    public void testReloginFromKeytabNoopForNonKerberos() throws KerberosAuthException {
        user.setLogin(mockLoginContext);
        ugi = new UserGroupInformation(subjectWithKerberosKeyTab);
        // do NOT set authentication method of UGI to KERBEROS, will cause NOOP for relogin
        session = new LoginSession("config", "principal", "keytab", ugi, subjectWithKerberosKeyTab, 1);

        pxfUserGroupInformation.reloginFromKeytab(serverName, session, DEFAULT_TICKET_RENEW_THRESHOLD);

        verifyNoInteractions(mockLoginContext); // proves noop
    }

    @Test
    public void testReloginFromKeytabNoopForNonKeytab() throws KerberosAuthException {
        user.setLogin(mockLoginContext);
        ugi = new UserGroupInformation(subject);
        ugi.setAuthenticationMethod(UserGroupInformation.AuthenticationMethod.KERBEROS);
        session = new LoginSession("config", "principal", "keytab", ugi, subject, 1);

        pxfUserGroupInformation.reloginFromKeytab(serverName, session, DEFAULT_TICKET_RENEW_THRESHOLD);

        verifyNoInteractions(mockLoginContext); // proves noop
    }

    @Test
    public void testReloginFromKeytabNoopInsufficientTimeElapsed() throws KerberosAuthException {
        user.setLogin(mockLoginContext);
        ugi = new UserGroupInformation(subjectWithKerberosKeyTab);
        ugi.setAuthenticationMethod(UserGroupInformation.AuthenticationMethod.KERBEROS);
        user.setLastLogin(nowMs); // simulate just logged in
        // set 33 secs between re-login attempts
        session = new LoginSession("config", "principal", "keytab", ugi, subjectWithKerberosKeyTab, 55000L);

        pxfUserGroupInformation.reloginFromKeytab(serverName, session, DEFAULT_TICKET_RENEW_THRESHOLD);

        verifyNoInteractions(mockLoginContext); // proves noop
    }

    @Test
    public void testReloginFromKeytabNoopTGTValidForLongTime() throws KerberosAuthException {
        user.setLogin(mockLoginContext);
        when(mockTGT.getServer()).thenReturn(tgtPrincipal);

        // TGT validity started 1 hr ago, valid for another 1 hr from now, we are at 50% of renew window
        when(mockTGT.getStartTime()).thenReturn(new Date(nowMs - 3600 * 1000L));
        when(mockTGT.getEndTime()).thenReturn(new Date(nowMs + 3600 * 1000L));

        ugi = new UserGroupInformation(subjectWithKerberosKeyTab);
        ugi.setAuthenticationMethod(UserGroupInformation.AuthenticationMethod.KERBEROS);
        // leave user.lastLogin at 0 to simulate old login
        session = new LoginSession("config", "principal", "keytab", ugi, subjectWithKerberosKeyTab, 1);

        pxfUserGroupInformation.reloginFromKeytab(serverName, session, DEFAULT_TICKET_RENEW_THRESHOLD);

        verifyNoInteractions(mockLoginContext);
    }

    @Test
    public void testReloginFromKeytabFailsNoLogin() {
        user.setLogin(null); // simulate missing login context for the user
        ugi = new UserGroupInformation(subjectWithKerberosKeyTab);
        ugi.setAuthenticationMethod(UserGroupInformation.AuthenticationMethod.KERBEROS);
        // leave user.lastLogin at 0 to simulate old login
        session = new LoginSession("config", "principal", "keytab", ugi, subjectWithKerberosKeyTab, 1);

        Exception e = assertThrows(KerberosAuthException.class,
                () -> pxfUserGroupInformation.reloginFromKeytab(serverName, session, DEFAULT_TICKET_RENEW_THRESHOLD));
        assertEquals(" loginUserFromKeyTab must be done first", e.getMessage());
    }

    @Test
    public void testReloginFromKeytabFailsNoKeytab() {
        user.setLogin(mockLoginContext);
        ugi = new UserGroupInformation(subjectWithKerberosKeyTab);
        ugi.setAuthenticationMethod(UserGroupInformation.AuthenticationMethod.KERBEROS);
        // leave user.lastLogin at 0 to simulate old login
        session = new LoginSession("config", "principal", null, ugi, subjectWithKerberosKeyTab, 1);

        Exception e = assertThrows(KerberosAuthException.class,
                () -> pxfUserGroupInformation.reloginFromKeytab(serverName, session, DEFAULT_TICKET_RENEW_THRESHOLD));
        assertEquals(" loginUserFromKeyTab must be done first", e.getMessage());
    }

    /* ---------- Test below follow full login path via a few alternatives ---------- */

    @Test
    public void testReloginFromKeytabNoValidTGT() throws Exception {

        assertEquals(2, subjectWithKerberosKeyTab.getPrivateCredentials().size()); // subject has 2 tickets

        user.setLogin(mockLoginContext);
        when(mockTGT.getServer()).thenReturn(nonTgtPrincipal); // ticket is not from krbtgt, so not valid

        ugi = new UserGroupInformation(subjectWithKerberosKeyTab);
        ugi.setAuthenticationMethod(UserGroupInformation.AuthenticationMethod.KERBEROS);
        // leave user.lastLogin at 0 to simulate old login
        session = new LoginSession("config", "principal", "keytab", ugi, subjectWithKerberosKeyTab, 1);

        // train to return another LoginContext when it is constructed during re-login
        mockAnotherLoginContext = mock(LoginContext.class);
        when(mockLoginContextProvider.newLoginContext(anyString(), any(), any())).thenReturn(mockAnotherLoginContext);

        pxfUserGroupInformation.reloginFromKeytab(serverName, session, DEFAULT_TICKET_RENEW_THRESHOLD);

        assertNotSame(mockLoginContext, user.getLogin());
        assertSame(mockAnotherLoginContext, user.getLogin());
        assertTrue(user.getLastLogin() > 0); // login timestamp is updated

        /* subject's non-TGT ticket has been removed, in reality another one would be created by login process,
         * but we are not mocking it here.
         */
        assertEquals(1, subject.getPrivateCredentials().size());

        verify(mockLoginContext).logout();
        verify(mockAnotherLoginContext).login();
        verify(mockTGT).destroy(); // subject's non-TGT ticket has been destroyed
    }

    @Test
    public void testReloginFromKeytabValidTGTWillExpireSoon() throws Exception {
        user.setLogin(mockLoginContext);
        when(mockTGT.getServer()).thenReturn(tgtPrincipal);

        // TGT validity started 1 hr ago, valid for another 10 mins, we are at 6/7 or 85% > 80% of renew window
        when(mockTGT.getStartTime()).thenReturn(new Date(nowMs - 3600 * 1000L));
        when(mockTGT.getEndTime()).thenReturn(new Date(nowMs + 600 * 1000L));

        ugi = new UserGroupInformation(subjectWithKerberosKeyTab);
        ugi.setAuthenticationMethod(UserGroupInformation.AuthenticationMethod.KERBEROS);
        // leave user.lastLogin at 0 to simulate old login
        session = new LoginSession("config", "principal", "keytab", ugi, subjectWithKerberosKeyTab, 1);

        // train to return another LoginContext when it is constructed during re-login
        mockAnotherLoginContext = mock(LoginContext.class);
        when(mockLoginContextProvider.newLoginContext(anyString(), any(), any())).thenReturn(mockAnotherLoginContext);

        pxfUserGroupInformation.reloginFromKeytab(serverName, session, DEFAULT_TICKET_RENEW_THRESHOLD);

        assertNotSame(mockLoginContext, user.getLogin());
        assertSame(mockAnotherLoginContext, user.getLogin());
        assertTrue(user.getLastLogin() > 0); // login timestamp is updated

        verify(mockLoginContext).logout();
        verify(mockAnotherLoginContext).login();
    }

    @Test
    public void testReloginFromKeytabThrowsExceptionOnLoginFailure() throws Exception {

        user.setLogin(mockLoginContext);
        when(mockTGT.getServer()).thenReturn(nonTgtPrincipal); // ticket is not from krbtgt, so not valid

        ugi = new UserGroupInformation(subjectWithKerberosKeyTab);
        ugi.setAuthenticationMethod(UserGroupInformation.AuthenticationMethod.KERBEROS);
        // leave user.lastLogin at 0 to simulate old login
        session = new LoginSession("config", "principal", "keytab", ugi, subjectWithKerberosKeyTab, 1);

        // train to return another LoginContext when it is constructed during re-login
        mockAnotherLoginContext = mock(LoginContext.class);
        when(mockLoginContextProvider.newLoginContext(anyString(), any(), any())).thenReturn(mockAnotherLoginContext);
        doThrow(new LoginException("foo")).when(mockAnotherLoginContext).login(); // simulate login failure

        Exception e = assertThrows(KerberosAuthException.class,
                () -> pxfUserGroupInformation.reloginFromKeytab(serverName, session, DEFAULT_TICKET_RENEW_THRESHOLD));
        assertEquals("Login failure for principal: principal from keytab keytab javax.security.auth.login.LoginException: foo", e.getMessage());
    }

    private static void resetProperty(String key, String val) {
        if (val != null) {
            System.setProperty(key, val);
            return;
        }
        System.clearProperty(key);
    }
}
