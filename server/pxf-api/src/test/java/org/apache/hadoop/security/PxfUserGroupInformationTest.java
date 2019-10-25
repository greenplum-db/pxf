package org.apache.hadoop.security;

import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.authentication.util.KerberosUtil;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.kerberos.KerberosTicket;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.net.InetAddress;
import java.util.Date;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({PxfUserGroupInformation.class, UserGroupInformation.class, KerberosUtil.class, KerberosTicket.class})
public class PxfUserGroupInformationTest {

    private static final String hostname;
    static {
        try {
            hostname = InetAddress.getLocalHost().getCanonicalHostName();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private String serverName;
    private Configuration configuration;
    private UserGroupInformation ugi;
    private Subject subject;
    private User user;
    private LoginContext mockLoginContext, mockAnotherLoginContext;
    private KerberosTicket mockTGT;
    private KerberosPrincipal tgtPrincipal = new KerberosPrincipal("krbtgt/EXAMPLE.COM@EXAMPLE.COM");
    private KerberosPrincipal nonTgtPrincipal = new KerberosPrincipal("some/somewhere@EXAMPLE.COM");
    private LoginSession session;
    private long nowMs;


    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setup() throws Exception {

        // prepare objects
        nowMs = System.currentTimeMillis();
        configuration = new Configuration();
        user = new User("user");
        serverName = "server";

        // prepare common mocks
        mockTGT = PowerMockito.mock(KerberosTicket.class); // has final methods, needs PowerMock to mock it

        // subject will have a known User as principal and mock TGT credential, train it to have appropriate expiration
        subject = new Subject(false, Sets.newHashSet(user), Sets.newHashSet(), Sets.newHashSet(mockTGT));

        // train to return mock Login Context when created with constructor
        mockLoginContext = mock(LoginContext.class);
        PowerMockito.whenNew(LoginContext.class).withAnyArguments().thenReturn(mockLoginContext);

        // setup PUGI to use a known subject instead of creating a brand new one
        Supplier<Subject> subjectProvider = () -> subject;
        Whitebox.setInternalState(PxfUserGroupInformation.class, subjectProvider);
        doNothing().when(mockLoginContext).login();
    }

    @Test
    public void testLoginFromKeytabMinMillisFromConfig() throws Exception {
        configuration.set("hadoop.kerberos.min.seconds.before.relogin", "33");
        ugi = new UserGroupInformation(subject);

        session = PxfUserGroupInformation.loginUserFromKeytab(configuration, "server", "config-dir", "principal/_HOST@EXAMPLE.COM", "/path/to/keytab");

        // assert that the login session was created with properly wired up ugi/subject/user/loginContext
        assertEquals(33000, session.getKerberosMinMillisBeforeRelogin()); // will pick from configuration
        assertEquals("/path/to/keytab", session.getKeytabPath());
        assertEquals(String.format("principal/%s@EXAMPLE.COM", hostname), session.getPrincipalName());
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

        session = PxfUserGroupInformation.loginUserFromKeytab(configuration, "server", "config-dir", "principal/_HOST@EXAMPLE.COM", "/path/to/keytab");

        // assert that the login session was created with properly wired up ugi/subject/user/loginContext
        assertEquals(60000, session.getKerberosMinMillisBeforeRelogin()); // will pick from default
        assertEquals("/path/to/keytab", session.getKeytabPath());
        assertEquals(String.format("principal/%s@EXAMPLE.COM", hostname), session.getPrincipalName());
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
        PowerMockito.mockStatic(KerberosUtil.class);
        when(KerberosUtil.hasKerberosKeyTab(subject)).thenReturn(true);
        ugi = new UserGroupInformation(subject);
        // do NOT set authentication method of UGI to KERBEROS, will cause NOOP for relogin
        session = new LoginSession("config", "principal", "keytab", ugi, subject, 1);

        PxfUserGroupInformation.reloginFromKeytab(serverName, session);

        verifyZeroInteractions(mockLoginContext); // proves noop
    }

    @Test
    public void testReloginFromKeytabNoopForNonKeytab() throws KerberosAuthException {
        user.setLogin(mockLoginContext);
        PowerMockito.mockStatic(KerberosUtil.class);
        when(KerberosUtil.hasKerberosKeyTab(subject)).thenReturn(false); // simulate no keytab for subject
        ugi = new UserGroupInformation(subject);
        ugi.setAuthenticationMethod(UserGroupInformation.AuthenticationMethod.KERBEROS);
        session = new LoginSession("config", "principal", "keytab", ugi, subject, 1);

        PxfUserGroupInformation.reloginFromKeytab(serverName, session);

        verifyZeroInteractions(mockLoginContext); // proves noop
    }

    @Test
    public void testReloginFromKeytabNoopInsufficientTimeElapsed() throws KerberosAuthException {
        user.setLogin(mockLoginContext);
        PowerMockito.mockStatic(KerberosUtil.class);
        when(KerberosUtil.hasKerberosKeyTab(subject)).thenReturn(true);
        ugi = new UserGroupInformation(subject);
        ugi.setAuthenticationMethod(UserGroupInformation.AuthenticationMethod.KERBEROS);
        user.setLastLogin(nowMs); // simulate just logged in
        // set 33 secs between re-login attempts
        session = new LoginSession("config", "principal", "keytab", ugi, subject, 55000L);

        PxfUserGroupInformation.reloginFromKeytab(serverName, session);

        verifyZeroInteractions(mockLoginContext); // proves noop
    }

    @Test
    public void testReloginFromKeytabNoopTGTValidForLongTime() throws KerberosAuthException {
        user.setLogin(mockLoginContext);
        PowerMockito.mockStatic(KerberosUtil.class);
        when(KerberosUtil.hasKerberosKeyTab(subject)).thenReturn(true);
        when(KerberosUtil.getKrb5LoginModuleName()).thenReturn("com.sun.security.auth.module.Krb5LoginModule");

        when(mockTGT.getServer()).thenReturn(tgtPrincipal);

        // TGT validity started 1 hr ago, valid for another 1 hr from now, we are at 50% of renew window
        when(mockTGT.getStartTime()).thenReturn(new Date(nowMs - 3600 * 1000L));
        when(mockTGT.getEndTime()).thenReturn(new Date(nowMs + 3600 * 1000L));

        ugi = new UserGroupInformation(subject);
        ugi.setAuthenticationMethod(UserGroupInformation.AuthenticationMethod.KERBEROS);
        // leave user.lastLogin at 0 to simulate old login
        session = new LoginSession("config", "principal", "keytab", ugi, subject, 1);

        PxfUserGroupInformation.reloginFromKeytab(serverName, session);

        verifyZeroInteractions(mockLoginContext);
    }

    @Test
    public void testReloginFromKeytabFailsNoLogin() throws KerberosAuthException {
        expectedException.expect(KerberosAuthException.class);
        expectedException.expectMessage("loginUserFromKeyTab must be done first");

        user.setLogin(null); // simulate missing login context for the user
        PowerMockito.mockStatic(KerberosUtil.class);
        when(KerberosUtil.hasKerberosKeyTab(subject)).thenReturn(true);
        when(KerberosUtil.getKrb5LoginModuleName()).thenReturn("com.sun.security.auth.module.Krb5LoginModule");
        ugi = new UserGroupInformation(subject);
        ugi.setAuthenticationMethod(UserGroupInformation.AuthenticationMethod.KERBEROS);
        // leave user.lastLogin at 0 to simulate old login
        session = new LoginSession("config", "principal", "keytab", ugi, subject, 1);

        PxfUserGroupInformation.reloginFromKeytab(serverName, session);
    }

    @Test
    public void testReloginFromKeytabFailsNoKeytab() throws KerberosAuthException {
        expectedException.expect(KerberosAuthException.class);
        expectedException.expectMessage("loginUserFromKeyTab must be done first");

        user.setLogin(mockLoginContext);
        PowerMockito.mockStatic(KerberosUtil.class);
        when(KerberosUtil.hasKerberosKeyTab(subject)).thenReturn(true);
        when(KerberosUtil.getKrb5LoginModuleName()).thenReturn("com.sun.security.auth.module.Krb5LoginModule");
        ugi = new UserGroupInformation(subject);
        ugi.setAuthenticationMethod(UserGroupInformation.AuthenticationMethod.KERBEROS);
        // leave user.lastLogin at 0 to simulate old login
        session = new LoginSession("config", "principal", null, ugi, subject, 1);

        PxfUserGroupInformation.reloginFromKeytab(serverName, session);
    }

    /* ---------- Test below follow full login path via a few alternatives ---------- */

    @Test
    public void testReloginFromKeytabNoValidTGT() throws Exception {

        assertEquals(1, subject.getPrivateCredentials().size()); // subject has 1 ticket

        user.setLogin(mockLoginContext);
        PowerMockito.mockStatic(KerberosUtil.class);
        when(KerberosUtil.hasKerberosKeyTab(subject)).thenReturn(true);
        when(KerberosUtil.getKrb5LoginModuleName()).thenReturn("com.sun.security.auth.module.Krb5LoginModule");  // need for login

        when(mockTGT.getServer()).thenReturn(nonTgtPrincipal); // ticket is not from krbtgt, so not valid

        ugi = new UserGroupInformation(subject);
        ugi.setAuthenticationMethod(UserGroupInformation.AuthenticationMethod.KERBEROS);
        // leave user.lastLogin at 0 to simulate old login
        session = new LoginSession("config", "principal", "keytab", ugi, subject, 1);

        // train to return another LoginContext when it is constructed during re-login
        mockAnotherLoginContext = PowerMockito.mock(LoginContext.class);
        PowerMockito.whenNew(LoginContext.class).withAnyArguments().thenReturn(mockAnotherLoginContext);

        PxfUserGroupInformation.reloginFromKeytab(serverName, session);

        assertNotSame(mockLoginContext, user.getLogin());
        assertSame(mockAnotherLoginContext, user.getLogin());
        assertTrue(user.getLastLogin() > 0); // login timestamp is updated

        /* subject's non-TGT ticket has been removed, in reality another one would be created by login process,
         * but we are not mocking it here.
         */
        assertTrue(subject.getPrivateCredentials().isEmpty());

        verify(mockLoginContext).logout();
        verify(mockAnotherLoginContext).login();
        verify(mockTGT).destroy(); // subject's non-TGT ticket has been destroyed
    }

    @Test
    public void testReloginFromKeytabValidTGTWillExpireSoon() throws Exception {
        user.setLogin(mockLoginContext);
        PowerMockito.mockStatic(KerberosUtil.class);
        when(KerberosUtil.hasKerberosKeyTab(subject)).thenReturn(true);
        when(KerberosUtil.getKrb5LoginModuleName()).thenReturn("com.sun.security.auth.module.Krb5LoginModule");  // need for login

        when(mockTGT.getServer()).thenReturn(tgtPrincipal);

        // TGT validity started 1 hr ago, valid for another 10 mins, we are at 6/7 or 85% > 80% of renew window
        when(mockTGT.getStartTime()).thenReturn(new Date(nowMs - 3600 * 1000L));
        when(mockTGT.getEndTime()).thenReturn(new Date(nowMs + 600 * 1000L));

        ugi = new UserGroupInformation(subject);
        ugi.setAuthenticationMethod(UserGroupInformation.AuthenticationMethod.KERBEROS);
        // leave user.lastLogin at 0 to simulate old login
        session = new LoginSession("config", "principal", "keytab", ugi, subject, 1);

        // train to return another LoginContext when it is constructed during re-login
        mockAnotherLoginContext = PowerMockito.mock(LoginContext.class);
        PowerMockito.whenNew(LoginContext.class).withAnyArguments().thenReturn(mockAnotherLoginContext);

        PxfUserGroupInformation.reloginFromKeytab(serverName, session);

        assertNotSame(mockLoginContext, user.getLogin());
        assertSame(mockAnotherLoginContext, user.getLogin());
        assertTrue(user.getLastLogin() > 0); // login timestamp is updated

        verify(mockLoginContext).logout();
        verify(mockAnotherLoginContext).login();
    }

    @Test
    public void testReloginFromKeytabThrowsExceptionOnLoginFailure() throws Exception {
        expectedException.expect(KerberosAuthException.class);
        expectedException.expectMessage("Login failure for principal: principal from keytab keytab");

        user.setLogin(mockLoginContext);
        PowerMockito.mockStatic(KerberosUtil.class);
        when(KerberosUtil.hasKerberosKeyTab(subject)).thenReturn(true);
        when(KerberosUtil.getKrb5LoginModuleName()).thenReturn("com.sun.security.auth.module.Krb5LoginModule");  // need for login

        when(mockTGT.getServer()).thenReturn(nonTgtPrincipal); // ticket is not from krbtgt, so not valid

        ugi = new UserGroupInformation(subject);
        ugi.setAuthenticationMethod(UserGroupInformation.AuthenticationMethod.KERBEROS);
        // leave user.lastLogin at 0 to simulate old login
        session = new LoginSession("config", "principal", "keytab", ugi, subject, 1);

        // train to return another LoginContext when it is constructed during re-login
        mockAnotherLoginContext = PowerMockito.mock(LoginContext.class);
        PowerMockito.whenNew(LoginContext.class).withAnyArguments().thenReturn(mockAnotherLoginContext);
        doThrow(new LoginException("foo")).when(mockAnotherLoginContext).login(); // simulate login failure

        PxfUserGroupInformation.reloginFromKeytab(serverName, session);
    }

}
