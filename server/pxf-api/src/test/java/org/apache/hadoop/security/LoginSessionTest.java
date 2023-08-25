package org.apache.hadoop.security;

import com.google.common.collect.Sets;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.security.auth.Subject;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

public class LoginSessionTest {

    private LoginSession session, sessionFoo, sessionBar;
    private UserGroupInformation ugiFoo, ugiBar;
    private Subject subjectFoo, subjectBar;
    private User userFoo, userBar;

    private static final String PROPERTY_KEY_KERBEROS_KDC = "java.security.krb5.kdc";
    private static final String PROPERTY_KEY_KERBEROS_REALM = "java.security.krb5.realm";
    private static String kdcDefault;
    private static String realmDefault;

    @BeforeAll
    public static void setProperties() {
        // simulate presence of krb.conf file, important for prevention of test pollution when creating Users
        kdcDefault = System.setProperty(PROPERTY_KEY_KERBEROS_KDC, "localhost");
        realmDefault = System.setProperty(PROPERTY_KEY_KERBEROS_REALM, "DEFAULT_REALM");
    }

    @AfterAll
    public static void resetProperties() {
        resetProperty(PROPERTY_KEY_KERBEROS_KDC, kdcDefault);
        resetProperty(PROPERTY_KEY_KERBEROS_REALM, realmDefault);
    }

    @BeforeEach
    public void setup() {
        ugiFoo = UserGroupInformation.createUserForTesting("foo", new String[]{});
        ugiBar = UserGroupInformation.createUserForTesting("bar", new String[]{});

        userFoo = new User("foo");
        userBar = new User("bar");

        subjectFoo = new Subject(true, Sets.newHashSet(userFoo), Sets.newHashSet(), Sets.newHashSet());
        subjectBar = new Subject(true, Sets.newHashSet(userBar), Sets.newHashSet(), Sets.newHashSet());
    }

    @Test
    public void testLoginSessionConfigurationConstructor() {
        session = new LoginSession("config");
        assertEquals(0, session.getKerberosMinMillisBeforeRelogin());
        assertEquals(0, session.getKerberosTicketRenewWindow());
        assertNull(session.getKeytabPath());
        assertNull(session.getPrincipalName());
        assertNull(session.getLoginUser());
        assertNull(session.getSubject());
        assertNull(session.getUser());
        assertEquals("LoginSession[config=config,principal=<null>,keytab=<null>,kerberosMinMillisBeforeRelogin=0,kerberosTicketRenewWindow=0.0]", session.toString());
    }

    @Test
    public void testLoginSessionConfigurationAndLoginUserConstructor() {
        session = new LoginSession("config", ugiFoo);
        assertEquals(0, session.getKerberosMinMillisBeforeRelogin());
        assertEquals(0, session.getKerberosTicketRenewWindow());
        assertSame(ugiFoo, session.getLoginUser());
        assertNull(session.getKeytabPath());
        assertNull(session.getPrincipalName());
        assertNull(session.getSubject());
        assertNull(session.getUser());
        assertEquals("LoginSession[config=config,principal=<null>,keytab=<null>,kerberosMinMillisBeforeRelogin=0,kerberosTicketRenewWindow=0.0]", session.toString());
    }

    @Test
    public void testLoginSessionShortConstructor() {
        session = new LoginSession("config", "principal", "keytab", 0, 0.8f);
        assertEquals(0, session.getKerberosMinMillisBeforeRelogin());
        assertEquals(0.8f, session.getKerberosTicketRenewWindow());
        assertEquals("keytab", session.getKeytabPath());
        assertEquals("principal", session.getPrincipalName());
        assertNull(session.getLoginUser());
        assertNull(session.getSubject());
        assertNull(session.getUser());
    }

    @Test
    public void testLoginSessionFullConstructor() {
        session = new LoginSession("config", "principal", "keytab", ugiFoo, subjectFoo, 1, 0.8f);
        assertEquals(1, session.getKerberosMinMillisBeforeRelogin());
        assertEquals(0.8f, session.getKerberosTicketRenewWindow());
        assertEquals("keytab", session.getKeytabPath());
        assertEquals("principal", session.getPrincipalName());
        assertSame(ugiFoo, session.getLoginUser());
        assertSame(subjectFoo, session.getSubject());
        assertSame(userFoo, session.getUser());
    }

    @Test
    public void testEquality() {
        sessionFoo = new LoginSession("config", "principal", "keytab", ugiFoo, subjectFoo, 1, 0.8f);

        sessionBar = new LoginSession("config", "principal", "keytab", ugiBar, subjectBar, 1, 0.8f);
        assertEquals(sessionFoo, sessionBar);
        assertEquals(sessionFoo.hashCode(), sessionBar.hashCode());


        sessionBar = new LoginSession("DIFFERENT", "principal", "keytab", ugiBar, subjectBar, 1, 0.8f);
        assertNotEquals(sessionFoo, sessionBar);
        assertNotEquals(sessionFoo.hashCode(), sessionBar.hashCode());

        sessionBar = new LoginSession("config", "DIFFERENT", "keytab", ugiBar, subjectBar, 1, 0.8f);
        assertNotEquals(sessionFoo, sessionBar);
        assertNotEquals(sessionFoo.hashCode(), sessionBar.hashCode());


        sessionBar = new LoginSession("config", "principal", "DIFFERENT", ugiBar, subjectBar, 1, 0.8f);
        assertNotEquals(sessionFoo, sessionBar);
        assertNotEquals(sessionFoo.hashCode(), sessionBar.hashCode());

        sessionBar = new LoginSession("config", "principal", "keytab", ugiBar, subjectBar, 999, 0.8f);
        assertNotEquals(sessionFoo, sessionBar);
        assertNotEquals(sessionFoo.hashCode(), sessionBar.hashCode());

        sessionBar = new LoginSession("config", "principal", "keytab", ugiBar, subjectBar, 1, 0.4f);
        assertNotEquals(sessionFoo, sessionBar);
        assertNotEquals(sessionFoo.hashCode(), sessionBar.hashCode());
    }

    @Test
    public void testToString() {
        session = new LoginSession("config", "principal", "keytab", ugiFoo, subjectFoo, 1, 0.8f);
        assertEquals("LoginSession[config=config,principal=principal,keytab=keytab,kerberosMinMillisBeforeRelogin=1,kerberosTicketRenewWindow=0.8]", session.toString());
    }

    private static void resetProperty(String key, String val) {
        if (val != null) {
            System.setProperty(key, val);
            return;
        }
        System.clearProperty(key);
    }
}
