package org.apache.hadoop.security;

import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Test;

import javax.security.auth.Subject;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

public class LoginSessionTest {

    private LoginSession session, sessionFoo, sessionBar;
    private UserGroupInformation ugiFoo, ugiBar;
    private Subject subjectFoo, subjectBar;
    private User userFoo, userBar;

    @Before
    public void setup() {
        ugiFoo = UserGroupInformation.createUserForTesting("foo", new String[]{});
        ugiBar = UserGroupInformation.createUserForTesting("bar", new String[]{});

        userFoo = new User("foo");
        userBar = new User("bar");

        subjectFoo = new Subject(true, Sets.newHashSet(userFoo), Sets.newHashSet(), Sets.newHashSet());
        subjectBar = new Subject(true, Sets.newHashSet(userBar), Sets.newHashSet(), Sets.newHashSet());
    }

    @Test
    public void testLoginSessionShortConstructor() {
        session = new LoginSession("config", "principal", "keytab", 0);
        assertEquals(0, session.getKerberosMinMillisBeforeRelogin());
        assertEquals("keytab", session.getKeytabPath());
        assertEquals("principal", session.getPrincipalName());
        assertNull(session.getLoginUser());
        assertNull(session.getSubject());
        assertNull(session.getUser());
    }

    @Test
    public void testLoginSessionFullConstructor() {
        session = new LoginSession("config", "principal", "keytab", ugiFoo, subjectFoo, 1);
        assertEquals(1, session.getKerberosMinMillisBeforeRelogin());
        assertEquals("keytab", session.getKeytabPath());
        assertEquals("principal", session.getPrincipalName());
        assertSame(ugiFoo, session.getLoginUser());
        assertSame(subjectFoo, session.getSubject());
        assertSame(userFoo, session.getUser());
    }

    @Test
    public void testEquality() {
        sessionFoo = new LoginSession("config", "principal", "keytab", ugiFoo, subjectFoo, 1);

        sessionBar = new LoginSession("config", "principal", "keytab", ugiBar, subjectBar, 1);
        assertEquals(sessionFoo, sessionBar);
        assertEquals(sessionFoo.hashCode(), sessionBar.hashCode());


        sessionBar = new LoginSession("DIFFERENT", "principal", "keytab", ugiBar, subjectBar, 1);
        assertNotEquals(sessionFoo, sessionBar);
        assertNotEquals(sessionFoo.hashCode(), sessionBar.hashCode());

        sessionBar = new LoginSession("config", "DIFFERENT", "keytab", ugiBar, subjectBar, 1);
        assertNotEquals(sessionFoo, sessionBar);
        assertNotEquals(sessionFoo.hashCode(), sessionBar.hashCode());


        sessionBar = new LoginSession("config", "principal", "DIFFERENT", ugiBar, subjectBar, 1);
        assertNotEquals(sessionFoo, sessionBar);
        assertNotEquals(sessionFoo.hashCode(), sessionBar.hashCode());

        sessionBar = new LoginSession("config", "principal", "keytab", ugiBar, subjectBar, 999);
        assertNotEquals(sessionFoo, sessionBar);
        assertNotEquals(sessionFoo.hashCode(), sessionBar.hashCode());
    }

    @Test
    public void testToString() {
        session = new LoginSession("config", "principal", "keytab", ugiFoo, subjectFoo, 1);
        assertEquals("LoginSession[config=config,principal=principal,keytab=keytab,kerberosMinMillisBeforeRelogin=1]", session.toString());
    }

}
