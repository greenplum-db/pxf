package org.greenplum.pxf.plugins.jdbc.utils;

import com.zaxxer.hikari.pool.HikariProxyConnection;
import com.zaxxer.hikari.util.DriverDataSource;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import java.util.Properties;

import static org.hamcrest.core.StringContains.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.contains;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.when;

@PrepareForTest({DriverManager.class, ConnectionManager.class, DriverDataSource.class})
@RunWith(PowerMockRunner.class)
public class ConnectionManagerTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private ConnectionManager manager = ConnectionManager.getInstance();
    private Properties connProps, poolProps;
    private Connection mockConnection;

    @Before
    public void before() throws SQLException {
        connProps = new Properties();
        poolProps = new Properties();
        mockConnection = mock(Connection.class);
        PowerMockito.mockStatic(DriverManager.class);
    }

    @Test
    public void testSingletonInstance() {
        assertSame(manager, ConnectionManager.getInstance());
    }

    @Test
    public void testMaskPassword () {
        assertEquals("********", ConnectionManager.maskPassword("12345678"));
        assertEquals("", ConnectionManager.maskPassword(""));
        assertEquals("", ConnectionManager.maskPassword(null));
    }

    @Test
    public void testGetConnectionPoolDisabled() throws SQLException {
        when(DriverManager.getConnection("test-url", connProps)).thenReturn(mockConnection);
        Connection conn = manager.getConnection("test-server", "test-url", connProps, false, null);
        assertSame(mockConnection, conn);
    }

    @Test
    public void testGetConnectionPoolEnabledNoPoolProps() throws SQLException {
        Driver mockDriver = mock(Driver.class);
        when(DriverManager.getDriver("test-url")).thenReturn(mockDriver);
        when(mockDriver.connect("test-url", connProps)).thenReturn(mockConnection);

        Driver mockDriver2 = mock(Driver.class); ;
        when(DriverManager.getDriver("test-url-2")).thenReturn(mockDriver2);
        Connection mockConnection2 = mock(Connection.class);
        when(mockDriver2.connect("test-url-2", connProps)).thenReturn(mockConnection2);

        Connection conn;
        for (int i=0; i< 5; i++) {
            conn = manager.getConnection("test-server", "test-url", connProps, true, poolProps);
            assertNotNull(conn);
            assertTrue(conn instanceof HikariProxyConnection);
            assertSame(mockConnection, conn.unwrap(Connection.class));
            conn.close();
        }

        Connection conn2 = manager.getConnection("test-server", "test-url-2", connProps, true, poolProps);
        assertNotNull(conn2);
        assertTrue(conn2 instanceof HikariProxyConnection);
        assertSame(mockConnection2, conn2.unwrap(Connection.class));

        verify(mockDriver, times(1)).connect("test-url", connProps);
        verify(mockDriver2, times(1)).connect("test-url-2", connProps);
    }

    @Test
    public void testGetConnectionPoolEnabledMaxConnOne() throws SQLException {
        expectedException.expect(SQLTransientConnectionException.class);
        expectedException.expectMessage(containsString(" - Connection is not available, request timed out after "));

        Driver mockDriver = mock(Driver.class);
        when(DriverManager.getDriver("test-url")).thenReturn(mockDriver);
        when(mockDriver.connect("test-url", connProps)).thenReturn(mockConnection);

        poolProps.setProperty("maximumPoolSize", "1");
        poolProps.setProperty("connectionTimeout", "250");

        // get connection, do not close it
        manager.getConnection("test-server", "test-url", connProps, true, poolProps);
        // ask for connection again, it should time out
        manager.getConnection("test-server", "test-url", connProps, true, poolProps);
    }

    @Test
    public void testGetConnectionPoolEnabledWithPoolProps() throws SQLException {
        Driver mockDriver = mock(Driver.class);
        when(DriverManager.getDriver("test-url")).thenReturn(mockDriver);
        when(mockDriver.connect(anyString(), anyObject())).thenReturn(mockConnection);

        connProps.setProperty("user", "foo");
        connProps.setProperty("password", "foo-password");
        connProps.setProperty("some-prop", "some-value");

        poolProps.setProperty("maximumPoolSize", "1");
        poolProps.setProperty("connectionTimeout", "250");
        poolProps.setProperty("dataSource.foo", "123");

        // get connection, do not close it
        Connection conn = manager.getConnection("test-server", "test-url", connProps, true, poolProps);
        assertNotNull(conn);

        // make sure all connProps and "dataSource.foo" from poolProps are passed to the DriverManager
        Properties calledWith = (Properties) connProps.clone();
        calledWith.setProperty("foo", "123");
        verify(mockDriver, times(1)).connect("test-url", calledWith);
    }
}
