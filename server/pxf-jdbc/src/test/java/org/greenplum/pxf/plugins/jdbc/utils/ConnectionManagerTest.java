package org.greenplum.pxf.plugins.jdbc.utils;

import com.google.common.base.Ticker;
import com.google.common.util.concurrent.Uninterruptibles;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.HikariPoolMXBean;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.core.StringContains.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.atLeast;
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

    @Test
    public void testPoolExpirationNoActiveConnections() throws SQLException {
        MockTicker ticker = new MockTicker();
        ConnectionManager.DataSourceFactory mockFactory = mock(ConnectionManager.DataSourceFactory.class);
        HikariDataSource mockDataSource = mock(HikariDataSource.class);
        when(mockFactory.createDataSource(anyObject())).thenReturn(mockDataSource);
        when(mockDataSource.getConnection()).thenReturn(mockConnection);

        HikariPoolMXBean mockMBean = mock(HikariPoolMXBean.class);
        when(mockDataSource.getHikariPoolMXBean()).thenReturn(mockMBean);
        when(mockMBean.getActiveConnections()).thenReturn(0);
        manager = new ConnectionManager(mockFactory, ticker, ConnectionManager.CLEANUP_SLEEP_INTERVAL_NANOS);

        manager.getConnection("test-server", "test-url", connProps, true, poolProps);

        ticker.advanceTime(ConnectionManager.POOL_EXPIRATION_TIMEOUT_HOURS + 1, TimeUnit.HOURS);
        manager.cleanCache();

        Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);

        verify(mockMBean, times(1)).getActiveConnections();
        verify(mockDataSource, times(1)).close(); // verify datasource is closed when evicted
    }


    @Test
    public void testPoolExpirationWithActiveConnections() throws SQLException {
        MockTicker ticker = new MockTicker();
        ConnectionManager.DataSourceFactory mockFactory = mock(ConnectionManager.DataSourceFactory.class);
        HikariDataSource mockDataSource = mock(HikariDataSource.class);
        when(mockFactory.createDataSource(anyObject())).thenReturn(mockDataSource);
        when(mockDataSource.getConnection()).thenReturn(mockConnection);

        HikariPoolMXBean mockMBean = mock(HikariPoolMXBean.class);
        when(mockDataSource.getHikariPoolMXBean()).thenReturn(mockMBean);
        when(mockMBean.getActiveConnections()).thenReturn(2, 1, 0);
        manager = new ConnectionManager(mockFactory, ticker, TimeUnit.MILLISECONDS.toNanos(50));

        manager.getConnection("test-server", "test-url", connProps, true, poolProps);

        ticker.advanceTime(ConnectionManager.POOL_EXPIRATION_TIMEOUT_HOURS + 1, TimeUnit.HOURS);
        manager.cleanCache();

        // wait for at least 3 iteration of sleeping
        Uninterruptibles.sleepUninterruptibly(500, TimeUnit.MILLISECONDS);

        verify(mockMBean, times(3)).getActiveConnections();
        verify(mockDataSource, times(1)).close(); // verify datasource is closed when evicted
    }

    @Test
    public void testPoolExpirationWithActiveConnectionsOver24Hours() throws SQLException {
        MockTicker ticker = new MockTicker();
        ConnectionManager.DataSourceFactory mockFactory = mock(ConnectionManager.DataSourceFactory.class);
        HikariDataSource mockDataSource = mock(HikariDataSource.class);
        when(mockFactory.createDataSource(anyObject())).thenReturn(mockDataSource);
        when(mockDataSource.getConnection()).thenReturn(mockConnection);

        HikariPoolMXBean mockMBean = mock(HikariPoolMXBean.class);
        when(mockDataSource.getHikariPoolMXBean()).thenReturn(mockMBean);
        when(mockMBean.getActiveConnections()).thenReturn(1); //always report pool has an active connection
        manager = new ConnectionManager(mockFactory, ticker, TimeUnit.MILLISECONDS.toNanos(50));

        manager.getConnection("test-server", "test-url", connProps, true, poolProps);

        ticker.advanceTime(ConnectionManager.POOL_EXPIRATION_TIMEOUT_HOURS + 1, TimeUnit.HOURS);
        manager.cleanCache();

        // wait for at least 3 iteration of sleeping (3 * 50ms = 150ms)
        Uninterruptibles.sleepUninterruptibly(150, TimeUnit.MILLISECONDS);

        ticker.advanceTime(ConnectionManager.CLEANUP_TIMEOUT_NANOS + 100000, TimeUnit.NANOSECONDS);

        // wait again as cleaner needs to pick new ticker value
        Uninterruptibles.sleepUninterruptibly(150, TimeUnit.MILLISECONDS);

        verify(mockMBean, atLeast(3)).getActiveConnections();
        verify(mockDataSource, times(1)).close(); // verify datasource is closed when evicted
    }

    class MockTicker extends Ticker {
        private final AtomicLong nanos = new AtomicLong();

        @Override
        public long read() {
            return nanos.get();
        }

        public void advanceTime(long value, TimeUnit unit) {
            nanos.addAndGet(unit.toNanos(value));
        }
    }
}

