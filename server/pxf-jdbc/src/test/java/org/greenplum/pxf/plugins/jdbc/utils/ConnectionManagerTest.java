package org.greenplum.pxf.plugins.jdbc.utils;

import com.google.common.base.Ticker;
import com.google.common.util.concurrent.Uninterruptibles;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.HikariPoolMXBean;
import com.zaxxer.hikari.pool.HikariProxyConnection;
import org.greenplum.pxf.plugins.jdbc.PxfJdbcProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ConnectionManagerTest {

    private ConnectionManager manager;

    private ConnectionManager.DriverManagerWrapper mockDriverManagerWrapper;

    private Properties connProps, poolProps;
    private Connection mockConnection;
    private PxfJdbcProperties properties;
    private PxfJdbcProperties.Connection connection;

    @BeforeEach
    public void before() {
        connProps = new Properties();
        poolProps = new Properties();
        mockConnection = mock(Connection.class);

        properties = new PxfJdbcProperties();
        connection = properties.getConnection();

        ConnectionManager.DataSourceFactory dataSourceFactory = new ConnectionManager.DataSourceFactory();

        mockDriverManagerWrapper = mock(ConnectionManager.DriverManagerWrapper.class);

        manager = new ConnectionManager(dataSourceFactory, Ticker.systemTicker(), properties, mockDriverManagerWrapper);
    }

    @Test
    public void testMaskPassword() {
        assertEquals("********", ConnectionManager.maskPassword("12345678"));
        assertEquals("", ConnectionManager.maskPassword(""));
        assertEquals("", ConnectionManager.maskPassword(null));
    }

    @Test
    public void testGetConnectionPoolDisabled() throws SQLException {
        when(mockDriverManagerWrapper.getConnection("test-url", connProps)).thenReturn(mockConnection);
        Connection conn = manager.getConnection("test-server", "test-url", connProps, false, null, null);
        assertSame(mockConnection, conn);
    }

    @Test
    public void testGetConnectionPoolEnabledNoPoolProps() throws SQLException {
        Driver mockDriver = mock(Driver.class);
        when(mockDriverManagerWrapper.getDriver("test-url")).thenReturn(mockDriver);
        when(mockDriver.connect("test-url", connProps)).thenReturn(mockConnection);
        when(mockDriver.acceptsURL("test-url")).thenReturn(true);
        DriverManager.registerDriver(mockDriver);

        Driver mockDriver2 = mock(Driver.class);
        when(mockDriverManagerWrapper.getDriver("test-url-2")).thenReturn(mockDriver2);
        Connection mockConnection2 = mock(Connection.class);
        when(mockDriver2.connect("test-url-2", connProps)).thenReturn(mockConnection2);
        when(mockDriver2.acceptsURL("test-url-2")).thenReturn(true);
        DriverManager.registerDriver(mockDriver2);

        Connection conn;
        for (int i = 0; i < 5; i++) {
            conn = manager.getConnection("test-server", "test-url", connProps, true, poolProps, null);
            assertNotNull(conn);
            assertTrue(conn instanceof HikariProxyConnection);
            assertSame(mockConnection, conn.unwrap(Connection.class));
            conn.close();
        }

        Connection conn2 = manager.getConnection("test-server", "test-url-2", connProps, true, poolProps, null);
        assertNotNull(conn2);
        assertTrue(conn2 instanceof HikariProxyConnection);
        assertSame(mockConnection2, conn2.unwrap(Connection.class));

        verify(mockDriver, times(1)).connect("test-url", connProps);
        verify(mockDriver2, times(1)).connect("test-url-2", connProps);

        DriverManager.deregisterDriver(mockDriver);
        DriverManager.deregisterDriver(mockDriver2);
    }

    @Test
    public void testGetConnectionPoolEnabledMaxConnOne() throws SQLException {
        Driver mockDriver = mock(Driver.class);
        when(mockDriverManagerWrapper.getDriver("test-url")).thenReturn(mockDriver);
        when(mockDriver.connect("test-url", connProps)).thenReturn(mockConnection);
        when(mockDriver.acceptsURL("test-url")).thenReturn(true);
        DriverManager.registerDriver(mockDriver);

        poolProps.setProperty("maximumPoolSize", "1");
        poolProps.setProperty("connectionTimeout", "250");

        // get connection, do not close it
        manager.getConnection("test-server", "test-url", connProps, true, poolProps, null);
        // ask for connection again, it should time out
        Exception ex = assertThrows(SQLTransientConnectionException.class,
                () -> manager.getConnection("test-server", "test-url", connProps, true, poolProps, null));
        assertTrue(ex.getMessage().contains(" - Connection is not available, request timed out after "));

        DriverManager.deregisterDriver(mockDriver);
    }

    @Test
    public void testGetConnectionPoolEnabledWithPoolProps() throws SQLException {
        Driver mockDriver = mock(Driver.class);
        when(mockDriverManagerWrapper.getDriver("test-url")).thenReturn(mockDriver);
        when(mockDriver.connect(anyString(), any())).thenReturn(mockConnection);
        when(mockDriver.acceptsURL("test-url")).thenReturn(true);
        DriverManager.registerDriver(mockDriver);

        connProps.setProperty("user", "foo");
        connProps.setProperty("password", "foo-password");
        connProps.setProperty("some-prop", "some-value");

        poolProps.setProperty("maximumPoolSize", "1");
        poolProps.setProperty("connectionTimeout", "250");
        poolProps.setProperty("dataSource.foo", "123");

        // get connection, do not close it
        Connection conn = manager.getConnection("test-server", "test-url", connProps, true, poolProps, null);
        assertNotNull(conn);

        // make sure all connProps and "dataSource.foo" from poolProps are passed to the DriverManager
        Properties calledWith = (Properties) connProps.clone();
        calledWith.setProperty("foo", "123");
        verify(mockDriver, times(1)).connect("test-url", calledWith);

        DriverManager.deregisterDriver(mockDriver);
    }

    @Test
    public void testPoolExpirationNoActiveConnections() throws SQLException {
        FakeTicker ticker = new FakeTicker();
        ConnectionManager.DataSourceFactory mockFactory = mock(ConnectionManager.DataSourceFactory.class);
        HikariDataSource mockDataSource = mock(HikariDataSource.class);
        when(mockFactory.createDataSource(any())).thenReturn(mockDataSource);
        when(mockDataSource.getConnection()).thenReturn(mockConnection);

        HikariPoolMXBean mockMBean = mock(HikariPoolMXBean.class);
        when(mockDataSource.getHikariPoolMXBean()).thenReturn(mockMBean);
        when(mockMBean.getActiveConnections()).thenReturn(0);
        manager = new ConnectionManager(mockFactory, ticker, properties, mockDriverManagerWrapper);

        manager.getConnection("test-server", "test-url", connProps, true, poolProps, null);

        ticker.advanceTime(connection.getPoolExpirationTimeout().toHours() + 1, TimeUnit.HOURS);
        manager.cleanCache();

        Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);

        verify(mockMBean, times(1)).getActiveConnections();
        verify(mockDataSource, times(1)).close(); // verify datasource is closed when evicted
    }

    @Test
    public void testPoolExpirationWithActiveConnections() throws SQLException {
        ConnectionManagerTest.FakeTicker ticker = new ConnectionManagerTest.FakeTicker();
        ConnectionManager.DataSourceFactory mockFactory = mock(ConnectionManager.DataSourceFactory.class);
        HikariDataSource mockDataSource = mock(HikariDataSource.class);
        when(mockFactory.createDataSource(any())).thenReturn(mockDataSource);
        when(mockDataSource.getConnection()).thenReturn(mockConnection);

        HikariPoolMXBean mockMBean = mock(HikariPoolMXBean.class);
        when(mockDataSource.getHikariPoolMXBean()).thenReturn(mockMBean);
        when(mockMBean.getActiveConnections()).thenReturn(2, 1, 0);

        connection.setCleanupSleepInterval(Duration.ofMillis(50));
        manager = new ConnectionManager(mockFactory, ticker, properties, mockDriverManagerWrapper);

        manager.getConnection("test-server", "test-url", connProps, true, poolProps, null);

        ticker.advanceTime(connection.getPoolExpirationTimeout().toHours() + 1, TimeUnit.HOURS);
        manager.cleanCache();

        // wait for at least 3 iteration of sleeping
        Uninterruptibles.sleepUninterruptibly(2500, TimeUnit.MILLISECONDS);

        verify(mockMBean, times(3)).getActiveConnections();
        verify(mockDataSource, times(1)).close(); // verify datasource is closed when evicted
    }

    @Test
    public void testPoolExpirationWithActiveConnectionsOver24Hours() throws SQLException {
        FakeTicker ticker = new FakeTicker();
        ConnectionManager.DataSourceFactory mockFactory = mock(ConnectionManager.DataSourceFactory.class);
        HikariDataSource mockDataSource = mock(HikariDataSource.class);
        when(mockFactory.createDataSource(any())).thenReturn(mockDataSource);
        when(mockDataSource.getConnection()).thenReturn(mockConnection);

        HikariPoolMXBean mockMBean = mock(HikariPoolMXBean.class);
        when(mockDataSource.getHikariPoolMXBean()).thenReturn(mockMBean);
        when(mockMBean.getActiveConnections()).thenReturn(1); //always report pool has an active connection
        connection.setCleanupSleepInterval(Duration.ofMillis(50));
        manager = new ConnectionManager(mockFactory, ticker, properties, mockDriverManagerWrapper);

        manager.getConnection("test-server", "test-url", connProps, true, poolProps, null);

        ticker.advanceTime(connection.getPoolExpirationTimeout().toHours() + 1, TimeUnit.HOURS);
        manager.cleanCache();

        // wait for at least 3 iteration of sleeping (3 * 50ms = 150ms)
        Uninterruptibles.sleepUninterruptibly(150, TimeUnit.MILLISECONDS);

        ticker.advanceTime(connection.getCleanupTimeout().toNanos() + 100000, TimeUnit.NANOSECONDS);

        // wait again as cleaner needs to pick new ticker value
        Uninterruptibles.sleepUninterruptibly(150, TimeUnit.MILLISECONDS);

        verify(mockMBean, atLeast(3)).getActiveConnections();
        verify(mockDataSource, times(1)).close(); // verify datasource is closed when evicted
    }

    static class FakeTicker extends Ticker {
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

