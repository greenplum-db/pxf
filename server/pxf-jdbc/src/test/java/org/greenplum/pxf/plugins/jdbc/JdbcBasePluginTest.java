package org.greenplum.pxf.plugins.jdbc;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import org.apache.hadoop.conf.Configuration;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.security.SecureLogin;
import org.greenplum.pxf.plugins.jdbc.utils.ConnectionManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class JdbcBasePluginTest {

    @Mock
    private ConnectionManager mockConnectionManager;
    @Mock
    private DatabaseMetaData mockMetaData;
    @Mock
    private Connection mockConnection;
    @Mock
    private PreparedStatement mockStatement;
    @Mock
    private SecureLogin mockSecureLogin;

    private final SQLException exception = new SQLException("some error");
    private Configuration configuration;
    private RequestContext context;
    private Properties poolProps;

    @BeforeEach
    public void before() {
        configuration = new Configuration();
        context = new RequestContext();
        context.setConfig("default");
        context.setDataSource("test-table");
        Map<String, String> additionalProps = new HashMap<>();
        context.setAdditionalConfigProps(additionalProps);
        context.setUser("test-user");
        context.setConfiguration(configuration);

        poolProps = new Properties();
        poolProps.setProperty("maximumPoolSize", "15");
        poolProps.setProperty("connectionTimeout", "30000");
        poolProps.setProperty("idleTimeout", "30000");
        poolProps.setProperty("minimumIdle", "0");
    }

    @Test
    public void testCloseConnectionWithCommit() throws Exception {
        when(mockMetaData.supportsTransactions()).thenReturn(true);
        when(mockConnection.getMetaData()).thenReturn(mockMetaData);
        when(mockConnection.isClosed()).thenReturn(false);
        when(mockConnection.getAutoCommit()).thenReturn(false);
        Mockito.doNothing().when(mockConnection).commit();
        Mockito.doNothing().when(mockConnection).close();
        when(mockStatement.getConnection()).thenReturn(mockConnection);
        Mockito.doNothing().when(mockStatement).close();

        JdbcBasePlugin.closeStatementAndConnection(mockStatement);

        verify(mockStatement, times(1)).close();
        verify(mockConnection, times(1)).close();
        verify(mockConnection, times(1)).commit();
    }

    @Test
    public void testCloseConnectionWithoutCommit() throws Exception {
        when(mockMetaData.supportsTransactions()).thenReturn(true);
        when(mockConnection.getMetaData()).thenReturn(mockMetaData);
        when(mockConnection.isClosed()).thenReturn(false);
        when(mockConnection.getAutoCommit()).thenReturn(true);
        Mockito.doNothing().when(mockConnection).close();
        when(mockStatement.getConnection()).thenReturn(mockConnection);
        Mockito.doNothing().when(mockStatement).close();

        JdbcBasePlugin.closeStatementAndConnection(mockStatement);

        verify(mockStatement, times(1)).close();
        verify(mockConnection, times(1)).close();
    }

    @Test
    public void testCloseConnectionWithoutTransactions() throws Exception {
        when(mockMetaData.supportsTransactions()).thenReturn(false);
        when(mockConnection.getMetaData()).thenReturn(mockMetaData);
        when(mockConnection.isClosed()).thenReturn(false);
        Mockito.doNothing().when(mockConnection).close();
        when(mockStatement.getConnection()).thenReturn(mockConnection);
        Mockito.doNothing().when(mockStatement).close();

        JdbcBasePlugin.closeStatementAndConnection(mockStatement);

        verify(mockStatement, times(1)).close();
        verify(mockConnection, times(1)).close();
    }

    @Test
    public void testCloseConnectionClosed() throws Exception {
        when(mockConnection.isClosed()).thenReturn(true);
        Mockito.doNothing().when(mockConnection).close();
        when(mockStatement.getConnection()).thenReturn(mockConnection);
        Mockito.doNothing().when(mockStatement).close();

        JdbcBasePlugin.closeStatementAndConnection(mockStatement);

        verify(mockStatement, times(1)).close();
    }

    @Test
    public void testCloseConnectionWithExceptionDatabaseMetaData() throws Exception {
        when(mockStatement.getConnection()).thenReturn(mockConnection);
        Mockito.doNothing().when(mockStatement).close();
        when(mockConnection.isClosed()).thenReturn(false);
        Mockito.doNothing().when(mockConnection).close();
        when(mockConnection.getMetaData()).thenReturn(mockMetaData);
        doThrow(exception).when(mockMetaData).supportsTransactions();

        Exception e = assertThrows(SQLException.class,
                () -> JdbcBasePlugin.closeStatementAndConnection(mockStatement),
                "SQLException must have been thrown");
        assertSame(exception, e);

        verify(mockStatement, times(1)).close();
        verify(mockConnection, times(1)).close();
    }

    @Test
    public void testCloseConnectionWithExceptionConnectionOnCommit() throws Exception {
        when(mockStatement.getConnection()).thenReturn(mockConnection);
        Mockito.doNothing().when(mockStatement).close();
        when(mockConnection.getMetaData()).thenReturn(mockMetaData);
        when(mockMetaData.supportsTransactions()).thenReturn(true);
        when(mockConnection.isClosed()).thenReturn(false);
        when(mockConnection.getAutoCommit()).thenReturn(false);
        doThrow(exception).when(mockConnection).commit();
        Mockito.doNothing().when(mockConnection).close();

        Exception e = assertThrows(SQLException.class,
                () -> JdbcBasePlugin.closeStatementAndConnection(mockStatement),
                "SQLException must have been thrown");
        assertSame(exception, e);

        verify(mockStatement, times(1)).close();
        verify(mockConnection, times(1)).close();
    }

    @Test
    public void testCloseConnectionWithExceptionConnectionOnClose() throws Exception {
        when(mockStatement.getConnection()).thenReturn(mockConnection);
        Mockito.doNothing().when(mockStatement).close();
        when(mockConnection.getMetaData()).thenReturn(mockMetaData);
        when(mockMetaData.supportsTransactions()).thenReturn(false);
        when(mockConnection.isClosed()).thenReturn(false);
        doThrow(exception).when(mockConnection).close();

        JdbcBasePlugin.closeStatementAndConnection(mockStatement);

        verify(mockStatement, times(1)).close();
    }

    @Test
    public void testCloseConnectionWithExceptionStatementOnClose() throws Exception {
        when(mockStatement.getConnection()).thenReturn(mockConnection);
        when(mockConnection.getMetaData()).thenReturn(mockMetaData);
        when(mockMetaData.supportsTransactions()).thenReturn(false);
        when(mockConnection.isClosed()).thenReturn(false);
        doThrow(exception).when(mockStatement).close();

        Exception e = assertThrows(SQLException.class,
                () -> JdbcBasePlugin.closeStatementAndConnection(mockStatement),
                "SQLException must have been thrown");
        assertSame(exception, e);

        verify(mockConnection, times(1)).close();
    }

    @Test
    public void testCloseConnectionWithExceptionStatementOnGetConnection() throws Exception {
        doThrow(exception).when(mockStatement).getConnection();
        Mockito.doNothing().when(mockStatement).close();

        Exception e = assertThrows(SQLException.class,
                () -> JdbcBasePlugin.closeStatementAndConnection(mockStatement),
                "SQLException must have been thrown");
        assertSame(exception, e);

        verify(mockStatement, times(1)).close();
    }

    @Test
    public void testTransactionIsolationNotSetByUser() throws SQLException {
        configuration.set("jdbc.driver", "org.greenplum.pxf.plugins.jdbc.FakeJdbcDriver");
        configuration.set("jdbc.url", "test-url");

        when(mockConnectionManager.getConnection(any(), any(), any(), anyBoolean(), any(), any())).thenReturn(mockConnection);
        when(mockConnection.getMetaData()).thenReturn(mockMetaData);

        JdbcBasePlugin plugin = new JdbcBasePlugin(mockConnectionManager, mockSecureLogin);
        plugin.setRequestContext(context);
        Connection conn = plugin.getConnection();

        verify(conn, never()).setTransactionIsolation(anyInt());
    }

    @Test
    public void testTransactionIsolationSetByUserToInvalidValue() {
        configuration.set("jdbc.driver", "org.greenplum.pxf.plugins.jdbc.FakeJdbcDriver");
        configuration.set("jdbc.url", "test-url");
        configuration.set("jdbc.connection.transactionIsolation", "foobarValue");

        assertThrows(IllegalArgumentException.class,
                () -> getPlugin(mockConnectionManager, mockSecureLogin, context));
    }

    @Test
    public void testTransactionIsolationSetByUserToUnsupportedValue() throws SQLException {
        configuration.set("jdbc.driver", "org.greenplum.pxf.plugins.jdbc.FakeJdbcDriver");
        configuration.set("jdbc.url", "test-url");
        configuration.set("jdbc.connection.transactionIsolation", "READ_UNCOMMITTED");

        when(mockConnectionManager.getConnection(any(), any(), any(), anyBoolean(), any(), any())).thenReturn(mockConnection);
        when(mockConnection.getMetaData()).thenReturn(mockMetaData);
        // READ_UNCOMMITTED is level 1
        when(mockMetaData.supportsTransactionIsolationLevel(1)).thenReturn(false);

        JdbcBasePlugin plugin = getPlugin(mockConnectionManager, mockSecureLogin, context);
        Exception e = assertThrows(SQLException.class, plugin::getConnection);
        assertEquals("Transaction isolation level READ_UNCOMMITTED is not supported", e.getMessage());
    }

    @Test
    public void testTransactionIsolationSetByUserToValidValue() throws SQLException {
        configuration.set("jdbc.driver", "org.greenplum.pxf.plugins.jdbc.FakeJdbcDriver");
        configuration.set("jdbc.url", "test-url");
        configuration.set("jdbc.connection.transactionIsolation", "READ_COMMITTED");

        when(mockConnectionManager.getConnection(any(), any(), any(), anyBoolean(), any(), any())).thenReturn(mockConnection);
        when(mockConnection.getMetaData()).thenReturn(mockMetaData);
        // READ_COMMITTED is level 2
        when(mockMetaData.supportsTransactionIsolationLevel(2)).thenReturn(true);

        JdbcBasePlugin plugin = getPlugin(mockConnectionManager, mockSecureLogin, context);
        Connection conn = plugin.getConnection();

        // READ_COMMITTED is level 2
        verify(conn).setTransactionIsolation(2);
    }

    @Test
    public void testTransactionIsolationSetByUserFailedToGetMetadata() throws SQLException {
        configuration.set("jdbc.driver", "org.greenplum.pxf.plugins.jdbc.FakeJdbcDriver");
        configuration.set("jdbc.url", "test-url");

        when(mockConnectionManager.getConnection(anyString(), anyString(), any(), anyBoolean(), any(), anyString())).thenReturn(mockConnection);
        doThrow(new SQLException("")).when(mockConnection).getMetaData();

        JdbcBasePlugin plugin = new JdbcBasePlugin(mockConnectionManager, mockSecureLogin);
        plugin.setRequestContext(context);
        assertThrows(SQLException.class, plugin::getConnection);
    }

    @Test
    public void testGetPreparedStatementSetsQueryTimeoutIfSpecified() throws SQLException {
        configuration.set("jdbc.driver", "org.greenplum.pxf.plugins.jdbc.FakeJdbcDriver");
        configuration.set("jdbc.url", "test-url");
        configuration.set("jdbc.statement.queryTimeout", "173");

        when(mockConnection.prepareStatement(anyString())).thenReturn(mockStatement);

        JdbcBasePlugin plugin = getPlugin(mockConnectionManager, mockSecureLogin, context);
        plugin.getPreparedStatement(mockConnection, "foo");

        verify(mockStatement).setQueryTimeout(173);
    }

    @Test
    public void testGetPreparedStatementDoesNotSetQueryTimeoutIfNotSpecified() throws SQLException {
        configuration.set("jdbc.driver", "org.greenplum.pxf.plugins.jdbc.FakeJdbcDriver");
        configuration.set("jdbc.url", "test-url");

        when(mockConnection.prepareStatement(anyString())).thenReturn(mockStatement);

        JdbcBasePlugin plugin = new JdbcBasePlugin(mockConnectionManager, mockSecureLogin);
        plugin.setRequestContext(context);
        plugin.getPreparedStatement(mockConnection, "foo");

        verify(mockStatement, never()).setQueryTimeout(anyInt());
    }

    @Test
    public void testGetConnectionNoConnPropsPoolDisabled() throws SQLException {
        context.setServerName("test-server");
        configuration.set("jdbc.driver", "org.greenplum.pxf.plugins.jdbc.FakeJdbcDriver");
        configuration.set("jdbc.url", "test-url");
        configuration.set("jdbc.pool.enabled", "false");

        when(mockConnectionManager.getConnection(any(), any(), any(), anyBoolean(), any(), any())).thenReturn(mockConnection);
        when(mockConnection.getMetaData()).thenReturn(mockMetaData);

        JdbcBasePlugin plugin = getPlugin(mockConnectionManager, mockSecureLogin, context);
        Connection conn = plugin.getConnection();

        assertSame(mockConnection, conn);

        verify(mockConnectionManager).getConnection("test-server", "test-url", new Properties(), false, null, null);
    }

    @Test
    public void testGetConnectionConnPropsPoolDisabled() throws SQLException {
        context.setServerName("test-server");

        Properties connProps = new Properties();
        connProps.setProperty("foo", "foo-val");
        connProps.setProperty("bar", "bar-val");

        when(mockConnectionManager.getConnection("test-server", "test-url", connProps, false, null, null)).thenReturn(mockConnection);
        when(mockConnection.getMetaData()).thenReturn(mockMetaData);

        configuration.set("jdbc.driver", "org.greenplum.pxf.plugins.jdbc.FakeJdbcDriver");
        configuration.set("jdbc.url", "test-url");
        configuration.set("jdbc.connection.property.foo", "foo-val");
        configuration.set("jdbc.connection.property.bar", "bar-val");
        configuration.set("jdbc.pool.enabled", "false");

        JdbcBasePlugin plugin = getPlugin(mockConnectionManager, mockSecureLogin, context);
        Connection conn = plugin.getConnection();

        assertSame(mockConnection, conn);

        verify(mockConnectionManager).getConnection("test-server", "test-url", connProps, false, null, null);
    }

    @Test
    public void testGetConnectionConnPropsPoolEnabledNoPoolProps() throws SQLException {
        context.setServerName("test-server");

        configuration.set("jdbc.driver", "org.greenplum.pxf.plugins.jdbc.FakeJdbcDriver");
        configuration.set("jdbc.url", "test-url");
        configuration.set("jdbc.connection.property.foo", "foo-val");
        configuration.set("jdbc.connection.property.bar", "bar-val");

        // pool is enabled by default
        when(mockConnectionManager.getConnection(any(), any(), any(), anyBoolean(), any(), any())).thenReturn(mockConnection);
        when(mockConnection.getMetaData()).thenReturn(mockMetaData);

        JdbcBasePlugin plugin = getPlugin(mockConnectionManager, mockSecureLogin, context);
        Connection conn = plugin.getConnection();

        assertSame(mockConnection, conn);

        Properties connProps = new Properties();
        connProps.setProperty("foo", "foo-val");
        connProps.setProperty("bar", "bar-val");

       verify(mockConnectionManager).getConnection("test-server", "test-url", connProps, true, poolProps, null);
    }

    @Test
    public void testGetConnectionConnPropsPoolEnabledWithQualifier() throws SQLException {
        context.setServerName("test-server");

        configuration.set("jdbc.driver", "org.greenplum.pxf.plugins.jdbc.FakeJdbcDriver");
        configuration.set("jdbc.url", "test-url");
        configuration.set("jdbc.connection.property.foo", "foo-val");
        configuration.set("jdbc.connection.property.bar", "bar-val");
        // pool is enabled by default

        configuration.set("jdbc.pool.qualifier", "qual");

        when(mockConnectionManager.getConnection(anyString(), anyString(), any(), anyBoolean(), any(), anyString())).thenReturn(mockConnection);
        when(mockConnection.getMetaData()).thenReturn(mockMetaData);

        JdbcBasePlugin plugin = getPlugin(mockConnectionManager, mockSecureLogin, context);
        Connection conn = plugin.getConnection();

        assertSame(mockConnection, conn);

        Properties connProps = new Properties();
        connProps.setProperty("foo", "foo-val");
        connProps.setProperty("bar", "bar-val");

        verify(mockConnectionManager).getConnection("test-server", "test-url", connProps, true, poolProps, "qual");
    }

    @Test
    public void testGetConnectionConnPropsPoolEnabledPoolProps() throws SQLException {
        context.setServerName("test-server");

        configuration.set("jdbc.driver", "org.greenplum.pxf.plugins.jdbc.FakeJdbcDriver");
        configuration.set("jdbc.url", "test-url");
        configuration.set("jdbc.connection.property.foo", "foo-val");
        configuration.set("jdbc.connection.property.bar", "bar-val");
        configuration.set("jdbc.pool.enabled", "true");
        configuration.set("jdbc.pool.property.abc", "abc-val");
        configuration.set("jdbc.pool.property.xyz", "xyz-val");
        configuration.set("jdbc.pool.property.maximumPoolSize", "99"); // overwrite default

        when(mockConnectionManager.getConnection(any(), any(), any(), anyBoolean(), any(), any())).thenReturn(mockConnection);
        when(mockConnection.getMetaData()).thenReturn(mockMetaData);

        JdbcBasePlugin plugin = getPlugin(mockConnectionManager, mockSecureLogin, context);
        Connection conn = plugin.getConnection();

        assertSame(mockConnection, conn);

        Properties connProps = new Properties();
        connProps.setProperty("foo", "foo-val");
        connProps.setProperty("bar", "bar-val");

        poolProps.setProperty("abc", "abc-val");
        poolProps.setProperty("xyz", "xyz-val");
        poolProps.setProperty("maximumPoolSize", "99");

        verify(mockConnectionManager).getConnection("test-server", "test-url", connProps, true, poolProps, null);
    }

    @Test
    public void testGetConnectionConnPropsPoolDisabledPoolProps() throws SQLException {
        context.setServerName("test-server");

        configuration.set("jdbc.driver", "org.greenplum.pxf.plugins.jdbc.FakeJdbcDriver");
        configuration.set("jdbc.url", "test-url");
        configuration.set("jdbc.connection.property.foo", "foo-val");
        configuration.set("jdbc.connection.property.bar", "bar-val");
        configuration.set("jdbc.pool.enabled", "false");
        configuration.set("jdbc.pool.property.abc", "abc-val");
        configuration.set("jdbc.pool.property.xyz", "xyz-val");

        when(mockConnectionManager.getConnection(any(), any(), any(), anyBoolean(), any(), any())).thenReturn(mockConnection);
        when(mockConnection.getMetaData()).thenReturn(mockMetaData);

        JdbcBasePlugin plugin = getPlugin(mockConnectionManager, mockSecureLogin, context);
        Connection conn = plugin.getConnection();

        assertSame(mockConnection, conn);

        Properties connProps = new Properties();
        connProps.setProperty("foo", "foo-val");
        connProps.setProperty("bar", "bar-val");

        verify(mockConnectionManager).getConnection("test-server", "test-url", connProps, false, null, null);
    }

    @Test
    public void testDateWideRangeFromConfiguration() throws SQLException {
        configuration.set("jdbc.driver", "org.greenplum.pxf.plugins.jdbc.FakeJdbcDriver");
        configuration.set("jdbc.url", "test-url");
        configuration.set("jdbc.date.wideRange", "true");
        JdbcBasePlugin plugin = getPlugin(mockConnectionManager, mockSecureLogin, context);
        assertTrue(plugin.isDateWideRange);
    }

    private JdbcBasePlugin getPlugin(ConnectionManager mockConnectionManager, SecureLogin mockSecureLogin, RequestContext context) {
        JdbcBasePlugin plugin = new JdbcBasePlugin(mockConnectionManager, mockSecureLogin);
        plugin.setRequestContext(context);
        plugin.afterPropertiesSet();
        return plugin;
    }
}
