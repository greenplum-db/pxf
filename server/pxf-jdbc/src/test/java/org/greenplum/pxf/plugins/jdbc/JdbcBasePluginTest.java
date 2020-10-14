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

import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.plugins.jdbc.utils.ConnectionManager;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyObject;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class JdbcBasePluginTest {

    @Rule public ExpectedException thrown = ExpectedException.none();

    @Mock private ConnectionManager mockConnectionManager;
    @Mock private DatabaseMetaData mockMetaData;
    @Mock private Connection mockConnection;
    @Mock private PreparedStatement mockStatement;

    private SQLException exception = new SQLException("some error");
    private RequestContext context;
    private Map<String, String> additionalProps;
    private Properties poolProps;

    @Before public void before() {
        context = new RequestContext();
        context.setConfig("default");
        context.setDataSource("test-table");
        additionalProps = new HashMap<>();
        context.setAdditionalConfigProps(additionalProps);
        context.setUser("test-user");

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
        when(mockConnection.getAutoCommit()).thenReturn(true);
        Mockito.doNothing().when(mockConnection).close();
        when(mockStatement.getConnection()).thenReturn(mockConnection);
        Mockito.doNothing().when(mockStatement).close();

        JdbcBasePlugin.closeStatementAndConnection(mockStatement);

        verify(mockStatement, times(1)).close();
        verify(mockConnection, times(1)).close();
    }

    @Test
    public void testCloseConnectionClosed() throws Exception {
        when(mockMetaData.supportsTransactions()).thenReturn(false);
        when(mockConnection.getMetaData()).thenReturn(mockMetaData);
        when(mockConnection.isClosed()).thenReturn(true);
        when(mockConnection.getAutoCommit()).thenReturn(true);
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
        when(mockConnection.getAutoCommit()).thenReturn(false);
        Mockito.doNothing().when(mockConnection).commit();
        Mockito.doNothing().when(mockConnection).close();
        when(mockConnection.getMetaData()).thenReturn(mockMetaData);
        doThrow(exception).when(mockMetaData).supportsTransactions();

        try {
            JdbcBasePlugin.closeStatementAndConnection(mockStatement);
            fail("SQLException must have been thrown");
        } catch (Exception e) {
            assertSame(exception, e);
        }

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

        try {
            JdbcBasePlugin.closeStatementAndConnection(mockStatement);
            fail("SQLException must have been thrown");
        } catch (Exception e) {
            assertSame(exception, e);
        }

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
        when(mockConnection.getAutoCommit()).thenReturn(false);
        Mockito.doNothing().when(mockConnection).commit();
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
        when(mockConnection.getAutoCommit()).thenReturn(false);
        Mockito.doNothing().when(mockConnection).commit();
        Mockito.doNothing().when(mockConnection).close();
        doThrow(exception).when(mockStatement).close();

        try {
            JdbcBasePlugin.closeStatementAndConnection(mockStatement);
            fail("SQLException must have been thrown");
        } catch (Exception e) {
            assertSame(exception, e);
        }

        verify(mockConnection, times(1)).close();
    }

    @Test
    public void testCloseConnectionWithExceptionStatementOnGetConnection() throws Exception {
        when(mockConnection.getMetaData()).thenReturn(mockMetaData);
        when(mockMetaData.supportsTransactions()).thenReturn(false);
        when(mockConnection.isClosed()).thenReturn(false);
        when(mockConnection.getAutoCommit()).thenReturn(false);
        Mockito.doNothing().when(mockConnection).commit();
        Mockito.doNothing().when(mockConnection).close();
        doThrow(exception).when(mockStatement).getConnection();
        Mockito.doNothing().when(mockStatement).close();

        try {
            JdbcBasePlugin.closeStatementAndConnection(mockStatement);
            fail("SQLException must have been thrown");
        } catch (Exception e) {
            assertSame(exception, e);
        }

        verify(mockStatement, times(1)).close();
    }

    @Test
    public void testTransactionIsolationNotSetByUser() throws SQLException {
        additionalProps.put("jdbc.driver", "org.greenplum.pxf.plugins.jdbc.FakeJdbcDriver");
        additionalProps.put("jdbc.url", "test-url");

        when(mockConnectionManager.getConnection(anyString(), anyString(), anyObject(), anyBoolean(), anyObject(), anyString())).thenReturn(mockConnection);
        when(mockConnection.getMetaData()).thenReturn(mockMetaData);

        JdbcBasePlugin plugin = new JdbcBasePlugin(mockConnectionManager);
        plugin.initialize(context);
        Connection conn = plugin.getConnection();

        verify(conn, never()).setTransactionIsolation(anyInt());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testTransactionIsolationSetByUserToInvalidValue() throws SQLException {
        additionalProps.put("jdbc.driver", "org.greenplum.pxf.plugins.jdbc.FakeJdbcDriver");
        additionalProps.put("jdbc.url", "test-url");
        additionalProps.put("jdbc.connection.transactionIsolation", "foobarValue");

        when(mockConnectionManager.getConnection(anyString(), anyString(), anyObject(), anyBoolean(), anyObject(), anyString())).thenReturn(mockConnection);
        when(mockConnection.getMetaData()).thenReturn(mockMetaData);

        JdbcBasePlugin plugin = new JdbcBasePlugin(mockConnectionManager);
        plugin.initialize(context);
    }


    @Test
    public void testTransactionIsolationSetByUserToUnsupportedValue() throws SQLException {
        thrown.expect(SQLException.class);
        thrown.expectMessage("Transaction isolation level READ_UNCOMMITTED is not supported");

        additionalProps.put("jdbc.driver", "org.greenplum.pxf.plugins.jdbc.FakeJdbcDriver");
        additionalProps.put("jdbc.url", "test-url");
        additionalProps.put("jdbc.connection.transactionIsolation", "READ_UNCOMMITTED");

        when(mockConnectionManager.getConnection(anyString(), anyString(), anyObject(), anyBoolean(), anyObject(), anyString())).thenReturn(mockConnection);
        when(mockConnection.getMetaData()).thenReturn(mockMetaData);
        // READ_UNCOMMITTED is level 1
        when(mockMetaData.supportsTransactionIsolationLevel(1)).thenReturn(false);

        JdbcBasePlugin plugin = new JdbcBasePlugin(mockConnectionManager);
        plugin.initialize(context);
        plugin.getConnection();
    }

    @Test
    public void testTransactionIsolationSetByUserToValidValue() throws SQLException {
        additionalProps.put("jdbc.driver", "org.greenplum.pxf.plugins.jdbc.FakeJdbcDriver");
        additionalProps.put("jdbc.url", "test-url");
        additionalProps.put("jdbc.connection.transactionIsolation", "READ_COMMITTED");

        when(mockConnectionManager.getConnection(anyString(), anyString(), anyObject(), anyBoolean(), anyObject(), anyString())).thenReturn(mockConnection);
        when(mockConnection.getMetaData()).thenReturn(mockMetaData);
        // READ_COMMITTED is level 2
        when(mockMetaData.supportsTransactionIsolationLevel(2)).thenReturn(true);

        JdbcBasePlugin plugin = new JdbcBasePlugin(mockConnectionManager);
        plugin.initialize(context);
        Connection conn = plugin.getConnection();

        // READ_COMMITTED is level 2
        verify(conn).setTransactionIsolation(2);
    }

    @Test(expected = SQLException.class)
    public void testTransactionIsolationSetByUserFailedToGetMetadata() throws SQLException {
        additionalProps.put("jdbc.driver", "org.greenplum.pxf.plugins.jdbc.FakeJdbcDriver");
        additionalProps.put("jdbc.url", "test-url");

        when(mockConnectionManager.getConnection(anyString(), anyString(), anyObject(), anyBoolean(), anyObject(), anyString())).thenReturn(mockConnection);
        doThrow(new SQLException("")).when(mockConnection).getMetaData();

        JdbcBasePlugin plugin = new JdbcBasePlugin(mockConnectionManager);
        plugin.initialize(context);
        plugin.getConnection();
    }

    @Test
    public void testGetPreparedStatementSetsQueryTimeoutIfSpecified() throws SQLException {
        additionalProps.put("jdbc.driver", "org.greenplum.pxf.plugins.jdbc.FakeJdbcDriver");
        additionalProps.put("jdbc.url", "test-url");
        additionalProps.put("jdbc.statement.queryTimeout", "173");

        when(mockConnectionManager.getConnection(anyString(), anyString(), anyObject(), anyBoolean(), anyObject(), anyString())).thenReturn(mockConnection);
        when(mockConnection.getMetaData()).thenReturn(mockMetaData);
        when(mockConnection.prepareStatement(anyString())).thenReturn(mockStatement);

        JdbcBasePlugin plugin = new JdbcBasePlugin(mockConnectionManager);
        plugin.initialize(context);
        plugin.getPreparedStatement(mockConnection, "foo");

        verify(mockStatement).setQueryTimeout(173);
    }

    @Test
    public void testGetPreparedStatementDoesNotSetQueryTimeoutIfNotSpecified() throws SQLException {
        additionalProps.put("jdbc.driver", "org.greenplum.pxf.plugins.jdbc.FakeJdbcDriver");
        additionalProps.put("jdbc.url", "test-url");

        when(mockConnectionManager.getConnection(anyString(), anyString(), anyObject(), anyBoolean(), anyObject(), anyString())).thenReturn(mockConnection);
        when(mockConnection.getMetaData()).thenReturn(mockMetaData);
        when(mockConnection.prepareStatement(anyString())).thenReturn(mockStatement);

        JdbcBasePlugin plugin = new JdbcBasePlugin(mockConnectionManager);
        plugin.initialize(context);
        plugin.getPreparedStatement(mockConnection, "foo");

        verify(mockStatement, never()).setQueryTimeout(anyInt());
    }

    @Test
    public void testGetConnectionNoConnPropsPoolDisabled() throws SQLException {
        context.setServerName("test-server");
        additionalProps.put("jdbc.driver", "org.greenplum.pxf.plugins.jdbc.FakeJdbcDriver");
        additionalProps.put("jdbc.url", "test-url");
        additionalProps.put("jdbc.pool.enabled", "false");

        when(mockConnectionManager.getConnection(anyString(), anyString(), anyObject(), anyBoolean(), anyObject(), anyString())).thenReturn(mockConnection);
        when(mockConnection.getMetaData()).thenReturn(mockMetaData);

        JdbcBasePlugin plugin = new JdbcBasePlugin(mockConnectionManager);
        plugin.initialize(context);
        Connection conn = plugin.getConnection();

        assertSame(mockConnection, conn);

        verify(mockConnectionManager).getConnection("test-server", "test-url", new Properties(), false, null, null);
    }

    @Test
    public void testGetConnectionConnPropsPoolDisabled() throws SQLException {
        context.setServerName("test-server");
        additionalProps.put("jdbc.driver", "org.greenplum.pxf.plugins.jdbc.FakeJdbcDriver");
        additionalProps.put("jdbc.url", "test-url");
        additionalProps.put("jdbc.connection.property.foo", "foo-val");
        additionalProps.put("jdbc.connection.property.bar", "bar-val");
        additionalProps.put("jdbc.pool.enabled", "false");

        when(mockConnectionManager.getConnection(anyString(), anyString(), anyObject(), anyBoolean(), anyObject(), anyString())).thenReturn(mockConnection);
        when(mockConnection.getMetaData()).thenReturn(mockMetaData);

        JdbcBasePlugin plugin = new JdbcBasePlugin(mockConnectionManager);
        plugin.initialize(context);
        Connection conn = plugin.getConnection();

        assertSame(mockConnection, conn);

        Properties connProps = new Properties();
        connProps.setProperty("foo", "foo-val");
        connProps.setProperty("bar", "bar-val");

        verify(mockConnectionManager).getConnection("test-server", "test-url", connProps, false, null, null);
    }

    @Test
    public void testGetConnectionConnPropsPoolEnabledNoPoolProps() throws SQLException {
        context.setServerName("test-server");
        additionalProps.put("jdbc.driver", "org.greenplum.pxf.plugins.jdbc.FakeJdbcDriver");
        additionalProps.put("jdbc.url", "test-url");
        additionalProps.put("jdbc.connection.property.foo", "foo-val");
        additionalProps.put("jdbc.connection.property.bar", "bar-val");
        // pool is enabled by default

        when(mockConnectionManager.getConnection(anyString(), anyString(), anyObject(), anyBoolean(), anyObject(), anyString())).thenReturn(mockConnection);
        when(mockConnection.getMetaData()).thenReturn(mockMetaData);

        JdbcBasePlugin plugin = new JdbcBasePlugin(mockConnectionManager);
        plugin.initialize(context);
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
        additionalProps.put("jdbc.driver", "org.greenplum.pxf.plugins.jdbc.FakeJdbcDriver");
        additionalProps.put("jdbc.url", "test-url");
        additionalProps.put("jdbc.connection.property.foo", "foo-val");
        additionalProps.put("jdbc.connection.property.bar", "bar-val");
        // pool is enabled by default

        additionalProps.put("jdbc.pool.qualifier", "qual");

        when(mockConnectionManager.getConnection(anyString(), anyString(), anyObject(), anyBoolean(), anyObject(), anyString())).thenReturn(mockConnection);
        when(mockConnection.getMetaData()).thenReturn(mockMetaData);

        JdbcBasePlugin plugin = new JdbcBasePlugin(mockConnectionManager);
        plugin.initialize(context);
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
        additionalProps.put("jdbc.driver", "org.greenplum.pxf.plugins.jdbc.FakeJdbcDriver");
        additionalProps.put("jdbc.url", "test-url");
        additionalProps.put("jdbc.connection.property.foo", "foo-val");
        additionalProps.put("jdbc.connection.property.bar", "bar-val");
        additionalProps.put("jdbc.pool.enabled", "true");
        additionalProps.put("jdbc.pool.property.abc", "abc-val");
        additionalProps.put("jdbc.pool.property.xyz", "xyz-val");
        additionalProps.put("jdbc.pool.property.maximumPoolSize", "99"); // overwrite default

        when(mockConnectionManager.getConnection(anyString(), anyString(), anyObject(), anyBoolean(), anyObject(), anyString())).thenReturn(mockConnection);
        when(mockConnection.getMetaData()).thenReturn(mockMetaData);

        JdbcBasePlugin plugin = new JdbcBasePlugin(mockConnectionManager);
        plugin.initialize(context);
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
        additionalProps.put("jdbc.driver", "org.greenplum.pxf.plugins.jdbc.FakeJdbcDriver");
        additionalProps.put("jdbc.url", "test-url");
        additionalProps.put("jdbc.connection.property.foo", "foo-val");
        additionalProps.put("jdbc.connection.property.bar", "bar-val");
        additionalProps.put("jdbc.pool.enabled", "false");
        additionalProps.put("jdbc.pool.property.abc", "abc-val");
        additionalProps.put("jdbc.pool.property.xyz", "xyz-val");

        when(mockConnectionManager.getConnection(anyString(), anyString(), anyObject(), anyBoolean(), anyObject(), anyString())).thenReturn(mockConnection);
        when(mockConnection.getMetaData()).thenReturn(mockMetaData);

        JdbcBasePlugin plugin = new JdbcBasePlugin(mockConnectionManager);
        plugin.initialize(context);
        Connection conn = plugin.getConnection();

        assertSame(mockConnection, conn);

        Properties connProps = new Properties();
        connProps.setProperty("foo", "foo-val");
        connProps.setProperty("bar", "bar-val");

        verify(mockConnectionManager).getConnection("test-server", "test-url", connProps, false, null, null);
    }
}
