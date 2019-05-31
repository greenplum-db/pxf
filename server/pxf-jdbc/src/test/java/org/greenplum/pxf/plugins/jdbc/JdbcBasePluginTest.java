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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyObject;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({DriverManager.class, JdbcBasePlugin.class})
public class JdbcBasePluginTest {
    @Test
    public void testCloseConnectionWithCommit() throws Exception {
        DatabaseMetaData databaseMetaData = mock(DatabaseMetaData.class);
        when(databaseMetaData.supportsTransactions()).thenReturn(true);

        Connection connection = mock(Connection.class);
        when(connection.getMetaData()).thenReturn(databaseMetaData);
        when(connection.isClosed()).thenReturn(false);
        when(connection.getAutoCommit()).thenReturn(false);
        Mockito.doNothing().when(connection).commit();
        Mockito.doNothing().when(connection).close();

        Statement statement = mock(Statement.class);
        when(statement.getConnection()).thenReturn(connection);
        Mockito.doNothing().when(statement).close();

        // Test method
        JdbcBasePlugin.closeStatementAndConnection(statement);

        // Check
        Mockito.verify(statement, times(1)).close();
        Mockito.verify(connection, times(1)).close();

        Mockito.verify(connection, times(1)).commit();
    }

    @Test
    public void testCloseConnectionWithoutCommit() throws Exception {
        DatabaseMetaData databaseMetaData = mock(DatabaseMetaData.class);
        when(databaseMetaData.supportsTransactions()).thenReturn(true);

        Connection connection = mock(Connection.class);
        when(connection.getMetaData()).thenReturn(databaseMetaData);
        when(connection.isClosed()).thenReturn(false);
        when(connection.getAutoCommit()).thenReturn(true);
        Mockito.doNothing().when(connection).close();

        Statement statement = mock(Statement.class);
        when(statement.getConnection()).thenReturn(connection);
        Mockito.doNothing().when(statement).close();

        // Test method
        JdbcBasePlugin.closeStatementAndConnection(statement);

        // Check
        Mockito.verify(statement, times(1)).close();
        Mockito.verify(connection, times(1)).close();
    }

    @Test
    public void testCloseConnectionWithoutTransactions() throws Exception {
        DatabaseMetaData databaseMetaData = mock(DatabaseMetaData.class);
        when(databaseMetaData.supportsTransactions()).thenReturn(false);

        Connection connection = mock(Connection.class);
        when(connection.getMetaData()).thenReturn(databaseMetaData);
        when(connection.isClosed()).thenReturn(false);
        when(connection.getAutoCommit()).thenReturn(true);
        Mockito.doNothing().when(connection).close();

        Statement statement = mock(Statement.class);
        when(statement.getConnection()).thenReturn(connection);
        Mockito.doNothing().when(statement).close();

        // Test method
        JdbcBasePlugin.closeStatementAndConnection(statement);

        // Check
        Mockito.verify(statement, times(1)).close();
        Mockito.verify(connection, times(1)).close();
    }

    @Test
    public void testCloseConnectionClosed() throws Exception {
        DatabaseMetaData databaseMetaData = mock(DatabaseMetaData.class);
        when(databaseMetaData.supportsTransactions()).thenReturn(false);

        Connection connection = mock(Connection.class);
        when(connection.getMetaData()).thenReturn(databaseMetaData);
        when(connection.isClosed()).thenReturn(true);
        when(connection.getAutoCommit()).thenReturn(true);
        Mockito.doNothing().when(connection).close();

        Statement statement = mock(Statement.class);
        when(statement.getConnection()).thenReturn(connection);
        Mockito.doNothing().when(statement).close();

        // Test method
        JdbcBasePlugin.closeStatementAndConnection(statement);

        // Check
        Mockito.verify(statement, times(1)).close();
    }

    @Test
    public void testCloseConnectionWithExceptionDatabaseMetaData() throws Exception {
        SQLException exception = new SQLException("SQLException");

        DatabaseMetaData databaseMetaData = mock(DatabaseMetaData.class);
        Mockito.doThrow(exception).when(databaseMetaData).supportsTransactions();

        Connection connection = mock(Connection.class);
        when(connection.getMetaData()).thenReturn(databaseMetaData);
        when(connection.isClosed()).thenReturn(false);
        when(connection.getAutoCommit()).thenReturn(false);
        Mockito.doNothing().when(connection).commit();
        Mockito.doNothing().when(connection).close();

        Statement statement = mock(Statement.class);
        when(statement.getConnection()).thenReturn(connection);
        Mockito.doNothing().when(statement).close();

        // Test method and check
        try {
            JdbcBasePlugin.closeStatementAndConnection(statement);
            fail("SQLException must have been thrown");
        } catch (Exception e) {
            assertSame(exception, e);
        }

        Mockito.verify(statement, times(1)).close();
        Mockito.verify(connection, times(1)).close();
    }

    @Test
    public void testCloseConnectionWithExceptionConnectionOnCommit() throws Exception {
        SQLException exception = new SQLException("SQLException");

        DatabaseMetaData databaseMetaData = mock(DatabaseMetaData.class);
        when(databaseMetaData.supportsTransactions()).thenReturn(true);

        Connection connection = mock(Connection.class);
        when(connection.getMetaData()).thenReturn(databaseMetaData);
        when(connection.isClosed()).thenReturn(false);
        when(connection.getAutoCommit()).thenReturn(false);
        Mockito.doThrow(exception).when(connection).commit();
        Mockito.doNothing().when(connection).close();

        Statement statement = mock(Statement.class);
        when(statement.getConnection()).thenReturn(connection);
        Mockito.doNothing().when(statement).close();

        // Test method and check
        try {
            JdbcBasePlugin.closeStatementAndConnection(statement);
            fail("SQLException must have been thrown");
        } catch (Exception e) {
            assertSame(exception, e);
        }

        Mockito.verify(statement, times(1)).close();
        Mockito.verify(connection, times(1)).close();
    }

    @Test
    public void testCloseConnectionWithExceptionConnectionOnClose() throws Exception {
        SQLException exception = new SQLException("SQLException");

        DatabaseMetaData databaseMetaData = mock(DatabaseMetaData.class);
        when(databaseMetaData.supportsTransactions()).thenReturn(false);

        Connection connection = mock(Connection.class);
        when(connection.getMetaData()).thenReturn(databaseMetaData);
        when(connection.isClosed()).thenReturn(false);
        when(connection.getAutoCommit()).thenReturn(false);
        Mockito.doNothing().when(connection).commit();
        Mockito.doThrow(exception).when(connection).close();

        Statement statement = mock(Statement.class);
        when(statement.getConnection()).thenReturn(connection);
        Mockito.doNothing().when(statement).close();

        JdbcBasePlugin.closeStatementAndConnection(statement);

        Mockito.verify(statement, times(1)).close();
    }

    @Test
    public void testCloseConnectionWithExceptionStatementOnClose() throws Exception {
        SQLException exception = new SQLException("SQLException");

        DatabaseMetaData databaseMetaData = mock(DatabaseMetaData.class);
        when(databaseMetaData.supportsTransactions()).thenReturn(false);

        Connection connection = mock(Connection.class);
        when(connection.getMetaData()).thenReturn(databaseMetaData);
        when(connection.isClosed()).thenReturn(false);
        when(connection.getAutoCommit()).thenReturn(false);
        Mockito.doNothing().when(connection).commit();
        Mockito.doNothing().when(connection).close();

        Statement statement = mock(Statement.class);
        when(statement.getConnection()).thenReturn(connection);
        Mockito.doThrow(exception).when(statement).close();

        // Test method and check
        try {
            JdbcBasePlugin.closeStatementAndConnection(statement);
            fail("SQLException must have been thrown");
        } catch (Exception e) {
            assertSame(exception, e);
        }

        Mockito.verify(connection, times(1)).close();
    }

    @Test
    public void testCloseConnectionWithExceptionStatementOnGetConnection() throws Exception {
        SQLException exception = new SQLException("SQLException");

        DatabaseMetaData databaseMetaData = mock(DatabaseMetaData.class);
        when(databaseMetaData.supportsTransactions()).thenReturn(false);

        Connection connection = mock(Connection.class);
        when(connection.getMetaData()).thenReturn(databaseMetaData);
        when(connection.isClosed()).thenReturn(false);
        when(connection.getAutoCommit()).thenReturn(false);
        Mockito.doNothing().when(connection).commit();
        Mockito.doNothing().when(connection).close();

        Statement statement = mock(Statement.class);
        Mockito.doThrow(exception).when(statement).getConnection();
        Mockito.doNothing().when(statement).close();

        // Test method and check
        try {
            JdbcBasePlugin.closeStatementAndConnection(statement);
            fail("SQLException must have been thrown");
        } catch (Exception e) {
            assertSame(exception, e);
        }

        Mockito.verify(statement, times(1)).close();
    }

    @Test
    public void testTransactionIsolationNotSetByUser() throws SQLException {
        PowerMockito.mockStatic(DriverManager.class);

        RequestContext context = new RequestContext();
        context.setDataSource("test-table");
        Map<String, String> additionalProps = new HashMap<>();
        additionalProps.put("jdbc.driver", "org.greenplum.pxf.plugins.jdbc.FakeJdbcDriver");
        additionalProps.put("jdbc.url", "test-url");
        context.setAdditionalConfigProps(additionalProps);

        Connection mockConnection = mock(Connection.class);
        DatabaseMetaData fakeMetaData = mock(DatabaseMetaData.class);
        when(mockConnection.getMetaData()).thenReturn(fakeMetaData);
        when(DriverManager.getConnection(anyString(), anyObject())).thenReturn(mockConnection);

        JdbcBasePlugin plugin = new JdbcBasePlugin();
        plugin.initialize(context);
        Connection conn = plugin.getConnection();

        verify(conn, never()).setTransactionIsolation(anyInt());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testTransactionIsolationSetByUserToInvalidValue() throws SQLException {
        PowerMockito.mockStatic(DriverManager.class);

        RequestContext context = new RequestContext();
        context.setDataSource("test-table");
        Map<String, String> additionalProps = new HashMap<>();
        additionalProps.put("jdbc.driver", "org.greenplum.pxf.plugins.jdbc.FakeJdbcDriver");
        additionalProps.put("jdbc.url", "test-url");
        additionalProps.put("jdbc.connection.transactionIsolation", "foobarValue");
        context.setAdditionalConfigProps(additionalProps);

        Connection mockConnection = mock(Connection.class);
        DatabaseMetaData fakeMetaData = mock(DatabaseMetaData.class);
        when(mockConnection.getMetaData()).thenReturn(fakeMetaData);
        when(DriverManager.getConnection(anyString(), anyObject())).thenReturn(mockConnection);

        JdbcBasePlugin plugin = new JdbcBasePlugin();
        plugin.initialize(context);
    }

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testTransactionIsolationSetByUserToUnsupportedValue() throws SQLException {
        thrown.expect(SQLException.class);
        thrown.expectMessage("Transaction isolation level READ_UNCOMMITTED is not supported");

        PowerMockito.mockStatic(DriverManager.class);
        RequestContext context = new RequestContext();
        context.setDataSource("test-table");
        Map<String, String> additionalProps = new HashMap<>();
        additionalProps.put("jdbc.driver", "org.greenplum.pxf.plugins.jdbc.FakeJdbcDriver");
        additionalProps.put("jdbc.url", "test-url");
        additionalProps.put("jdbc.connection.transactionIsolation", "READ_UNCOMMITTED");
        context.setAdditionalConfigProps(additionalProps);

        Connection mockConnection = mock(Connection.class);
        DatabaseMetaData fakeMetaData = mock(DatabaseMetaData.class);
        // READ_UNCOMMITTED is level 1
        when(fakeMetaData.supportsTransactionIsolationLevel(1)).thenReturn(false);
        when(mockConnection.getMetaData()).thenReturn(fakeMetaData);
        when(DriverManager.getConnection(anyString(), anyObject())).thenReturn(mockConnection);

        JdbcBasePlugin plugin = new JdbcBasePlugin();

        plugin.initialize(context);
        plugin.getConnection();
    }

    @Test
    public void testTransactionIsolationSetByUserToValidValue() throws SQLException {
        PowerMockito.mockStatic(DriverManager.class);

        RequestContext context = new RequestContext();
        context.setDataSource("test-table");
        Map<String, String> additionalProps = new HashMap<>();
        additionalProps.put("jdbc.driver", "org.greenplum.pxf.plugins.jdbc.FakeJdbcDriver");
        additionalProps.put("jdbc.url", "test-url");
        additionalProps.put("jdbc.connection.transactionIsolation", "READ_COMMITTED");
        context.setAdditionalConfigProps(additionalProps);

        Connection mockConnection = mock(Connection.class);
        DatabaseMetaData fakeMetaData = mock(DatabaseMetaData.class);
        // READ_COMMITTED is level 2
        when(fakeMetaData.supportsTransactionIsolationLevel(2)).thenReturn(true);
        when(mockConnection.getMetaData()).thenReturn(fakeMetaData);
        when(DriverManager.getConnection(anyString(), anyObject())).thenReturn(mockConnection);

        JdbcBasePlugin plugin = new JdbcBasePlugin();
        plugin.initialize(context);
        Connection conn = plugin.getConnection();

        // READ_COMMITTED is level 2
        verify(conn).setTransactionIsolation(2);
    }

    @Test(expected = SQLException.class)
    public void testTransactionIsolationSetByUserFailedToGetMetadata() throws SQLException {
        PowerMockito.mockStatic(DriverManager.class);

        RequestContext context = new RequestContext();
        context.setDataSource("test-table");
        Map<String, String> additionalProps = new HashMap<>();
        additionalProps.put("jdbc.driver", "org.greenplum.pxf.plugins.jdbc.FakeJdbcDriver");
        additionalProps.put("jdbc.url", "test-url");
        context.setAdditionalConfigProps(additionalProps);

        Connection mockConnection = mock(Connection.class);
        doThrow(new SQLException("")).when(mockConnection).getMetaData();
        when(DriverManager.getConnection(anyString(), anyObject())).thenReturn(mockConnection);

        JdbcBasePlugin plugin = new JdbcBasePlugin();
        plugin.initialize(context);
        plugin.getConnection();
    }

    @Test
    public void testGetPreparedStatementSetsQueryTimeoutIfSpecified() throws SQLException {
        PowerMockito.mockStatic(DriverManager.class);

        RequestContext context = new RequestContext();
        context.setDataSource("test-table");
        Map<String, String> additionalProps = new HashMap<>();
        additionalProps.put("jdbc.driver", "org.greenplum.pxf.plugins.jdbc.FakeJdbcDriver");
        additionalProps.put("jdbc.url", "test-url");
        additionalProps.put("jdbc.statement.queryTimeout", "173");
        context.setAdditionalConfigProps(additionalProps);

        Connection mockConnection = mock(Connection.class);
        DatabaseMetaData fakeMetaData = mock(DatabaseMetaData.class);
        when(mockConnection.getMetaData()).thenReturn(fakeMetaData);
        when(DriverManager.getConnection(anyString(), anyObject())).thenReturn(mockConnection);

        PreparedStatement mockStatement = mock(PreparedStatement.class);
        when(mockConnection.prepareStatement(anyString())).thenReturn(mockStatement);

        JdbcBasePlugin plugin = new JdbcBasePlugin();
        plugin.initialize(context);
        plugin.getPreparedStatement(mockConnection, "foo");

        verify(mockStatement).setQueryTimeout(173);
    }

    @Test
    public void testGetPreparedStatementDoesNotSetQueryTimeoutIfNotSpecified() throws SQLException {
        PowerMockito.mockStatic(DriverManager.class);

        RequestContext context = new RequestContext();
        context.setDataSource("test-table");
        Map<String, String> additionalProps = new HashMap<>();
        additionalProps.put("jdbc.driver", "org.greenplum.pxf.plugins.jdbc.FakeJdbcDriver");
        additionalProps.put("jdbc.url", "test-url");
        context.setAdditionalConfigProps(additionalProps);

        Connection mockConnection = mock(Connection.class);
        DatabaseMetaData fakeMetaData = mock(DatabaseMetaData.class);
        when(mockConnection.getMetaData()).thenReturn(fakeMetaData);
        when(DriverManager.getConnection(anyString(), anyObject())).thenReturn(mockConnection);

        PreparedStatement mockStatement = mock(PreparedStatement.class);
        when(mockConnection.prepareStatement(anyString())).thenReturn(mockStatement);

        JdbcBasePlugin plugin = new JdbcBasePlugin();
        plugin.initialize(context);
        plugin.getPreparedStatement(mockConnection, "foo");

        verify(mockStatement, never()).setQueryTimeout(anyInt());
    }
}