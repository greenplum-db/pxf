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

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.Test;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

import org.mockito.Mockito;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;

public class JdbcBasePluginTest {
    @Test
    public void testCloseConnectionWithCommit() throws Exception {
        DatabaseMetaData databaseMetaData = mock(DatabaseMetaData.class);
        Mockito.when(databaseMetaData.supportsTransactions()).thenReturn(true);

        Connection connection = mock(Connection.class);
        Mockito.when(connection.getMetaData()).thenReturn(databaseMetaData);
        Mockito.when(connection.isClosed()).thenReturn(false);
        Mockito.when(connection.getAutoCommit()).thenReturn(false);
        Mockito.doNothing().when(connection).commit();
        Mockito.doNothing().when(connection).close();

        Statement statement = mock(Statement.class);
        Mockito.when(statement.getConnection()).thenReturn(connection);
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
        Mockito.when(databaseMetaData.supportsTransactions()).thenReturn(true);

        Connection connection = mock(Connection.class);
        Mockito.when(connection.getMetaData()).thenReturn(databaseMetaData);
        Mockito.when(connection.isClosed()).thenReturn(false);
        Mockito.when(connection.getAutoCommit()).thenReturn(true);
        Mockito.doNothing().when(connection).close();

        Statement statement = mock(Statement.class);
        Mockito.when(statement.getConnection()).thenReturn(connection);
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
        Mockito.when(databaseMetaData.supportsTransactions()).thenReturn(false);

        Connection connection = mock(Connection.class);
        Mockito.when(connection.getMetaData()).thenReturn(databaseMetaData);
        Mockito.when(connection.isClosed()).thenReturn(false);
        Mockito.when(connection.getAutoCommit()).thenReturn(true);
        Mockito.doNothing().when(connection).close();

        Statement statement = mock(Statement.class);
        Mockito.when(statement.getConnection()).thenReturn(connection);
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
        Mockito.when(databaseMetaData.supportsTransactions()).thenReturn(false);

        Connection connection = mock(Connection.class);
        Mockito.when(connection.getMetaData()).thenReturn(databaseMetaData);
        Mockito.when(connection.isClosed()).thenReturn(true);
        Mockito.when(connection.getAutoCommit()).thenReturn(true);
        Mockito.doNothing().when(connection).close();

        Statement statement = mock(Statement.class);
        Mockito.when(statement.getConnection()).thenReturn(connection);
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
        Mockito.when(connection.getMetaData()).thenReturn(databaseMetaData);
        Mockito.when(connection.isClosed()).thenReturn(false);
        Mockito.when(connection.getAutoCommit()).thenReturn(false);
        Mockito.doNothing().when(connection).commit();
        Mockito.doNothing().when(connection).close();

        Statement statement = mock(Statement.class);
        Mockito.when(statement.getConnection()).thenReturn(connection);
        Mockito.doNothing().when(statement).close();

        // Test method and check
        try {
            JdbcBasePlugin.closeStatementAndConnection(statement);
            fail("SQLException must have been thrown");
        }
        catch (Exception e) {
            assertSame(exception, e);
        }

        Mockito.verify(statement, times(1)).close();
        Mockito.verify(connection, times(1)).close();
    }

    @Test
    public void testCloseConnectionWithExceptionConnectionOnCommit() throws Exception {
        SQLException exception = new SQLException("SQLException");

        DatabaseMetaData databaseMetaData = mock(DatabaseMetaData.class);
        Mockito.when(databaseMetaData.supportsTransactions()).thenReturn(true);

        Connection connection = mock(Connection.class);
        Mockito.when(connection.getMetaData()).thenReturn(databaseMetaData);
        Mockito.when(connection.isClosed()).thenReturn(false);
        Mockito.when(connection.getAutoCommit()).thenReturn(false);
        Mockito.doThrow(exception).when(connection).commit();
        Mockito.doNothing().when(connection).close();

        Statement statement = mock(Statement.class);
        Mockito.when(statement.getConnection()).thenReturn(connection);
        Mockito.doNothing().when(statement).close();

        // Test method and check
        try {
            JdbcBasePlugin.closeStatementAndConnection(statement);
            fail("SQLException must have been thrown");
        }
        catch (Exception e) {
            assertSame(exception, e);
        }

        Mockito.verify(statement, times(1)).close();
        Mockito.verify(connection, times(1)).close();
    }

    @Test
    public void testCloseConnectionWithExceptionConnectionOnClose() throws Exception {
        SQLException exception = new SQLException("SQLException");

        DatabaseMetaData databaseMetaData = mock(DatabaseMetaData.class);
        Mockito.when(databaseMetaData.supportsTransactions()).thenReturn(false);

        Connection connection = mock(Connection.class);
        Mockito.when(connection.getMetaData()).thenReturn(databaseMetaData);
        Mockito.when(connection.isClosed()).thenReturn(false);
        Mockito.when(connection.getAutoCommit()).thenReturn(false);
        Mockito.doNothing().when(connection).commit();
        Mockito.doThrow(exception).when(connection).close();

        Statement statement = mock(Statement.class);
        Mockito.when(statement.getConnection()).thenReturn(connection);
        Mockito.doNothing().when(statement).close();

        // Test method and check
        try {
            JdbcBasePlugin.closeStatementAndConnection(statement);
            fail("SQLException must have been thrown");
        }
        catch (Exception e) {
            assertSame(exception, e);
        }

        Mockito.verify(statement, times(1)).close();
    }

    @Test
    public void testCloseConnectionWithExceptionStatementOnClose() throws Exception {
        SQLException exception = new SQLException("SQLException");

        DatabaseMetaData databaseMetaData = mock(DatabaseMetaData.class);
        Mockito.when(databaseMetaData.supportsTransactions()).thenReturn(false);

        Connection connection = mock(Connection.class);
        Mockito.when(connection.getMetaData()).thenReturn(databaseMetaData);
        Mockito.when(connection.isClosed()).thenReturn(false);
        Mockito.when(connection.getAutoCommit()).thenReturn(false);
        Mockito.doNothing().when(connection).commit();
        Mockito.doNothing().when(connection).close();

        Statement statement = mock(Statement.class);
        Mockito.when(statement.getConnection()).thenReturn(connection);
        Mockito.doThrow(exception).when(statement).close();

        // Test method and check
        try {
            JdbcBasePlugin.closeStatementAndConnection(statement);
            fail("SQLException must have been thrown");
        }
        catch (Exception e) {
            assertSame(exception, e);
        }

        Mockito.verify(connection, times(1)).close();
    }

    @Test
    public void testCloseConnectionWithExceptionStatementOnGetConnection() throws Exception {
        SQLException exception = new SQLException("SQLException");

        DatabaseMetaData databaseMetaData = mock(DatabaseMetaData.class);
        Mockito.when(databaseMetaData.supportsTransactions()).thenReturn(false);

        Connection connection = mock(Connection.class);
        Mockito.when(connection.getMetaData()).thenReturn(databaseMetaData);
        Mockito.when(connection.isClosed()).thenReturn(false);
        Mockito.when(connection.getAutoCommit()).thenReturn(false);
        Mockito.doNothing().when(connection).commit();
        Mockito.doNothing().when(connection).close();

        Statement statement = mock(Statement.class);
        Mockito.doThrow(exception).when(statement).getConnection();
        Mockito.doNothing().when(statement).close();

        // Test method and check
        try {
            JdbcBasePlugin.closeStatementAndConnection(statement);
            fail("SQLException must have been thrown");
        }
        catch (Exception e) {
            assertSame(exception, e);
        }

        Mockito.verify(statement, times(1)).close();
    }
}

