package org.greenplum.pxf.plugins.jdbc.utils;

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
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.Before;
import org.junit.Test;

import org.mockito.Mockito;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SimpleSQLQueryExecutorTest {
    private static final String QUERY = "SET guc_variable = 1;";
    private static final boolean CONNECTION_AUTO_COMMIT = false;

    private Connection connection = mock(Connection.class);

    @Before
    public void setup() throws Exception {
        when(connection.isClosed()).thenReturn(false);
        when(connection.getAutoCommit()).thenReturn(CONNECTION_AUTO_COMMIT);
    }

    @Test
    public void testPreQuerySQLNormal() throws Exception {
        Statement statement = mock(Statement.class);
        when(statement.execute(QUERY)).thenReturn(true);
        when(connection.createStatement()).thenReturn(statement);

        SimpleSQLQueryExecutor.execute(connection, QUERY, true);

        verify(statement, Mockito.times(1)).execute(QUERY);
        verify(connection, Mockito.times(1)).commit();
        verify(statement, Mockito.times(1)).close();
    }

    @Test(expected=SQLException.class)
    public void testPreQuerySQLThrow() throws Exception {
        Statement statement = mock(Statement.class);
        when(statement.execute(QUERY)).thenThrow(new SQLException());
        when(connection.createStatement()).thenReturn(statement);

        SimpleSQLQueryExecutor.execute(connection, QUERY, true);
    }

    @Test
    public void testPreQuerySQLNoThrow() throws Exception {
        Statement statement = mock(Statement.class);
        when(statement.execute(QUERY)).thenThrow(new SQLException());
        when(connection.createStatement()).thenReturn(statement);

        SimpleSQLQueryExecutor.execute(connection, QUERY, false);

        verify(statement, Mockito.times(1)).execute(QUERY);
        verify(statement, Mockito.times(1)).close();
    }
}
