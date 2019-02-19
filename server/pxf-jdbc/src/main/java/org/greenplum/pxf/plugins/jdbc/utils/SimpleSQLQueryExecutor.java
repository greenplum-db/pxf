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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A simple SQL queries executor class. Processes exceptions inside according to given parameters.
 *
 * Used by {@link org.greenplum.pxf.plugins.jdbc.JdbcAccessor} to execute PRE_SQL statements
 */
public class SimpleSQLQueryExecutor {
    /**
     * Execute a SQL query in a dedicated {@link Statement} and process its exceptions
     *
     * @param connection JDBC connection to use
     * @param query Query to execute
     * @param allowThrow allow this procedure to throw {@link SQLException} from query execution
     *
     * @throws SQLException if it happens during 'statement.execute()'' AND 'allowThrow' is 'true'
     */
    public static void execute(Connection connection, String query, boolean allowThrow) throws SQLException, IllegalArgumentException {
        if (query == null) {
            throw new IllegalArgumentException("The provided SQL query is null");
        }
        if ((connection == null) || (connection.isClosed())) {
            throw new IllegalArgumentException("The provided connection is null or closed");
        }

        try (Statement statement = connection.createStatement()) {
            statement.execute(query);
            if (!connection.getAutoCommit()) {
                connection.commit();
            }
        }
        catch (SQLException e) {
            if (allowThrow) {
                throw e;
            }
            else {
                LOG.warn("An exception '" + e.toString() + "' happened when running SQL query '" + query + "'");
            }
        }
    }

    private static final Log LOG = LogFactory.getLog(SimpleSQLQueryExecutor.class);
}
