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

import org.greenplum.pxf.api.model.BasePlugin;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.sql.Statement;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * JDBC tables plugin (base class)
 *
 * Implemented subclasses: {@link JdbcAccessor}, {@link JdbcResolver}.
 */
public class JdbcBasePlugin extends BasePlugin {
    @Override
    public void initialize(RequestContext context) {
        super.initialize(context);
        loadPluginSettings();
        validatePluginSettings();
    }

    /**
     * Open a new JDBC connection
     *
     * @throws ClassNotFoundException if the JDBC driver was not found
     * @throws SQLException if a database access error occurs
     * @throws SQLTimeoutException if a connection problem occurs
     *
     * @return connection
     */
    public Connection getConnection() throws ClassNotFoundException, SQLException, SQLTimeoutException {
        Connection connection;
        if (jdbcUser != null) {
            LOG.debug("Open JDBC connection: driver={}, url={}, user={}, pass={}, table={}",
                    jdbcDriver, jdbcUrl, jdbcUser, jdbcPassword, tableName);
        } else {
            LOG.debug("Open JDBC connection: driver={}, url={}, table={}",
                    jdbcDriver, jdbcUrl, tableName);
        }
        Class.forName(jdbcDriver);
        if (jdbcUser != null) {
            connection = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword);
        }
        else {
            connection = DriverManager.getConnection(jdbcUrl);
        }
        return connection;
    }

    /**
     * Close a JDBC connection
     *
     * @param connection connection to close
     */
    public static void closeConnection(Connection connection) {
        try {
            if ((connection != null) && (!connection.isClosed())) {
                if ((connection.getMetaData().supportsTransactions()) && (!connection.getAutoCommit())) {
                    connection.commit();
                }
                connection.close();
            }
        }
        catch (SQLException e) {
            LOG.error("JDBC connection close error", e);
        }
    }

    /**
     * Prepare a JDBC PreparedStatement
     *
     * @param connection connection to use for creating the statement
     * @param query query to execute
     *
     * @return PreparedStatement
     *
     * @throws ClassNotFoundException if the JDBC driver was not found
     * @throws SQLException if a database access error occurs
     * @throws SQLTimeoutException if a connection problem occurs
     */
    public PreparedStatement getPreparedStatement(Connection connection, String query) throws SQLException, SQLTimeoutException, ClassNotFoundException {
        if ((connection == null) || (query == null)) {
            throw new IllegalArgumentException("The provided query or connection is null");
        }
        if (connection.getMetaData().supportsTransactions()) {
            connection.setAutoCommit(false);
        }
        return connection.prepareStatement(query);
    }

    /**
     * Close a JDBC Statement (and the connection it is based on)
     *
     * @param statement statement to close
     */
    public static void closeStatement(Statement statement) {
        if (statement == null) {
            return;
        }
        Connection connection = null;
        try {
            if (!statement.isClosed()) {
                connection = statement.getConnection();
                statement.close();
            }
        }
        catch (Exception e) {}
        closeConnection(connection);
    }

    /**
     * Load plugin settings from configuration and context.
     * 'this.context' and 'this.configuration' are used as sources.
     *
     * This method also parses non-string arguments.
     */
    private void loadPluginSettings() throws IllegalArgumentException {
        jdbcDriver = JdbcPluginSettings.jdbcDriver.loadFromContextOrConfiguration(context, configuration);
        jdbcUrl = JdbcPluginSettings.jdbcUrl.loadFromContextOrConfiguration(context, configuration);

        tableName = context.getDataSource();
        tableColumns = context.getTupleDescription();

        // Both user and password must be loaded from one source
        jdbcUser = JdbcPluginSettings.jdbcUser.loadFromContext(context);
        if (jdbcUser != null) {
            jdbcPassword = JdbcPluginSettings.jdbcPassword.loadFromContext(context);
        }
        else {
            jdbcUser = JdbcPluginSettings.jdbcUser.loadFromConfiguration(configuration);
            jdbcPassword = jdbcUser != null ? JdbcPluginSettings.jdbcPassword.loadFromConfiguration(configuration) : null;
        }

        String batchSizeRaw = JdbcPluginSettings.batchSize.loadFromContextOrConfiguration(context, configuration);
        if (batchSizeRaw != null) {
            try {
                batchSize = Integer.parseInt(batchSizeRaw);
                batchSizeIsSetByUser = true;
            }
            catch (NumberFormatException e) {
                throw new IllegalArgumentException(JdbcPluginSettings.batchSize + " is incorrect: must be an integer");
            }
        }

        String poolSizeRaw = JdbcPluginSettings.poolSize.loadFromContextOrConfiguration(context, configuration);
        if (poolSizeRaw != null) {
            try {
                poolSize = Integer.parseInt(poolSizeRaw);
            }
            catch (NumberFormatException e) {
                throw new IllegalArgumentException(JdbcPluginSettings.poolSize + " is incorrect: must be an integer");
            }
        }

        preQuerySql = JdbcPluginSettings.preQuerySql.loadFromContextOrConfiguration(context, configuration);
        stopIfPreQueryFails = JdbcPluginSettings.stopIfPreQueryFails.loadFromContextOrConfiguration(context, configuration) != null;
    }

    /**
     * Check that:
     *  * All required plugin settings are set
     *  * All non-string settings have correct values
     *
     * This method may change values of some settings
     */
    private void validatePluginSettings() {
        if (jdbcDriver == null) {
            throw new IllegalArgumentException(JdbcPluginSettings.jdbcDriver + " must be provided");
        }

        if (jdbcUrl == null) {
            throw new IllegalArgumentException(JdbcPluginSettings.jdbcUrl + " must be provided");
        }

        if (tableName == null) {
            throw new IllegalArgumentException("Table name must be provided");
        }

        if (tableColumns == null) {
            throw new IllegalArgumentException("Tuple description must be provided");
        }

        if (batchSize < 0) {
            throw new IllegalArgumentException(JdbcPluginSettings.batchSize + " must be a non-negative integer");
        }
        else if (batchSize == 0) {
            batchSize = 1;
        }

        /*
        At the moment, when writing into some table, table name is
        concatenated with a special string that is necessary to write into HDFS.
        However, raw table name is required by JDBC drivers. It is extracted here
        */
        Matcher matcher = tableNamePattern.matcher(tableName);
        if (matcher.matches()) {
            tableName = matcher.group(1);
        }

        if (poolSize < 1) {
            poolSize = Runtime.getRuntime().availableProcessors();
            LOG.info(JdbcPluginSettings.poolSize + " is set to number of CPUs available (" + Integer.toString(poolSize) + ")");
        }
    }

    // JDBC parameters
    protected String jdbcDriver = null;
    protected String jdbcUrl = null;
    protected String jdbcUser = null;
    protected String jdbcPassword = null;

    // Table and column identifiers
    protected String tableName = null;
    protected List<ColumnDescriptor> tableColumns = null;

    // Pre-SQL
    protected String preQuerySql = null;
    protected boolean stopIfPreQueryFails = false;

    // '100' is a recommended value: https://docs.oracle.com/cd/E11882_01/java.112/e16548/oraperf.htm#JJDBC28754
    public static final int DEFAULT_BATCH_SIZE = 100;
    // After argument parsing, this value is guaranteed to be >= 1
    protected int batchSize = DEFAULT_BATCH_SIZE;
    protected boolean batchSizeIsSetByUser = false;

    // Thread pool size
    protected int poolSize = 1;

    private static final Logger LOG = LoggerFactory.getLogger(JdbcBasePlugin.class);

    // At the moment, when writing into some table, table name is concatenated with a special string that is necessary to write into HDFS. However, raw table name is required by JDBC drivers. This Pattern is used to extract it
    private static final Pattern tableNamePattern = Pattern.compile("/(.*)/[0-9]*-[0-9]*_[0-9]*");
}
