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

import org.apache.commons.lang.StringUtils;
import org.greenplum.pxf.api.model.BasePlugin;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.greenplum.pxf.plugins.jdbc.utils.PropertiesParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * JDBC tables plugin (base class)
 *
 * Implemented subclasses: {@link JdbcAccessor}, {@link JdbcResolver}.
 */
public class JdbcBasePlugin extends BasePlugin {

    // '100' is a recommended value: https://docs.oracle.com/cd/E11882_01/java.112/e16548/oraperf.htm#JJDBC28754
    private static final int DEFAULT_BATCH_SIZE = 100;
    private static final int DEFAULT_POOL_SIZE = 1;

    // configuration parameter names
    private static final String JDBC_DRIVER_PROPERTY_NAME = "jdbc.driver";
    private static final String JDBC_URL_PROPERTY_NAME = "jdbc.url";
    private static final String JDBC_USER_PROPERTY_NAME = "jdbc.user";
    private static final String JDBC_PASSWORD_PROPERTY_NAME = "jdbc.password";
    private static final String JDBC_ENV_PROPERTY_NAME = "jdbc.env";
    private static final String JDBC_INFO_PROPERTY_NAME = "jdbc.info";

    // DDL option names
    private static final String JDBC_DRIVER_OPTION_NAME = "JDBC_DRIVER";
    private static final String JDBC_URL_OPTION_NAME = "DB_URL";

    // JDBC parameters from config file or specified in DDL
    private String jdbcDriver, jdbcUrl, jdbcPassword;

    protected String tableName = null;

    // After argument parsing, this value is guaranteed to be >= 1
    protected int batchSize = DEFAULT_BATCH_SIZE;
    protected boolean batchSizeIsSetByUser = false;

    // Thread pool size
    protected int poolSize = DEFAULT_POOL_SIZE;

    // Quote columns setting set by user (three values are possible)
    protected Boolean quoteColumns = null;

    // Environment variables to SET before query execution
    protected Properties sessionConfiguration = new Properties();

    // Properties object to pass to JDBC Driver when connection is created
    protected Properties info = new Properties();

    // Columns description
    protected List<ColumnDescriptor> columns = null;

    private static final Logger LOG = LoggerFactory.getLogger(JdbcBasePlugin.class);

    @Override
    public void initialize(RequestContext context) {
        super.initialize(context);

        // process configuration based params that could be auto-overwritten by user options
        jdbcDriver = configuration.get(JDBC_DRIVER_PROPERTY_NAME);
        assertMandatoryParameter(jdbcDriver, JDBC_DRIVER_PROPERTY_NAME, JDBC_DRIVER_OPTION_NAME);

        jdbcUrl = configuration.get(JDBC_URL_PROPERTY_NAME);
        assertMandatoryParameter(jdbcUrl, JDBC_URL_PROPERTY_NAME, JDBC_URL_OPTION_NAME);

        // authentication information is not required
        String userRaw = configuration.get(JDBC_USER_PROPERTY_NAME);
        if (userRaw != null) {
            info.setProperty("user", userRaw);
            jdbcPassword = retrievePassword();
        }

        // process additional options
        tableName = context.getDataSource();
        if (tableName == null) {
            throw new IllegalArgumentException("Data source must be provided");
        }

        tableName = context.getDataSource();

        columns = context.getTupleDescription();

        // This parameter is not required. The default value is 100
        batchSizeIsSetByUser = context.getOption("BATCH_SIZE") != null;
        batchSize = context.getOption("BATCH_SIZE", DEFAULT_BATCH_SIZE, true);
        if (batchSize == 0) {
            batchSize = 1; // if user set to 0, it is the same as batchsize of 1
        }

        // This parameter is not required. The default value is 1
        poolSize = context.getOption("POOL_SIZE", DEFAULT_POOL_SIZE);

        // This parameter is not required. The default value is null
        String quoteColumnsRaw = context.getOption("QUOTE_COLUMNS");
        if (quoteColumnsRaw != null) {
            quoteColumns = Boolean.parseBoolean(quoteColumnsRaw);
        }

        // This parameter is not required. The default value is empty map
        String sessionConfigurationRaw = configuration.get(JDBC_ENV_PROPERTY_NAME);
        if (sessionConfigurationRaw != null) {
            sessionConfiguration.putAll(PropertiesParser.parse(sessionConfigurationRaw));
            LOG.debug(String.format(
                "Session configuration: %s",
                sessionConfiguration.entrySet().stream()
                        .map(entry -> "'" + entry.getKey().toString() + "'='" + entry.getValue() + "'")
                        .collect(Collectors.joining(", "))
            ));
        }

        // This parameter is not required. The default value is empty map
        String infoRaw = configuration.get(JDBC_INFO_PROPERTY_NAME);
        if (infoRaw != null) {
            info.putAll(PropertiesParser.parse(infoRaw));
        }
    }

    /**
     * Open a new JDBC connection
     *
     * @throws ClassNotFoundException if the JDBC driver was not found
     * @throws SQLException if a database access or connection error occurs
     *
     * @return connection
     */
    public Connection getConnection() throws ClassNotFoundException, SQLException {
        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format(
                "New JDBC connection. Driver: '%s'; URL: '%s'",
                jdbcDriver, jdbcUrl
            ));
            LOG.debug(String.format(
                "JDBC connection info: %s",
                info.entrySet().stream()
                        .map(entry -> "'" + entry.getKey().toString() + "'='" + entry.getValue() + "'")
                        .collect(Collectors.joining(", "))
            ));
            if (jdbcPassword != null) {
                LOG.debug(String.format(
                    "JDBC connection has password set: '%s'",
                    maskPassword(jdbcPassword)
                ));
            }
        }

        if (jdbcPassword != null) {
            info.setProperty("password", jdbcPassword);
        }

        Class.forName(jdbcDriver);
        return DriverManager.getConnection(jdbcUrl, info);
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
     * Asserts whether a given parameter has non-empty value, throws IllegalArgumentException otherwise
     * @param value value to check
     * @param paramName parameter name
     * @param optionName name of the option for a given parameter
     */
    private static void assertMandatoryParameter(String value, String paramName, String optionName) {
        if (StringUtils.isBlank(value)) {
            throw new IllegalArgumentException(String.format(
                    "Required parameter %s is missing or empty in jdbc-site.xml and option %s is not specified in table definition.", paramName, optionName)
            );
        }
    }

    /**
     * Masks all password characters with asterisks, used for logging password values
     * @param password password to mask
     * @return masked value consisting of asterisks
     */
    private static String maskPassword(String password) {
        return password == null ? "" : StringUtils.repeat("*", password.length());
    }

    /**
     * Retrieve password from data available to this plugin
     */
    private String retrievePassword() {
        return configuration.get(JDBC_PASSWORD_PROPERTY_NAME);
    }
}
