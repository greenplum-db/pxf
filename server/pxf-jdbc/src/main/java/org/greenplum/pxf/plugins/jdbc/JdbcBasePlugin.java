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
import org.greenplum.pxf.plugins.jdbc.utils.DbProduct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * JDBC tables plugin (base class)
 * <p>
 * Implemented subclasses: {@link JdbcAccessor}, {@link JdbcResolver}.
 */
public class JdbcBasePlugin extends BasePlugin {

    // '100' is a recommended value: https://docs.oracle.com/cd/E11882_01/java.112/e16548/oraperf.htm#JJDBC28754
    private static final int DEFAULT_BATCH_SIZE = 100;
    private static final int DEFAULT_FETCH_SIZE = 1000;
    private static final int DEFAULT_POOL_SIZE = 1;
    private static final int DEFAULT_QUERY_TIMEOUT = 0;

    // configuration parameter names
    private static final String JDBC_DRIVER_PROPERTY_NAME = "jdbc.driver";
    private static final String JDBC_URL_PROPERTY_NAME = "jdbc.url";
    private static final String JDBC_USER_PROPERTY_NAME = "jdbc.user";
    private static final String JDBC_PASSWORD_PROPERTY_NAME = "jdbc.password";
    private static final String JDBC_SESSION_PROPERTY_PREFIX = "jdbc.session.property.";
    private static final String JDBC_CONNECTION_PROPERTY_PREFIX = "jdbc.connection.property.";

    // connection parameter names
    private static final String JDBC_CONNECTION_TRANSACTION_ISOLATION = "jdbc.connection.transactionIsolation";

    // statement properties
    private static final String JDBC_STATEMENT_BATCH_SIZE_PROPERTY_NAME = "jdbc.statement.batchSize";
    private static final String JDBC_STATEMENT_FETCH_SIZE_PROPERTY_NAME = "jdbc.statement.fetchSize";
    private static final String JDBC_STATEMENT_QUERY_TIMEOUT_PROPERTY_NAME = "jdbc.statement.queryTimeout";

    // DDL option names
    private static final String JDBC_DRIVER_OPTION_NAME = "JDBC_DRIVER";
    private static final String JDBC_URL_OPTION_NAME = "DB_URL";

    private static final String FORBIDDEN_SESSION_PROPERTY_CHARACTERS = ";\n\b\0";
    private static final String QUERY_NAME_PREFIX = "query:";
    private static final int QUERY_NAME_PREFIX_LENGTH = QUERY_NAME_PREFIX.length();


    private enum TransactionIsolation {
        READ_UNCOMMITTED(1),
        READ_COMMITTED(2),
        REPEATABLE_READ(4),
        SERIALIZABLE(8),
        NOT_PROVIDED(-1);

        private int isolationLevel;

        TransactionIsolation(int transactionIsolation) {
            isolationLevel = transactionIsolation;
        }

        public int getLevel() {
            return isolationLevel;
        }

        public static TransactionIsolation typeOf(String str) {
            return valueOf(str);
        }
    }

    // JDBC parameters from config file or specified in DDL

    private String jdbcUrl;

    protected String tableName;

    // Write batch size
    protected int batchSize;
    protected boolean batchSizeIsSetByUser = false;

    // Read batch size
    protected int fetchSize;

    // Thread pool size
    protected int poolSize;

    // Query timeout.
    protected int queryTimeout;

    // Quote columns setting set by user (three values are possible)
    protected Boolean quoteColumns = null;

    // Environment variables to SET before query execution
    protected Map<String, String> sessionConfiguration = new HashMap<String, String>();

    // Properties object to pass to JDBC Driver when connection is created
    protected Properties connectionConfiguration = new Properties();

    // Transaction isolation level that a user can configure
    private TransactionIsolation transactionIsolation = TransactionIsolation.NOT_PROVIDED;

    // Columns description
    protected List<ColumnDescriptor> columns = null;

    // Name of query to execute for read flow (optional)
    protected String queryName;

    private static final Logger LOG = LoggerFactory.getLogger(JdbcBasePlugin.class);

    @Override
    public void initialize(RequestContext context) {
        super.initialize(context);

        // Required parameter. Can be auto-overwritten by user options
        String jdbcDriver = configuration.get(JDBC_DRIVER_PROPERTY_NAME);
        assertMandatoryParameter(jdbcDriver, JDBC_DRIVER_PROPERTY_NAME, JDBC_DRIVER_OPTION_NAME);
        try {
            LOG.debug("JDBC driver: '{}'", jdbcDriver);
            Class.forName(jdbcDriver);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

        // Required parameter. Can be auto-overwritten by user options
        jdbcUrl = configuration.get(JDBC_URL_PROPERTY_NAME);
        assertMandatoryParameter(jdbcUrl, JDBC_URL_PROPERTY_NAME, JDBC_URL_OPTION_NAME);

        // Required metadata
        String dataSource = context.getDataSource();
        if (StringUtils.isBlank(dataSource)) {
            throw new IllegalArgumentException("Data source must be provided");
        }

        // Determine if the datasource is a table name or a query name
        if (dataSource.startsWith(QUERY_NAME_PREFIX)) {
            queryName = dataSource.substring(QUERY_NAME_PREFIX_LENGTH);
            if (StringUtils.isBlank(queryName)) {
                throw new IllegalArgumentException(String.format("Query name is not provided in data source [%s]", dataSource));
            }
            LOG.debug("Query name is {}", queryName);
        } else {
            tableName = dataSource;
            LOG.debug("Table name is {}", tableName);
        }

        // Required metadata
        columns = context.getTupleDescription();

        // Optional parameters
        batchSizeIsSetByUser = configuration.get(JDBC_STATEMENT_BATCH_SIZE_PROPERTY_NAME) != null;
        batchSize = configuration.getInt(JDBC_STATEMENT_BATCH_SIZE_PROPERTY_NAME, DEFAULT_BATCH_SIZE);

        if (batchSize == 0) {
            batchSize = 1; // if user set to 0, it is the same as batchSize of 1
        } else if(batchSize < 0) {
            throw new IllegalArgumentException(String.format(
                    "Property %s has incorrect value %s : must be a non-negative integer", JDBC_STATEMENT_BATCH_SIZE_PROPERTY_NAME, batchSize));
        }

        fetchSize = configuration.getInt(JDBC_STATEMENT_FETCH_SIZE_PROPERTY_NAME, DEFAULT_FETCH_SIZE);

        poolSize = context.getOption("POOL_SIZE", DEFAULT_POOL_SIZE);

        queryTimeout = configuration.getInt(JDBC_STATEMENT_QUERY_TIMEOUT_PROPERTY_NAME, DEFAULT_QUERY_TIMEOUT);

        // Optional parameter. The default value is null
        String quoteColumnsRaw = context.getOption("QUOTE_COLUMNS");
        if (quoteColumnsRaw != null) {
            quoteColumns = Boolean.parseBoolean(quoteColumnsRaw);
        }

        // Optional parameter. The default value is empty map
        sessionConfiguration.putAll(configuration.getPropsWithPrefix(JDBC_SESSION_PROPERTY_PREFIX));
        // Check forbidden symbols
        // Note: PreparedStatement tnables us to skip this check: its values are distinct from its SQL code
        // However, SET queries cannot be executed this way. This is why we do this check
        if (sessionConfiguration.entrySet().stream()
                .anyMatch(
                        entry ->
                                StringUtils.containsAny(
                                        entry.getKey(), FORBIDDEN_SESSION_PROPERTY_CHARACTERS
                                ) ||
                                        StringUtils.containsAny(
                                                entry.getValue(), FORBIDDEN_SESSION_PROPERTY_CHARACTERS
                                        )
                )
        ) {
            throw new IllegalArgumentException("Some session configuration parameter contains forbidden characters");
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Session configuration: {}",
                    sessionConfiguration.entrySet().stream()
                            .map(entry -> "'" + entry.getKey() + "'='" + entry.getValue() + "'")
                            .collect(Collectors.joining(", "))
            );
        }

        // Optional parameter. The default value is empty map
        connectionConfiguration.putAll(configuration.getPropsWithPrefix(JDBC_CONNECTION_PROPERTY_PREFIX));

        // Optional parameter. The default value depends on the database
        String transactionIsolationString = configuration.get(JDBC_CONNECTION_TRANSACTION_ISOLATION, "NOT_PROVIDED");
        transactionIsolation = TransactionIsolation.typeOf(transactionIsolationString);

        // Optional parameter. By default, corresponding connectionConfiguration property is not set
        String jdbcUser = configuration.get(JDBC_USER_PROPERTY_NAME);
        if (jdbcUser != null) {
            connectionConfiguration.setProperty("user", jdbcUser);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Connection configuration: {}",
                    connectionConfiguration.entrySet().stream()
                            .map(entry -> "'" + entry.getKey() + "'='" + entry.getValue() + "'")
                            .collect(Collectors.joining(", "))
            );
        }

        // This must be the last parameter parsed, as we output connectionConfiguration earlier
        // Optional parameter. By default, corresponding connectionConfiguration property is not set
        if (jdbcUser != null) {
            String jdbcPassword = configuration.get(JDBC_PASSWORD_PROPERTY_NAME);
            if (jdbcPassword != null) {
                LOG.debug("Connection password: {}", maskPassword(jdbcPassword));
                connectionConfiguration.setProperty("password", jdbcPassword);
            }
        }
    }

    /**
     * Open a new JDBC connection
     *
     * @return {@link Connection}
     * @throws SQLException if a database access or connection error occurs
     */
    public Connection getConnection() throws SQLException {
        LOG.debug("Requesting a new JDBC connection. URL={} table={} txid:seg={}:{}", jdbcUrl, tableName, context.getTransactionId(), context.getSegmentId());

        Connection connection = DriverManager.getConnection(jdbcUrl, connectionConfiguration);

        LOG.debug("Obtained a JDBC connection {} for URL={} table={} txid:seg={}:{}", connection, jdbcUrl, tableName, context.getTransactionId(), context.getSegmentId());

        try {
            prepareConnection(connection);
        } catch (SQLException e) {
            closeConnection(connection);
            throw e;
        }

        return connection;
    }

    /**
     * Prepare a JDBC PreparedStatement
     *
     * @param connection connection to use for creating the statement
     * @param query      query to execute
     * @return PreparedStatement
     * @throws SQLException if a database access error occurs
     */
    public PreparedStatement getPreparedStatement(Connection connection, String query) throws SQLException {
        if ((connection == null) || (query == null)) {
            throw new IllegalArgumentException("The provided query or connection is null");
        }
        return connection.prepareStatement(query);
    }

    /**
     * Close a JDBC statement and underlying {@link Connection}
     *
     * @param statement statement to close
     * @throws SQLException
     */
    public static void closeStatementAndConnection(Statement statement) throws SQLException {
        if (statement == null) {
            LOG.warn("Call to close statement and connection is ignored as statement provided was null");
            return;
        }

        SQLException exception = null;
        Connection connection = null;

        try {
            connection = statement.getConnection();
        } catch (SQLException e) {
            LOG.error("Exception when retrieving Connection from Statement", e);
            exception = e;
        }

        try {
            LOG.debug("Closing statement for connection {}", connection);
            statement.close();
        } catch (SQLException e) {
            LOG.error("Exception when closing Statement", e);
            exception = e;
        }

        try {
            closeConnection(connection);
        } catch (SQLException e) {
            LOG.error(String.format("Exception when closing connection %s", connection), e);
            exception = e;
        }

        if (exception != null) {
            throw exception;
        }
    }

    /**
     * Close a JDBC connection
     *
     * @param connection connection to close
     * @throws SQLException
     */
    private static void closeConnection(Connection connection) throws SQLException {
        if (connection == null) {
            LOG.warn("Call to close connection is ignored as connection provided was null");
            return;
        }
        try {
            if (!connection.isClosed() &&
                connection.getMetaData().supportsTransactions() &&
                !connection.getAutoCommit()) {

                LOG.debug("Committing transaction (as part of connection.close()) on connection {}", connection);
                connection.commit();
            }
        } finally {
            try {
                LOG.debug("Closing connection {}", connection);
                connection.close();
            } catch (Exception e) {
                // ignore
                LOG.warn(String.format("Failed to close JDBC connection %s, ignoring the error.", connection), e);
            }
        }
    }

    /**
     * Prepare JDBC connection by setting session-level variables in external database
     *
     * @param connection {@link Connection} to prepare
     */
    private void prepareConnection(Connection connection) throws SQLException {
        if (connection == null) {
            throw new IllegalArgumentException("The provided connection is null");
        }

        DatabaseMetaData metadata = connection.getMetaData();
        
        // Handle optional connection transaction isolation level
        if (transactionIsolation != TransactionIsolation.NOT_PROVIDED) {
            // user wants to set isolation level explicitly
            if (metadata.supportsTransactionIsolationLevel(transactionIsolation.getLevel())) {
                LOG.debug("Setting transaction isolation level to {} on connection {}", transactionIsolation.toString(), connection);
                connection.setTransactionIsolation(transactionIsolation.getLevel());
            } else {
                throw new RuntimeException(
                        String.format("Transaction isolation level %s is not supported", transactionIsolation.toString())
                );
            }
        }

        // Disable autocommit
        if (metadata.supportsTransactions()) {
            LOG.debug("Setting autoCommit to false on connection {}", connection);
            connection.setAutoCommit(false);
        }

        // Prepare session (process sessionConfiguration)
        if (!sessionConfiguration.isEmpty()) {
            DbProduct dbProduct = DbProduct.getDbProduct(metadata.getDatabaseProductName());

            try (Statement statement = connection.createStatement()) {
                for (Map.Entry<String, String> e : sessionConfiguration.entrySet()) {
                    String sessionQuery = dbProduct.buildSessionQuery(e.getKey(), e.getValue());
                    LOG.debug("Executing statement {} on connection {}", sessionQuery, connection);
                    statement.execute(sessionQuery);
                }
            }
        }
    }

    /**
     * Asserts whether a given parameter has non-empty value, throws IllegalArgumentException otherwise
     *
     * @param value      value to check
     * @param paramName  parameter name
     * @param optionName name of the option for a given parameter
     */
    private void assertMandatoryParameter(String value, String paramName, String optionName) {
        if (StringUtils.isBlank(value)) {
            throw new IllegalArgumentException(String.format(
                    "Required parameter %s is missing or empty in jdbc-site.xml and option %s is not specified in table definition.", paramName, optionName)
            );
        }
    }

    /**
     * Masks all password characters with asterisks, used for logging password values
     *
     * @param password password to mask
     * @return masked value consisting of asterisks
     */
    private String maskPassword(String password) {
        return password == null ? "" : StringUtils.repeat("*", password.length());
    }
}
