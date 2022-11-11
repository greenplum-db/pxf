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
import org.apache.hadoop.conf.Configuration;
import org.greenplum.pxf.api.model.BasePlugin;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.security.SecureLogin;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.greenplum.pxf.api.utilities.SpringContext;
import org.greenplum.pxf.api.utilities.Utilities;
import org.greenplum.pxf.plugins.jdbc.utils.ConnectionManager;
import org.greenplum.pxf.plugins.jdbc.utils.DbProduct;
import org.greenplum.pxf.plugins.jdbc.utils.HiveJdbcUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.PrivilegedExceptionAction;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.greenplum.pxf.api.security.SecureLogin.CONFIG_KEY_SERVICE_USER_IMPERSONATION;

/**
 * JDBC tables plugin (base class)
 * <p>
 * Implemented subclasses: {@link JdbcAccessor}, {@link JdbcResolver}.
 */
public class JdbcBasePlugin extends BasePlugin {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcBasePlugin.class);

    // '100' is a recommended value: https://docs.oracle.com/cd/E11882_01/java.112/e16548/oraperf.htm#JJDBC28754
    private static final int DEFAULT_BATCH_SIZE = 100;
    private static final int DEFAULT_FETCH_SIZE = 1000;
    // MySQL fetches all data in memory first unless streaming is enabled by setting fetchSize to Integer.MIN_VALUE
    // see https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-reference-implementation-notes.html
    private static final int DEFAULT_MYSQL_FETCH_SIZE = Integer.MIN_VALUE;
    private static final int DEFAULT_POOL_SIZE = 1;

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

    // connection pool properties
    private static final String JDBC_CONNECTION_POOL_ENABLED_PROPERTY_NAME = "jdbc.pool.enabled";
    private static final String JDBC_CONNECTION_POOL_PROPERTY_PREFIX = "jdbc.pool.property.";
    private static final String JDBC_POOL_QUALIFIER_PROPERTY_NAME = "jdbc.pool.qualifier";

    // DDL option names
    private static final String JDBC_DRIVER_OPTION_NAME = "JDBC_DRIVER";
    private static final String JDBC_URL_OPTION_NAME = "DB_URL";

    private static final String FORBIDDEN_SESSION_PROPERTY_CHARACTERS = ";\n\b\0";
    private static final String QUERY_NAME_PREFIX = "query:";
    private static final int QUERY_NAME_PREFIX_LENGTH = QUERY_NAME_PREFIX.length();

    private static final String HIVE_URL_PREFIX = "jdbc:hive2://";
    private static final String HIVE_DEFAULT_DRIVER_CLASS = "org.apache.hive.jdbc.HiveDriver";
    private static final String MYSQL_DRIVER_PREFIX = "com.mysql.";

    private enum TransactionIsolation {
        READ_UNCOMMITTED(1),
        READ_COMMITTED(2),
        REPEATABLE_READ(4),
        SERIALIZABLE(8),
        NOT_PROVIDED(-1);

        private final int isolationLevel;

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
    protected Integer queryTimeout;

    // Quote columns setting set by user (three values are possible)
    protected Boolean quoteColumns = null;

    // Environment variables to SET before query execution
    protected Map<String, String> sessionConfiguration = new HashMap<>();

    // Properties object to pass to JDBC Driver when connection is created
    protected Properties connectionConfiguration = new Properties();

    // Transaction isolation level that a user can configure
    private TransactionIsolation transactionIsolation = TransactionIsolation.NOT_PROVIDED;

    // Columns description
    protected List<ColumnDescriptor> columns = null;

    // Name of query to execute for read flow (optional)
    protected String queryName;

    // connection pool fields
    private boolean isConnectionPoolUsed;
    private Properties poolConfiguration;
    private String poolQualifier;

    private final ConnectionManager connectionManager;
    private final SecureLogin secureLogin;

    static {
        // Deprecated as of Oct 22, 2019 in version 5.9.2+
        Configuration.addDeprecation("pxf.impersonation.jdbc",
                CONFIG_KEY_SERVICE_USER_IMPERSONATION,
                "The property \"pxf.impersonation.jdbc\" has been deprecated in favor of \"pxf.service.user.impersonation\".");
    }

    /**
     * Creates a new instance with default (singleton) instances of
     * ConnectionManager and SecureLogin.
     */
    JdbcBasePlugin() {
        this(SpringContext.getBean(ConnectionManager.class), SpringContext.getBean(SecureLogin.class));
    }

    /**
     * Creates a new instance with the given ConnectionManager and ConfigurationFactory
     *
     * @param connectionManager connection manager instance
     */
    JdbcBasePlugin(ConnectionManager connectionManager, SecureLogin secureLogin) {
        this.connectionManager = connectionManager;
        this.secureLogin = secureLogin;
    }

    @Override
    public void afterPropertiesSet() {
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
        if (context.getRequestType() == RequestContext.RequestType.WRITE_BRIDGE) {
            batchSize = configuration.getInt(JDBC_STATEMENT_BATCH_SIZE_PROPERTY_NAME, DEFAULT_BATCH_SIZE);

            if (batchSize == 0) {
                batchSize = 1; // if user set to 0, it is the same as batchSize of 1
            } else if (batchSize < 0) {
                throw new IllegalArgumentException(String.format(
                        "Property %s has incorrect value %s : must be a non-negative integer", JDBC_STATEMENT_BATCH_SIZE_PROPERTY_NAME, batchSize));
            }
        }

        // determine fetchSize for read operations, with different default values for MySQL driver and all others
        int defaultFetchSize = jdbcDriver.startsWith(MYSQL_DRIVER_PREFIX) ? DEFAULT_MYSQL_FETCH_SIZE : DEFAULT_FETCH_SIZE;
        fetchSize = configuration.getInt(JDBC_STATEMENT_FETCH_SIZE_PROPERTY_NAME, defaultFetchSize);
        LOG.debug("Will be using fetchSize {}", fetchSize);

        poolSize = context.getOption("POOL_SIZE", DEFAULT_POOL_SIZE);

        String queryTimeoutString = configuration.get(JDBC_STATEMENT_QUERY_TIMEOUT_PROPERTY_NAME);
        if (StringUtils.isNotBlank(queryTimeoutString)) {
            try {
                queryTimeout = Integer.parseUnsignedInt(queryTimeoutString);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(String.format(
                        "Property %s has incorrect value %s : must be a non-negative integer",
                        JDBC_STATEMENT_QUERY_TIMEOUT_PROPERTY_NAME, queryTimeoutString), e);
            }
        }

        // Optional parameter. The default value is null
        String quoteColumnsRaw = context.getOption("QUOTE_COLUMNS");
        if (quoteColumnsRaw != null) {
            quoteColumns = Boolean.parseBoolean(quoteColumnsRaw);
        }

        // Optional parameter. The default value is empty map
        sessionConfiguration.putAll(getPropsWithPrefix(configuration, JDBC_SESSION_PROPERTY_PREFIX));
        // Check forbidden symbols
        // Note: PreparedStatement enables us to skip this check: its values are distinct from its SQL code
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
        connectionConfiguration.putAll(getPropsWithPrefix(configuration, JDBC_CONNECTION_PROPERTY_PREFIX));

        // Optional parameter. The default value depends on the database
        String transactionIsolationString = configuration.get(JDBC_CONNECTION_TRANSACTION_ISOLATION, "NOT_PROVIDED");
        transactionIsolation = TransactionIsolation.typeOf(transactionIsolationString);

        // Set optional user parameter, taking into account impersonation setting for the server.
        String jdbcUser = configuration.get(JDBC_USER_PROPERTY_NAME);
        boolean impersonationEnabledForServer = configuration.getBoolean(CONFIG_KEY_SERVICE_USER_IMPERSONATION, false);
        LOG.debug("JDBC impersonation is {}enabled for server {}", impersonationEnabledForServer ? "" : "not ", context.getServerName());
        if (impersonationEnabledForServer) {
            if (Utilities.isSecurityEnabled(configuration) && StringUtils.startsWith(jdbcUrl, HIVE_URL_PREFIX)) {
                // secure impersonation for Hive JDBC driver requires setting URL fragment that cannot be overwritten by properties
                String updatedJdbcUrl = HiveJdbcUtils.updateImpersonationPropertyInHiveJdbcUrl(jdbcUrl, context.getUser());
                LOG.debug("Replaced JDBC URL {} with {}", jdbcUrl, updatedJdbcUrl);
                jdbcUrl = updatedJdbcUrl;
            } else {
                // the jdbcUser is the GPDB user
                jdbcUser = context.getUser();
            }
        }
        if (jdbcUser != null) {
            LOG.debug("Effective JDBC user {}", jdbcUser);
            connectionConfiguration.setProperty("user", jdbcUser);
        } else {
            LOG.debug("JDBC user has not been set");
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
                LOG.debug("Connection password: {}", ConnectionManager.maskPassword(jdbcPassword));
                connectionConfiguration.setProperty("password", jdbcPassword);
            }
        }

        // connection pool is optional, enabled by default
        isConnectionPoolUsed = configuration.getBoolean(JDBC_CONNECTION_POOL_ENABLED_PROPERTY_NAME, true);
        LOG.debug("Connection pool is {}enabled", isConnectionPoolUsed ? "" : "not ");
        if (isConnectionPoolUsed) {
            poolConfiguration = new Properties();
            // for PXF upgrades where jdbc-site template has not been updated, make sure there're sensible defaults
            poolConfiguration.setProperty("maximumPoolSize", "15");
            poolConfiguration.setProperty("connectionTimeout", "30000");
            poolConfiguration.setProperty("idleTimeout", "30000");
            poolConfiguration.setProperty("minimumIdle", "0");
            // apply values read from the template
            poolConfiguration.putAll(getPropsWithPrefix(configuration, JDBC_CONNECTION_POOL_PROPERTY_PREFIX));

            // packaged Hive JDBC Driver does not support connection.isValid() method, so we need to force set
            // connectionTestQuery parameter in this case, unless already set by the user
            if (jdbcUrl.startsWith(HIVE_URL_PREFIX) && HIVE_DEFAULT_DRIVER_CLASS.equals(jdbcDriver) && poolConfiguration.getProperty("connectionTestQuery") == null) {
                poolConfiguration.setProperty("connectionTestQuery", "SELECT 1");
            }

            // get the qualifier for connection pool, if configured. Might be used when connection session authorization is employed
            // to switch effective user once connection is established
            poolQualifier = configuration.get(JDBC_POOL_QUALIFIER_PROPERTY_NAME);
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

        Connection connection = null;
        try {
            connection = getConnectionInternal();
            LOG.debug("Obtained a JDBC connection {} for URL={} table={} txid:seg={}:{}", connection, jdbcUrl, tableName, context.getTransactionId(), context.getSegmentId());

            prepareConnection(connection);
        } catch (Exception e) {
            closeConnection(connection);
            if (e instanceof SQLException) {
                throw (SQLException) e;
            } else {
                String msg = e.getMessage();
                if (msg == null) {
                    Throwable t = e.getCause();
                    if (t != null) msg = t.getMessage();
                }
                throw new SQLException(msg, e);
            }
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
        PreparedStatement statement = connection.prepareStatement(query);
        if (queryTimeout != null) {
            LOG.debug("Setting query timeout to {} seconds", queryTimeout);
            statement.setQueryTimeout(queryTimeout);
        }
        return statement;
    }

    /**
     * Close a JDBC statement and underlying {@link Connection}
     *
     * @param statement statement to close
     * @throws SQLException throws when a SQLException occurs
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
     * For a Kerberized Hive JDBC connection, it creates a connection as the loginUser.
     * Otherwise, it returns a new connection.
     *
     * @return for a Kerberized Hive JDBC connection, returns a new connection as the loginUser.
     * Otherwise, it returns a new connection.
     * @throws Exception throws when an error occurs
     */
    private Connection getConnectionInternal() throws Exception {
        Configuration configuration = context.getConfiguration();
        if (Utilities.isSecurityEnabled(configuration) && StringUtils.startsWith(jdbcUrl, HIVE_URL_PREFIX)) {
            return secureLogin.getLoginUser(context, configuration).doAs((PrivilegedExceptionAction<Connection>) () ->
                    connectionManager.getConnection(context.getServerName(), jdbcUrl, connectionConfiguration, isConnectionPoolUsed, poolConfiguration, poolQualifier));
        } else {
            return connectionManager.getConnection(context.getServerName(), jdbcUrl, connectionConfiguration, isConnectionPoolUsed, poolConfiguration, poolQualifier);
        }
    }

    /**
     * Close a JDBC connection
     *
     * @param connection connection to close
     * @throws SQLException throws when a SQLException occurs
     */
    static void closeConnection(Connection connection) throws SQLException {
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
     * Constructs a mapping of configuration and includes all properties that start with the specified
     * configuration prefix.  Property names in the mapping are trimmed to remove the configuration prefix.
     * This is a method from Hadoop's Configuration class ported here to make older and custom versions of Hadoop
     * work with JDBC profile.
     *
     * @param configuration configuration map
     * @param confPrefix    configuration prefix
     * @return mapping of configuration properties with prefix stripped
     */
    private Map<String, String> getPropsWithPrefix(Configuration configuration, String confPrefix) {
        Map<String, String> configMap = new HashMap<>();
        for (Map.Entry<String, String> stringStringEntry : configuration) {
            String propertyName = stringStringEntry.getKey();
            if (propertyName.startsWith(confPrefix)) {
                // do not use value from the iterator as it might not come with variable substitution
                String value = configuration.get(propertyName);
                String keyName = propertyName.substring(confPrefix.length());
                configMap.put(keyName, value);
            }
        }
        return configMap;
    }

}
