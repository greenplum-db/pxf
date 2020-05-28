package org.greenplum.pxf.plugins.jdbc.utils;

import com.google.common.base.Ticker;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalListeners;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.google.common.util.concurrent.Uninterruptibles;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.SneakyThrows;
import org.apache.commons.lang.StringUtils;
import org.greenplum.pxf.plugins.jdbc.PxfJdbcProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.time.temporal.TemporalUnit;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Responsible for obtaining and maintaining JDBC connections to databases. If configured for a given server,
 * uses Hikari Connection Pool to pool database connections.
 */
@Component
public class ConnectionManager {

    private static final Logger LOG = LoggerFactory.getLogger(ConnectionManager.class);

    private final Executor datasourceClosingExecutor;
    private final LoadingCache<PoolDescriptor, HikariDataSource> dataSources;
    private final DriverManagerWrapper driverManagerWrapper;

    public ConnectionManager(DataSourceFactory factory, Ticker ticker, PxfJdbcProperties properties, DriverManagerWrapper driverManagerWrapper) {
        this.driverManagerWrapper = driverManagerWrapper;
        this.datasourceClosingExecutor = Executors.newCachedThreadPool();

        // the connection properties
        final PxfJdbcProperties.Connection connection = properties.getConnection();

        this.dataSources = CacheBuilder.newBuilder()
                .ticker(ticker)
                .expireAfterAccess(connection.getPoolExpirationTimeout().toNanos(), TimeUnit.NANOSECONDS)
                .removalListener(RemovalListeners.asynchronous((RemovalListener<PoolDescriptor, HikariDataSource>) notification ->
                        {
                            HikariDataSource hds = notification.getValue();
                            LOG.debug("Processing cache removal of pool {} for server {} and user {} with cause {}",
                                    hds.getPoolName(),
                                    notification.getKey().getServer(),
                                    notification.getKey().getUser(),
                                    notification.getCause().toString());
                            // if connection pool has been removed from the cache while active query is executing
                            // wait until all connections finish execution and become idle, but no longer that cleanupTimeout
                            final long startTime = ticker.read();
                            final long cleanupTimeoutNanos = connection.getCleanupTimeout().toNanos();
                            final long cleanupSleepIntervalNanos = connection.getCleanupSleepInterval().toNanos();
                            while (hds.getHikariPoolMXBean().getActiveConnections() > 0) {
                                if ((ticker.read() - startTime) > cleanupTimeoutNanos) {
                                    LOG.warn("Pool {} has active connections for too long, destroying it", hds.getPoolName());
                                    break;
                                }
                                Uninterruptibles.sleepUninterruptibly(cleanupSleepIntervalNanos, TimeUnit.NANOSECONDS);
                            }
                            LOG.debug("Destroying the pool {}", hds.getPoolName());
                            hds.close();
                        },
                        datasourceClosingExecutor))
                .build(CacheLoader.from(factory::createDataSource));
    }

    /**
     * Explicitly runs cache maintenance operations.
     */
    void cleanCache() {
        dataSources.cleanUp();
    }

    /**
     * Returns a connection to the target database either directly from the
     * DriverManagerWrapper or from a Hikari connection pool that manages
     * connections.
     *
     * @param server                  configuration server
     * @param jdbcUrl                 JDBC url of the target database
     * @param connectionConfiguration connection configuration properties
     * @param isPoolEnabled           true if the connection pool is enabled, false otherwise
     * @param poolConfiguration       pool configuration properties
     * @return connection instance
     * @throws SQLException if connection can not be obtained
     */
    public Connection getConnection(String server, String jdbcUrl, Properties connectionConfiguration, boolean isPoolEnabled, Properties poolConfiguration, String qualifier) throws SQLException {

        Connection result;
        if (!isPoolEnabled) {
            LOG.debug("Requesting driverManagerWrapper.getConnection for server={}", server);
            result = driverManagerWrapper.getConnection(jdbcUrl, connectionConfiguration);
        } else {

            PoolDescriptor poolDescriptor = new PoolDescriptor(server, jdbcUrl, connectionConfiguration, poolConfiguration, qualifier);

            DataSource dataSource;
            try {
                LOG.debug("Requesting datasource for server={} and {}", server, poolDescriptor);
                dataSource = dataSources.getUnchecked(poolDescriptor);
                LOG.debug("Obtained datasource {} for server={} and {}", dataSource.hashCode(), server, poolDescriptor);
            } catch (UncheckedExecutionException e) {
                Throwable cause = e.getCause() != null ? e.getCause() : e;
                throw new SQLException(String.format("Could not obtain datasource for server %s and %s : %s", server, poolDescriptor, cause.getMessage()), cause);
            }
            result = dataSource.getConnection();
        }
        LOG.debug("Returning JDBC connection {} for server={}", result, server);

        return result;
    }

    /**
     * Masks all password characters with asterisks, used for logging password values
     *
     * @param password password to mask
     * @return masked value consisting of asterisks
     */
    public static String maskPassword(String password) {
        return password == null ? "" : StringUtils.repeat("*", password.length());
    }

    /**
     * Provides the default system ticker as a component
     */
    @Component
    static class DefaultTicker extends Ticker {

        private final Ticker ticker;

        public DefaultTicker() {
            ticker = Ticker.systemTicker();
        }

        @Override
        public long read() {
            return ticker.read();
        }
    }

    /**
     * Factory class to create instances of datasources.
     * Default implementation creates instances of HikariDataSource.
     */
    @Component
    public static class DataSourceFactory {

        /**
         * Creates a new datasource instance based on parameters contained in PoolDescriptor.
         *
         * @param poolDescriptor descriptor containing pool parameters
         * @return instance of HikariDataSource
         */
        @SneakyThrows
        HikariDataSource createDataSource(PoolDescriptor poolDescriptor) {

            // initialize Hikari config with provided properties
            Properties configProperties = poolDescriptor.getPoolConfig() != null ? poolDescriptor.getPoolConfig() : new Properties();
            HikariConfig config = new HikariConfig(configProperties);

            // overwrite jdbcUrl / userName / password with the values provided explicitly
            config.setJdbcUrl(poolDescriptor.getJdbcUrl());
            config.setUsername(poolDescriptor.getUser());
            config.setPassword(poolDescriptor.getPassword());

            // set connection properties as datasource properties
            if (poolDescriptor.getConnectionConfig() != null) {
                poolDescriptor.getConnectionConfig().forEach((key, value) ->
                        config.addDataSourceProperty((String) key, value));
            }

            HikariDataSource result = new HikariDataSource(config);
            LOG.debug("Created new instance of HikariDataSource: {}", result);

            return result;
        }
    }

    /**
     * Wrap calls to the DriverManager
     */
    @Component
    public static class DriverManagerWrapper {

        /**
         * Attempts to establish a connection to the given database URL.
         * The <code>DriverManager</code> attempts to select an appropriate driver from
         * the set of registered JDBC drivers.
         * <p>
         * <B>Note:</B> If a property is specified as part of the {@code url} and
         * is also specified in the {@code Properties} object, it is
         * implementation-defined as to which value will take precedence.
         * For maximum portability, an application should only specify a
         * property once.
         *
         * @param url  a database url of the form
         *             <code> jdbc:<em>subprotocol</em>:<em>subname</em></code>
         * @param info a list of arbitrary string tag/value pairs as
         *             connection arguments; normally at least a "user" and
         *             "password" property should be included
         * @return a Connection to the URL
         * @throws SQLException        if a database access error occurs or the url is
         *                             {@code null}
         * @throws SQLTimeoutException when the driver has determined that the
         *                             timeout value specified by the {@code setLoginTimeout} method
         *                             has been exceeded and has at least tried to cancel the
         *                             current database connection attempt
         */
        public Connection getConnection(String url, java.util.Properties info) throws SQLException {
            return DriverManager.getConnection(url, info);
        }

        /**
         * Attempts to locate a driver that understands the given URL.
         * The <code>DriverManager</code> attempts to select an appropriate driver from
         * the set of registered JDBC drivers.
         *
         * @param url a database URL of the form
         *            <code>jdbc:<em>subprotocol</em>:<em>subname</em></code>
         * @return a <code>Driver</code> object representing a driver
         * that can connect to the given URL
         * @throws SQLException if a database access error occurs
         */
        public Driver getDriver(String url) throws SQLException {
            return DriverManager.getDriver(url);
        }

    }
}

