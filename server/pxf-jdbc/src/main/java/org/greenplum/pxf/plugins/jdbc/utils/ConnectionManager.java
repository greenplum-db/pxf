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
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Responsible for obtaining and maintaining JDBC connections to databases. If configured for a given server,
 * uses Hikari Connection Pool to pool database connections.
 */
public class ConnectionManager {

    private static final Logger LOG = LoggerFactory.getLogger(ConnectionManager.class);

    static final long CLEANUP_SLEEP_INTERVAL_NANOS = TimeUnit.MINUTES.toNanos(5);
    static final long CLEANUP_TIMEOUT_NANOS = TimeUnit.HOURS.toNanos(24);
    static final long POOL_EXPIRATION_TIMEOUT_HOURS = 6;

    /**
     * Singleton instance of the ConnectionManager
     */
    private static final ConnectionManager instance = new ConnectionManager();

    private Executor datasourceClosingExecutor;
    private LoadingCache<PoolDescriptor, HikariDataSource> dataSources;

    /**
     * Creates an instance of the connection manager.
     */
    private ConnectionManager() {
        this(DataSourceFactory.getInstance(), Ticker.systemTicker(), CLEANUP_SLEEP_INTERVAL_NANOS);
    }

    ConnectionManager(DataSourceFactory factory, Ticker ticker, long sleepIntervalNanos) {
        this.datasourceClosingExecutor = Executors.newCachedThreadPool();
        this.dataSources = CacheBuilder.newBuilder()
                .ticker(ticker)
                .expireAfterAccess(POOL_EXPIRATION_TIMEOUT_HOURS, TimeUnit.HOURS)
                .removalListener(RemovalListeners.asynchronous((RemovalListener<PoolDescriptor, HikariDataSource>) notification ->
                        {
                            HikariDataSource hds = notification.getValue();
                            LOG.debug("Processing cache removal of pool {} for server {} and user {} with cause {}",
                                    hds.getPoolName(),
                                    notification.getKey().getServer(),
                                    notification.getKey().getUser(),
                                    notification.getCause().toString());
                            // if connection pool has been removed from the cache while active query is executing
                            // wait until all connections finish execution and become idle, but no longer that CLEANUP_TIMEOUT
                            long startTime = ticker.read();
                            while (hds.getHikariPoolMXBean().getActiveConnections() > 0) {
                                if ((ticker.read() - startTime) > CLEANUP_TIMEOUT_NANOS) {
                                    LOG.warn("Pool {} has active connections for too long, destroying it", hds.getPoolName());
                                    break;
                                }
                                Uninterruptibles.sleepUninterruptibly(sleepIntervalNanos, TimeUnit.NANOSECONDS);
                            }
                            LOG.debug("Destroying the pool {}", hds.getPoolName());
                            hds.close();
                        }
                        , datasourceClosingExecutor))
                .build(CacheLoader.from(key -> factory.createDataSource(key)));
    }

    /**
     * Explicitly runs cache maintenance operations.
     */
    void cleanCache() {
        dataSources.cleanUp();
    }

    /**
     * @return a singleton instance of the connection manager.
     */
    public static ConnectionManager getInstance() {
        return instance;
    }

    /**
     * Returns a connection to the target database either directly from the DriverManager or
     * from a Hikari connection pool that manages connections.
     * @param server configuration server
     * @param jdbcUrl JDBC url of the target database
     * @param connectionConfiguration connection configuration properties
     * @param isPoolEnabled true if the connection pool is enabled, false otherwise
     * @param poolConfiguration pool configuration properties
     * @return connection instance
     * @throws SQLException if connection can not be obtained
     */
    public Connection getConnection(String server, String jdbcUrl, Properties connectionConfiguration, boolean isPoolEnabled, Properties poolConfiguration, String qualifier) throws SQLException {

        Connection result;
        if (!isPoolEnabled) {
            LOG.debug("Requesting DriverManager.getConnection for server={}", server);
            result = DriverManager.getConnection(jdbcUrl, connectionConfiguration);
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
     * Factory class to create instances of datasources.
     * Default implementation creates instances of HikariDataSource.
     */
    static class DataSourceFactory {

        private static final DataSourceFactory dataSourceFactoryInstance = new DataSourceFactory();

        /**
         * Creates a new datasource instance based on parameters contained in PoolDescriptor.
         * @param poolDescriptor descriptor containing pool parameters
         * @return instance of HikariDataSource
         */
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

        /**
         * Returns a singleton instance of the data source factory.
         * @return a singleton instance of the data source factory
         */
        static DataSourceFactory getInstance() {
            return dataSourceFactoryInstance;
        }
    }
}

