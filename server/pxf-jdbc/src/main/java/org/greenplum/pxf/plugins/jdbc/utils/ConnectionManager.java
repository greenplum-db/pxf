package org.greenplum.pxf.plugins.jdbc.utils;

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

public class ConnectionManager {

    private static final Logger LOG = LoggerFactory.getLogger(ConnectionManager.class);

    /**
     * Singleton instance of the ConnectionManager
     */
    private static final ConnectionManager instance = new ConnectionManager();
    private static long CLEANUP_TIMEOUT = TimeUnit.HOURS.toMillis(24);

    private Executor datasourceClosingExecutor;
    private LoadingCache<PoolDescriptor, DataSource> dataSources;

    private ConnectionManager() {
        this.datasourceClosingExecutor = Executors.newCachedThreadPool();
        this.dataSources = CacheBuilder.newBuilder()
                .expireAfterAccess(6, TimeUnit.HOURS)
                .removalListener(RemovalListeners.asynchronous((RemovalListener<PoolDescriptor, DataSource>) notification ->
                        {
                            HikariDataSource hds = (HikariDataSource) notification.getValue();
                            LOG.debug("Processing cache removal of pool {} for server {} and user {} with cause {}",
                                    hds.getPoolName(),
                                    notification.getKey().getServer(),
                                    notification.getKey().getUser(),
                                    notification.getCause().toString());
                            // if connection pool has been removed from the cache while active query is executing
                            // wait until all connections finish execution and become idle, but no longer that CLEANUP_TIMEOUT
                            long startTime = System.currentTimeMillis();
                            while (hds.getHikariPoolMXBean().getActiveConnections() > 0) {
                                if ((System.currentTimeMillis() - startTime) > CLEANUP_TIMEOUT) {
                                    LOG.warn("Pool {} has active connections for too long, destroying it", hds.getPoolName());
                                    break;
                                }
                                Uninterruptibles.sleepUninterruptibly(5, TimeUnit.MINUTES);
                            }
                            LOG.debug("Destroying the pool {}", hds.getPoolName());
                            hds.close();
                        }
                        , datasourceClosingExecutor))
                .build(CacheLoader.from(key -> createDataSource(key)));
    }

    /**
     * @return a singleton instance of the connection manager.
     */
    public static ConnectionManager getInstance() {
        return instance;
    }

    public Connection getConnection(String server, String jdbcUrl, Properties connectionConfiguration, boolean isPoolEnabled, Properties poolConfiguration) throws SQLException {

        Connection result;
        if (!isPoolEnabled) {
            result = DriverManager.getConnection(jdbcUrl, connectionConfiguration);
        } else {

            PoolDescriptor poolDescriptor = new PoolDescriptor(server, jdbcUrl, connectionConfiguration, poolConfiguration);

            DataSource dataSource;
            try {
                LOG.debug("Requesting datasource for server={} and {}", server, poolDescriptor);
                dataSource = dataSources.getUnchecked(poolDescriptor);
                LOG.debug("Obtained datasource {} for server={} and {}", dataSource.hashCode(), server, poolDescriptor);
            } catch (UncheckedExecutionException e) {
                Throwable cause = e.getCause() != null ? e.getCause() : e;
                throw new SQLException(String.format("Could not obtain datasource for server %s and %s : %s", server, poolDescriptor, cause.getMessage()), cause);
            }
            return dataSource.getConnection();
        }
        return result;
    }

    private DataSource createDataSource(PoolDescriptor poolDescriptor) {

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

        return new HikariDataSource(config);
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

}

