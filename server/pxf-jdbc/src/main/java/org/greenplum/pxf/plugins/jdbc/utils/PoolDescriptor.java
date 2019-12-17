package org.greenplum.pxf.plugins.jdbc.utils;

import com.google.common.collect.Sets;

import java.util.Objects;
import java.util.Properties;
import java.util.Set;

public class PoolDescriptor {

    private static final String USER_PROPERTY_NAME = "user";
    private static final String PASSWORD_PROPERTY_NAME = "password";

    // have users define connection properties in jdbc-site.xml with jdbc. prefix
    // prohibit redefining these properties at the pool level
    private static final Set<String> PROHIBITED_PROPERTIES =
            Sets.newHashSet("username", "password", "dataSource.user", "dataSource.password", "dataSourceClassName", "jdbcUrl");

    private String server;
    private String jdbcUrl;
    private String user;
    private String password;
    private Properties connectionConfig, poolConfig;
    private String qualifier;


    public PoolDescriptor(String server, String jdbcUrl, Properties connectionConfig, Properties poolConfig, String qualifier) {
        this.server = server;
        this.jdbcUrl = jdbcUrl;

        if (connectionConfig != null) {
            this.connectionConfig = (Properties) connectionConfig.clone();
            // extract credentials to treat them explicitly, remove from connection properties
            this.user = (String) this.connectionConfig.remove(USER_PROPERTY_NAME);
            this.password = (String) this.connectionConfig.remove(PASSWORD_PROPERTY_NAME);
        }

        this.poolConfig = (Properties) poolConfig.clone();
        this.qualifier = qualifier;

        // validate pool configuration
        PROHIBITED_PROPERTIES.forEach(p -> ensurePoolPropertyNotPresent(p));
    }

    public String getServer() {
        return server;
    }

    public String getJdbcUrl() {
        return jdbcUrl;
    }

    public String getUser() {
        return user;
    }

    public String getPassword() {
        return password;
    }

    public Properties getConnectionConfig() {
        return connectionConfig;
    }

    public Properties getPoolConfig() {
        return poolConfig;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PoolDescriptor that = (PoolDescriptor) o;
        return Objects.equals(server, that.server) &&
                Objects.equals(jdbcUrl, that.jdbcUrl) &&
                Objects.equals(user, that.user) &&
                Objects.equals(password, that.password) &&
                Objects.equals(connectionConfig, that.connectionConfig) &&
                Objects.equals(poolConfig, that.poolConfig) &&
                Objects.equals(qualifier, that.qualifier);
    }

    @Override
    public int hashCode() {
        return Objects.hash(server, jdbcUrl, user, password, connectionConfig, poolConfig, qualifier);
    }


    @Override
    public String toString() {
        return "PoolDescriptor{" +
                "jdbcUrl=" + jdbcUrl +
                ", user=" + user +
                ", password=" + ConnectionManager.maskPassword(password) +
                ", connectionConfig=" + connectionConfig +
                ", poolConfig=" + poolConfig +
                ", qualifier=" + qualifier + '}';
    }

    private void ensurePoolPropertyNotPresent(String propName) {
        if (poolConfig.getProperty(propName) != null) {
            throw new RuntimeException(
                    String.format("Property '%s' should not be configured for the JDBC connection pool", propName));
        }
    }

}

