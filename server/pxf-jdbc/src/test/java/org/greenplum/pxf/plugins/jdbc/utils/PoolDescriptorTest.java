package org.greenplum.pxf.plugins.jdbc.utils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

public class PoolDescriptorTest {

    private Properties connConfig, poolConfig;
    private PoolDescriptor poolDescriptor;

    @BeforeEach
    public void setup() {
        connConfig = new Properties();
        connConfig.setProperty("user", "test-user");
        connConfig.setProperty("password", "test-password");
        connConfig.setProperty("test-other-property", "test-other-property-value");

        poolConfig = new Properties();
        poolConfig.setProperty("test-pool-property", "test-pool-property-value");
    }

    @Test
    public void testPoolDescriptor() {
        poolDescriptor = new PoolDescriptor("test-server", "test-jdbcUrl", connConfig, poolConfig, null);
        assertEquals("test-server", poolDescriptor.getServer());
        assertEquals("test-jdbcUrl", poolDescriptor.getJdbcUrl());
        assertEquals("test-password", poolDescriptor.getPassword());

        assertNotEquals(connConfig, poolDescriptor.getConnectionConfig());
        connConfig.remove("user");
        connConfig.remove("password");
        assertEquals(connConfig, poolDescriptor.getConnectionConfig());
        assertNotSame(connConfig, poolDescriptor.getConnectionConfig());

        assertEquals(poolConfig, poolDescriptor.getPoolConfig());
        assertNotSame(poolConfig, poolDescriptor.getPoolConfig());
    }

    @Test
    public void testPoolDescriptorWithProhibitedPoolPropertyUsername() {
        testInvalidProperty("username");
    }

    @Test
    public void testPoolDescriptorWithProhibitedPoolPropertyPassword() {
        testInvalidProperty("password");
    }

    @Test
    public void testPoolDescriptorWithProhibitedPoolPropertyDatasourceUser() {
        testInvalidProperty("dataSource.user");
    }

    @Test
    public void testPoolDescriptorWithProhibitedPoolPropertyDatasourcePassword() {
        testInvalidProperty("dataSource.password");
    }

    @Test
    public void testPoolDescriptorWithProhibitedPoolPropertyDataSourceClassName() {
        testInvalidProperty("dataSourceClassName");
    }

    @Test
    public void testPoolDescriptorWithProhibitedPoolPropertyjdbcUrl() {
        testInvalidProperty("jdbcUrl");
    }

    @Test
    public void testPoolDescriptorHashCodeAndEquals() {
        poolDescriptor = new PoolDescriptor("test-server", "test-jdbcUrl", connConfig, poolConfig, "foo");

        Properties sameConnConfig = (Properties) connConfig.clone();
        Properties samePoolConfig = (Properties) poolConfig.clone();
        PoolDescriptor samePoolDescriptor = new PoolDescriptor("test-server", "test-jdbcUrl", sameConnConfig, samePoolConfig, "foo");

        assertEquals(poolDescriptor.hashCode(), samePoolDescriptor.hashCode());
        assertEquals(poolDescriptor, samePoolDescriptor);

        // test different configs
        PoolDescriptor diffPoolDescriptor = new PoolDescriptor("diff-server", "test-jdbcUrl", sameConnConfig, samePoolConfig, "foo");
        assertNotEquals(poolDescriptor.hashCode(), diffPoolDescriptor.hashCode());
        assertNotEquals(poolDescriptor, diffPoolDescriptor);

        diffPoolDescriptor = new PoolDescriptor("test-server", "diff-jdbcUrl", sameConnConfig, samePoolConfig, "foo");
        assertNotEquals(poolDescriptor.hashCode(), diffPoolDescriptor.hashCode());
        assertNotEquals(poolDescriptor, diffPoolDescriptor);


        Properties diffConnConfig = (Properties) connConfig.clone();
        diffConnConfig.setProperty("user", "foo");
        diffPoolDescriptor = new PoolDescriptor("test-server", "test-jdbcUrl", diffConnConfig, samePoolConfig, "foo");
        assertNotEquals(poolDescriptor.hashCode(), diffPoolDescriptor.hashCode());
        assertNotEquals(poolDescriptor, diffPoolDescriptor);

        diffConnConfig = (Properties) connConfig.clone();
        diffConnConfig.setProperty("password", "bar");
        diffPoolDescriptor = new PoolDescriptor("test-server", "test-jdbcUrl", diffConnConfig, samePoolConfig, "foo");
        assertNotEquals(poolDescriptor.hashCode(), diffPoolDescriptor.hashCode());
        assertNotEquals(poolDescriptor, diffPoolDescriptor);

        diffConnConfig = (Properties) connConfig.clone();
        diffConnConfig.setProperty("foo", "bar");
        diffPoolDescriptor = new PoolDescriptor("test-server", "test-jdbcUrl", diffConnConfig, samePoolConfig, "foo");
        assertNotEquals(poolDescriptor.hashCode(), diffPoolDescriptor.hashCode());
        assertNotEquals(poolDescriptor, diffPoolDescriptor);

        Properties diffPoolConfig = (Properties) poolConfig.clone();
        diffPoolConfig.setProperty("abc", "xyz");
        diffPoolDescriptor = new PoolDescriptor("test-server", "test-jdbcUrl", sameConnConfig, diffPoolConfig, "foo");
        assertNotEquals(poolDescriptor.hashCode(), diffPoolDescriptor.hashCode());
        assertNotEquals(poolDescriptor, diffPoolDescriptor);

        diffPoolDescriptor = new PoolDescriptor("test-server", "test-jdbcUrl", sameConnConfig, samePoolConfig, "bar");
        assertNotEquals(poolDescriptor.hashCode(), diffPoolDescriptor.hashCode());
        assertNotEquals(poolDescriptor, diffPoolDescriptor);

        diffPoolDescriptor = new PoolDescriptor("test-server", "test-jdbcUrl", sameConnConfig, samePoolConfig, null);
        assertNotEquals(poolDescriptor.hashCode(), diffPoolDescriptor.hashCode());
        assertNotEquals(poolDescriptor, diffPoolDescriptor);

        poolDescriptor = new PoolDescriptor("test-server", "test-jdbcUrl", connConfig, poolConfig, null);
        diffPoolDescriptor = new PoolDescriptor("test-server", "test-jdbcUrl", sameConnConfig, samePoolConfig, null);
        assertEquals(poolDescriptor.hashCode(), diffPoolDescriptor.hashCode());
        assertEquals(poolDescriptor, diffPoolDescriptor);
    }

    @Test
    public void testPoolDescriptorToString() {
        poolDescriptor = new PoolDescriptor("test-server", "test-jdbcUrl", connConfig, poolConfig, null);
        assertEquals("PoolDescriptor{jdbcUrl=test-jdbcUrl, user=test-user, password=*************, connectionConfig={test-other-property=test-other-property-value}, poolConfig={test-pool-property=test-pool-property-value}, qualifier=null}", poolDescriptor.toString());
        poolDescriptor = new PoolDescriptor("test-server", "test-jdbcUrl", connConfig, poolConfig, "foo");
        assertEquals("PoolDescriptor{jdbcUrl=test-jdbcUrl, user=test-user, password=*************, connectionConfig={test-other-property=test-other-property-value}, poolConfig={test-pool-property=test-pool-property-value}, qualifier=foo}", poolDescriptor.toString());
    }

    private void testInvalidProperty(String property) {
        poolConfig.setProperty(property, property + " not allowed");
        Exception ex = assertThrows(RuntimeException.class,
                () -> new PoolDescriptor("test-server", "test-jdbcUrl", connConfig, poolConfig, null));
        assertEquals("Property '" + property + "' should not be configured for the JDBC connection pool", ex.getMessage());
    }
}
