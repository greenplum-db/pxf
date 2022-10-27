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

import com.google.common.base.Ticker;
import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.PxfUserGroupInformation;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.security.SecureLogin;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.greenplum.pxf.plugins.jdbc.utils.ConnectionManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.internal.util.reflection.FieldReader;
import org.mockito.junit.jupiter.MockitoExtension;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

@ExtendWith(MockitoExtension.class)
public class JdbcBasePluginTestInitialize {

    private static final String DATA_SOURCE = "t";
    private static final String JDBC_DRIVER = "java.lang.Object";  // we cannot mock Class.forName()
    private static final String JDBC_URL = "jdbc:postgresql://localhost/postgres";
    private static final List<ColumnDescriptor> COLUMNS;

    static {
        COLUMNS = new ArrayList<>();
        COLUMNS.add(new ColumnDescriptor("c1", DataType.INTEGER.getOID(), 1, null, null, true));
        COLUMNS.add(new ColumnDescriptor("c2", DataType.VARCHAR.getOID(), 2, null, null, true));
    }

    private static final String OPTION_POOL_SIZE = "POOL_SIZE";
    private static final String OPTION_QUOTE_COLUMNS = "QUOTE_COLUMNS";
    private static final String CONFIG_SESSION_KEY_PREFIX = "jdbc.session.property.";
    private static final String CONFIG_CONNECTION_KEY_PREFIX = "jdbc.connection.property.";
    private static final String[] CONFIG_PROPERTIES_KEYS = {"k1", "k2"};
    private static final String CONFIG_USER = "jdbc.user";
    private static final String CONFIG_PASSWORD = "jdbc.password";

    private JdbcBasePlugin plugin;

    @BeforeEach
    public void setup() {

        PxfJdbcProperties properties = new PxfJdbcProperties();

        ConnectionManager.DriverManagerWrapper mockDriverManagerWrapper = mock(ConnectionManager.DriverManagerWrapper.class);

        ConnectionManager connectionManager = new ConnectionManager(
                new ConnectionManager.DataSourceFactory(),
                Ticker.systemTicker(),
                properties,
                mockDriverManagerWrapper
        );

        PxfUserGroupInformation mockPxfUserGroupInformation = mock(PxfUserGroupInformation.class);
        plugin = new JdbcBasePlugin(connectionManager, new SecureLogin(mockPxfUserGroupInformation));
    }

    /**
     * Create and prepare {@link RequestContext}
     */
    private RequestContext makeContext(Configuration configuration) {
        RequestContext context = new RequestContext();
        context.setDataSource(DATA_SOURCE);
        context.setTupleDescription(COLUMNS);
        context.setRequestType(RequestContext.RequestType.WRITE_BRIDGE);
        context.setTotalSegments(1);
        context.setConfiguration(configuration);
        return context;
    }

    /**
     * Create and prepare {@link RequestContext}
     */
    private RequestContext makeContextWithDataSource(Configuration configuration, String datasource) {
        RequestContext context = new RequestContext();
        context.setDataSource(datasource);
        context.setTupleDescription(COLUMNS);
        context.setConfiguration(configuration);
        return context;
    }

    /**
     * Create and prepare {@link Configuration}
     */
    private Configuration makeConfiguration() {
        Configuration configuration = new Configuration();
        configuration.set("jdbc.driver", JDBC_DRIVER);
        configuration.set("jdbc.url", JDBC_URL);
        return configuration;
    }

    @Test
    public void testMinimumSettings() throws Exception {
        // Configuration
        Configuration configuration = makeConfiguration();

        // Context
        RequestContext context = makeContext(configuration);

        // Initialize plugin
        plugin.setRequestContext(context);
        plugin.afterPropertiesSet();

        // Checks
        Class.forName(JDBC_DRIVER);
        assertEquals(JDBC_URL, getInternalState(plugin, "jdbcUrl"));
        assertEquals(COLUMNS, getInternalState(plugin, "columns"));

        assertEquals(getInternalState(plugin, "DEFAULT_BATCH_SIZE"), getInternalState(plugin, "batchSize"));
        assertEquals(getInternalState(plugin, "DEFAULT_POOL_SIZE"), getInternalState(plugin, "poolSize"));
        assertNull(getInternalState(plugin, "quoteColumns"));
        assertEquals(getInternalState(plugin, "DEFAULT_FETCH_SIZE"), getInternalState(plugin, "fetchSize"));
        assertNull(getInternalState(plugin, "queryTimeout"));
        assertNull(getInternalState(plugin, "poolMaximumSize"));
        assertNull(getInternalState(plugin, "poolConnectionTimeout"));
        assertNull(getInternalState(plugin, "poolIdleTimeout"));
        assertNull(getInternalState(plugin, "poolMinimumIdle"));
    }

    @Test
    public void testBatchSize0() throws Exception {
        // Configuration
        Configuration configuration = makeConfiguration();
        configuration.set("jdbc.statement.batchSize", "0");

        // Initialize plugin
        plugin.setRequestContext(makeContext(configuration));
        plugin.afterPropertiesSet();

        // Checks
        assertEquals(1, getInternalState(plugin, "batchSize"));
        assertTrue((boolean) getInternalState(plugin, "batchSizeIsSetByUser"));
    }

    @Test
    public void testBatchSize1() throws Exception {
        // Configuration
        Configuration configuration = makeConfiguration();
        configuration.set("jdbc.statement.batchSize", "1");

        // Initialize plugin
        plugin.setRequestContext(makeContext(configuration));
        plugin.afterPropertiesSet();

        // Checks
        assertEquals(1, getInternalState(plugin, "batchSize"));
        assertTrue((boolean) getInternalState(plugin, "batchSizeIsSetByUser"));
    }

    @Test
    public void testBatchSize2() throws Exception {
        // Configuration
        Configuration configuration = makeConfiguration();
        configuration.set("jdbc.statement.batchSize", "2");

        // Initialize plugin
        plugin.setRequestContext(makeContext(configuration));
        plugin.afterPropertiesSet();

        // Checks
        assertEquals(2, getInternalState(plugin, "batchSize"));
        assertTrue((boolean) getInternalState(plugin, "batchSizeIsSetByUser"));
    }

    @Test
    public void testBatchSizeOnRead() {
        // Configuration
        Configuration configuration = makeConfiguration();
        configuration.set("jdbc.statement.batchSize", "foobar");

        // Initialize plugin
        RequestContext context = makeContext(configuration);
        context.setRequestType(RequestContext.RequestType.READ_BRIDGE);
        plugin.setRequestContext(context);
        plugin.afterPropertiesSet();

        // should not error because we don't validate this on the READ path
    }

    @Test
    public void testBatchSizeOnWrite() {
        // Configuration
        Configuration configuration = makeConfiguration();
        configuration.set("jdbc.statement.batchSize", "foobar");

        plugin.setRequestContext(makeContext(configuration));
        // Initialize plugin
        Exception ex = assertThrows(NumberFormatException.class, plugin::afterPropertiesSet);

        assertEquals("For input string: \"foobar\"", ex.getMessage());
    }

    @Test
    public void testBatchSizeNegative() {
        // Configuration
        Configuration configuration = makeConfiguration();
        configuration.set("jdbc.statement.batchSize", "-1");

        plugin.setRequestContext(makeContext(configuration));
        // Initialize plugin
        Exception ex = assertThrows(IllegalArgumentException.class, plugin::afterPropertiesSet);
        assertEquals("Property jdbc.statement.batchSize has incorrect value -1 : must be a non-negative integer", ex.getMessage());
    }

    @Test
    public void testPoolSize1() throws Exception {
        // Configuration
        Configuration configuration = makeConfiguration();

        // Context
        RequestContext context = makeContext(configuration);
        context.addOption(OPTION_POOL_SIZE, "1");

        // Initialize plugin
        plugin.setRequestContext(context);
        plugin.afterPropertiesSet();

        // Checks
        assertEquals(1, getInternalState(plugin, "poolSize"));
    }

    @Test
    public void testPoolSizeNegative() throws Exception {
        // Configuration
        Configuration configuration = makeConfiguration();

        // Context
        RequestContext context = makeContext(configuration);
        context.addOption(OPTION_POOL_SIZE, "-1");

        // Initialize plugin
        plugin.setRequestContext(context);
        plugin.afterPropertiesSet();

        // Checks
        assertEquals(-1, getInternalState(plugin, "poolSize"));
    }

    @Test
    public void testFetchSize() throws Exception {
        // Configuration
        Configuration configuration = makeConfiguration();
        configuration.set("jdbc.statement.fetchSize", "4");

        // Initialize plugin
        plugin.setRequestContext(makeContext(configuration));
        plugin.afterPropertiesSet();

        // Checks
        assertEquals(4, getInternalState(plugin, "fetchSize"));
    }

    @Test
    public void testQueryTimeout() throws Exception {
        // Configuration
        Configuration configuration = makeConfiguration();
        configuration.set("jdbc.statement.queryTimeout", "200");

        // Initialize plugin
        plugin.setRequestContext(makeContext(configuration));
        plugin.afterPropertiesSet();

        // Checks
        assertEquals(200, getInternalState(plugin, "queryTimeout"));
    }

    @Test
    public void testInvalidStringQueryTimeout() {
        // Configuration
        Configuration configuration = makeConfiguration();
        configuration.set("jdbc.statement.queryTimeout", "foo");

        plugin.setRequestContext(makeContext(configuration));
        // Initialize plugin
        Exception ex = assertThrows(IllegalArgumentException.class, plugin::afterPropertiesSet);
        assertEquals("Property jdbc.statement.queryTimeout has incorrect value foo : must be a non-negative integer", ex.getMessage());
    }

    @Test
    public void testInvalidNegativeQueryTimeout() {

        // Configuration
        Configuration configuration = makeConfiguration();
        configuration.set("jdbc.statement.queryTimeout", "-1");

        plugin.setRequestContext(makeContext(configuration));
        // Initialize plugin
        Exception ex = assertThrows(IllegalArgumentException.class, plugin::afterPropertiesSet);
        assertEquals("Property jdbc.statement.queryTimeout has incorrect value -1 : must be a non-negative integer", ex.getMessage());
    }

    @Test
    public void testQuoteColumnsFalse() throws Exception {
        // Configuration
        Configuration configuration = makeConfiguration();

        // Context
        RequestContext context = makeContext(configuration);
        context.addOption(OPTION_QUOTE_COLUMNS, "false");

        // Initialize plugin
        plugin.setRequestContext(context);
        plugin.afterPropertiesSet();

        // Checks
        assertFalse((Boolean) getInternalState(plugin, "quoteColumns"));
    }

    @Test
    public void testQuoteColumnsTrue() throws Exception {
        // Configuration
        Configuration configuration = makeConfiguration();

        // Context
        RequestContext context = makeContext(configuration);
        context.addOption(OPTION_QUOTE_COLUMNS, "true");

        // Initialize plugin
        plugin.setRequestContext(context);
        plugin.afterPropertiesSet();

        // Checks
        assertTrue((Boolean) getInternalState(plugin, "quoteColumns"));
    }

    @Test
    public void testQuoteColumnsOther() throws Exception {
        // Configuration
        Configuration configuration = makeConfiguration();

        // Context
        RequestContext context = makeContext(configuration);
        context.addOption(OPTION_QUOTE_COLUMNS, "some_other_value");

        // Initialize plugin
        plugin.setRequestContext(context);
        plugin.afterPropertiesSet();

        // Checks
        assertFalse((Boolean) getInternalState(plugin, "quoteColumns"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testSessionConfiguration() throws Exception {
        // Configuration
        Configuration configuration = makeConfiguration();
        configuration.set(CONFIG_SESSION_KEY_PREFIX + CONFIG_PROPERTIES_KEYS[0], "v1");
        configuration.set(CONFIG_SESSION_KEY_PREFIX + CONFIG_PROPERTIES_KEYS[1], "v2");

        // Context
        RequestContext context = makeContext(configuration);

        // Initialize plugin
        plugin.setRequestContext(context);
        plugin.afterPropertiesSet();

        // Checks
        Map<String, String> expected = new HashMap<>();
        expected.put(CONFIG_PROPERTIES_KEYS[0], "v1");
        expected.put(CONFIG_PROPERTIES_KEYS[1], "v2");
        assertEquals(expected.entrySet(), ((Map<String, String>) getInternalState(plugin, "sessionConfiguration")).entrySet());
    }

    @Test
    public void testSessionConfigurationForbiddenSymbols() {
        // Configuration
        Configuration configuration = makeConfiguration();
        configuration.set(CONFIG_SESSION_KEY_PREFIX + CONFIG_PROPERTIES_KEYS[0], "v1");
        configuration.set(CONFIG_SESSION_KEY_PREFIX + CONFIG_PROPERTIES_KEYS[1], "v2; SELECT * FROM secrets; ");

        // Context
        RequestContext context = makeContext(configuration);

        plugin.setRequestContext(context);
        // Initialize plugin
        Exception ex = assertThrows(IllegalArgumentException.class, plugin::afterPropertiesSet);
        assertEquals("Some session configuration parameter contains forbidden characters", ex.getMessage());
    }

    @Test
    public void testConnectionConfiguration() throws Exception {
        // Configuration
        Configuration configuration = makeConfiguration();
        configuration.set(CONFIG_CONNECTION_KEY_PREFIX + CONFIG_PROPERTIES_KEYS[0], "v1");
        configuration.set(CONFIG_CONNECTION_KEY_PREFIX + CONFIG_PROPERTIES_KEYS[1], "v2");

        // Context
        RequestContext context = makeContext(configuration);

        // Initialize plugin
        plugin.setRequestContext(context);
        plugin.afterPropertiesSet();

        // Checks
        // Note password and user are not set, thus configuration will be equal to the expected one
        Properties expected = new Properties();
        expected.setProperty(CONFIG_PROPERTIES_KEYS[0], "v1");
        expected.setProperty(CONFIG_PROPERTIES_KEYS[1], "v2");
        assertEntrySetEquals(expected.entrySet(), ((Properties) getInternalState(plugin, "connectionConfiguration")).entrySet());
    }

    @Test
    public void testUser() throws Exception {
        // Configuration
        Configuration configuration = makeConfiguration();
        configuration.set(CONFIG_USER, "user");

        // Context
        RequestContext context = makeContext(configuration);

        // Initialize plugin
        plugin.setRequestContext(context);
        plugin.afterPropertiesSet();

        // Checks
        Properties expected = new Properties();
        expected.setProperty("user", "user");
        assertEntrySetEquals(expected.entrySet(), ((Properties) getInternalState(plugin, "connectionConfiguration")).entrySet());
    }

    @Test
    public void testUserWithImpersonation() throws Exception {
        // Configuration
        Configuration configuration = makeConfiguration();
        configuration.set("pxf.service.user.impersonation", "true");

        // Context
        RequestContext context = makeContext(configuration);
        context.setUser("proxy");

        // Initialize plugin
        plugin.setRequestContext(context);
        plugin.afterPropertiesSet();

        // Checks
        Properties expected = new Properties();
        expected.setProperty("user", "proxy");
        assertEntrySetEquals(expected.entrySet(), ((Properties) getInternalState(plugin, "connectionConfiguration")).entrySet());
    }

    @Test
    public void testUserWithImpersonationOverwrite() throws Exception {
        // Configuration
        Configuration configuration = makeConfiguration();
        configuration.set(CONFIG_USER, "user");
        configuration.set("pxf.service.user.impersonation", "true");

        // Context
        RequestContext context = makeContext(configuration);
        context.setUser("proxy");

        // Initialize plugin
        plugin.setRequestContext(context);
        plugin.afterPropertiesSet();

        // Checks
        Properties expected = new Properties();
        expected.setProperty("user", "proxy");
        assertEntrySetEquals(expected.entrySet(), ((Properties) getInternalState(plugin, "connectionConfiguration")).entrySet());
    }

    @Test
    public void testUserWithoutImpersonationNoOverwrite() throws Exception {
        // Configuration
        Configuration configuration = makeConfiguration();
        configuration.set(CONFIG_USER, "user");
        configuration.set("pxf.service.user.impersonation", "false");

        // Context
        RequestContext context = makeContext(configuration);
        context.setUser("proxy");

        // Initialize plugin
        plugin.setRequestContext(context);
        plugin.afterPropertiesSet();

        // Checks
        Properties expected = new Properties();
        expected.setProperty("user", "user");
        assertEntrySetEquals(expected.entrySet(), ((Properties) getInternalState(plugin, "connectionConfiguration")).entrySet());
    }

    @Test
    public void testUserDefaultImpersonationNoOverwrite() throws Exception {
        // Configuration
        Configuration configuration = makeConfiguration();
        configuration.set(CONFIG_USER, "user");

        // Context
        RequestContext context = makeContext(configuration);
        context.setUser("proxy");

        // Initialize plugin
        plugin.setRequestContext(context);
        plugin.afterPropertiesSet();

        // Checks
        Properties expected = new Properties();
        expected.setProperty("user", "user");
        assertEntrySetEquals(expected.entrySet(), ((Properties) getInternalState(plugin, "connectionConfiguration")).entrySet());
    }


    @Test
    public void testUserPassword() throws Exception {
        // Configuration
        Configuration configuration = makeConfiguration();
        configuration.set(CONFIG_USER, "user");
        configuration.set(CONFIG_PASSWORD, "password");

        // Context
        RequestContext context = makeContext(configuration);

        // Initialize plugin
        plugin.setRequestContext(context);
        plugin.afterPropertiesSet();

        // Checks
        Properties expected = new Properties();
        expected.setProperty("user", "user");
        expected.setProperty("password", "password");
        assertEntrySetEquals(expected.entrySet(), ((Properties) getInternalState(plugin, "connectionConfiguration")).entrySet());
    }

    @Test
    public void testPassword() throws Exception {
        // Configuration
        Configuration configuration = makeConfiguration();
        configuration.set(CONFIG_PASSWORD, "password");

        // Context
        RequestContext context = makeContext(configuration);

        // Initialize plugin
        plugin.setRequestContext(context);
        plugin.afterPropertiesSet();

        // Checks
        Properties expected = new Properties();
        assertEntrySetEquals(expected.entrySet(), ((Properties) getInternalState(plugin, "connectionConfiguration")).entrySet());
    }

    @Test
    public void testDatasourceIsTable() throws Exception {
        plugin.setRequestContext(makeContextWithDataSource(makeConfiguration(), "foo"));
        plugin.afterPropertiesSet();

        assertEquals("foo", getInternalState(plugin, "tableName"));
        assertNull(getInternalState(plugin, "queryName"));
    }

    @Test
    public void testDatasourceIsQuery() throws Exception {
        plugin.setRequestContext(makeContextWithDataSource(makeConfiguration(), "query:foo"));
        plugin.afterPropertiesSet();

        assertEquals("foo", getInternalState(plugin, "queryName"));
        assertNull(getInternalState(plugin, "tableName"));
    }

    @Test
    public void testInitializationFailsWhenDatasourceIsEmptyQuery() {
        plugin.setRequestContext(makeContextWithDataSource(makeConfiguration(), "query:"));
        Exception ex = assertThrows(IllegalArgumentException.class, plugin::afterPropertiesSet);
        assertEquals("Query name is not provided in data source [query:]", ex.getMessage());
    }

    @Test
    public void testConnectionPoolEnabledPropertyNotDefined() throws Exception {
        Configuration configuration = makeConfiguration();
        plugin.setRequestContext(makeContext(configuration));
        plugin.afterPropertiesSet();

        Properties poolConfiguration = (Properties) getInternalState(plugin, "poolConfiguration");
        assertNotNull(poolConfiguration);
        assertEquals(4, poolConfiguration.size());
        assertEquals("15", poolConfiguration.getProperty("maximumPoolSize"));
        assertEquals("30000", poolConfiguration.getProperty("connectionTimeout"));
        assertEquals("30000", poolConfiguration.getProperty("idleTimeout"));
        assertEquals("0", poolConfiguration.getProperty("minimumIdle"));
    }

    @Test
    public void testConnectionPoolNotEnabledPropertyDefined() throws Exception {
        Configuration configuration = makeConfiguration();
        configuration.set("jdbc.pool.enabled", "false");

        plugin.setRequestContext(makeContext(configuration));
        plugin.afterPropertiesSet();

        assertNull(getInternalState(plugin, "poolConfiguration"));
    }

    @Test
    public void testConnectionPoolEnabledPropertyDefined() throws Exception {
        Configuration configuration = makeConfiguration();
        configuration.set("jdbc.pool.enabled", "true");
        configuration.set("jdbc.pool.property.foo", "include-foo");
        configuration.set("jdbc.pool.property.bar", "include-bar");
        configuration.set("jdbc.whatever", "exclude-whatever");

        plugin.setRequestContext(makeContext(configuration));
        plugin.afterPropertiesSet();

        Properties poolProps = (Properties) getInternalState(plugin, "poolConfiguration");
        assertNotNull(poolProps);
        assertEquals(6, poolProps.size());

        Properties expectedProps = new Properties();
        expectedProps.setProperty("maximumPoolSize", "15");
        expectedProps.setProperty("connectionTimeout", "30000");
        expectedProps.setProperty("idleTimeout", "30000");
        expectedProps.setProperty("minimumIdle", "0");
        expectedProps.setProperty("foo", "include-foo");
        expectedProps.setProperty("bar", "include-bar");
        assertEquals(expectedProps, poolProps);
    }

    private void assertEntrySetEquals(Set<Map.Entry<Object, Object>> expected, Set<Map.Entry<Object, Object>> actual) {
        assertTrue(CollectionUtils.isEqualCollection(expected, actual));
    }

    private Object getInternalState(Object target, String fieldName) throws NoSuchFieldException {
        Class<?> clazz = target.getClass();
        if (clazz.getName().contains("$")) {
            clazz = clazz.getSuperclass();
        }
        Field field = clazz.getDeclaredField(fieldName);
        return new FieldReader(target, field).read();
    }
}
