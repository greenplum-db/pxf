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

import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.greenplum.pxf.api.model.BaseConfigurationFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.mockito.Mockito;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.internal.util.reflection.Whitebox.getInternalState;

import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@PrepareForTest({BaseConfigurationFactory.class, Class.class})
@RunWith(PowerMockRunner.class)
public class JdbcBasePluginTestInitialize {
    private static final String DATA_SOURCE = "t";
    private static final String JDBC_DRIVER = "java.lang.Object";  // we cannot mock Class.forName()
    private static final String JDBC_URL = "jdbc:postgresql://localhost/postgres";
    private static final List<ColumnDescriptor> COLUMNS;
    static {
        COLUMNS = new ArrayList<ColumnDescriptor>();
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

    /**
     * Create and prepare {@link RequestContext}
     */
    private RequestContext makeContext() {
        RequestContext context = new RequestContext();
        context.setDataSource(DATA_SOURCE);
        context.setTupleDescription(COLUMNS);
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

    /**
     * Prepare {@link BaseConfigurationFactory} getInstance() method to return
     * provided configuration
     * @param configuration
     */
    private void prepareBaseConfigurationFactory(Configuration configuration) throws Exception {
        BaseConfigurationFactory configurationFactory = mock(BaseConfigurationFactory.class);
        Mockito.when(configurationFactory.initConfiguration(Mockito.anyString(), Mockito.any())).thenReturn(configuration);
        PowerMockito.mockStatic(BaseConfigurationFactory.class);
        PowerMockito.when(BaseConfigurationFactory.getInstance()).thenReturn(configurationFactory);
    }

    @Test
    public void testMinimumSettings() throws Exception {
        // Configuration
        Configuration configuration = makeConfiguration();

        // Context
        RequestContext context = makeContext();

        // Initialize plugin
        prepareBaseConfigurationFactory(configuration);
        JdbcBasePlugin plugin = new JdbcBasePlugin();
        plugin.initialize(context);

        // Checks
        PowerMockito.verifyStatic(times(1));
        Class.forName(JDBC_DRIVER);
        assertEquals(JDBC_URL, getInternalState(plugin, "jdbcUrl"));
        assertEquals(COLUMNS, getInternalState(plugin, "columns"));

        assertEquals(getInternalState(plugin, "DEFAULT_WRITE_SIZE"), getInternalState(plugin, "writeSize"));
        assertEquals(getInternalState(plugin, "DEFAULT_POOL_SIZE"), getInternalState(plugin, "poolSize"));
        assertNull(getInternalState(plugin, "quoteColumns"));
        assertEquals(getInternalState(plugin, "DEFAULT_FETCH_SIZE"), getInternalState(plugin, "fetchSize"));
        assertEquals(getInternalState(plugin, "DEFAULT_QUERY_TIMEOUT"), getInternalState(plugin, "queryTimeout"));
    }

    @Test
    public void testWriteSize0() throws Exception {
        // Configuration
        Configuration configuration = makeConfiguration();
        configuration.set("jdbc.statement.writeSize", "0");

        // Initialize plugin
        prepareBaseConfigurationFactory(configuration);
        JdbcBasePlugin plugin = new JdbcBasePlugin();
        plugin.initialize(makeContext());

        // Checks
        assertEquals(1, getInternalState(plugin, "writeSize"));
        assertTrue((boolean)getInternalState(plugin, "writeSizeIsSetByUser"));
    }

    @Test
    public void testWriteSize1() throws Exception {
        // Configuration
        Configuration configuration = makeConfiguration();
        configuration.set("jdbc.statement.writeSize", "1");

        // Initialize plugin
        prepareBaseConfigurationFactory(configuration);
        JdbcBasePlugin plugin = new JdbcBasePlugin();
        plugin.initialize(makeContext());

        // Checks
        assertEquals(1, getInternalState(plugin, "writeSize"));
        assertTrue((boolean)getInternalState(plugin, "writeSizeIsSetByUser"));
    }

    @Test
    public void testWriteSize2() throws Exception {
        // Configuration
        Configuration configuration = makeConfiguration();
        configuration.set("jdbc.statement.writeSize", "2");

        // Initialize plugin
        prepareBaseConfigurationFactory(configuration);
        JdbcBasePlugin plugin = new JdbcBasePlugin();
        plugin.initialize(makeContext());

        // Checks
        assertEquals(2, getInternalState(plugin, "writeSize"));
        assertTrue((boolean)getInternalState(plugin, "writeSizeIsSetByUser"));
    }

    @Test
    public void testWriteSizeNegative() throws Exception {
        // Configuration
        Configuration configuration = makeConfiguration();
        configuration.set("jdbc.statement.writeSize", "-1");

        // Initialize plugin
        prepareBaseConfigurationFactory(configuration);
        JdbcBasePlugin plugin = new JdbcBasePlugin();
        plugin.initialize(makeContext());

        // Checks
        assertEquals(1, getInternalState(plugin, "writeSize"));
    }

    @Test
    public void testPoolSize1() throws Exception {
        // Configuration
        Configuration configuration = makeConfiguration();

        // Context
        RequestContext context = makeContext();
        context.addOption(OPTION_POOL_SIZE, "1");

        // Initialize plugin
        prepareBaseConfigurationFactory(configuration);
        JdbcBasePlugin plugin = new JdbcBasePlugin();
        plugin.initialize(context);

        // Checks
        assertEquals(1, getInternalState(plugin, "poolSize"));
    }

    @Test
    public void testPoolSizeNegative() throws Exception {
        // Configuration
        Configuration configuration = makeConfiguration();

        // Context
        RequestContext context = makeContext();
        context.addOption(OPTION_POOL_SIZE, "-1");

        // Initialize plugin
        prepareBaseConfigurationFactory(configuration);
        JdbcBasePlugin plugin = new JdbcBasePlugin();
        plugin.initialize(context);

        // Checks
        assertEquals(-1, getInternalState(plugin, "poolSize"));
    }

    @Test
    public void testFetchSize() throws Exception {
        // Configuration
        Configuration configuration = makeConfiguration();
        configuration.set("jdbc.statement.fetchSize", "4");

        // Initialize plugin
        prepareBaseConfigurationFactory(configuration);
        JdbcBasePlugin plugin = new JdbcBasePlugin();
        plugin.initialize(makeContext());

        // Checks
        assertEquals(4, getInternalState(plugin, "fetchSize"));
    }

    @Test
    public void testQueryTimeout() throws Exception {
        // Configuration
        Configuration configuration = makeConfiguration();
        configuration.set("jdbc.statement.queryTimeout", "200");

        // Initialize plugin
        prepareBaseConfigurationFactory(configuration);
        JdbcBasePlugin plugin = new JdbcBasePlugin();
        plugin.initialize(makeContext());

        // Checks
        assertEquals(200, getInternalState(plugin, "queryTimeout"));
    }

    @Test
    public void testQuoteColumnsFalse() throws Exception {
        // Configuration
        Configuration configuration = makeConfiguration();

        // Context
        RequestContext context = makeContext();
        context.addOption(OPTION_QUOTE_COLUMNS, "false");

        // Initialize plugin
        prepareBaseConfigurationFactory(configuration);
        JdbcBasePlugin plugin = new JdbcBasePlugin();
        plugin.initialize(context);

        // Checks
        assertFalse((Boolean)getInternalState(plugin, "quoteColumns"));
    }

    @Test
    public void testQuoteColumnsTrue() throws Exception {
        // Configuration
        Configuration configuration = makeConfiguration();

        // Context
        RequestContext context = makeContext();
        context.addOption(OPTION_QUOTE_COLUMNS, "true");

        // Initialize plugin
        prepareBaseConfigurationFactory(configuration);
        JdbcBasePlugin plugin = new JdbcBasePlugin();
        plugin.initialize(context);

        // Checks
        assertTrue((Boolean)getInternalState(plugin, "quoteColumns"));
    }

    @Test
    public void testQuoteColumnsOther() throws Exception {
        // Configuration
        Configuration configuration = makeConfiguration();

        // Context
        RequestContext context = makeContext();
        context.addOption(OPTION_QUOTE_COLUMNS, "some_other_value");

        // Initialize plugin
        prepareBaseConfigurationFactory(configuration);
        JdbcBasePlugin plugin = new JdbcBasePlugin();
        plugin.initialize(context);

        // Checks
        assertFalse((Boolean)getInternalState(plugin, "quoteColumns"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testSessionConfiguration() throws Exception {
        // Configuration
        Configuration configuration = makeConfiguration();
        configuration.set(CONFIG_SESSION_KEY_PREFIX + CONFIG_PROPERTIES_KEYS[0], "v1");
        configuration.set(CONFIG_SESSION_KEY_PREFIX + CONFIG_PROPERTIES_KEYS[1], "v2");

        // Context
        RequestContext context = makeContext();

        // Initialize plugin
        prepareBaseConfigurationFactory(configuration);
        JdbcBasePlugin plugin = new JdbcBasePlugin();
        plugin.initialize(context);

        // Checks
        Map<String, String> expected = new HashMap<String, String>();
        expected.put(CONFIG_PROPERTIES_KEYS[0], "v1");
        expected.put(CONFIG_PROPERTIES_KEYS[1], "v2");
        assertEquals(expected.entrySet(), ((Map<String, String>)getInternalState(plugin, "sessionConfiguration")).entrySet());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSessionConfigurationForbiddenSymbols() throws Exception {
        // Configuration
        Configuration configuration = makeConfiguration();
        configuration.set(CONFIG_SESSION_KEY_PREFIX + CONFIG_PROPERTIES_KEYS[0], "v1");
        configuration.set(CONFIG_SESSION_KEY_PREFIX + CONFIG_PROPERTIES_KEYS[1], "v2; SELECT * FROM secrets; ");

        // Context
        RequestContext context = makeContext();

        // Initialize plugin
        prepareBaseConfigurationFactory(configuration);
        JdbcBasePlugin plugin = new JdbcBasePlugin();
        plugin.initialize(context);
    }

    @Test
    public void testConnectionConfiguration() throws Exception {
        // Configuration
        Configuration configuration = makeConfiguration();
        configuration.set(CONFIG_CONNECTION_KEY_PREFIX + CONFIG_PROPERTIES_KEYS[0], "v1");
        configuration.set(CONFIG_CONNECTION_KEY_PREFIX + CONFIG_PROPERTIES_KEYS[1], "v2");

        // Context
        RequestContext context = makeContext();

        // Initialize plugin
        prepareBaseConfigurationFactory(configuration);
        JdbcBasePlugin plugin = new JdbcBasePlugin();
        plugin.initialize(context);

        // Checks
        // Note password and user are not set, thus configuration will be equal to the expected one
        Properties expected = new Properties();
        expected.setProperty(CONFIG_PROPERTIES_KEYS[0], "v1");
        expected.setProperty(CONFIG_PROPERTIES_KEYS[1], "v2");
        assertEquals(expected.entrySet(), ((Properties)getInternalState(plugin, "connectionConfiguration")).entrySet());
    }

    @Test
    public void testUser() throws Exception {
        // Configuration
        Configuration configuration = makeConfiguration();
        configuration.set(CONFIG_USER, "user");

        // Context
        RequestContext context = makeContext();

        // Initialize plugin
        prepareBaseConfigurationFactory(configuration);
        JdbcBasePlugin plugin = new JdbcBasePlugin();
        plugin.initialize(context);

        // Checks
        Properties expected = new Properties();
        expected.setProperty("user", "user");
        assertEquals(expected.entrySet(), ((Properties)getInternalState(plugin, "connectionConfiguration")).entrySet());
    }

    @Test
    public void testUserPassword() throws Exception {
        // Configuration
        Configuration configuration = makeConfiguration();
        configuration.set(CONFIG_USER, "user");
        configuration.set(CONFIG_PASSWORD, "password");

        // Context
        RequestContext context = makeContext();

        // Initialize plugin
        prepareBaseConfigurationFactory(configuration);
        JdbcBasePlugin plugin = new JdbcBasePlugin();
        plugin.initialize(context);

        // Checks
        Properties expected = new Properties();
        expected.setProperty("user", "user");
        expected.setProperty("password", "password");
        assertEquals(expected.entrySet(), ((Properties)getInternalState(plugin, "connectionConfiguration")).entrySet());
    }

    @Test
    public void testPassword() throws Exception {
        // Configuration
        Configuration configuration = makeConfiguration();
        configuration.set(CONFIG_PASSWORD, "password");

        // Context
        RequestContext context = makeContext();

        // Initialize plugin
        prepareBaseConfigurationFactory(configuration);
        JdbcBasePlugin plugin = new JdbcBasePlugin();
        plugin.initialize(context);

        // Checks
        Properties expected = new Properties();
        assertEquals(expected.entrySet(), ((Properties)getInternalState(plugin, "connectionConfiguration")).entrySet());
    }
}
