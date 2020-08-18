package org.greenplum.pxf.api.model;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BasePluginTest {

    @Test
    public void testDefaults() {
        BasePlugin basePlugin = new BasePlugin();
        assertFalse(basePlugin.isInitialized());
    }

    @Test
    public void testInitialize() {
        ConfigurationFactory mockConfigurationFactory = mock(ConfigurationFactory.class);

        Configuration configuration = new Configuration();
        RequestContext context = new RequestContext();

        when(mockConfigurationFactory.
                initConfiguration(context.getConfig(), context.getServerName(), context.getUser(), context.getAdditionalConfigProps()))
                .thenReturn(configuration);

        BasePlugin basePlugin = new BasePlugin(mockConfigurationFactory);
        basePlugin.initialize(context);
        assertTrue(basePlugin.isInitialized());
        assertEquals(configuration, basePlugin.configuration);
        assertEquals(context, basePlugin.context);
    }
}
