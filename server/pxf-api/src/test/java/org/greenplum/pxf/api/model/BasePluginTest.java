package org.greenplum.pxf.api.model;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BasePluginTest {

    @Test
    public void testDefaults() {
        BasePlugin basePlugin = new BasePlugin();

        assertTrue(basePlugin.isThreadSafe());
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
