package org.greenplum.pxf.api.model;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BasePluginTest {

    @Test
    public void testDefaults() {
        BasePlugin basePlugin = new BasePlugin();

        assertTrue(basePlugin.isThreadSafe());
        assertFalse(basePlugin.isInitialized());
    }

    @Test
    public void testInitialize() {
        Configuration configuration = new Configuration();
        RequestContext context = new RequestContext();
        context.setConfiguration(configuration);

        BasePlugin basePlugin = new BasePlugin();
        basePlugin.initialize(context);
        assertTrue(basePlugin.isInitialized());
        assertEquals(configuration, basePlugin.configuration);
        assertEquals(context, basePlugin.context);
    }
}
