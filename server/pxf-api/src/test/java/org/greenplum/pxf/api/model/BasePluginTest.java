package org.greenplum.pxf.api.model;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class BasePluginTest {

    @Test
    public void testDefaults() {
        BasePlugin basePlugin = new BasePlugin();
        assertTrue(basePlugin.isThreadSafe());
    }

    @Test
    public void testInitialize() {
        Configuration configuration = new Configuration();
        RequestContext context = new RequestContext();
        context.setConfiguration(configuration);

        BasePlugin basePlugin = new BasePlugin();
        basePlugin.setRequestContext(context);
        basePlugin.initialize();
        assertSame(configuration, basePlugin.configuration);
        assertSame(context, basePlugin.context);
    }
}