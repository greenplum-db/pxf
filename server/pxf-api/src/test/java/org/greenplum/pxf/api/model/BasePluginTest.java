package org.greenplum.pxf.api.model;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertSame;

class BasePluginTest {

    @Test
    public void testInitialize() {
        Configuration configuration = new Configuration();
        RequestContext context = new RequestContext();
        context.setConfiguration(configuration);

        BasePlugin basePlugin = new BasePlugin();
        basePlugin.setRequestContext(context);
        basePlugin.afterPropertiesSet();
        assertSame(configuration, basePlugin.configuration);
        assertSame(context, basePlugin.context);
    }
}