package org.greenplum.pxf.service.bridge;

import org.apache.hadoop.conf.Configuration;
import org.greenplum.pxf.api.io.Writable;
import org.greenplum.pxf.api.model.Accessor;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.model.Resolver;
import org.greenplum.pxf.service.utilities.BasePluginFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.DataInputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BaseBridgeTest {

    private RequestContext context;
    private BasePluginFactory pluginFactory;

    @BeforeEach
    public void setup() {
        context = new RequestContext();
        context.setConfiguration(new Configuration());

        pluginFactory = new BasePluginFactory();
    }

    @Test
    public void testContextConstructor() {
        context.setAccessor("org.greenplum.pxf.service.bridge.TestAccessor");
        context.setResolver("org.greenplum.pxf.service.bridge.TestResolver");

        TestBridge bridge = new TestBridge(pluginFactory, context);
        assertTrue(bridge.getAccessor() instanceof TestAccessor);
        assertTrue(bridge.getResolver() instanceof TestResolver);
    }

    @Test
    public void testContextConstructorUnknownAccessor() {
        context.setAccessor("org.greenplum.pxf.unknown-accessor");
        context.setResolver("org.greenplum.pxf.service.bridge.TestResolver");

        RuntimeException e = assertThrows(RuntimeException.class, () -> new TestBridge(pluginFactory, context));
        assertEquals("Class org.greenplum.pxf.unknown-accessor is not found", e.getMessage());
    }

    @Test
    public void testContextConstructorUnknownResolver() {
        context.setAccessor("org.greenplum.pxf.service.bridge.TestAccessor");
        context.setResolver("org.greenplum.pxf.unknown-resolver");

        Exception e = assertThrows(RuntimeException.class, () -> new TestBridge(pluginFactory, context));
        assertEquals("Class org.greenplum.pxf.unknown-resolver is not found", e.getMessage());
    }

    static class TestBridge extends BaseBridge {

        public TestBridge(BasePluginFactory pluginFactory, RequestContext context) {
            super(pluginFactory, context);
        }

        @Override
        public boolean beginIteration() {
            return false;
        }

        @Override
        public Writable getNext() {
            return null;
        }

        @Override
        public boolean setNext(DataInputStream inputStream) {
            return false;
        }

        @Override
        public void endIteration() {
        }

        Accessor getAccessor() {
            return accessor;
        }

        Resolver getResolver() {
            return resolver;
        }
    }
}
