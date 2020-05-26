package org.greenplum.pxf.service.bridge;

import org.apache.hadoop.conf.Configuration;
import org.greenplum.pxf.api.io.Writable;
import org.greenplum.pxf.api.model.Accessor;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.model.Resolver;
import org.greenplum.pxf.api.utilities.AccessorFactory;
import org.greenplum.pxf.api.utilities.ResolverFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.DataInputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BaseBridgeTest {

    private Accessor mockAccessor;
    private AccessorFactory mockAccessorFactory;
    private RequestContext context;
    private Resolver mockResolver;
    private ResolverFactory mockResolverFactory;
    private TestBridge bridge;

    @BeforeEach
    public void setup() {
        bridge = new TestBridge();
        context = new RequestContext();
        context.setConfiguration(new Configuration());

        mockAccessorFactory = mock(AccessorFactory.class);
        mockResolverFactory = mock(ResolverFactory.class);
        mockAccessor = mock(Accessor.class);
        mockResolver = mock(Resolver.class);
    }

    @Test
    public void testContextConstructor() {
        context.setAccessor("org.greenplum.pxf.service.bridge.TestAccessor");
        context.setResolver("org.greenplum.pxf.service.bridge.TestResolver");
        bridge.initialize(context);
        assertTrue(bridge.getAccessor() instanceof TestAccessor);
        assertTrue(bridge.getResolver() instanceof TestResolver);
    }

    @Test
    public void testContextConstructorUnknownAccessor() {

        context.setAccessor("unknown-accessor");
        context.setResolver("org.greenplum.pxf.service.bridge.TestResolver");

        Exception e = assertThrows(RuntimeException.class,
                () -> bridge.initialize(context));
        assertEquals("Class unknown-accessor is not found", e.getMessage());
    }

    @Test
    public void testContextConstructorUnknownResolver() {
        context.setAccessor("org.greenplum.pxf.service.bridge.TestAccessor");
        context.setResolver("unknown-resolver");

        Exception e = assertThrows(RuntimeException.class,
                () -> bridge.initialize(context));
        assertEquals("Class unknown-resolver is not found", e.getMessage());
    }

    @Test
    public void testIsThreadSafeTT() {
        when(mockAccessorFactory.getPlugin(context)).thenReturn(mockAccessor);
        when(mockResolverFactory.getPlugin(context)).thenReturn(mockResolver);
        when(mockAccessor.isThreadSafe()).thenReturn(true);
        when(mockResolver.isThreadSafe()).thenReturn(true);
        bridge = new TestBridge(mockAccessorFactory, mockResolverFactory);
        bridge.initialize(context);
        assertTrue(bridge.isThreadSafe());
    }

    @Test
    public void testIsThreadSafeTF() {
        when(mockAccessorFactory.getPlugin(context)).thenReturn(mockAccessor);
        when(mockResolverFactory.getPlugin(context)).thenReturn(mockResolver);
        when(mockAccessor.isThreadSafe()).thenReturn(true);
        when(mockResolver.isThreadSafe()).thenReturn(false);
        bridge = new TestBridge(mockAccessorFactory, mockResolverFactory);
        bridge.initialize(context);
        assertFalse(bridge.isThreadSafe());
    }

    @Test
    public void testIsThreadSafeFT() {
        when(mockAccessorFactory.getPlugin(context)).thenReturn(mockAccessor);
        when(mockResolverFactory.getPlugin(context)).thenReturn(mockResolver);
        when(mockAccessor.isThreadSafe()).thenReturn(false);
        when(mockResolver.isThreadSafe()).thenReturn(true);
        bridge = new TestBridge(mockAccessorFactory, mockResolverFactory);
        bridge.initialize(context);
        assertFalse(bridge.isThreadSafe());
    }

    @Test
    public void testIsThreadSafeFF() {
        when(mockAccessorFactory.getPlugin(context)).thenReturn(mockAccessor);
        when(mockResolverFactory.getPlugin(context)).thenReturn(mockResolver);
        when(mockAccessor.isThreadSafe()).thenReturn(false);
        when(mockResolver.isThreadSafe()).thenReturn(false);
        bridge = new TestBridge(mockAccessorFactory, mockResolverFactory);
        bridge.initialize(context);
        assertFalse(bridge.isThreadSafe());
    }

    static class TestBridge extends BaseBridge {

        public TestBridge() {
            this(new AccessorFactory(), new ResolverFactory());
        }

        public TestBridge(AccessorFactory accessorFactory, ResolverFactory resolverFactory) {
            super(accessorFactory, resolverFactory);
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
