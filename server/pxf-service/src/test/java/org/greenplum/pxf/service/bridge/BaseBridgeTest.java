package org.greenplum.pxf.service.bridge;

import org.greenplum.pxf.api.model.Accessor;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.model.Resolver;
import org.greenplum.pxf.api.utilities.AccessorFactory;
import org.greenplum.pxf.api.utilities.ResolverFactory;
import org.greenplum.pxf.api.io.Writable;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.DataInputStream;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class BaseBridgeTest {

    @Rule public ExpectedException failure = ExpectedException.none();

    private TestBridge bridge;

    @Mock private RequestContext mockContext;
    @Mock private AccessorFactory mockAccessorFactory;
    @Mock private ResolverFactory mockResolverFactory;
    @Mock private Accessor mockAccessor;
    @Mock private Resolver mockResolver;

    @Test public void testContextConstructor() {
        when(mockContext.getAccessor()).thenReturn("org.greenplum.pxf.service.bridge.TestAccessor");
        when(mockContext.getResolver()).thenReturn("org.greenplum.pxf.service.bridge.TestResolver");
        bridge = new TestBridge(mockContext);
        assertTrue(bridge.getAccessor() instanceof TestAccessor);
        assertTrue(bridge.getResolver() instanceof TestResolver);
    }

    @Test public void testContextConstructorUnknownAccessor() {
        failure.expect(RuntimeException.class);
        failure.expectMessage("Class unknown-accessor is not found");

        when(mockContext.getAccessor()).thenReturn("unknown-accessor");
        when(mockContext.getResolver()).thenReturn("org.greenplum.pxf.service.bridge.TestResolver");
        bridge = new TestBridge(mockContext);
    }

    @Test public void testContextConstructorUnknownResolver() {
        failure.expect(RuntimeException.class);
        failure.expectMessage("Class unknown-resolver is not found");

        when(mockContext.getAccessor()).thenReturn("org.greenplum.pxf.service.bridge.TestAccessor");
        when(mockContext.getResolver()).thenReturn("unknown-resolver");
        bridge = new TestBridge(mockContext);
    }

    @Test public void testIsThreadSafeTT() {
        when(mockAccessorFactory.getPlugin(mockContext)).thenReturn(mockAccessor);
        when(mockResolverFactory.getPlugin(mockContext)).thenReturn(mockResolver);
        when(mockAccessor.isThreadSafe()).thenReturn(true);
        when(mockResolver.isThreadSafe()).thenReturn(true);
        bridge = new TestBridge(mockContext, mockAccessorFactory, mockResolverFactory);
        assertTrue(bridge.isThreadSafe());
    }

    @Test public void testIsThreadSafeTF() {
        when(mockAccessorFactory.getPlugin(mockContext)).thenReturn(mockAccessor);
        when(mockResolverFactory.getPlugin(mockContext)).thenReturn(mockResolver);
        when(mockAccessor.isThreadSafe()).thenReturn(true);
        when(mockResolver.isThreadSafe()).thenReturn(false);
        bridge = new TestBridge(mockContext, mockAccessorFactory, mockResolverFactory);
        assertFalse(bridge.isThreadSafe());
    }

    @Test public void testIsThreadSafeFT() {
        when(mockAccessorFactory.getPlugin(mockContext)).thenReturn(mockAccessor);
        when(mockResolverFactory.getPlugin(mockContext)).thenReturn(mockResolver);
        when(mockAccessor.isThreadSafe()).thenReturn(false);
        when(mockResolver.isThreadSafe()).thenReturn(true);
        bridge = new TestBridge(mockContext, mockAccessorFactory, mockResolverFactory);
        assertFalse(bridge.isThreadSafe());
    }

    @Test public void testIsThreadSafeFF() {
        when(mockAccessorFactory.getPlugin(mockContext)).thenReturn(mockAccessor);
        when(mockResolverFactory.getPlugin(mockContext)).thenReturn(mockResolver);
        when(mockAccessor.isThreadSafe()).thenReturn(false);
        when(mockResolver.isThreadSafe()).thenReturn(false);
        bridge = new TestBridge(mockContext, mockAccessorFactory, mockResolverFactory);
        assertFalse(bridge.isThreadSafe());
    }

    class TestBridge extends BaseBridge {

        public TestBridge(RequestContext context) {
            super(context);
        }

        public TestBridge(RequestContext context, AccessorFactory accessorFactory, ResolverFactory resolverFactory) {
            super(context, accessorFactory, resolverFactory);
        }

        @Override
        public boolean beginIteration() throws Exception {
            return false;
        }

        @Override
        public Writable getNext() throws Exception {
            return null;
        }

        @Override
        public boolean setNext(DataInputStream inputStream) throws Exception {
            return false;
        }

        @Override
        public void endIteration() throws Exception {
        }

        Accessor getAccessor() {
            return accessor;
        }

        Resolver getResolver() {
            return resolver;
        }

    }

}
