package org.greenplum.pxf.service.bridge;

import org.apache.hadoop.conf.Configuration;
import org.greenplum.pxf.api.io.Writable;
import org.greenplum.pxf.api.model.Accessor;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.model.Resolver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.context.ApplicationContext;

import java.io.DataInputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BaseBridgeTest {

    private Accessor mockAccessor;
    private ApplicationContext mockApplicationContext;
    private RequestContext context;
    private Resolver mockResolver;
    private TestBridge bridge;

    @BeforeEach
    public void setup() {
        context = new RequestContext();
        context.setConfiguration(new Configuration());

        mockAccessor = mock(Accessor.class);
        mockResolver = mock(Resolver.class);
        mockApplicationContext = mock(ApplicationContext.class);
    }

    @Test
    public void testContextConstructor() {
        context.setAccessor("org.greenplum.pxf.service.bridge.TestAccessor");
        context.setResolver("org.greenplum.pxf.service.bridge.TestResolver");
        when(mockApplicationContext.getBean("TestAccessor", Accessor.class)).thenReturn(new TestAccessor());
        when(mockApplicationContext.getBean("TestResolver", Resolver.class)).thenReturn(new TestResolver());

        bridge = new TestBridge(mockApplicationContext, context);
        assertTrue(bridge.getAccessor() instanceof TestAccessor);
        assertTrue(bridge.getResolver() instanceof TestResolver);
    }

    @Test
    public void testContextConstructorUnknownAccessor() {
        context.setAccessor("org.greenplum.pxf.unknown-accessor");
        context.setResolver("org.greenplum.pxf.service.bridge.TestResolver");
        when(mockApplicationContext.getBean("unknown-accessor", Accessor.class)).thenThrow(new NoSuchBeanDefinitionException("unknown-accessor"));

        NoSuchBeanDefinitionException e = assertThrows(NoSuchBeanDefinitionException.class,
                () -> new TestBridge(mockApplicationContext, context));
        assertEquals("No bean named 'unknown-accessor' available", e.getMessage());
    }

    @Test
    public void testContextConstructorUnknownResolver() {
        context.setAccessor("org.greenplum.pxf.service.bridge.TestAccessor");
        context.setResolver("org.greenplum.pxf.unknown-resolver");
        when(mockApplicationContext.getBean("TestAccessor", Accessor.class)).thenReturn(new TestAccessor());
        when(mockApplicationContext.getBean("unknown-resolver", Resolver.class)).thenThrow(new NoSuchBeanDefinitionException("unknown-resolver"));

        Exception e = assertThrows(RuntimeException.class,
                () -> new TestBridge(mockApplicationContext, context));
        assertEquals("No bean named 'unknown-resolver' available", e.getMessage());
    }

    @Test
    public void testIsThreadSafeTT() {
        context.setAccessor("org.greenplum.pxf.service.bridge.MockAccessor");
        context.setResolver("org.greenplum.pxf.service.bridge.MockResolver");
        when(mockApplicationContext.getBean("MockAccessor", Accessor.class)).thenReturn(mockAccessor);
        when(mockApplicationContext.getBean("MockResolver", Resolver.class)).thenReturn(mockResolver);
        when(mockAccessor.isThreadSafe()).thenReturn(true);
        when(mockResolver.isThreadSafe()).thenReturn(true);
        bridge = new TestBridge(mockApplicationContext, context);
        assertTrue(bridge.isThreadSafe());
    }

    @Test
    public void testIsThreadSafeTF() {
        context.setAccessor("org.greenplum.pxf.service.bridge.MockAccessor");
        context.setResolver("org.greenplum.pxf.service.bridge.MockResolver");
        when(mockApplicationContext.getBean("MockAccessor", Accessor.class)).thenReturn(mockAccessor);
        when(mockApplicationContext.getBean("MockResolver", Resolver.class)).thenReturn(mockResolver);
        when(mockAccessor.isThreadSafe()).thenReturn(true);
        when(mockResolver.isThreadSafe()).thenReturn(false);
        bridge = new TestBridge(mockApplicationContext, context);
        assertFalse(bridge.isThreadSafe());
    }

    @Test
    public void testIsThreadSafeFT() {
        context.setAccessor("org.greenplum.pxf.service.bridge.MockAccessor");
        context.setResolver("org.greenplum.pxf.service.bridge.MockResolver");
        when(mockApplicationContext.getBean("MockAccessor", Accessor.class)).thenReturn(mockAccessor);
        when(mockApplicationContext.getBean("MockResolver", Resolver.class)).thenReturn(mockResolver);
        when(mockAccessor.isThreadSafe()).thenReturn(false);
        when(mockResolver.isThreadSafe()).thenReturn(true);
        bridge = new TestBridge(mockApplicationContext, context);
        assertFalse(bridge.isThreadSafe());
    }

    @Test
    public void testIsThreadSafeFF() {
        context.setAccessor("org.greenplum.pxf.service.bridge.MockAccessor");
        context.setResolver("org.greenplum.pxf.service.bridge.MockResolver");
        when(mockApplicationContext.getBean("MockAccessor", Accessor.class)).thenReturn(mockAccessor);
        when(mockApplicationContext.getBean("MockResolver", Resolver.class)).thenReturn(mockResolver);
        when(mockAccessor.isThreadSafe()).thenReturn(false);
        when(mockResolver.isThreadSafe()).thenReturn(false);
        bridge = new TestBridge(mockApplicationContext, context);
        assertFalse(bridge.isThreadSafe());
    }

    static class TestBridge extends BaseBridge {

        public TestBridge(ApplicationContext applicationContext, RequestContext context) {
            super(applicationContext, context);
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
