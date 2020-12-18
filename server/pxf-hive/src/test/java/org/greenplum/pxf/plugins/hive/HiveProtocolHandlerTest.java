package org.greenplum.pxf.plugins.hive;

import org.greenplum.pxf.api.model.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class HiveProtocolHandlerTest {

    @Mock
    RequestContext context;

    private HiveProtocolHandler handler;

    @BeforeEach
    public void setup() {
        handler = new HiveProtocolHandler();
        when(context.getFragmenter()).thenReturn("fragmenter-from-context");
        Mockito.lenient().when(context.getAccessor()).thenReturn("accessor-from-context");
        Mockito.lenient().when(context.getResolver()).thenReturn("resolver-from-context");
    }

    @Test
    public void testVectorizedChosenForOrc() {
        when(context.getProfile()).thenReturn("hive:orc");
        when(context.getOption("VECTORIZE", false)).thenReturn(true);
        assertEquals("fragmenter-from-context", handler.getFragmenterClassName(context));
        assertEquals("org.greenplum.pxf.plugins.hive.HiveORCVectorizedAccessor", handler.getAccessorClassName(context));
        assertEquals("org.greenplum.pxf.plugins.hive.HiveORCVectorizedResolver", handler.getResolverClassName(context));
    }

    @Test
    public void testVectorizedNotChosenForOrc_VectorizedOptionMissing() {
        when(context.getProfile()).thenReturn("hive:orc");
        assertEquals("fragmenter-from-context", handler.getFragmenterClassName(context));
        assertEquals("accessor-from-context", handler.getAccessorClassName(context));
        assertEquals("resolver-from-context", handler.getResolverClassName(context));
    }

    @Test
    public void testVectorizedNotChosenForOrc_VectorizedOptionSetFalse() {
        when(context.getProfile()).thenReturn("hive:orc");
        when(context.getOption("VECTORIZE", false)).thenReturn(false);
        assertEquals("fragmenter-from-context", handler.getFragmenterClassName(context));
        assertEquals("accessor-from-context", handler.getAccessorClassName(context));
        assertEquals("resolver-from-context", handler.getResolverClassName(context));
    }

    @Test
    public void testVectorizedNotChosenForOther() {
        when(context.getProfile()).thenReturn("hive:other");
        Mockito.lenient().when(context.getOption("VECTORIZE", false)).thenReturn(true);
        assertEquals("fragmenter-from-context", handler.getFragmenterClassName(context));
        assertEquals("accessor-from-context", handler.getAccessorClassName(context));
        assertEquals("resolver-from-context", handler.getResolverClassName(context));
    }
}
