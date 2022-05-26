package org.greenplum.pxf.service.bridge;

import org.apache.hadoop.conf.Configuration;
import org.greenplum.pxf.api.model.Accessor;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.service.utilities.BasePluginFactory;
import org.greenplum.pxf.service.utilities.GSSFailureHandler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class ReadBridgeTest {

    private ReadBridge bridge;
    private RequestContext context;
    private Configuration configuration;
    private GSSFailureHandler handler;

    @Mock
    private BasePluginFactory mockPluginFactory;
    @Mock
    private Accessor mockAccessor1;
    @Mock
    private Accessor mockAccessor2;
    @Mock
    private Accessor mockAccessor3;

    @BeforeEach
    public void setup() {
        handler = new GSSFailureHandler();
        context = new RequestContext();
        configuration = new Configuration();
        configuration.set("hadoop.security.authentication", "kerberos");
        context.setConfiguration(configuration);
        context.setAccessor("org.greenplum.pxf.api.model.Accessor");
        context.setAccessor("org.greenplum.pxf.api.model.Resolver");
    }

    @Test
    public void testBeginIterationFailureNoRetries() throws Exception {
        when(mockPluginFactory.getPlugin(context, context.getAccessor())).thenReturn(mockAccessor1);
        when(mockAccessor1.openForRead()).thenThrow(new IOException("Something Else"));

        // constructor will call into mock factories, that's why we do not create ReadBridge in @Before method
        bridge = new ReadBridge(mockPluginFactory, context, handler);
        Exception e = assertThrows(IOException.class, () -> bridge.beginIteration());
        assertEquals("Something Else", e.getMessage());

        verify(mockPluginFactory).getPlugin(context, context.getAccessor());
        verify(mockPluginFactory).getPlugin(context, context.getResolver());
        verify(mockAccessor1).openForRead();
        verifyNoMoreInteractions(mockPluginFactory, mockAccessor1);
    }

    @Test
    public void testBeginIterationGSSFailureRetriedOnce() throws Exception {
        when(mockPluginFactory.getPlugin(context, context.getAccessor()))
                .thenReturn(mockAccessor1)
                .thenReturn(mockAccessor2);
        when(mockAccessor1.openForRead()).thenThrow(new IOException("GSS initiate failed"));
        when(mockAccessor2.openForRead()).thenReturn(true);

        // constructor will call into mock factories, that's why we do not create ReadBridge in @Before method
        bridge = new ReadBridge(mockPluginFactory, context, handler);
        boolean result = bridge.beginIteration();
        assertTrue(result);

        verify(mockPluginFactory, times(2)).getPlugin(context, context.getAccessor());
        verify(mockPluginFactory, times(1)).getPlugin(context, context.getResolver());
        InOrder inOrder = inOrder(mockAccessor1, mockAccessor2);
        inOrder.verify(mockAccessor1).openForRead(); // first  attempt on accessor #1
        inOrder.verify(mockAccessor2).openForRead(); // second attempt on accessor #2
        inOrder.verifyNoMoreInteractions();
        verifyNoMoreInteractions(mockPluginFactory);
    }

    @Test
    public void testBeginIterationGSSFailureRetriedTwice() throws Exception {
        when(mockPluginFactory.getPlugin(context, context.getAccessor()))
                .thenReturn(mockAccessor1)
                .thenReturn(mockAccessor2)
                .thenReturn(mockAccessor3);
        when(mockAccessor1.openForRead()).thenThrow(new IOException("GSS initiate failed"));
        when(mockAccessor2.openForRead()).thenThrow(new IOException("GSS initiate failed"));
        when(mockAccessor3.openForRead()).thenReturn(true);

        // constructor will call into mock factories, that's why we do not create ReadBridge in @Before method
        bridge = new ReadBridge(mockPluginFactory, context, handler);
        boolean result = bridge.beginIteration();
        assertTrue(result);

        verify(mockPluginFactory, times(3)).getPlugin(context, context.getAccessor());
        verify(mockPluginFactory, times(1)).getPlugin(context, context.getResolver());
        InOrder inOrder = inOrder(mockAccessor1, mockAccessor2, mockAccessor3);
        inOrder.verify(mockAccessor1).openForRead(); // first  attempt on accessor #1
        inOrder.verify(mockAccessor2).openForRead(); // second attempt on accessor #2
        inOrder.verify(mockAccessor3).openForRead(); // third attempt on accessor #3
        inOrder.verifyNoMoreInteractions();
        verifyNoMoreInteractions(mockPluginFactory);
    }

    @Test
    public void testBeginIterationGSSFailureAfterMaxRetries() throws Exception {
        configuration.set("pxf.sasl.connection.retries", "2");
        when(mockPluginFactory.getPlugin(context, context.getAccessor()))
                .thenReturn(mockAccessor1)
                .thenReturn(mockAccessor2)
                .thenReturn(mockAccessor3);
        when(mockAccessor1.openForRead()).thenThrow(new IOException("GSS initiate failed"));
        when(mockAccessor2.openForRead()).thenThrow(new IOException("GSS initiate failed"));
        when(mockAccessor3.openForRead()).thenThrow(new IOException("GSS initiate failed"));

        // constructor will call into mock factories, that's why we do not create ReadBridge in @Before method
        bridge = new ReadBridge(mockPluginFactory, context, handler);
        Exception e = assertThrows(IOException.class, () -> bridge.beginIteration());
        assertEquals("GSS initiate failed", e.getMessage());

        verify(mockPluginFactory, times(3)).getPlugin(context, context.getAccessor());
        verify(mockPluginFactory, times(1)).getPlugin(context, context.getResolver());
        InOrder inOrder = inOrder(mockAccessor1, mockAccessor2, mockAccessor3);
        inOrder.verify(mockAccessor1).openForRead(); // first  attempt on accessor #1
        inOrder.verify(mockAccessor2).openForRead(); // second attempt on accessor #2
        inOrder.verify(mockAccessor3).openForRead(); // third attempt on accessor #3
        inOrder.verifyNoMoreInteractions();
        verifyNoMoreInteractions(mockPluginFactory);
    }

    @Test
    public void testSetNextIsNotSupported() {
        bridge = new ReadBridge(mockPluginFactory, context, handler);

        Exception e = assertThrows(UnsupportedOperationException.class, () -> bridge.setNext(null));
        assertEquals("Write operation is not supported.", e.getMessage());
    }
}
