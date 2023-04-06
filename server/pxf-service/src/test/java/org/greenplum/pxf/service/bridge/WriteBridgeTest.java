package org.greenplum.pxf.service.bridge;

import org.apache.hadoop.conf.Configuration;
import org.greenplum.pxf.api.model.Accessor;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.service.serde.RecordReaderFactory;
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
public class WriteBridgeTest {

    private WriteBridge bridge;
    private RequestContext context;
    private Configuration configuration;
    private GSSFailureHandler handler;

    @Mock
    private BasePluginFactory mockPluginFactory;
    @Mock
    private RecordReaderFactory mockRecordReaderFactory;
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
        context.setAccessor("org.greenplum.pxf.service.bridge.TestAccessor");
        context.setResolver("org.greenplum.pxf.service.bridge.TestResolver");
    }

    @Test
    public void testBeginIterationFailureNoRetries() throws Exception {
        when(mockPluginFactory.getPlugin(context, context.getAccessor())).thenReturn(mockAccessor1);
        when(mockAccessor1.openForWrite()).thenThrow(new IOException("Something Else"));

        // constructor will call into mock factories, that's why we do not create WriteBridge in @Before method
        bridge = createWriteBridge();
        Exception e = assertThrows(IOException.class, () -> bridge.beginIteration());
        assertEquals("Something Else", e.getMessage());

        verify(mockPluginFactory).getPlugin(context, context.getAccessor());
        verify(mockPluginFactory).getPlugin(context, context.getResolver());
        verify(mockAccessor1).openForWrite();
        verifyNoMoreInteractions(mockPluginFactory, mockAccessor1);
    }

    @Test
    public void testBeginIterationGSSFailureRetriedOnce() throws Exception {
        when(mockPluginFactory.getPlugin(context, context.getAccessor()))
                .thenReturn(mockAccessor1)
                .thenReturn(mockAccessor2);
        when(mockAccessor1.openForWrite()).thenThrow(new IOException("GSS initiate failed"));
        when(mockAccessor2.openForWrite()).thenReturn(true);

        // constructor will call into mock factories, that's why we do not create WriteBridge in @Before method
        bridge = createWriteBridge();
        boolean result = bridge.beginIteration();
        assertTrue(result);

        verify(mockPluginFactory, times(2)).getPlugin(context, context.getAccessor());
        verify(mockPluginFactory, times(1)).getPlugin(context, context.getResolver());
        InOrder inOrder = inOrder(mockAccessor1, mockAccessor2);
        inOrder.verify(mockAccessor1).openForWrite(); // first  attempt on accessor #1
        inOrder.verify(mockAccessor2).openForWrite(); // second attempt on accessor #2
        inOrder.verifyNoMoreInteractions();
        verifyNoMoreInteractions(mockPluginFactory);
    }

    @Test
    public void testBeginIterationGSSFailureRetriedTwice() throws Exception {
        when(mockPluginFactory.getPlugin(context, context.getAccessor()))
                .thenReturn(mockAccessor1)
                .thenReturn(mockAccessor2)
                .thenReturn(mockAccessor3);
        when(mockAccessor1.openForWrite()).thenThrow(new IOException("GSS initiate failed"));
        when(mockAccessor2.openForWrite()).thenThrow(new IOException("GSS initiate failed"));
        when(mockAccessor3.openForWrite()).thenReturn(true);

        // constructor will call into mock factories, that's why we do not create WriteBridge in @Before method
        bridge = createWriteBridge();
        boolean result = bridge.beginIteration();
        assertTrue(result);

        verify(mockPluginFactory, times(3)).getPlugin(context, context.getAccessor());
        verify(mockPluginFactory, times(1)).getPlugin(context, context.getResolver());
        InOrder inOrder = inOrder(mockAccessor1, mockAccessor2, mockAccessor3);
        inOrder.verify(mockAccessor1).openForWrite(); // first  attempt on accessor #1
        inOrder.verify(mockAccessor2).openForWrite(); // second attempt on accessor #2
        inOrder.verify(mockAccessor3).openForWrite(); // third attempt on accessor #3
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
        when(mockAccessor1.openForWrite()).thenThrow(new IOException("GSS initiate failed"));
        when(mockAccessor2.openForWrite()).thenThrow(new IOException("GSS initiate failed"));
        when(mockAccessor3.openForWrite()).thenThrow(new IOException("GSS initiate failed"));

        // constructor will call into mock factories, that's why we do not create WriteBridge in @Before method
        bridge = createWriteBridge();
        Exception e = assertThrows(IOException.class, () -> bridge.beginIteration());
        assertEquals("GSS initiate failed", e.getMessage());

        verify(mockPluginFactory, times(3)).getPlugin(context, context.getAccessor());
        verify(mockPluginFactory, times(1)).getPlugin(context, context.getResolver());
        InOrder inOrder = inOrder(mockAccessor1, mockAccessor2, mockAccessor3);
        inOrder.verify(mockAccessor1).openForWrite(); // first  attempt on accessor #1
        inOrder.verify(mockAccessor2).openForWrite(); // second attempt on accessor #2
        inOrder.verify(mockAccessor3).openForWrite(); // third attempt on accessor #3
        inOrder.verifyNoMoreInteractions();
        verifyNoMoreInteractions(mockPluginFactory);
    }

    @Test
    public void testGetNextIsNotSupported() {
        when(mockPluginFactory.getPlugin(context, context.getAccessor())).thenReturn(mockAccessor1);
        bridge = createWriteBridge();

        Exception e = assertThrows(UnsupportedOperationException.class, () -> bridge.getNext());
        assertEquals("Current operation is not supported", e.getMessage());
    }

    private WriteBridge createWriteBridge() {
        // resolver will be inspected for annotation, so we need to have a real object here
        when(mockPluginFactory.getPlugin(context, "org.greenplum.pxf.service.bridge.TestResolver"))
                .thenReturn(new TestResolver());
        return new WriteBridge(mockPluginFactory, mockRecordReaderFactory, context, handler);
    }
}
