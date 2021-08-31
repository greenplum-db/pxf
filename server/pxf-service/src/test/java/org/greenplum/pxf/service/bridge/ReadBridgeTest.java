package org.greenplum.pxf.service.bridge;

import org.apache.hadoop.conf.Configuration;
import org.greenplum.pxf.api.model.Accessor;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.AccessorFactory;
import org.greenplum.pxf.api.utilities.ResolverFactory;
import org.greenplum.pxf.service.utilities.GSSFailureHandler;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ReadBridgeTest {

    private ReadBridge bridge;
    private RequestContext context;
    private Configuration configuration;
    private GSSFailureHandler handler;
    @Mock
    private AccessorFactory mockAccessorFactory;
    @Mock
    private ResolverFactory mockResolverFactory;
    @Mock
    private Accessor mockAccessor1;
    @Mock
    private Accessor mockAccessor2;
    @Mock
    private Accessor mockAccessor3;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setup() {
        handler = GSSFailureHandler.getInstance();
        context = new RequestContext();
        configuration = new Configuration();
    }

    @Test
    public void testBeginIterationFailureNoRetries() throws Exception {
        expectedException.expect(IOException.class);
        expectedException.expectMessage("Something Else");

        configuration.set("hadoop.security.authentication", "kerberos");
        when(mockAccessorFactory.getPlugin(context)).thenReturn(mockAccessor1);
        when(mockAccessor1.getConfiguration()).thenReturn(configuration);
        when(mockAccessor1.openForRead()).thenThrow(new IOException("Something Else"));

        // constructor will call into mock factories, that's why we do not create ReadBridge in @Before method
        bridge = new ReadBridge(context, mockAccessorFactory, mockResolverFactory, handler);
        bridge.beginIteration();
        verify(mockAccessorFactory).getPlugin(context);
        verify(mockAccessor1.getConfiguration());
        verify(mockAccessor1).openForRead();
        verifyNoMoreInteractions();
    }

    @Test
    public void testBeginIterationGSSFailureRetriedOnce() throws Exception {
        configuration.set("hadoop.security.authentication", "kerberos");
        when(mockAccessorFactory.getPlugin(context)).thenReturn(mockAccessor1).thenReturn(mockAccessor2);
        when(mockAccessor1.getConfiguration()).thenReturn(configuration);
        when(mockAccessor1.openForRead()).thenThrow(new IOException("GSS initiate failed"));
        when(mockAccessor2.openForRead()).thenReturn(true);

        bridge = new ReadBridge(context, mockAccessorFactory, mockResolverFactory, handler);
        boolean result = bridge.beginIteration();

        assertTrue(result);
        verify(mockAccessorFactory, times(2)).getPlugin(context);
        InOrder inOrder = inOrder(mockAccessor1, mockAccessor2);
        inOrder.verify(mockAccessor1).openForRead(); // first  attempt on accessor #1
        inOrder.verify(mockAccessor2).openForRead(); // second attempt on accessor #2
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    public void testBeginIterationGSSFailureRetriedTwice() throws Exception {
        configuration.set("hadoop.security.authentication", "kerberos");
        when(mockAccessorFactory.getPlugin(context))
                .thenReturn(mockAccessor1)
                .thenReturn(mockAccessor2)
                .thenReturn(mockAccessor3);
        when(mockAccessor1.getConfiguration()).thenReturn(configuration);
        when(mockAccessor1.openForRead()).thenThrow(new IOException("GSS initiate failed"));
        when(mockAccessor2.openForRead()).thenThrow(new IOException("GSS initiate failed"));
        when(mockAccessor3.openForRead()).thenReturn(true);

        bridge = new ReadBridge(context, mockAccessorFactory, mockResolverFactory, handler);
        boolean result = bridge.beginIteration();

        assertTrue(result);
        verify(mockAccessorFactory, times(3)).getPlugin(context);
        InOrder inOrder = inOrder(mockAccessor1, mockAccessor2, mockAccessor3);
        inOrder.verify(mockAccessor1).openForRead(); // first  attempt on accessor #1
        inOrder.verify(mockAccessor2).openForRead(); // second attempt on accessor #2
        inOrder.verify(mockAccessor3).openForRead(); // third  attempt on accessor #3
        inOrder.verifyNoMoreInteractions();
    }

}
