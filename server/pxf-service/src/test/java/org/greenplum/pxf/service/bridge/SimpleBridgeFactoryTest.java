package org.greenplum.pxf.service.bridge;

import org.greenplum.pxf.api.model.GreenplumCSV;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.EnumAggregationType;
import org.greenplum.pxf.service.serde.RecordReaderFactory;
import org.greenplum.pxf.service.utilities.BasePluginFactory;
import org.greenplum.pxf.service.utilities.GSSFailureHandler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class SimpleBridgeFactoryTest {

    @Mock
    private BasePluginFactory mockPluginFactory;
    @Mock
    private RecordReaderFactory mockRecordReaderFactory;
    @Mock
    private GSSFailureHandler mockFailureHandler;
    @Mock
    private RequestContext mockRequestContext;
    @Mock
    private GreenplumCSV mockGreenplumCSV;

    private SimpleBridgeFactory factory;
    private Bridge bridge;

    @BeforeEach
    public void setup() {
        factory = new SimpleBridgeFactory(mockPluginFactory, mockRecordReaderFactory, mockFailureHandler);
    }

    @Test
    public void testWriteVectorized() {
        when(mockRequestContext.getRequestType()).thenReturn(RequestContext.RequestType.WRITE_BRIDGE);
        when(mockRequestContext.getResolver()).thenReturn("org.greenplum.pxf.service.bridge.TestWriteVectorizedResolver");
        when(mockPluginFactory.getPlugin(mockRequestContext, null)).thenReturn(null); // accessor
        // resolver will be inspected for annotation, so we need to have a real object here
        when(mockPluginFactory.getPlugin(mockRequestContext, "org.greenplum.pxf.service.bridge.TestWriteVectorizedResolver"))
                .thenReturn(new TestWriteVectorizedResolver());
        bridge = factory.getBridge(mockRequestContext);
        assertTrue(bridge instanceof WriteVectorizedBridge);
    }

    @Test
    public void testWrite() {
        when(mockRequestContext.getRequestType()).thenReturn(RequestContext.RequestType.WRITE_BRIDGE);
        when(mockRequestContext.getResolver()).thenReturn("org.greenplum.pxf.service.bridge.TestResolver");
        when(mockPluginFactory.getPlugin(mockRequestContext, null)).thenReturn(null); // accessor
        // resolver will be inspected for annotation, so we need to have a real object here
        when(mockPluginFactory.getPlugin(mockRequestContext, "org.greenplum.pxf.service.bridge.TestResolver"))
                .thenReturn(new TestResolver());
        bridge = factory.getBridge(mockRequestContext);
        assertTrue(bridge instanceof WriteBridge);
        assertFalse(bridge instanceof WriteVectorizedBridge);
    }

    @Test
    public void testReadVectorized() {
        mockForRead();
        when(mockRequestContext.getAccessor()).thenReturn("org.greenplum.pxf.service.bridge.TestAccessor");
        when(mockRequestContext.getResolver()).thenReturn("org.greenplum.pxf.service.bridge.TestReadVectorizedResolver");
        bridge = factory.getBridge(mockRequestContext);
        assertTrue(bridge instanceof ReadVectorizedBridge);
    }

    @Test
    public void testRead() {
        mockForRead();
        when(mockRequestContext.getAccessor()).thenReturn("org.greenplum.pxf.service.bridge.TestAccessor");
        when(mockRequestContext.getResolver()).thenReturn("org.greenplum.pxf.service.bridge.TestResolver");
        bridge = factory.getBridge(mockRequestContext);
        assertTrue(bridge instanceof ReadBridge);
        assertFalse(bridge instanceof ReadVectorizedBridge);
    }

    @Test
    public void testReadSampling() {
        mockForRead();
        when(mockRequestContext.getStatsSampleRatio()).thenReturn(1F);
        bridge = factory.getBridge(mockRequestContext);
        assertTrue(bridge instanceof ReadSamplingBridge);
    }

    @Test
    public void testAgg() {
        mockForRead();
        when(mockRequestContext.getAccessor()).thenReturn("org.greenplum.pxf.service.bridge.TestStatsAccessor");
        when(mockRequestContext.getAggType()).thenReturn(EnumAggregationType.COUNT);
        bridge = factory.getBridge(mockRequestContext);
        assertTrue(bridge instanceof AggBridge);
    }

    @Test
    public void testNoRequestType() {
        assertThrows(UnsupportedOperationException.class, () -> factory.getBridge(mockRequestContext));
    }


    private void mockForRead() {
        when(mockRequestContext.getRequestType()).thenReturn(RequestContext.RequestType.READ_BRIDGE);
        when(mockRequestContext.getGreenplumCSV()).thenReturn(mockGreenplumCSV);
        when(mockGreenplumCSV.getNewline()).thenReturn("\n");
    }

}
