package org.greenplum.pxf.service.controller;

import org.apache.hadoop.conf.Configuration;
import org.greenplum.pxf.api.io.Writable;
import org.greenplum.pxf.api.model.ConfigurationFactory;
import org.greenplum.pxf.api.model.Fragment;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.service.FragmenterService;
import org.greenplum.pxf.service.MetricsReporter;
import org.greenplum.pxf.service.bridge.Bridge;
import org.greenplum.pxf.service.bridge.BridgeFactory;
import org.greenplum.pxf.service.security.SecurityService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.stubbing.Answer;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.security.PrivilegedExceptionAction;
import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
public class ReadServiceImplTest {

    @Mock
    private ConfigurationFactory mockConfigurationFactory;
    @Mock
    private BridgeFactory mockBridgeFactory;
    @Mock
    private SecurityService mockSecurityService;
    @Mock
    private FragmenterService mockFragmenterService;
    @Mock
    private MetricsReporter mockMetricReporter;
    @Mock
    private OutputStream mockOutputStream;
    @Mock
    private Configuration mockConfiguration;
    @Mock
    private List<Fragment> mockFragmentList;
    @Mock
    private Fragment mockFragment1, mockFragment2;
    @Mock
    private Bridge mockBridge1, mockBridge2;
    @Mock
    private Writable mockRecord1, mockRecord2, mockRecord3;
    @Mock
    private RequestContext mockContext;

    private ReadServiceImpl readService;

    @BeforeEach
    public void setup() throws IOException {
        when(mockConfigurationFactory.initConfiguration(any(), any(), any(), any())).thenReturn(mockConfiguration);
        when(mockFragmenterService.getFragmentsForSegment(mockContext)).thenReturn(mockFragmentList);
        when(mockSecurityService.doAs(same(mockContext), any())).thenAnswer(invocation -> {
            PrivilegedExceptionAction<OperationStats> action = invocation.getArgument(1);
            OperationStats result = action.run();
            return result;
        });

        readService = new ReadServiceImpl(mockConfigurationFactory, mockBridgeFactory, mockSecurityService, mockFragmenterService, mockMetricReporter);
    }

    @Test
    public void testReadDataOneFragOneRecord() throws Exception {
        when(mockMetricReporter.getReportFrequency()).thenReturn(1L);
        when(mockFragmentList.size()).thenReturn(1);
        when(mockFragmentList.get(0)).thenReturn(mockFragment1);
        when(mockBridgeFactory.getBridge(mockContext)).thenReturn(mockBridge1);
        when(mockBridge1.beginIteration()).thenReturn(true);
        when(mockBridge1.getNext()).thenReturn(mockRecord1).thenReturn(null);
        doAnswer(writeTestData("hello")).when(mockRecord1).write(any(DataOutputStream.class));

        readService.readData(mockContext, mockOutputStream);

        InOrder inOrder = inOrder(mockOutputStream, mockMetricReporter);
        inOrder.verify(mockOutputStream).write("hello".getBytes(StandardCharsets.UTF_8), 0, 5);
        inOrder.verify(mockMetricReporter).reportCounter(MetricsReporter.PxfMetric.RECORDS_SENT, 1, mockContext);
        inOrder.verify(mockMetricReporter).reportCounter(MetricsReporter.PxfMetric.BYTES_SENT, 5, mockContext);
        inOrder.verify(mockMetricReporter).reportTimer(same(MetricsReporter.PxfMetric.FRAGMENTS_SENT), any(Duration.class), same(mockContext), eq(true));
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    public void testReadDataOneFragMultiRecordsReportBatch() throws Exception {
        when(mockMetricReporter.getReportFrequency()).thenReturn(2L);
        when(mockFragmentList.size()).thenReturn(1);
        when(mockFragmentList.get(0)).thenReturn(mockFragment1);
        when(mockBridgeFactory.getBridge(mockContext)).thenReturn(mockBridge1);
        when(mockBridge1.beginIteration()).thenReturn(true);
        when(mockBridge1.getNext()).thenReturn(mockRecord1, mockRecord2, null);
        doAnswer(writeTestData("hello")).when(mockRecord1).write(any(DataOutputStream.class));
        doAnswer(writeTestData("world!")).when(mockRecord2).write(any(DataOutputStream.class));

        readService.readData(mockContext, mockOutputStream);

        InOrder inOrder = inOrder(mockOutputStream, mockMetricReporter);
        inOrder.verify(mockOutputStream).write("hello".getBytes(StandardCharsets.UTF_8), 0, 5);
        inOrder.verify(mockOutputStream).write("world!".getBytes(StandardCharsets.UTF_8), 0, 6);
        inOrder.verify(mockMetricReporter).reportCounter(MetricsReporter.PxfMetric.RECORDS_SENT, 2, mockContext);
        inOrder.verify(mockMetricReporter).reportCounter(MetricsReporter.PxfMetric.BYTES_SENT, 11, mockContext);
        inOrder.verify(mockMetricReporter).reportTimer(same(MetricsReporter.PxfMetric.FRAGMENTS_SENT), any(Duration.class), same(mockContext), eq(true));
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    public void testReadDataOneFragMultiRecordsRemainder() throws Exception {
        when(mockMetricReporter.getReportFrequency()).thenReturn(5L);
        when(mockFragmentList.size()).thenReturn(1);
        when(mockFragmentList.get(0)).thenReturn(mockFragment1);
        when(mockBridgeFactory.getBridge(mockContext)).thenReturn(mockBridge1);
        when(mockBridge1.beginIteration()).thenReturn(true);
        when(mockBridge1.getNext()).thenReturn(mockRecord1, mockRecord2, null);
        doAnswer(writeTestData("hello")).when(mockRecord1).write(any(DataOutputStream.class));
        doAnswer(writeTestData("world!")).when(mockRecord2).write(any(DataOutputStream.class));

        readService.readData(mockContext, mockOutputStream);

        InOrder inOrder = inOrder(mockOutputStream, mockMetricReporter);
        inOrder.verify(mockOutputStream).write("hello".getBytes(StandardCharsets.UTF_8), 0, 5);
        inOrder.verify(mockOutputStream).write("world!".getBytes(StandardCharsets.UTF_8), 0, 6);
        inOrder.verify(mockMetricReporter).reportCounter(MetricsReporter.PxfMetric.RECORDS_SENT, 2, mockContext);
        inOrder.verify(mockMetricReporter).reportCounter(MetricsReporter.PxfMetric.BYTES_SENT, 11, mockContext);
        inOrder.verify(mockMetricReporter).reportTimer(same(MetricsReporter.PxfMetric.FRAGMENTS_SENT), any(Duration.class), same(mockContext), eq(true));
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    public void testReadDataOneFragMultiRecordsRemainderAfterBatch() throws Exception {
        when(mockMetricReporter.getReportFrequency()).thenReturn(2L);
        when(mockFragmentList.size()).thenReturn(1);
        when(mockFragmentList.get(0)).thenReturn(mockFragment1);
        when(mockBridgeFactory.getBridge(mockContext)).thenReturn(mockBridge1);
        when(mockBridge1.beginIteration()).thenReturn(true);
        when(mockBridge1.getNext()).thenReturn(mockRecord1, mockRecord2, mockRecord3, null);
        doAnswer(writeTestData("hello")).when(mockRecord1).write(any(DataOutputStream.class));
        doAnswer(writeTestData("world!")).when(mockRecord2).write(any(DataOutputStream.class));
        doAnswer(writeTestData("Boo!")).when(mockRecord3).write(any(DataOutputStream.class));

        readService.readData(mockContext, mockOutputStream);

        InOrder inOrder = inOrder(mockOutputStream, mockMetricReporter);
        inOrder.verify(mockOutputStream).write("hello".getBytes(StandardCharsets.UTF_8), 0, 5);
        inOrder.verify(mockOutputStream).write("world!".getBytes(StandardCharsets.UTF_8), 0, 6);
        inOrder.verify(mockMetricReporter).reportCounter(MetricsReporter.PxfMetric.RECORDS_SENT, 2, mockContext);
        inOrder.verify(mockMetricReporter).reportCounter(MetricsReporter.PxfMetric.BYTES_SENT, 11, mockContext);
        inOrder.verify(mockOutputStream).write("Boo!".getBytes(StandardCharsets.UTF_8), 0, 4);
        inOrder.verify(mockMetricReporter).reportCounter(MetricsReporter.PxfMetric.RECORDS_SENT, 1, mockContext);
        inOrder.verify(mockMetricReporter).reportCounter(MetricsReporter.PxfMetric.BYTES_SENT, 4, mockContext);
        inOrder.verify(mockMetricReporter).reportTimer(same(MetricsReporter.PxfMetric.FRAGMENTS_SENT), any(Duration.class), same(mockContext), eq(true));
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    public void testReadDataOneFragRecordsException() throws Exception {
        when(mockMetricReporter.getReportFrequency()).thenReturn(5L);
        when(mockFragmentList.size()).thenReturn(1);
        when(mockFragmentList.get(0)).thenReturn(mockFragment1);
        when(mockBridgeFactory.getBridge(mockContext)).thenReturn(mockBridge1);
        when(mockBridge1.beginIteration()).thenReturn(true);
        when(mockBridge1.getNext()).thenReturn(mockRecord1).thenThrow(new Exception());
        doAnswer(writeTestData("hello")).when(mockRecord1).write(any(DataOutputStream.class));

        assertThrows(Exception.class, () -> readService.readData(mockContext, mockOutputStream));
        InOrder inOrder = inOrder(mockOutputStream, mockMetricReporter);
        inOrder.verify(mockOutputStream).write("hello".getBytes(StandardCharsets.UTF_8), 0, 5);
        inOrder.verify(mockMetricReporter).reportCounter(MetricsReporter.PxfMetric.RECORDS_SENT, 1, mockContext);
        inOrder.verify(mockMetricReporter).reportCounter(MetricsReporter.PxfMetric.BYTES_SENT, 5, mockContext);
        inOrder.verify(mockMetricReporter).reportTimer(same(MetricsReporter.PxfMetric.FRAGMENTS_SENT), any(Duration.class), same(mockContext), eq(false));
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    public void testReadDataMultiFragmentMultiRecord() throws Exception {
        when(mockMetricReporter.getReportFrequency()).thenReturn(2L);
        when(mockFragmentList.size()).thenReturn(2);
        when(mockBridgeFactory.getBridge(mockContext)).thenReturn(mockBridge1, mockBridge2);

        // 1st frag
        when(mockFragmentList.get(0)).thenReturn(mockFragment1);
        when(mockBridge1.beginIteration()).thenReturn(true);
        when(mockBridge1.getNext()).thenReturn(mockRecord1).thenReturn(null);
        doAnswer(writeTestData("hello")).when(mockRecord1).write(any(DataOutputStream.class));

        // 2nd frag
        when(mockFragmentList.get(1)).thenReturn(mockFragment2);
        when(mockBridge2.beginIteration()).thenReturn(true);
        when(mockBridge2.getNext()).thenReturn(mockRecord2, mockRecord3, null);
        doAnswer(writeTestData("world!")).when(mockRecord2).write(any(DataOutputStream.class));
        doAnswer(writeTestData("Boo!")).when(mockRecord3).write(any(DataOutputStream.class));

        readService.readData(mockContext, mockOutputStream);

        InOrder inOrder = inOrder(mockOutputStream, mockMetricReporter);
        inOrder.verify(mockOutputStream).write("hello".getBytes(StandardCharsets.UTF_8), 0, 5);
        inOrder.verify(mockMetricReporter).reportCounter(MetricsReporter.PxfMetric.RECORDS_SENT, 1, mockContext);
        inOrder.verify(mockMetricReporter).reportCounter(MetricsReporter.PxfMetric.BYTES_SENT, 5, mockContext);
        inOrder.verify(mockMetricReporter).reportTimer(same(MetricsReporter.PxfMetric.FRAGMENTS_SENT), any(Duration.class), same(mockContext), eq(true));
        inOrder.verify(mockOutputStream).write("world!".getBytes(StandardCharsets.UTF_8), 0, 6);
        inOrder.verify(mockOutputStream).write("Boo!".getBytes(StandardCharsets.UTF_8), 0, 4);
        inOrder.verify(mockMetricReporter).reportCounter(MetricsReporter.PxfMetric.RECORDS_SENT, 2, mockContext);
        inOrder.verify(mockMetricReporter).reportCounter(MetricsReporter.PxfMetric.BYTES_SENT, 10, mockContext);
        inOrder.verify(mockMetricReporter).reportTimer(same(MetricsReporter.PxfMetric.FRAGMENTS_SENT), any(Duration.class), same(mockContext), eq(true));
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    public void testReadDataZeroReportFrequency() throws Exception {
        when(mockMetricReporter.getReportFrequency()).thenReturn(0L);
        when(mockFragmentList.size()).thenReturn(1);
        when(mockFragmentList.get(0)).thenReturn(mockFragment1);
        when(mockBridgeFactory.getBridge(mockContext)).thenReturn(mockBridge1);
        when(mockBridge1.beginIteration()).thenReturn(true);
        when(mockBridge1.getNext()).thenReturn(mockRecord1).thenReturn(null);
        doAnswer(writeTestData("hello")).when(mockRecord1).write(any(DataOutputStream.class));

        readService.readData(mockContext, mockOutputStream);

        InOrder inOrder = inOrder(mockOutputStream, mockMetricReporter);
        inOrder.verify(mockOutputStream).write("hello".getBytes(StandardCharsets.UTF_8), 0, 5);
        inOrder.verify(mockMetricReporter).reportTimer(same(MetricsReporter.PxfMetric.FRAGMENTS_SENT), any(Duration.class), same(mockContext), eq(true));
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    public void testReadDataBeginIterationFalse() throws Exception {
        when(mockMetricReporter.getReportFrequency()).thenReturn(1L);
        when(mockFragmentList.size()).thenReturn(1);
        when(mockFragmentList.get(0)).thenReturn(mockFragment1);
        when(mockBridgeFactory.getBridge(mockContext)).thenReturn(mockBridge1);
        when(mockBridge1.beginIteration()).thenReturn(false);

        readService.readData(mockContext, mockOutputStream);

        InOrder inOrder = inOrder(mockBridge1, mockMetricReporter);
        inOrder.verify(mockBridge1).endIteration();
        inOrder.verify(mockMetricReporter).reportTimer(same(MetricsReporter.PxfMetric.FRAGMENTS_SENT), any(Duration.class), same(mockContext), eq(true));
        inOrder.verifyNoMoreInteractions();
    }

    // helper for writing mock record to a mock output stream
    // mockOutputStream -> CountingOutputStream -> DataOutputStream
    // in order for the us to see the side-effect of CountingOutputStream,
    // we need to actually call the `write` method of DataOutputStream.
    private Answer writeTestData(String testData) {
        return invocation -> {
            DataOutputStream dos = invocation.getArgument(0);
            dos.write(testData.getBytes(StandardCharsets.UTF_8));
            return null;
        };
    }
}
