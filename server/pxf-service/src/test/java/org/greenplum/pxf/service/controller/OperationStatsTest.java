package org.greenplum.pxf.service.controller;

import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.service.MetricsReporter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class OperationStatsTest {
    @Mock
    private MetricsReporter mockMetricReporter;
    @Mock
    private RequestContext mockContext;

    @Test
    public void testDefaultValues() {
        when(mockMetricReporter.getReportFrequency()).thenReturn(5L);
        OperationStats defaultStats = new OperationStats(OperationStats.Operation.READ, mockMetricReporter, mockContext);

        assertEquals(OperationStats.Operation.READ, defaultStats.getOperation());
        assertEquals(0L, defaultStats.getRecordCount());
        assertEquals(0L, defaultStats.getByteCount());
    }

    @Test
    public void testReportCurrentStatsZeroReportFrequency() {
        when(mockMetricReporter.getReportFrequency()).thenReturn(0L);
        OperationStats stats = new OperationStats(OperationStats.Operation.READ, mockMetricReporter, mockContext);
        stats.reportCompletedRecord(15L);

        assertEquals(1L, stats.getRecordCount());
        assertEquals(15L, stats.getByteCount());
        verifyNoMoreInteractions(mockMetricReporter);
    }

    @Test
    public void testReportCurrentStatsNoReport() {
        when(mockMetricReporter.getReportFrequency()).thenReturn(5L);
        OperationStats stats = new OperationStats(OperationStats.Operation.READ, mockMetricReporter, mockContext);
        stats.reportCompletedRecord(15L);

        assertEquals(1L, stats.getRecordCount());
        assertEquals(15L, stats.getByteCount());
        verifyNoMoreInteractions(mockMetricReporter);
    }

    @Test
    public void testReportCurrentStatsReport() {
        when(mockMetricReporter.getReportFrequency()).thenReturn(1L);
        OperationStats stats = new OperationStats(OperationStats.Operation.READ, mockMetricReporter, mockContext);
        stats.reportCompletedRecord(15L);

        assertEquals(1L, stats.getRecordCount());
        assertEquals(15L, stats.getByteCount());
        verify(mockMetricReporter).reportCounter(MetricsReporter.PxfMetric.RECORDS_SENT, 1, mockContext);
        verify(mockMetricReporter).reportCounter(MetricsReporter.PxfMetric.BYTES_SENT, 15, mockContext);
        verifyNoMoreInteractions(mockMetricReporter);
    }

    @Test
    public void testReportCurrentStatsMultiRecordReport() {
        when(mockMetricReporter.getReportFrequency()).thenReturn(2L);
        OperationStats stats = new OperationStats(OperationStats.Operation.READ, mockMetricReporter, mockContext);
        stats.reportCompletedRecord(15L);
        stats.reportCompletedRecord(25L);

        assertEquals(2L, stats.getRecordCount());
        assertEquals(25L, stats.getByteCount());
        verify(mockMetricReporter).reportCounter(MetricsReporter.PxfMetric.RECORDS_SENT, 2, mockContext);
        verify(mockMetricReporter).reportCounter(MetricsReporter.PxfMetric.BYTES_SENT, 25, mockContext);
        verifyNoMoreInteractions(mockMetricReporter);
    }

    @Test
    public void testFlushStatsZeroReportFrequency() {
        when(mockMetricReporter.getReportFrequency()).thenReturn(0L);
        OperationStats stats = new OperationStats(OperationStats.Operation.READ, mockMetricReporter, mockContext);
        stats.setByteCount(15L);
        stats.flushStats();

        assertEquals(0L, stats.getRecordCount());
        assertEquals(15L, stats.getByteCount());
        verifyNoMoreInteractions(mockMetricReporter);
    }

    @Test
    public void testFlushStatsReport() {
        when(mockMetricReporter.getReportFrequency()).thenReturn(1L);
        OperationStats stats = new OperationStats(OperationStats.Operation.READ, mockMetricReporter, mockContext);
        stats.setByteCount(15l);
        stats.flushStats();

        assertEquals(0L, stats.getRecordCount());
        assertEquals(15L, stats.getByteCount());
        verify(mockMetricReporter).reportCounter(MetricsReporter.PxfMetric.BYTES_SENT, 15, mockContext);
        verifyNoMoreInteractions(mockMetricReporter);
    }

    @Test
    public void testFlushStatsReportAfterBatch() {
        when(mockMetricReporter.getReportFrequency()).thenReturn(2L);
        OperationStats stats = new OperationStats(OperationStats.Operation.READ, mockMetricReporter, mockContext);
        stats.reportCompletedRecord(5L);
        stats.reportCompletedRecord(15L);
        stats.setByteCount(35L);
        stats.flushStats();

        assertEquals(2L, stats.getRecordCount());
        assertEquals(35L, stats.getByteCount());
        // report from reportCompletedRecord
        verify(mockMetricReporter).reportCounter(MetricsReporter.PxfMetric.RECORDS_SENT, 2, mockContext);
        verify(mockMetricReporter).reportCounter(MetricsReporter.PxfMetric.BYTES_SENT, 15, mockContext);
        // report from flushStats
        verify(mockMetricReporter).reportCounter(MetricsReporter.PxfMetric.BYTES_SENT, 20, mockContext);
        verifyNoMoreInteractions(mockMetricReporter);
    }

    @Test
    public void testFlushStatsReportAfterBatchWithRecord() {
        when(mockMetricReporter.getReportFrequency()).thenReturn(2L);
        OperationStats stats = new OperationStats(OperationStats.Operation.READ, mockMetricReporter, mockContext);
        stats.reportCompletedRecord(5L);
        stats.reportCompletedRecord(15L);
        stats.reportCompletedRecord(35L);
        stats.flushStats();

        assertEquals(3L, stats.getRecordCount());
        assertEquals(35L, stats.getByteCount());
        // report from reportCompletedRecord
        verify(mockMetricReporter).reportCounter(MetricsReporter.PxfMetric.RECORDS_SENT, 2, mockContext);
        verify(mockMetricReporter).reportCounter(MetricsReporter.PxfMetric.BYTES_SENT, 15, mockContext);
        // report from flushStats
        verify(mockMetricReporter).reportCounter(MetricsReporter.PxfMetric.RECORDS_SENT, 1, mockContext);
        verify(mockMetricReporter).reportCounter(MetricsReporter.PxfMetric.BYTES_SENT, 20, mockContext);
        verifyNoMoreInteractions(mockMetricReporter);
    }

    @Test
    public void testFlushStatsReportAfterBatchWithRecordNoBytes() {
        when(mockMetricReporter.getReportFrequency()).thenReturn(2L);
        OperationStats stats = new OperationStats(OperationStats.Operation.READ, mockMetricReporter, mockContext);
        stats.reportCompletedRecord(5L);
        stats.reportCompletedRecord(15L);
        stats.reportCompletedRecord(15L);
        stats.flushStats();

        assertEquals(3L, stats.getRecordCount());
        assertEquals(15L, stats.getByteCount());
        // report from reportCompletedRecord
        verify(mockMetricReporter).reportCounter(MetricsReporter.PxfMetric.RECORDS_SENT, 2, mockContext);
        verify(mockMetricReporter).reportCounter(MetricsReporter.PxfMetric.BYTES_SENT, 15, mockContext);
        // report from flushStats
        verify(mockMetricReporter).reportCounter(MetricsReporter.PxfMetric.RECORDS_SENT, 1, mockContext);
        verifyNoMoreInteractions(mockMetricReporter);
    }

    @Test
    public void testUpdate() {
        when(mockMetricReporter.getReportFrequency()).thenReturn(0L);
        OperationStats startingStats = new OperationStats(OperationStats.Operation.READ, mockMetricReporter, mockContext);
        OperationStats addMe = new OperationStats(OperationStats.Operation.READ, mockMetricReporter, mockContext);
        addMe.reportCompletedRecord(15L);

        startingStats.update(addMe);

        assertEquals(OperationStats.Operation.READ, startingStats.getOperation());
        assertEquals(1L, startingStats.getRecordCount());
        assertEquals(15L, startingStats.getByteCount());

        OperationStats addMeToo = new OperationStats(OperationStats.Operation.READ, mockMetricReporter, mockContext);
        addMeToo.reportCompletedRecord(25L);

        startingStats.update(addMeToo);

        assertEquals(OperationStats.Operation.READ, startingStats.getOperation());
        assertEquals(2L, startingStats.getRecordCount());
        assertEquals(40L, startingStats.getByteCount());

        verifyNoMoreInteractions(mockMetricReporter);
    }
}
