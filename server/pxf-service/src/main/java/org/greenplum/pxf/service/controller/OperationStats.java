package org.greenplum.pxf.service.controller;

import lombok.Getter;
import lombok.Setter;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.service.MetricsReporter;

/**
 * Holds statistics about performed operation.
 */
public class OperationStats {
    @Getter
    private final Operation operation;
    private final RequestContext context;
    private final MetricsReporter metricsReporter;
    private final long reportFrequency;
    @Getter
    private long recordCount = 0;
    @Getter
    @Setter
    private long byteCount = 0;
    private long lastReportedRecordCount = 0;
    private long lastReportedByteCount = 0;

    enum Operation {
        READ(MetricsReporter.PxfMetric.RECORDS_SENT, MetricsReporter.PxfMetric.BYTES_SENT),
        WRITE(MetricsReporter.PxfMetric.RECORDS_RECEIVED, MetricsReporter.PxfMetric.BYTES_RECEIVED);

        private final MetricsReporter.PxfMetric recordMetric;
        private final MetricsReporter.PxfMetric byteMetric;

        Operation(MetricsReporter.PxfMetric recordMetric, MetricsReporter.PxfMetric byteMetric) {
            this.recordMetric = recordMetric;
            this.byteMetric = byteMetric;
        }
    }

    public OperationStats(Operation operation, MetricsReporter metricsReporter, RequestContext context) {
        this.operation = operation;
        this.context = context;
        this.metricsReporter = metricsReporter;
        this.reportFrequency = metricsReporter.getReportFrequency();
    }
    /**
     * Increments the values of the object using the values from the passed in stats
     *
     * Note: we do not check to see if the operation matches because the operation stats
     * only live within the context of processing data, which is confined to a single
     * operation type.
     * @param operationStats statistics to add to the existing object
     */
    public void update(OperationStats operationStats) {
        this.recordCount += operationStats.getRecordCount();
        this.byteCount += operationStats.getByteCount();
    }

    /**
     * Add a completed record to the operation's stats. Report the stats when necessary.
     *
     * @param byteCount the total number of bytes written to date for the entire operation
     */
    public void reportCompletedRecord(long byteCount) {
        recordCount++;
        this.byteCount = byteCount;

        if ((reportFrequency != 0) && (recordCount % reportFrequency == 0)) {
            flushStats();
        }
    }

    /**
     * Send all the stats to the metric reporter. Set last reported values.
     */
    public void flushStats() {
        if (reportFrequency == 0) {
            return;
        }

        long recordsProcessed = recordCount - lastReportedRecordCount;
        if (recordsProcessed != 0) {
            metricsReporter.reportCounter(operation.recordMetric, recordsProcessed, context);
            lastReportedRecordCount = recordCount;
        }

        long bytesProcessed = byteCount - lastReportedByteCount;
        if (bytesProcessed != 0) {
            metricsReporter.reportCounter(operation.byteMetric, bytesProcessed, context);
            lastReportedByteCount = byteCount;
        }
    }
}
