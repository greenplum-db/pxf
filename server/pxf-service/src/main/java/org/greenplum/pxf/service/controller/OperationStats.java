package org.greenplum.pxf.service.controller;

import lombok.Builder;
import lombok.Getter;

/**
 * Holds statistics about performed operation.
 */
@Getter
@Builder
public class OperationStats {
    private String operation;
    private long recordCount;
    private long batchCount;
    private long byteCount;

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
        this.batchCount += operationStats.getBatchCount();
        this.byteCount += operationStats.getByteCount();
    }
}
