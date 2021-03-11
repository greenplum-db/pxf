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
    private Long recordCount;
    private Long batchCount;
    private Long byteCount;
}
