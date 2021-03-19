package org.greenplum.pxf.service.controller;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
public class OperationResult {
    private Exception exception;
    private OperationStats stats;
}
