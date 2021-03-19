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

//    public OperationResult(OperationStats stats) {
//        this.e = null;
//        this.stats = stats;
//    }
//
//    public OperationResult(Exception e, OperationStats stats) {
//        this.e = e;
//        this.stats = stats;
//    }
//
////    public boolean isError() {
////        return this.e != null;
////    }
////
////    public  boolean isSuccess() {
////        return this.e == null;
////    }
}
