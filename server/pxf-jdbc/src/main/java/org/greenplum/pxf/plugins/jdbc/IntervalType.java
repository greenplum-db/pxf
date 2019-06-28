package org.greenplum.pxf.plugins.jdbc;

public enum IntervalType {
    DAY,
    MONTH,
    YEAR,
    NUMBER;

    public static IntervalType typeOf(String str) {
        return valueOf(str.toUpperCase());
    }
}
