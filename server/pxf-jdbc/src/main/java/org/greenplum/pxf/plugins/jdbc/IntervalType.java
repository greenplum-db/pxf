package org.greenplum.pxf.plugins.jdbc;

public enum IntervalType {
    DAY,
    MONTH,
    YEAR;

    public static IntervalType typeOf(String str) {
        return valueOf(str.toUpperCase());
    }
}
