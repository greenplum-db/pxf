package org.greenplum.pxf.plugins.jdbc;

public enum PartitionType {
    DATE,
    INT,
    ENUM;

    public static PartitionType typeOf(String str) {
        return valueOf(str.toUpperCase());
    }
}
