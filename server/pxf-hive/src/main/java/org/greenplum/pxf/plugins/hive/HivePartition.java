package org.greenplum.pxf.plugins.hive;

/**
 * Holds the column name, column type and value for a Hive partition
 */
public class HivePartition {
    private final String name;
    private final String type;
    private final String value;

    public HivePartition(String name, String type, String value) {
        this.name = name;
        this.type = type;
        this.value = value;
    }

    public String getName() {
        return name;
    }

    public String getType() {
        return type;
    }

    public String getValue() {
        return value;
    }
}
