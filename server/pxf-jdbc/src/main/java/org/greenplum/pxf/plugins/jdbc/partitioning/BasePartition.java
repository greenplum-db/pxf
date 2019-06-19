package org.greenplum.pxf.plugins.jdbc.partitioning;

/**
 * A base class for partition of any type.
 *
 * All partitions use some column as a partition column. It is processed by this class.
 */
abstract class BasePartition implements JdbcFragmentMetadata {
    private static final long serialVersionUID = 0L;

    protected final String column;

    /**
     * @param column column name to use as a partition column. Must not be null
     */
    protected BasePartition(String column) {
        if (column == null) {
            throw new RuntimeException("Column name must not be null");
        }
        this.column = column;
    }

    /**
     * Getter
     */
    public String getColumn() {
        return column;
    }
}
