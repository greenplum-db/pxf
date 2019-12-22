package org.greenplum.pxf.api.filter;

/**
 * Represents a column index.
 */
public class ColumnIndexOperandNode extends OperandNode {

    private final int index;

    public ColumnIndexOperandNode(int idx) {
        super(null);
        index = idx;
    }

    /**
     * Returns the column index
     *
     * @return the column index
     */
    public int index() {
        return index;
    }

    @Override
    public String toString() {
        return String.format("_%d_", index);
    }
}
