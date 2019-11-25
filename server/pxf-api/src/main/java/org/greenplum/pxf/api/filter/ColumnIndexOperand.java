package org.greenplum.pxf.api.filter;

/**
 * Represents a column index.
 */
public class ColumnIndexOperand extends Operand {

    private final int index;

    public ColumnIndexOperand(int idx) {
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
