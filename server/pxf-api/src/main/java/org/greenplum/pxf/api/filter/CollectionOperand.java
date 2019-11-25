package org.greenplum.pxf.api.filter;

import org.greenplum.pxf.api.io.DataType;

import java.util.List;

/**
 * Represents a collection of values
 */
public class CollectionOperand extends Operand {

    private final List<String> data;

    public CollectionOperand(DataType dataType, List<String> data) {
        super(dataType);
        this.data = data;
    }

    /**
     * Returns the collection of values
     *
     * @return the collection of values
     */
    public List<String> getData() {
        return data;
    }

    @Override
    public String toString() {
        return String.format("(%s)", String.join(",", data));
    }
}
