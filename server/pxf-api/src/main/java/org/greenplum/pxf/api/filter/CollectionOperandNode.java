package org.greenplum.pxf.api.filter;

import org.greenplum.pxf.api.io.DataType;

import java.util.List;

/**
 * Represents a collection of values
 */
public class CollectionOperandNode extends OperandNode {

    private final List<String> data;

    /**
     * Constructs a CollectionOperandNode with the given data type and a data
     * list
     *
     * @param dataType the data type
     * @param data     the data list
     */
    public CollectionOperandNode(DataType dataType, List<String> data) {
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
