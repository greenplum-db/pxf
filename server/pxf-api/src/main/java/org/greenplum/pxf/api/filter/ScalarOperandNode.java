package org.greenplum.pxf.api.filter;

import org.greenplum.pxf.api.io.DataType;

/**
 * Represents a scalar value (String, Long, Int).
 */
public class ScalarOperandNode extends OperandNode {

    private final String value;

    /**
     * Constructs an ScalarOperandNode with the datum data type and value
     *
     * @param dataType the data type
     * @param value    the value
     */
    public ScalarOperandNode(DataType dataType, String value) {
        super(dataType);
        this.value = value;
    }

    /**
     * Returns the value of the scalar
     *
     * @return the value of the scalar
     */
    public String getValue() {
        return value;
    }

    @Override
    public String toString() {
        return value;
    }
}
