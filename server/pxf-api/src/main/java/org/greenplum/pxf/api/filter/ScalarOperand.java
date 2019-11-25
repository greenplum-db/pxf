package org.greenplum.pxf.api.filter;

import org.greenplum.pxf.api.io.DataType;

/**
 * Represents a scalar value (String, Long, Int).
 */
public class ScalarOperand extends Operand {

    private String value;

    public ScalarOperand(DataType dataType, String value) {
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
