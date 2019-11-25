package org.greenplum.pxf.api.filter;

import org.greenplum.pxf.api.io.DataType;

/**
 * Scalar, Column Index, List
 */
public class Operand extends Node {

    private final DataType dataType;

    public Operand(DataType dataType) {
        this.dataType = dataType;
    }

    /**
     * Returns the data type of the operand
     *
     * @return the data type of the operand
     */
    public DataType getDataType() {
        return dataType;
    }
}
