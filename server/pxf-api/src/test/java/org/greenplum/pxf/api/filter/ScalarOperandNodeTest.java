package org.greenplum.pxf.api.filter;

import org.greenplum.pxf.api.io.DataType;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

public class ScalarOperandNodeTest {

    @Test
    public void testConstructor() {
        ScalarOperandNode scalarOperandNode = new ScalarOperandNode(DataType.INTEGER, "5");

        assertSame(DataType.INTEGER, scalarOperandNode.getDataType());
        assertEquals("5", scalarOperandNode.getValue());
    }

    @Test
    public void testToString() {
        ScalarOperandNode scalarOperandNode = new ScalarOperandNode(DataType.VARCHAR, "abc");

        assertSame(DataType.VARCHAR, scalarOperandNode.getDataType());
        assertEquals("abc", scalarOperandNode.toString());
    }

}