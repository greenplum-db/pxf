package org.greenplum.pxf.api.filter;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class ColumnIndexOperandNodeTest {

    @Test
    public void testConstructor() {
        ColumnIndexOperandNode columnIndexOperandNode = new ColumnIndexOperandNode(5);

        assertEquals(5, columnIndexOperandNode.index());
        assertNull(columnIndexOperandNode.getDataType());
    }

    @Test
    public void testToString() {
        ColumnIndexOperandNode columnIndexOperandNode = new ColumnIndexOperandNode(10);

        assertEquals("_10_", columnIndexOperandNode.toString());
    }
}
