package org.greenplum.pxf.api.filter;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class ColumnIndexOperandNodeTest {

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