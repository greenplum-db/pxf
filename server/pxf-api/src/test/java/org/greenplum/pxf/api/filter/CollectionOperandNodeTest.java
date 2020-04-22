package org.greenplum.pxf.api.filter;

import org.greenplum.pxf.api.io.DataType;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;


class CollectionOperandNodeTest {

    @Test
    public void testGetDataType() {
        OperandNode operandNode = new CollectionOperandNode(DataType.INTEGER, new ArrayList<>());

        assertEquals(DataType.INTEGER, operandNode.getDataType());
    }

    @Test
    public void testGetData() {
        List<String> data = new ArrayList<>();
        data.add("s");
        CollectionOperandNode operand = new CollectionOperandNode(DataType.INTEGER, data);

        assertSame(data, operand.getData());
        assertEquals("s", operand.getData().get(0));
    }

    @Test
    public void testToString() {
        List<String> data = new ArrayList<>();
        data.add("s");
        CollectionOperandNode operand = new CollectionOperandNode(DataType.INTEGER, data);

        assertEquals("(s)", operand.toString());

        data.add("t");
        operand = new CollectionOperandNode(DataType.INTEGER, data);

        assertEquals("(s,t)", operand.toString());
    }
}
