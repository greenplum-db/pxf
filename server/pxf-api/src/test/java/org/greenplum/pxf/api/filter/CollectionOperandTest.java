package org.greenplum.pxf.api.filter;

import org.greenplum.pxf.api.io.DataType;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class CollectionOperandTest {

    @Test
    public void testConstructor() {
        new CollectionOperand(DataType.INTEGER, new ArrayList<>());
    }

    @Test
    public void testGetDataType() {
        Operand operand = new CollectionOperand(DataType.INTEGER, new ArrayList<>());
        assertEquals(DataType.INTEGER, operand.getDataType());
    }

    @Test
    public void testGetData() {
        List<String> data = new ArrayList<>();
        data.add("s");
        CollectionOperand operand = new CollectionOperand(DataType.INTEGER, data);
        assertSame(data, operand.getData());
        assertEquals("s", operand.getData().get(0));
    }

    @Test
    public void testToString() {
        List<String> data = new ArrayList<>();
        data.add("s");
        CollectionOperand operand = new CollectionOperand(DataType.INTEGER, data);
        assertEquals("(s)", operand.toString());

        data.add("t");
        operand = new CollectionOperand(DataType.INTEGER, data);
        assertEquals("(s,t)", operand.toString());
    }
}