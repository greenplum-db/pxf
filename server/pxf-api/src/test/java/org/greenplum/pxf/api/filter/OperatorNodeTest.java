package org.greenplum.pxf.api.filter;

import org.greenplum.pxf.api.io.DataType;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

public class OperatorNodeTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testConstructorWithSingleOperand() {
        ColumnIndexOperandNode columnIndexOperand = new ColumnIndexOperandNode(5);
        OperatorNode operatorNode = new OperatorNode(Operator.NOT, columnIndexOperand);

        assertSame(Operator.NOT, operatorNode.getOperator());
        assertSame(columnIndexOperand, operatorNode.getColumnIndexOperand());
    }

    @Test
    public void testConstructorWithTwoOperands() {
        ColumnIndexOperandNode columnIndexOperand = new ColumnIndexOperandNode(5);
        ScalarOperandNode scalarOperandNode = new ScalarOperandNode(DataType.INTEGER, "5");
        OperatorNode operatorNode = new OperatorNode(Operator.GREATER_THAN, columnIndexOperand, scalarOperandNode);

        assertSame(Operator.GREATER_THAN, operatorNode.getOperator());
        assertSame(columnIndexOperand, operatorNode.getLeft());
        assertSame(columnIndexOperand, operatorNode.getColumnIndexOperand());
        assertSame(scalarOperandNode, operatorNode.getRight());
        assertSame(scalarOperandNode, operatorNode.getValueOperand());
    }

    @Test
    public void testColumnIndexOperandMissing() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Operator <= does not contain a column index operand");

        ScalarOperandNode scalarOperandNode1 = new ScalarOperandNode(DataType.INTEGER, "5");
        ScalarOperandNode scalarOperandNode2 = new ScalarOperandNode(DataType.INTEGER, "5");
        OperatorNode operatorNode = new OperatorNode(Operator.LESS_THAN_OR_EQUAL, scalarOperandNode1, scalarOperandNode2);

        assertSame(Operator.LESS_THAN_OR_EQUAL, operatorNode.getOperator());
        operatorNode.getColumnIndexOperand();
    }

    @Test
    public void testScalarOrCollectionOperandMissing() {
        ColumnIndexOperandNode columnIndexOperand1 = new ColumnIndexOperandNode(5);
        ColumnIndexOperandNode columnIndexOperand2 = new ColumnIndexOperandNode(5);

        OperatorNode operatorNode = new OperatorNode(Operator.LIKE, columnIndexOperand1, columnIndexOperand2);

        assertSame(Operator.LIKE, operatorNode.getOperator());
        assertSame(columnIndexOperand1, operatorNode.getLeft());
        assertSame(columnIndexOperand1, operatorNode.getColumnIndexOperand());
        assertSame(columnIndexOperand2, operatorNode.getRight());
        assertNull(operatorNode.getValueOperand());

    }
}