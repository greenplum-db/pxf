package org.greenplum.pxf.plugins.hdfs.parquet;

import org.greenplum.pxf.api.filter.ColumnIndexOperandNode;
import org.greenplum.pxf.api.filter.FilterParser;
import org.greenplum.pxf.api.filter.Node;
import org.greenplum.pxf.api.filter.OperandNode;
import org.greenplum.pxf.api.filter.Operator;
import org.greenplum.pxf.api.filter.OperatorNode;
import org.greenplum.pxf.api.filter.TreeVisitor;
import org.greenplum.pxf.plugins.hdfs.ParquetFileAccessor;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ParquetOperatorPrunerAndTransformerTest extends ParquetBaseTest {

    @Test
    public void testIntegerFilter() throws Exception {
        // a16 = 11
        Node result = helper("a16c23s2d11o5");
        assertNotNull(result);
        assertTrue(result instanceof OperatorNode);
        OperatorNode operatorNode = (OperatorNode) result;
        assertEquals(Operator.EQUALS, operatorNode.getOperator());
        assertTrue(operatorNode.getLeft() instanceof ColumnIndexOperandNode);
        assertEquals(16, ((ColumnIndexOperandNode) operatorNode.getLeft()).index());
        assertTrue(operatorNode.getRight() instanceof OperandNode);
        assertEquals("11", operatorNode.getRight().toString());
    }

    @Test
    public void testNotBooleanFilter() throws Exception {
        // NOT (a5 == true)
        Node result = helper("a5c16s4dtrueo0l2");
        assertNotNull(result);
        assertTrue(result instanceof OperatorNode);
        OperatorNode operatorNode = (OperatorNode) result;
        assertEquals(Operator.NOT, operatorNode.getOperator());
        assertTrue(result.getLeft() instanceof OperatorNode);
        OperatorNode noopOperatorNode = (OperatorNode) result.getLeft();
        assertEquals(Operator.NOOP, noopOperatorNode.getOperator());
        assertTrue(noopOperatorNode.getLeft() instanceof ColumnIndexOperandNode);
        assertEquals(5, ((ColumnIndexOperandNode) noopOperatorNode.getLeft()).index());
        assertTrue(noopOperatorNode.getRight() instanceof OperandNode);
        assertEquals("true", noopOperatorNode.getRight().toString());
    }

    @Test
    public void testWhitespacePaddedChar() throws Exception {
        // a12 = 'EUR'
        Node result = helper("a12c1042s4dEUR o5");
        assertNotNull(result);
        assertTrue(result instanceof OperatorNode);
        OperatorNode operatorNode = (OperatorNode) result;
        assertEquals(Operator.OR, operatorNode.getOperator());
        assertTrue(result.getLeft() instanceof OperatorNode);
        OperatorNode leftOperatorNode = (OperatorNode) result.getLeft();
        assertEquals(Operator.EQUALS, leftOperatorNode.getOperator());
        assertTrue(leftOperatorNode.getLeft() instanceof ColumnIndexOperandNode);
        assertEquals(12, ((ColumnIndexOperandNode) leftOperatorNode.getLeft()).index());
        assertTrue(leftOperatorNode.getRight() instanceof OperandNode);
        assertEquals("EUR ", leftOperatorNode.getRight().toString());
        OperatorNode rightOperatorNode = (OperatorNode) result.getRight();
        assertEquals(Operator.EQUALS, rightOperatorNode.getOperator());
        assertTrue(rightOperatorNode.getLeft() instanceof ColumnIndexOperandNode);
        assertEquals(12, ((ColumnIndexOperandNode) rightOperatorNode.getLeft()).index());
        assertTrue(rightOperatorNode.getRight() instanceof OperandNode);
        assertEquals("EUR", rightOperatorNode.getRight().toString());

        // a12 <> 'USD '
        result = helper("a12c1042s4dUSD o6");
        assertNotNull(result);
        assertTrue(result instanceof OperatorNode);
        operatorNode = (OperatorNode) result;
        assertEquals(Operator.AND, operatorNode.getOperator());
        assertTrue(result.getLeft() instanceof OperatorNode);
        leftOperatorNode = (OperatorNode) result.getLeft();
        assertEquals(Operator.NOT_EQUALS, leftOperatorNode.getOperator());
        assertTrue(leftOperatorNode.getLeft() instanceof ColumnIndexOperandNode);
        assertEquals(12, ((ColumnIndexOperandNode) leftOperatorNode.getLeft()).index());
        assertTrue(leftOperatorNode.getRight() instanceof OperandNode);
        assertEquals("USD ", leftOperatorNode.getRight().toString());
        rightOperatorNode = (OperatorNode) result.getRight();
        assertEquals(Operator.NOT_EQUALS, rightOperatorNode.getOperator());
        assertTrue(rightOperatorNode.getLeft() instanceof ColumnIndexOperandNode);
        assertEquals(12, ((ColumnIndexOperandNode) rightOperatorNode.getLeft()).index());
        assertTrue(rightOperatorNode.getRight() instanceof OperandNode);
        assertEquals("USD", rightOperatorNode.getRight().toString());
    }

    @Test
    public void testUnsupportedINT96Filter() throws Exception {
        // tm = '2013-07-23 21:00:00' -> null
        Node result = helper("a6c1114s19d2013-07-23 21:00:00o5");
        assertNull(result);

        // name = 'row2' and tm = '2013-07-23 21:00:00' -> name = 'row2'
        result = helper("a1c25s4drow2o5a6c1114s19d2013-07-23 21:00:00o5l0");
        assertNotNull(result);
        assertTrue(result instanceof OperatorNode);
        OperatorNode operatorNode = (OperatorNode) result;
        assertEquals(Operator.EQUALS, operatorNode.getOperator());
        assertTrue(operatorNode.getLeft() instanceof ColumnIndexOperandNode);
        assertEquals(1, ((ColumnIndexOperandNode) operatorNode.getLeft()).index());
        assertTrue(operatorNode.getRight() instanceof OperandNode);
        assertEquals("row2", operatorNode.getRight().toString());

        // name = 'row2' or tm = '2013-07-23 21:00:00' -> null
        result = helper("a1c25s4drow2o5a6c1114s19d2013-07-23 21:00:00o5l1");
        assertNull(result);
    }

    @Test
    public void testUnsupportedFixedLenByteArrayFilter() throws Exception {
        // dec2 = 0
        Node result = helper("a14c23s1d0o5");
        assertNull(result);

        // name = 'row2' and dec2 = 0 -> name = 'row2'
        result = helper("a1c25s4drow2o5a14c23s1d0o5l0");
        assertNotNull(result);
        assertTrue(result instanceof OperatorNode);
        OperatorNode operatorNode = (OperatorNode) result;
        assertEquals(Operator.EQUALS, operatorNode.getOperator());
        assertTrue(operatorNode.getLeft() instanceof ColumnIndexOperandNode);
        assertEquals(1, ((ColumnIndexOperandNode) operatorNode.getLeft()).index());
        assertTrue(operatorNode.getRight() instanceof OperandNode);
        assertEquals("row2", operatorNode.getRight().toString());

        // name = 'row2' or dec2 = 0 -> null
        result = helper("a1c25s4drow2o5a14c23s1d0o5l1");
        assertNull(result);
    }

    @Test
    public void testUnsupportedInOperationFilter() throws Exception {
        // a16 in (11, 12)
        Node result = helper("a16m1007s2d11s2d12o10");
        assertNull(result);
    }

    private Node helper(String filterString) throws Exception {

        TreeVisitor pruner = new ParquetOperatorPrunerAndTransformer(
                columnDescriptors, originalFieldsMap, ParquetFileAccessor.SUPPORTED_OPERATORS);

        // Parse the filter string into a expression tree Node
        Node root = new FilterParser().parse(filterString);
        // Prune the parsed tree with valid supported operators and then
        // traverse the pruned tree with the ParquetRecordFilterBuilder to
        // produce a record filter for parquet
        return TRAVERSER.traverse(root, pruner);
    }
}
