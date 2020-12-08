package org.greenplum.pxf.plugins.hdfs.filter;

import org.apache.commons.lang.StringUtils;
import org.greenplum.pxf.api.filter.ColumnIndexOperandNode;
import org.greenplum.pxf.api.filter.Node;
import org.greenplum.pxf.api.filter.Operator;
import org.greenplum.pxf.api.filter.OperatorNode;
import org.greenplum.pxf.api.filter.ScalarOperandNode;
import org.greenplum.pxf.api.filter.TreeVisitor;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.greenplum.pxf.api.utilities.Utilities;

import java.util.List;

/**
 * Transforms non-logical operator nodes that have scalar operand nodes as its
 * children of BPCHAR type and which length does not match the width of the
 * BPCHAR field, but also when the field has a whitespace at the end, the
 * tree is transformed to contain the trimmed version of the field.
 */
public class BPCharOperatorTransformer implements TreeVisitor {

    private final List<ColumnDescriptor> columnDescriptors;

    public BPCharOperatorTransformer(List<ColumnDescriptor> columnDescriptors) {
        this.columnDescriptors = columnDescriptors;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Node before(Node node, final int level) {
        return node;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Node visit(Node node, int level) {
        return node;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Node after(Node node, int level) {

        if (!(node instanceof OperatorNode)) {
            return node;
        }

        OperatorNode operatorNode = (OperatorNode) node;
        Operator operator = operatorNode.getOperator();

        if (!(!operator.isLogical()
                && operatorNode.getLeft() instanceof ColumnIndexOperandNode
                && operatorNode.getRight() instanceof ScalarOperandNode)) {
            return node;
        }

        // We have a ColumnIndexOperandNode on the left node and a 
        // ScalarOperandNode on the right node with a non-logical operator
        ScalarOperandNode scalarOperandNode = (ScalarOperandNode) operatorNode.getRight();

        // We are only interested in nodes with BPCHAR type
        if (scalarOperandNode.getDataType() != DataType.BPCHAR) {
            return node;
        }

        Integer width = getBPCharWidth(operatorNode);
        String value = scalarOperandNode.getValue();

        boolean needsPadding = false;
        boolean needsTrimming = false;

        if (width != null && value.length() < width) {
            // Supports the case where the predicate has not been
            // right padded by Greenplum during the filter 
            needsPadding = true;
        }

        /* Determine whether the string has whitespace at the end */
        if (value.length() > 0 && value.charAt(value.length() - 1) == ' ') {
            // Supports the case where the the BPChar has been
            // right trimmed during the store operation. The
            // predicate has to account for the trimmed case
            // and the unmodified filter we receive from Greenplum
            needsTrimming = true;
        }

        if (!needsPadding && !needsTrimming) {
            // no transformations needed, return
            return node;
        }

        Operator logicalOperator = Operator.OR;

        // For the case of not equals, we need to transform
        // it to AND
        if (operatorNode.getOperator() == Operator.NOT_EQUALS) {
            logicalOperator = Operator.AND;
        }

        OperatorNode leftNode = operatorNode;

        if (needsPadding) {
            // When the size of the predicate of a char field does not match
            // the width of the char field, we need to whitespace pad it
            // to support the case where the BPCHAR field is padded during
            // storage

            ScalarOperandNode rightValueNode = new ScalarOperandNode(scalarOperandNode.getDataType(), StringUtils.rightPad(value, width, ' '));
            OperatorNode rightNode = new OperatorNode(operatorNode.getOperator(), operatorNode.getLeft(), rightValueNode);
            leftNode = new OperatorNode(logicalOperator, leftNode, rightNode);
        }

        if (needsTrimming) {
            // When the predicate of a char field has whitespace at the end
            // of the predicate we transform the node from (c1 = 'a ') to
            // (c1 = 'a ' OR c1 = 'a'). The original branch looks like this:

            //      =,>,etc
            //        |
            //    --------
            //    |      |
            //   _1_    'a '
            //
            //  The transformed branch will look like this:

            //                         OR
            //                          |
            //               ------------------------
            //               |                      |
            //             =,>,etc                 =,>,etc
            //               |                      |
            //           ---------              ---------
            //           |       |              |       |
            //          _1_     'a '           _1_     'a'

            ScalarOperandNode rightValueNode = new ScalarOperandNode(scalarOperandNode.getDataType(), Utilities.rightTrimWhiteSpace(value));
            OperatorNode rightNode = new OperatorNode(operatorNode.getOperator(), operatorNode.getLeft(), rightValueNode);
            leftNode = new OperatorNode(logicalOperator, leftNode, rightNode);
        }
        return leftNode;
    }

    /**
     * Returns the width of the BPChar field if available, null otherwise
     *
     * @param operatorNode the operator node
     * @return the width of the BPChar field
     */
    private Integer getBPCharWidth(OperatorNode operatorNode) {
        ColumnIndexOperandNode columnIndexOperand = operatorNode.getColumnIndexOperand();
        ColumnDescriptor columnDescriptor = columnDescriptors.get(columnIndexOperand.index());
        Integer[] modifiers = columnDescriptor.columnTypeModifiers();
        return (modifiers != null && modifiers.length > 0) ? modifiers[0] : null;
    }
}
