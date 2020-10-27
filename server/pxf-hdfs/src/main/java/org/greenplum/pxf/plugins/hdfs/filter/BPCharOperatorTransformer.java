package org.greenplum.pxf.plugins.hdfs.filter;

import org.greenplum.pxf.api.filter.ColumnIndexOperandNode;
import org.greenplum.pxf.api.filter.Node;
import org.greenplum.pxf.api.filter.Operator;
import org.greenplum.pxf.api.filter.OperatorNode;
import org.greenplum.pxf.api.filter.ScalarOperandNode;
import org.greenplum.pxf.api.filter.TreeVisitor;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.utilities.Utilities;

/**
 * Transforms non-logical operator nodes that have scalar operand nodes as its
 * children of BPCHAR type and which values have whitespace at the end of the
 * string.
 */
public class BPCharOperatorTransformer implements TreeVisitor {
    @Override
    public Node before(Node node, final int level) {
        return node;
    }

    @Override
    public Node visit(Node node, int level) {

        if (node instanceof OperatorNode) {

            OperatorNode operatorNode = (OperatorNode) node;
            Operator operator = operatorNode.getOperator();

            if (!operator.isLogical()
                    && operatorNode.getLeft() instanceof ColumnIndexOperandNode
                    && operatorNode.getRight() instanceof ScalarOperandNode) {
                ScalarOperandNode scalarOperandNode = (ScalarOperandNode) operatorNode.getRight();

                if (scalarOperandNode.getDataType() == DataType.BPCHAR) {
                    String value = scalarOperandNode.getValue();

                    /* Determine whether the string has whitespace at the end */
                    if (value.length() > 0 && value.charAt(value.length() - 1) == ' ') {
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

                        Operator logicalOperator = Operator.OR;

                        // For the case of not equals, we need to transform
                        // it to AND
                        if (operatorNode.getOperator() == Operator.NOT_EQUALS) {
                            logicalOperator = Operator.AND;
                        }

                        ScalarOperandNode rightValueNode = new ScalarOperandNode(scalarOperandNode.getDataType(), Utilities.rightTrimWhiteSpace(value));
                        OperatorNode rightNode = new OperatorNode(operatorNode.getOperator(), operatorNode.getLeft(), rightValueNode);
                        return new OperatorNode(logicalOperator, operatorNode, rightNode);
                    }
                }
            }
        }

        return node;
    }

    @Override
    public Node after(Node node, int level) {
        return node;
    }
}
