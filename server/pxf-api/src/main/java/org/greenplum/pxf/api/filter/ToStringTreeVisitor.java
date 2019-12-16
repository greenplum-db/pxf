package org.greenplum.pxf.api.filter;

import org.greenplum.pxf.api.io.DataType;

import static org.greenplum.pxf.api.filter.Operator.IS_NOT_NULL;
import static org.greenplum.pxf.api.filter.Operator.IS_NULL;
import static org.greenplum.pxf.api.filter.Operator.NOOP;
import static org.greenplum.pxf.api.filter.Operator.NOT;

/**
 * A tree visitor that produces a string representation of the input Node
 */
public class ToStringTreeVisitor implements TreeVisitor {

    protected final StringBuilder sb;

    /**
     * Default constructor
     */
    public ToStringTreeVisitor() {
        sb = new StringBuilder();
    }

    @Override
    public Node before(Node node) {
        if (node instanceof OperatorNode) {
            OperatorNode operatorNode = (OperatorNode) node;
            Operator operator = operatorNode.getOperator();

            if (operator == Operator.NOT) {
                sb.append(getOperatorName(operatorNode)).append(" ");
            }

            if (operator.isLogical()) {
                sb.append("(");
            }
        }
        return node;
    }

    @Override
    public Node visit(Node node) {
        if (node instanceof Operand) {

            if (node instanceof ScalarOperand) {
                ScalarOperand scalarOperand = (ScalarOperand) node;
                // boolean does not need to be rendered
                if (scalarOperand.getDataType() == DataType.BOOLEAN) {
                    return node;
                }
            }

            sb.append(getNodeValue((Operand) node));
        } else if (node instanceof OperatorNode) {
            OperatorNode operatorNode = (OperatorNode) node;
            Operator operator = operatorNode.getOperator();

            // Skip NOOP and NOT is already handled in the before method
            if (operator == NOOP || operator == NOT) {
                return node;
            }

            sb.append(" ").append(getOperatorName(operatorNode));
            if (operator != IS_NULL && operator != IS_NOT_NULL) {
                sb.append(" ");
            }
        }
        return node;
    }

    @Override
    public Node after(Node node) {
        if (node instanceof OperatorNode) {
            OperatorNode operatorNode = (OperatorNode) node;
            if (operatorNode.getOperator().isLogical()) {
                sb.append(")");
            }
        }
        return node;
    }

    /**
     * Returns the generated string
     *
     * @return the generated string
     */
    @Override
    public String toString() {
        return sb.toString();
    }

    /**
     * Returns the string representation of the value of the operand
     *
     * @param operand the operand
     * @return the string representation of the operand's value
     */
    protected String getNodeValue(Operand operand) {
        return operand.toString();
    }

    /**
     * Returns the string representation of the operator
     *
     * @param operatorNode the operator node
     * @return the string representation of the operator
     */
    private String getOperatorName(OperatorNode operatorNode) {
        return operatorNode.getOperator().toString();
    }
}
