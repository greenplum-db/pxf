package org.greenplum.pxf.api.filter;

/**
 * Operator node (i.e. AND, OR, =)
 */
public class OperatorNode extends Node {

    private final Operator operator;

    /**
     * Constructs a new {@link OperatorNode} with a left operand
     *
     * @param operator    the operator
     * @param leftOperand the left operand
     */
    public OperatorNode(Operator operator, Node leftOperand) {
        this(operator, leftOperand, null);
    }

    /**
     * Constructs a new {@link OperatorNode} with left and right operands
     *
     * @param operator     the operator
     * @param leftOperand  the left operand
     * @param rightOperand the right operand
     */
    public OperatorNode(Operator operator, Node leftOperand, Node rightOperand) {
        super(leftOperand, rightOperand);
        this.operator = operator;

    }

    /**
     * Returns the operator
     *
     * @return the operator
     */
    public Operator getOperator() {
        return operator;
    }

    /**
     * Returns the {@link ColumnIndexOperand} for this {@link OperatorNode}
     *
     * @return the {@link ColumnIndexOperand} for this {@link OperatorNode}
     */
    public ColumnIndexOperand getColumnIndexOperand() {
        Node left = getLeft();
        if (!(left instanceof ColumnIndexOperand)) {
            throw new IllegalArgumentException(String.format(
                    "Operator %s does not contain a column index operand", operator));
        }
        return (ColumnIndexOperand) left;
    }

    /**
     * Returns the {@link Operand} for this {@link OperatorNode}
     *
     * @return the {@link Operand} for this {@link OperatorNode}
     */
    public Operand getValueOperand() {
        Node right = getRight();
        if (right instanceof ScalarOperand || right instanceof CollectionOperand) {
            return (Operand) right;
        }
        return null;
    }
}
