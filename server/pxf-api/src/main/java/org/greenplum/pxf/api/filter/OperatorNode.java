package org.greenplum.pxf.api.filter;

import java.util.Arrays;
import java.util.Optional;

/**
 * Operator node (i.e. AND, OR, =, >=)
 */
public class OperatorNode extends Node {

    private final Operator operator;

    /**
     * Constructs a new OperatorNode with an operator and a list of children
     *
     * @param operator the operator
     * @param children the list of children
     */
    public OperatorNode(Operator operator, Node... children) {
        this.operator = operator;
        if (children != null) {
            Arrays.stream(children).forEach(this::addChild);
        }
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
        Optional<ColumnIndexOperand> columnIndexOperand = getChildren()
                .stream()
                .filter(op -> op instanceof ColumnIndexOperand)
                .map(op -> (ColumnIndexOperand) op)
                .findFirst();

        if (!columnIndexOperand.isPresent()) {
            throw new IllegalArgumentException(
                    String.format("Operator %s does not contain a column index operand", operator));
        }

        return columnIndexOperand.get();
    }

    /**
     * Returns the {@link Optional} {@link Operand} for this {@link OperatorNode}
     *
     * @return the {@link Optional} {@link Operand} for this {@link OperatorNode}
     */
    public Optional<Operand> getOperand() {
        return getChildren()
                .stream()
                .filter(op -> op instanceof ScalarOperand || op instanceof CollectionOperand)
                .map(op -> (Operand) op)
                .findFirst();
    }
}
