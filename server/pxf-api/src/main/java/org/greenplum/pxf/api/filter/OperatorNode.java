package org.greenplum.pxf.api.filter;

import java.util.Arrays;

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
}
