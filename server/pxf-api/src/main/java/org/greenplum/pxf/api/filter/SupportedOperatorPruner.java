package org.greenplum.pxf.api.filter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.EnumSet;
import java.util.Optional;

import static org.greenplum.pxf.api.filter.Operator.AND;
import static org.greenplum.pxf.api.filter.Operator.NOT;
import static org.greenplum.pxf.api.filter.Operator.OR;

/**
 * A tree pruner that prunes a tree based on the supported operators.
 */
public class SupportedOperatorPruner implements TreeVisitor {

    private static final Logger LOG = LoggerFactory.getLogger(SupportedOperatorPruner.class);

    private final EnumSet<Operator> supportedOperators;

    /**
     * Constructor
     *
     * @param supportedOperators the set of supported operators
     */
    public SupportedOperatorPruner(EnumSet<Operator> supportedOperators) {
        this.supportedOperators = supportedOperators;
    }

    @Override
    public Node before(Node node, final int level) {
        return node;
    }

    @Override
    public Node visit(Node node, final int level) {
        if (node instanceof OperatorNode) {
            OperatorNode operatorNode = (OperatorNode) node;
            Operator operator = operatorNode.getOperator();
            if (!supportedOperators.contains(operator)) {
                LOG.debug("Operator {} is not supported", operator);
                // Not supported
                return null;
            }
        }
        return node;
    }

    @Override
    public Node after(Node node, final int level) {
        if (node instanceof OperatorNode) {
            OperatorNode operatorNode = (OperatorNode) node;
            Operator operator = operatorNode.getOperator();
            int childCount = operatorNode.childCount();
            if (AND == operator && childCount == 1) {
                Node promoted = Optional.ofNullable(operatorNode.getLeft()).orElse(operatorNode.getRight());
                LOG.debug("Child {} was promoted higher in the tree", promoted);
                // AND need at least two children. If the operator has a
                // single child node left, we promote the child one level up
                // the tree
                return promoted;
            } else if (OR == operator && childCount <= 1) {
                LOG.debug("Child with operator {} will be pruned because it has {} children", operator, childCount);
                operatorNode.setLeft(null);
                operatorNode.setRight(null);
                // OR need two or more children
                return null;
            } else if ((AND == operator || NOT == operator) && childCount == 0) {
                LOG.debug("Child with operator {} will be pruned because it has no children", operator);
                // AND needs 2 children / NOT needs 1 child
                return null;
            }
        }
        return node;
    }
}
