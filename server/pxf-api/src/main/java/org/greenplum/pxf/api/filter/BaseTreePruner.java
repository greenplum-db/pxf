package org.greenplum.pxf.api.filter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.EnumSet;
import java.util.List;

import static org.greenplum.pxf.api.filter.Operator.AND;
import static org.greenplum.pxf.api.filter.Operator.NOT;
import static org.greenplum.pxf.api.filter.Operator.OR;

/**
 * A tree pruner that prunes a tree based on the supported operators.
 */
public class BaseTreePruner implements TreePruner {

    private static final Logger LOG = LoggerFactory.getLogger(BaseTreePruner.class);

    private final EnumSet<Operator> supportedOperators;

    /**
     * Constructor
     *
     * @param supportedOperators the set of supported operators
     */
    public BaseTreePruner(EnumSet<Operator> supportedOperators) {
        this.supportedOperators = supportedOperators;
    }

    @Override
    public Node prune(Node node) {
        if (node == null) return null;

        if (node instanceof OperatorNode) {
            OperatorNode operatorNode = (OperatorNode) node;
            if (!supportedOperators.contains(operatorNode.getOperator())) {

                LOG.debug("Operator {} is not supported", operatorNode.getOperator());

                // Not supported
                node.getChildren().clear();
                return null;
            }
        }

        List<Node> children = node.getChildren();
        for (int i = children.size() - 1; i >= 0; i--) {
            Node child = children.get(i);
            Node pruned = prune(child);

            if (pruned == null) {
                LOG.debug("Child {} at index {} was pruned", child, i);
                child.getChildren().clear();
                // If pruned remove it from list of children
                children.remove(i);
            } else if (pruned != child) {

                // This happens when AND operation end up with a single
                // child. For example:
                //                          AND
                //                           |
                //               ------------------------
                //               |                      |
                //              AND                     >
                //               |                      |
                //        ----------------          ---------
                //        |              |          |       |
                //        >              <         _2_     1200
                //        |              |
                //    --------       --------
                //    |      |       |      |
                //   _1_     5      _1_     10
                //
                // If only the AND and > operators are supported, the right
                // branch of the second AND ( _1_ < 10 ) will be dropped and
                // the left branch will be promoted up in the tree. The
                // resulting tree will look like this:
                //                         AND
                //                          |
                //               ------------------------
                //               |                      |
                //               >                      >
                //               |                      |
                //           ---------              ---------
                //           |       |              |       |
                //          _1_      5             _2_     1200

                LOG.debug("Child {} at index {} was pruned, and child {} was promoted higher in the tree",
                        child, i, pruned);

                child.getChildren().clear();
                children.remove(i);
                children.add(i, pruned);
            }
        }

        if (node instanceof OperatorNode) {
            OperatorNode operatorNode = (OperatorNode) node;
            Operator operator = operatorNode.getOperator();

            if (AND == operator && children.size() == 1) {

                LOG.debug("Child {} was promoted higher in the tree", children.get(0));

                // AND need at least two children. If the operator has a
                // single child node left, we promote the child one level up
                // the tree
                return children.get(0);
            } else if (OR == operator && children.size() <= 1) {
                LOG.debug("Child with operator {} will be pruned because it has {} children",
                        operator, children.size());

                // OR need two or more children
                return null;
            } else if ((AND == operator || NOT == operator) && children.size() == 0) {
                LOG.debug("Child with operator {} will be pruned because it has {} children",
                        operator, children.size());

                // NOT needs 1 child
                return null;
            }
        }

        return node;
    }

    /**
     * Returns a list of supported operators
     *
     * @return a list of supported operators
     */
    public EnumSet<Operator> getSupportedOperators() {
        return supportedOperators;
    }
}
