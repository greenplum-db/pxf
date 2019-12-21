package org.greenplum.pxf.api.filter;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Traverses the expression tree
 */
public class TreeTraverser {

    private static final Logger LOG = LoggerFactory.getLogger(TreeTraverser.class);

    /**
     * In order depth-first traversal L-M-R
     *
     * @param node    the node
     * @param visitor the visitor interface implementation
     * @return the traversed node
     */
    public Node traverse(Node node, TreeVisitor visitor) {
        return traverse(node, visitor, 0);
    }

    /**
     * In order depth-first traversal L-M-R
     *
     * @param node    the node
     * @param visitor the visitor interface implementation
     * @return the traversed node
     */
    protected Node traverse(Node node, TreeVisitor visitor, final int level) {
        if (node == null) return null;

        node = visitor.before(node, level);
        // Traverse the left node
        traverseHelper(node, 0, visitor, level + 1);
        // Visit this node
        node = visitor.visit(node, level);
        // Traverse the right node
        traverseHelper(node, 1, visitor, level + 1);
        node = visitor.after(node, level);

        return node;
    }

    private void traverseHelper(Node node, int index, TreeVisitor visitor, int level) {
        if (node == null) return;
        Node child = index == 0 ? node.getLeft() : node.getRight();
        Node processed = traverse(child, visitor, level);

        if (processed == child) {
            return;
        }

        if (processed == null) {
            if (index == 0) {
                LOG.debug("Left child {} was pruned", child);
            } else {
                LOG.debug("Right child {} was pruned", child);
            }
        } else {

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

            // replace it with the new child
            if (index == 0) {
                LOG.debug("Left child {} was pruned, and child {} was promoted higher in the tree", child, processed);
            } else {
                LOG.debug("Right child {} was pruned, and child {} was promoted higher in the tree", child, processed);
            }
        }

        if (index == 0) {
            node.setLeft(processed);
        } else {
            node.setRight(processed);
        }
    }
}
