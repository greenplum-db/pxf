package org.greenplum.pxf.api.filter;

import java.util.List;

/**
 * Traverses the expression tree
 */
public class TreeTraverser {

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

        List<Node> children = node.getChildren();
        if (children.isEmpty()) {
            // visit this node if it has no children
            node = visitor.visit(node, level);
        } else {
            for (int i = 0; i < children.size(); i++) {
                Node child = children.get(i);
                children.set(i, traverse(child, visitor, level + 1));

                // always visit if there is only one child
                if (children.size() == 1 || i < children.size() - 1) {
                    node = visitor.visit(node, level);
                }
            }
        }
        node = visitor.after(node, level);
        return node;
    }
}
