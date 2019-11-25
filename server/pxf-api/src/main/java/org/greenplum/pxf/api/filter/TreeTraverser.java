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
     */
    public void inOrderTraversal(Node node, TreeVisitor visitor) {
        if (node == null) return;

        visitor.before(node);

        List<Node> children = node.getChildren();
        if (children.isEmpty()) {
            // visit this node if it has no children
            visitor.visit(node);
        } else {
            for (int i = 0; i < children.size(); i++) {
                Node child = children.get(i);
                inOrderTraversal(child, visitor);

                // always visit if there is only one child
                if (children.size() == 1 || i < children.size() - 1) {
                    visitor.visit(node);
                }
            }
        }
        visitor.after(node);
    }

    /**
     * Post order depth-first traversal
     *
     * @param node    the node
     * @param visitor the visitor interface implementation
     */
    public void postOrderTraversal(Node node, TreeVisitor visitor) {
        if (node == null) return;

        visitor.before(node);
        for (Node child : node.getChildren()) {
            postOrderTraversal(child, visitor);
        }
        visitor.visit(node);
        visitor.after(node);
    }
}
