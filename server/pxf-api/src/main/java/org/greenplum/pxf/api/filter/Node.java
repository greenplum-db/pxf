package org.greenplum.pxf.api.filter;

import java.util.ArrayList;
import java.util.List;

/**
 * A node in the expression tree
 */
public class Node {

    private List<Node> children = new ArrayList<>();

    /**
     * Add a child to the list of children
     *
     * @param child the child to be added
     */
    public void addChild(Node child) {
        children.add(child);
    }

    /**
     * Returns the list of children for this node
     *
     * @return the list of children for this node
     */
    public List<Node> getChildren() {
        return children;
    }
}
