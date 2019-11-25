package org.greenplum.pxf.api.filter;

/**
 * Tree visitor interface
 */
public interface TreeVisitor {

    /**
     * Called right before a Node is visited
     *
     * @param node the Node that will be visited next
     */
    void before(Node node);

    /**
     * Called during the visit of a Node
     *
     * @param node the Node being visited
     */
    void visit(Node node);

    /**
     * Called right after the Node has been visited
     *
     * @param node the Node that completed the visit
     */
    void after(Node node);

}
