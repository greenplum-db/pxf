package org.greenplum.pxf.api.filter;

/**
 * Interface to prune a tree
 */
public interface TreePruner {

    /**
     * Prunes the tree
     *
     * @param root the root of the tree
     * @return the pruned tree
     */
    Node prune(Node root);
}
