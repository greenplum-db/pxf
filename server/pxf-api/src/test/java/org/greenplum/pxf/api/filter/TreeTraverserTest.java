package org.greenplum.pxf.api.filter;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class TreeTraverserTest {

    @Test
    public void testFailsWhenNoVisitorIsProvided() {
        Exception ex = assertThrows(
            IllegalArgumentException.class,
            () -> new TreeTraverser().traverse(new Node()));
        assertEquals("You need to provide at least one visitor for this traverser", ex.getMessage());
    }

    @Test
    public void testTraverseLeafNode() {
        Node node = new Node();
        TreeVisitor visitor = mock(TreeVisitor.class);
        when(visitor.before(node, 0)).thenReturn(node);
        when(visitor.visit(node, 0)).thenReturn(node);
        when(visitor.after(node, 0)).thenReturn(node);

        Node result = new TreeTraverser().traverse(node, visitor);

        verify(visitor).before(node, 0);
        verify(visitor).visit(node, 0);
        verify(visitor).after(node, 0);
        verifyNoMoreInteractions(visitor);

        assertSame(node, result);
    }

    @Test
    public void testTraverseTree() {
        TreeVisitor visitor = mock(TreeVisitor.class);
        Node l0 = new Node();
        l0.setLeft(new Node());
        l0.setRight(new Node());
        l0.getLeft().setLeft(new Node());
        l0.getLeft().setRight(new Node());
        l0.getLeft().getRight().setLeft(new Node());
        l0.getLeft().getRight().setRight(new Node());


        //                          l0
        //                           |
        //               ------------------------
        //               |                      |
        //              l1-l                   l1-r
        //               |
        //        ----------------
        //        |              |
        //       l2-l           l2-r
        //                       |
        //                   --------
        //                   |      |
        //                  l3-l   l3-r

        // DFS in order traversal
        when(visitor.before(l0, 0)).thenReturn(l0); // l0 - before
        when(visitor.before(l0.getLeft(), 1)).thenReturn(l0.getLeft()); // l1-l before
        when(visitor.before(l0.getLeft().getLeft(), 2)).thenReturn(l0.getLeft().getLeft()); // l2-l before
        when(visitor.visit(l0.getLeft().getLeft(), 2)).thenReturn(l0.getLeft().getLeft()); // l2-l visit
        when(visitor.after(l0.getLeft().getLeft(), 2)).thenReturn(l0.getLeft().getLeft()); // l2-l after
        when(visitor.visit(l0.getLeft(), 1)).thenReturn(l0.getLeft()); // l1-l visit
        when(visitor.before(l0.getLeft().getRight(), 2)).thenReturn(l0.getLeft().getRight()); // l2-r before
        when(visitor.before(l0.getLeft().getRight().getLeft(), 3)).thenReturn(l0.getLeft().getRight().getLeft()); // l3-l before
        when(visitor.visit(l0.getLeft().getRight().getLeft(), 3)).thenReturn(l0.getLeft().getRight().getLeft()); // l3-l visit
        when(visitor.after(l0.getLeft().getRight().getLeft(), 3)).thenReturn(l0.getLeft().getRight().getLeft()); // l3-l after
        when(visitor.visit(l0.getLeft().getRight(), 2)).thenReturn(l0.getLeft().getRight()); // l2-r visit
        when(visitor.before(l0.getLeft().getRight().getRight(), 3)).thenReturn(l0.getLeft().getRight().getRight()); // l3-r before
        when(visitor.visit(l0.getLeft().getRight().getRight(), 3)).thenReturn(l0.getLeft().getRight().getRight()); // l3-r visit
        when(visitor.after(l0.getLeft().getRight().getRight(), 3)).thenReturn(l0.getLeft().getRight().getRight()); // l3-r after
        when(visitor.after(l0.getLeft().getRight(), 2)).thenReturn(l0.getLeft().getRight()); // l2-r after
        when(visitor.after(l0.getLeft(), 1)).thenReturn(l0.getLeft()); // l1-l after
        when(visitor.visit(l0, 0)).thenReturn(l0); // l0 - visit
        when(visitor.before(l0.getRight(), 1)).thenReturn(l0.getRight()); // l1-r before
        when(visitor.visit(l0.getRight(), 1)).thenReturn(l0.getRight()); // l1-r visit
        when(visitor.after(l0.getRight(), 1)).thenReturn(l0.getRight()); // l1-r after
        when(visitor.after(l0, 0)).thenReturn(l0); // l0 - after

        Node result = new TreeTraverser().traverse(l0, visitor);

        verify(visitor).before(l0, 0); // l0 - before
        verify(visitor).before(l0.getLeft(), 1); // l1-l before
        verify(visitor).before(l0.getLeft().getLeft(), 2); // l2-l before
        verify(visitor).visit(l0.getLeft().getLeft(), 2); // l2-l visit
        verify(visitor).after(l0.getLeft().getLeft(), 2); // l2-l after
        verify(visitor).visit(l0.getLeft(), 1); // l1-l visit
        verify(visitor).before(l0.getLeft().getRight(), 2); // l2-r before
        verify(visitor).before(l0.getLeft().getRight().getLeft(), 3); // l3-l before
        verify(visitor).visit(l0.getLeft().getRight().getLeft(), 3); // l3-l visit
        verify(visitor).after(l0.getLeft().getRight().getLeft(), 3); // l3-l after
        verify(visitor).visit(l0.getLeft().getRight(), 2); // l2-r visit
        verify(visitor).before(l0.getLeft().getRight().getRight(), 3); // l3-r before
        verify(visitor).visit(l0.getLeft().getRight().getRight(), 3); // l3-r visit
        verify(visitor).after(l0.getLeft().getRight().getRight(), 3); // l3-r after
        verify(visitor).after(l0.getLeft().getRight(), 2); // l2-r after
        verify(visitor).after(l0.getLeft(), 1); // l1-l after
        verify(visitor).visit(l0, 0); // l0 - visit
        verify(visitor).before(l0.getRight(), 1); // l1-r before
        verify(visitor).visit(l0.getRight(), 1); // l1-r visit
        verify(visitor).after(l0.getRight(), 1); // l1-r after
        verify(visitor).after(l0, 0); // l0 - after
        verifyNoMoreInteractions(visitor);

        assertSame(l0, result);
    }

    @Test
    public void testChainingVisitors() {
        TreeVisitor visitor1 = mock(TreeVisitor.class);
        TreeVisitor visitor2 = mock(TreeVisitor.class);
        TreeVisitor visitor3 = mock(TreeVisitor.class);

        Node l0 = new Node();
        l0.setLeft(new Node());
        l0.setRight(new Node());
        l0.getRight().setRight(new Node());

        //                l0
        //                 |
        //     ------------------------
        //     |                      |
        //    l1-l                   l1-r
        //                            |
        //                            --------
        //                                   |
        //                                 l2-r

        when(visitor1.before(l0, 0)).thenReturn(l0); // visitor1 - l0 - before
        when(visitor1.before(l0.getLeft(), 1)).thenReturn(l0.getLeft()); // visitor1 - l1-l - before
        when(visitor1.visit(l0.getLeft(), 1)).thenReturn(l0.getLeft()); // visitor1 - l1-l - visit
        when(visitor1.after(l0.getLeft(), 1)).thenReturn(l0.getLeft()); // visitor1 - l1-l - after
        when(visitor1.visit(l0, 0)).thenReturn(l0); // visitor1 - l0 - visit
        when(visitor1.before(l0.getRight(), 1)).thenReturn(l0.getRight()); // visitor1 - l1-r - before
        when(visitor1.visit(l0.getRight(), 1)).thenReturn(l0.getRight()); // visitor1 - l1-r - visit
        when(visitor1.before(l0.getRight().getRight(), 2)).thenReturn(l0.getRight().getRight()); // visitor1 - l2-r - before
        when(visitor1.visit(l0.getRight().getRight(), 2)).thenReturn(l0.getRight().getRight()); // visitor1 - l2-r - visit
        when(visitor1.after(l0.getRight().getRight(), 2)).thenReturn(l0.getRight().getRight()); // visitor1 - l2-r - after
        when(visitor1.after(l0.getRight(), 1)).thenReturn(l0.getRight()); // visitor1 - l1-r - after
        when(visitor1.after(l0, 0)).thenReturn(l0); // visitor1 - l0 - after

        when(visitor2.before(l0, 0)).thenReturn(l0); // visitor2 - l0 - before
        when(visitor2.before(l0.getLeft(), 1)).thenReturn(l0.getLeft()); // visitor2 - l1-l - before
        when(visitor2.visit(l0.getLeft(), 1)).thenReturn(l0.getLeft()); // visitor2 - l1-l - visit
        when(visitor2.after(l0.getLeft(), 1)).thenReturn(l0.getLeft()); // visitor2 - l1-l - after
        when(visitor2.visit(l0, 0)).thenReturn(l0); // visitor2 - l0 - visit
        when(visitor2.before(l0.getRight(), 1)).thenReturn(l0.getRight()); // visitor2 - l1-r - before
        when(visitor2.visit(l0.getRight(), 1)).thenReturn(l0.getRight()); // visitor2 - l1-r - visit
        when(visitor2.before(l0.getRight().getRight(), 2)).thenReturn(l0.getRight().getRight()); // visitor2 - l2-r - before
        when(visitor2.visit(l0.getRight().getRight(), 2)).thenReturn(l0.getRight().getRight()); // visitor2 - l2-r - visit
        when(visitor2.after(l0.getRight().getRight(), 2)).thenReturn(l0.getRight().getRight()); // visitor2 - l2-r - after
        when(visitor2.after(l0.getRight(), 1)).thenReturn(l0.getRight()); // visitor2 - l1-r - after
        when(visitor2.after(l0, 0)).thenReturn(l0); // visitor2 - l0 - after

        when(visitor3.before(l0, 0)).thenReturn(l0); // visitor3 - l0 - before
        when(visitor3.before(l0.getLeft(), 1)).thenReturn(l0.getLeft()); // visitor3 - l1-l - before
        when(visitor3.visit(l0.getLeft(), 1)).thenReturn(l0.getLeft()); // visitor3 - l1-l - visit
        when(visitor3.after(l0.getLeft(), 1)).thenReturn(l0.getLeft()); // visitor3 - l1-l - after
        when(visitor3.visit(l0, 0)).thenReturn(l0); // visitor3 - l0 - visit
        when(visitor3.before(l0.getRight(), 1)).thenReturn(l0.getRight()); // visitor3 - l1-r - before
        when(visitor3.visit(l0.getRight(), 1)).thenReturn(l0.getRight()); // visitor3 - l1-r - visit
        when(visitor3.before(l0.getRight().getRight(), 2)).thenReturn(l0.getRight().getRight()); // visitor3 - l2-r - before
        when(visitor3.visit(l0.getRight().getRight(), 2)).thenReturn(l0.getRight().getRight()); // visitor3 - l2-r - visit
        when(visitor3.after(l0.getRight().getRight(), 2)).thenReturn(l0.getRight().getRight()); // visitor3 - l2-r - after
        when(visitor3.after(l0.getRight(), 1)).thenReturn(l0.getRight()); // visitor3 - l1-r - after
        when(visitor3.after(l0, 0)).thenReturn(l0); // visitor3 - l0 - after

        Node result = new TreeTraverser().traverse(l0, visitor1, visitor2, visitor3);

        verify(visitor1).before(l0, 0); // visitor1 - l0 - before
        verify(visitor1).before(l0.getLeft(), 1); // visitor1 - l1-l - before
        verify(visitor1).visit(l0.getLeft(), 1); // visitor1 - l1-l - visit
        verify(visitor1).after(l0.getLeft(), 1); // visitor1 - l1-l - after
        verify(visitor1).visit(l0, 0); // visitor1 - l0 - visit
        verify(visitor1).before(l0.getRight(), 1); // visitor1 - l1-r - before
        verify(visitor1).visit(l0.getRight(), 1); // visitor1 - l1-r - visit
        verify(visitor1).before(l0.getRight().getRight(), 2); // visitor1 - l2-r - before
        verify(visitor1).visit(l0.getRight().getRight(), 2); // visitor1 - l2-r - visit
        verify(visitor1).after(l0.getRight().getRight(), 2); // visitor1 - l2-r - after
        verify(visitor1).after(l0.getRight(), 1); // visitor1 - l1-r - after
        verify(visitor1).after(l0, 0); // visitor1 - l0 - after
        verifyNoMoreInteractions(visitor1);

        verify(visitor2).before(l0, 0); // visitor2 - l0 - before
        verify(visitor2).before(l0.getLeft(), 1); // visitor2 - l1-l - before
        verify(visitor2).visit(l0.getLeft(), 1); // visitor2 - l1-l - visit
        verify(visitor2).after(l0.getLeft(), 1); // visitor2 - l1-l - after
        verify(visitor2).visit(l0, 0); // visitor2 - l0 - visit
        verify(visitor2).before(l0.getRight(), 1); // visitor2 - l1-r - before
        verify(visitor2).visit(l0.getRight(), 1); // visitor2 - l1-r - visit
        verify(visitor2).before(l0.getRight().getRight(), 2); // visitor2 - l2-r - before
        verify(visitor2).visit(l0.getRight().getRight(), 2); // visitor2 - l2-r - visit
        verify(visitor2).after(l0.getRight().getRight(), 2); // visitor2 - l2-r - after
        verify(visitor2).after(l0.getRight(), 1); // visitor2 - l1-r - after
        verify(visitor2).after(l0, 0); // visitor2 - l0 - after
        verifyNoMoreInteractions(visitor2);

        verify(visitor3).before(l0, 0); // visitor3 - l0 - before
        verify(visitor3).before(l0.getLeft(), 1); // visitor3 - l1-l - before
        verify(visitor3).visit(l0.getLeft(), 1); // visitor3 - l1-l - visit
        verify(visitor3).after(l0.getLeft(), 1); // visitor3 - l1-l - after
        verify(visitor3).visit(l0, 0); // visitor3 - l0 - visit
        verify(visitor3).before(l0.getRight(), 1); // visitor3 - l1-r - before
        verify(visitor3).visit(l0.getRight(), 1); // visitor3 - l1-r - visit
        verify(visitor3).before(l0.getRight().getRight(), 2); // visitor3 - l2-r - before
        verify(visitor3).visit(l0.getRight().getRight(), 2); // visitor3 - l2-r - visit
        verify(visitor3).after(l0.getRight().getRight(), 2); // visitor3 - l2-r - after
        verify(visitor3).after(l0.getRight(), 1); // visitor3 - l1-r - after
        verify(visitor3).after(l0, 0); // visitor3 - l0 - after
        verifyNoMoreInteractions(visitor3);

        assertSame(l0, result);
    }

    @Test
    public void testPruningRootNodeDuringBefore() {
        Node root = new Node();
        TreeVisitor visitor = mock(TreeVisitor.class);
        Node result = new TreeTraverser().traverse(root, visitor);

        verify(visitor).before(root, 0);
        verify(visitor).visit(result, 0);
        verify(visitor).after(result, 0);
        verifyNoMoreInteractions(visitor);
        // the root is pruned so we end up with a null result
        assertNull(result);
    }

    @Test
    public void testPruningRootNodeDuringVisit() {
        Node root = new Node();
        TreeVisitor visitor = mock(TreeVisitor.class);

        when(visitor.before(root, 0)).thenReturn(root);

        Node result = new TreeTraverser().traverse(root, visitor);

        verify(visitor).before(root, 0);
        verify(visitor).visit(root, 0);
        verify(visitor).after(result, 0);
        verifyNoMoreInteractions(visitor);
        // the root is pruned so we end up with a null result
        assertNull(result);
    }

    @Test
    public void testPruningRootNodeDuringAfter() {
        Node root = new Node();
        TreeVisitor visitor = mock(TreeVisitor.class);

        when(visitor.before(root, 0)).thenReturn(root);
        when(visitor.visit(root, 0)).thenReturn(root);

        Node result = new TreeTraverser().traverse(root, visitor);

        verify(visitor).before(root, 0);
        verify(visitor).visit(root, 0);
        verify(visitor).after(root, 0);
        verifyNoMoreInteractions(visitor);
        // the root is pruned so we end up with a null result
        assertNull(result);
    }

    @Test
    public void testPruningLeftNodeDuringBefore() {
        Node root = new Node();
        Node left = new Node();
        root.setLeft(left);
        TreeVisitor visitor = mock(TreeVisitor.class);

        //              l0
        //               |
        //     -----------
        //     |
        //    l1-l

        when(visitor.before(root, 0)).thenReturn(root);
        when(visitor.visit(root, 0)).thenReturn(root);
        when(visitor.after(root, 0)).thenReturn(root);
        when(visitor.before(root.getLeft(), 1)).thenReturn(null);

        Node result = new TreeTraverser().traverse(root, visitor);

        verify(visitor).before(root, 0);
        // before returns null for the left node, and visit and after take a
        // a null as input
        verify(visitor).before(left, 1);
        verify(visitor).visit(null, 1);
        verify(visitor).after(null, 1);
        verify(visitor).visit(root, 0);
        verify(visitor).after(root, 0);
        verifyNoMoreInteractions(visitor);

        assertSame(root, result);
        // the left child is pruned, so we end up with a root node with no children
        assertEquals(0, result.childCount());
    }

    @Test
    public void testPruningRightNodeDuringVisit() {
        Node root = new Node();
        Node right = new Node();
        root.setRight(right);
        TreeVisitor visitor = mock(TreeVisitor.class);

        //       l0
        //        |
        //        -----------
        //                  |
        //                 l1-r

        when(visitor.before(root, 0)).thenReturn(root);
        when(visitor.visit(root, 0)).thenReturn(root);
        when(visitor.after(root, 0)).thenReturn(root);
        when(visitor.before(right, 1)).thenReturn(right);
        when(visitor.visit(right, 1)).thenReturn(null);

        Node result = new TreeTraverser().traverse(root, visitor);

        verify(visitor).before(root, 0);
        verify(visitor).visit(root, 0);
        verify(visitor).before(right, 1);
        // visit returns null for the right node, and visit and after take a
        // a null as input
        verify(visitor).visit(right, 1);
        verify(visitor).after(null, 1);
        verify(visitor).after(root, 0);
        verifyNoMoreInteractions(visitor);

        assertSame(root, result);
        // the right child is pruned, so we end up with a root with no children
        assertEquals(0, result.childCount());
    }

    @Test
    public void testPruningRightNodeDuringAfter() {
        Node root = new Node();
        Node right = new Node();
        root.setRight(right);
        TreeVisitor visitor = mock(TreeVisitor.class);

        //       l0
        //        |
        //        -----------
        //                  |
        //                 l1-r

        when(visitor.before(root, 0)).thenReturn(root);
        when(visitor.visit(root, 0)).thenReturn(root);
        when(visitor.after(root, 0)).thenReturn(root);
        when(visitor.before(right, 1)).thenReturn(right);
        when(visitor.visit(right, 1)).thenReturn(right);
        when(visitor.after(right, 1)).thenReturn(null);

        Node result = new TreeTraverser().traverse(root, visitor);

        verify(visitor).before(root, 0);
        verify(visitor).visit(root, 0);
        verify(visitor).before(right, 1);
        // visit returns null for the right node, and visit and after take a
        // a null as input
        verify(visitor).visit(right, 1);
        verify(visitor).after(right, 1);
        verify(visitor).after(root, 0);
        verifyNoMoreInteractions(visitor);

        assertSame(root, result);
        // the right child is pruned, so we end up with a root with no children
        assertEquals(0, result.childCount());
    }

    @Test
    public void testPruningRootNodeAndPromotingLeftChildToRootDuringVisit() {
        Node root = new Node();
        Node left = new Node();
        root.setLeft(left);
        TreeVisitor visitor = mock(TreeVisitor.class);

        //              l0
        //               |
        //     -----------
        //     |
        //    l1-l

        when(visitor.before(root, 0)).thenReturn(root);
        when(visitor.visit(root, 0)).thenReturn(left);
        when(visitor.after(left, 0)).thenReturn(left);
        when(visitor.before(left, 1)).thenReturn(left);
        when(visitor.visit(left, 1)).thenReturn(left);
        when(visitor.after(left, 1)).thenReturn(left);

        Node result = new TreeTraverser().traverse(root, visitor);

        verify(visitor).before(root, 0);
        verify(visitor).before(left, 1);
        verify(visitor).visit(left, 1);
        verify(visitor).after(left, 1);
        verify(visitor).visit(root, 0);
        verify(visitor).after(left, 0);
        verifyNoMoreInteractions(visitor);

        assertSame(left, result);
        assertEquals(0, result.childCount());
    }

    @Test
    public void testPruningRootNodeAndPromotingRightChildToRootDuringVisit() {
        Node root = new Node();
        Node right = new Node();
        root.setRight(right);
        TreeVisitor visitor = mock(TreeVisitor.class);

        //       l0
        //        |
        //        -----------
        //                  |
        //                 l1-r

        when(visitor.before(root, 0)).thenReturn(root);
        when(visitor.visit(root, 0)).thenReturn(right);
        when(visitor.after(right, 0)).thenReturn(right);
        when(visitor.before(right, 1)).thenReturn(right);
        when(visitor.visit(right, 1)).thenReturn(right);
        when(visitor.after(right, 1)).thenReturn(right);

        Node result = new TreeTraverser().traverse(root, visitor);

        verify(visitor).before(root, 0);
        verify(visitor).visit(root, 0);
        verify(visitor).after(right, 0);
        verifyNoMoreInteractions(visitor);

        assertSame(right, result);
        assertEquals(0, result.childCount());
    }

}
