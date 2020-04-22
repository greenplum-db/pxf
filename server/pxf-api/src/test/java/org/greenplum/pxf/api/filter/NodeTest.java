package org.greenplum.pxf.api.filter;


import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

public class NodeTest {

    @Test
    public void testEmptyConstructor() {
        Node root = new Node();
        assertNull(root.getLeft());
        assertNull(root.getRight());
    }

    @Test
    public void testConstructorWithOneNode() {
        Node left = new Node();
        Node root = new Node(left);

        assertSame(left, root.getLeft());
        assertNull(root.getRight());
    }

    @Test
    public void testConstructorWithTwoNodes() {
        Node left = new Node();
        Node right = new Node();
        Node root = new Node(left, right);

        assertSame(left, root.getLeft());
        assertSame(right, root.getRight());
    }

    @Test
    public void testSettingLeftNode() {
        Node root = new Node();

        assertNull(root.getLeft());

        Node left = new Node();
        root.setLeft(left);

        assertSame(left, root.getLeft());
    }

    @Test
    public void testSettingRightNode() {
        Node root = new Node();

        assertNull(root.getRight());

        Node right = new Node();
        root.setRight(right);

        assertSame(right, root.getRight());
    }

    @Test
    public void testChildCount() {
        Node root = new Node();

        assertEquals(0, root.childCount());

        Node left = new Node();
        root.setLeft(left);

        assertEquals(1, root.childCount());

        Node right = new Node();
        root.setRight(right);

        assertEquals(2, root.childCount());
    }
}
