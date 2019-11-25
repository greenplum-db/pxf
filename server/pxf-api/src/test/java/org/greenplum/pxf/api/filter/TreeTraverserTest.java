package org.greenplum.pxf.api.filter;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TreeTraverserTest {

    @Test
    public void testGreaterThanOperator() throws Exception {
        helper("_1_ >= 2016-01-03", "a1c25s10d2016-01-03o4");
    }

    @Test
    public void testAndedAndOr() throws Exception {
        // (P1 AND P2) AND (P1 OR P3)
        helper("((_1_ = foobar AND _2_ <> 999) AND (_1_ = foobar OR _3_ <> 999))", "a1c25s6dfoobaro5a2c23s3d999o6l0a1c25s6dfoobaro5a3c20s3d999o6l1l0");
    }

    @Test
    public void testTwoAndOperators() throws Exception {
        // _1_ > '2008-02-01' and _1_ < '2008-12-01' and _2_ > 1200
        helper("((_1_ > 2008-02-01 AND _1_ < 2008-12-01) AND _2_ > 1200)", "a1c25s10d2008-02-01o2a1c25s10d2008-12-01o1l0a2c20s4d1200o2l0");
    }

    @Test
    public void testNotOperator() throws Exception {
        helper("NOT (_1_ = 0)", "a1c20s1d0o5l2");
    }

    @Test
    public void testFieldsWithText() throws Exception {
        helper("(_1_ < l2 AND _1_ > 2)", "a1c25s2dl2o1a1c20s1d2o2l0");
    }

    @Test
    public void testTwoNotWithOrAndAnd() throws Exception{
        helper("(_1_ >= 9 AND (NOT (_3_ = 4) OR NOT (_2_ = s_9)))", "a1c701s1d9o4a3c23s1d4o5l2a2c25s3ds_9o5l2l1l0");
    }

    @Test
    public void testIsNotNullOperator() throws Exception {
        // a3 IS NOT NULL
        helper("_3_ IS NOT NULL", "a3o9");
    }

    @Test
    public void testNotBoolean() throws Exception {
        // NOT a4
        helper("NOT (_4_)", "a4c16s4dtrueo0l2");
    }

    private void helper(String expected, String filterString) throws Exception {
        Node root = new FilterParser().parse(filterString.getBytes());

        ToStringTreeVisitor visitor = new ToStringTreeVisitor();
        new TreeTraverser().inOrderTraversal(root, visitor);

        assertEquals(expected, visitor.toString());
    }
}