package org.greenplum.pxf.api.filter;


import org.junit.Test;

import java.util.EnumSet;

import static org.junit.Assert.*;

public class BaseTreePrunerTest {

    private final EnumSet<Operator> ALL_SUPPORTED = EnumSet.allOf(Operator.class);
    private final EnumSet<Operator> NONE_SUPPORTED = EnumSet.noneOf(Operator.class);

    @Test
    public void testGreaterThanOperatorSupported() throws Exception {
        EnumSet<Operator> supportedOperators = EnumSet.of(Operator.GREATER_THAN_OR_EQUAL);
        helper("_1_ >= 2016-01-03", "a1c25s10d2016-01-03o4", supportedOperators);
    }

    @Test
    public void testGreaterThanOperatorNotSupported() throws Exception {
        helper("", "a1c25s10d2016-01-03o4", NONE_SUPPORTED);
    }

    @Test
    public void testAndedAndOrOperatorSupported() throws Exception {
        EnumSet<Operator> supportedOperators = EnumSet.of(
                Operator.EQUALS,
                Operator.NOT_EQUALS,
                Operator.AND,
                Operator.OR
        );
        // (P1 AND P2) AND (P1 OR P3)
        helper("((_1_ = foobar AND _2_ <> 999) AND (_1_ = foobar OR _3_ <> 999))",
                "a1c25s6dfoobaro5a2c23s3d999o6l0a1c25s6dfoobaro5a3c20s3d999o6l1l0",
                supportedOperators);
    }

    @Test
    public void testOrOperatorNotSupported() throws Exception {
        EnumSet<Operator> supportedOperators = EnumSet.of(
                Operator.EQUALS,
                Operator.NOT_EQUALS,
                Operator.AND
        );
        // (P1 AND P2) AND (P1 OR P3)
        helper("(_1_ = foobar AND _2_ <> 999)",
                "a1c25s6dfoobaro5a2c23s3d999o6l0a1c25s6dfoobaro5a3c20s3d999o6l1l0",
                supportedOperators);
    }

    @Test
    public void testAndNotOperatorsSupported() throws Exception {
        EnumSet<Operator> supportedOperators = EnumSet.of(
                Operator.EQUALS,
                Operator.NOT_EQUALS,
                Operator.OR
        );
        // (P1 AND P2) AND (P1 OR P3)
        helper("",
                "a1c25s6dfoobaro5a2c23s3d999o6l0a1c25s6dfoobaro5a3c20s3d999o6l1l0",
                supportedOperators);
    }

    @Test
    public void testAllSupportedOperators() throws Exception {
        // _1_ > '2008-02-01' and _1_ < '2008-12-01' and _2_ > 1200
        helper("((_1_ > 2008-02-01 AND _1_ < 2008-12-01) AND _2_ > 1200)",
                "a1c25s10d2008-02-01o2a1c25s10d2008-12-01o1l0a2c20s4d1200o2l0",
                ALL_SUPPORTED);
    }

    @Test
    public void testLessThanNotOperators() throws Exception {
        EnumSet<Operator> supportedOperators = EnumSet.of(
                Operator.GREATER_THAN,
                Operator.AND
        );
        // _1_ > '2008-02-01' and _1_ < '2008-12-01' and _2_ > 1200
        helper("(_1_ > 2008-02-01 AND _2_ > 1200)",
                "a1c25s10d2008-02-01o2a1c25s10d2008-12-01o1l0a2c20s4d1200o2l0",
                supportedOperators);
    }

    @Test
    public void testPruningOrOperatorWhenOneBranchIsPruned() throws Exception {
        EnumSet<Operator> supportedOperators = EnumSet.of(
                Operator.GREATER_THAN,
                Operator.AND
        );
        // (_1_ > '2008-02-01' or _1_ < '2008-12-01') and _2_ > 1200
        helper("_2_ > 1200",
                "a1c25s10d2008-02-01o2a1c25s10d2008-12-01o1l1a2c20s4d1200o2l0",
                supportedOperators);
    }

    @Test
    public void testNotOperatorSupported() throws Exception {
        helper("NOT (_1_ = 0)", "a1c20s1d0o5l2", ALL_SUPPORTED);
    }

    @Test
    public void testNotOperatorNOTSupported() throws Exception {
        helper("", "a1c20s1d0o5l2", NONE_SUPPORTED);
    }

    @Test
    public void testTwoNotWithOrAndAnd() throws Exception {
        helper("(_1_ >= 9 AND (NOT (_3_ = 4) OR NOT (_2_ = s_9)))",
                "a1c701s1d9o4a3c23s1d4o5l2a2c25s3ds_9o5l2l1l0",
                ALL_SUPPORTED);
    }

    @Test
    public void testTwoNotWithOrAndAndOperatorNOTNotSupported() throws Exception {
        helper("(_1_ >= 9 AND (NOT (_3_ = 4) OR NOT (_2_ = s_9)))",
                "a1c701s1d9o4a3c23s1d4o5l2a2c25s3ds_9o5l2l1l0",
                ALL_SUPPORTED);
    }

    @Test
    public void testSupportedIsNotNullOperator() throws Exception {
        // a3 IS NOT NULL
        helper("_3_ IS NOT NULL", "a3o9", ALL_SUPPORTED);
    }

    @Test
    public void testNotSupportedIsNotNullOperator() throws Exception {
        // a3 IS NOT NULL
        helper("", "a3o9", NONE_SUPPORTED);
    }

    @Test
    public void testNotBoolean() throws Exception {
        // NOT a4
        helper("NOT (_4_)", "a4c16s4dtrueo0l2", ALL_SUPPORTED);
    }

    @Test
    public void testInSupported() throws Exception {
        EnumSet<Operator> supportedOperators = EnumSet.of(Operator.IN);
        helper("_3_ IN bad", "a3c25s3dbado10", supportedOperators);
    }

    @Test
    public void testInNotSupported() throws Exception {
        helper("", "a3c25s3dbado10", NONE_SUPPORTED);
    }

    @Test
    public void twoBranchesAreRemovedInOr() throws Exception {
        EnumSet<Operator> supportedOperators = EnumSet.of(
                Operator.EQUALS,
                Operator.OR
        );
        // (NOT (_3_ = 4) OR NOT (_2_ = s_9))
        helper("", "a3c23s1d4o5l2a2c25s3ds_9o5l2l1", supportedOperators);
    }

    @Test
    public void testNotInWithNotSupportedIn() throws Exception {
        EnumSet<Operator> supportedOperators = EnumSet.of(
                Operator.NOT
        );
        // NOT (_8_)
        helper("", "a8c16s4dtrueo0l2", supportedOperators);
    }

    private void helper(String expected,
                        String filterString,
                        EnumSet<Operator> supportedOperators) throws Exception {
        Node root = new FilterParser().parse(filterString.getBytes());
        root = new BaseTreePruner(supportedOperators).prune(root);

        ToStringTreeVisitor visitor = new ToStringTreeVisitor();
        new TreeTraverser().inOrderTraversal(root, visitor);

        assertEquals(expected, visitor.toString());
    }

}