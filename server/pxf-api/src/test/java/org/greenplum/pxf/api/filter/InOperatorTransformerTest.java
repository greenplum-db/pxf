package org.greenplum.pxf.api.filter;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class InOperatorTransformerTest {

    private static final TreeTraverser TRAVERSER = new TreeTraverser();
    private static final TreeVisitor IN_OPERATOR_TRANSFORMER = new InOperatorTransformer();

    @Test
    void transformOneValue() throws Exception {
        helper("_1_ = 100", "a1m1007s3d100o10");
    }

    @Test
    void transformTwoValues() throws Exception {
        helper("(_1_ = 11 OR _1_ = 12)", "a1m1007s2d11s2d12o10");
    }

    @Test
    void transformThreeValues() throws Exception {
        helper("((_1_ = 11 OR _1_ = 12) OR _1_ = 15)", "a1m1007s2d11s2d12s2d15o10");
    }

    @Test
    void transformFiveValues() throws Exception {
        helper("((((_1_ = 11 OR _1_ = 12) OR _1_ = 15) OR _1_ = 100) OR _1_ = 5)", "a1m1007s2d11s2d12s2d15s3d100s1d5o10");
    }

    private void helper(String expected, String filterString)
            throws Exception {
        Node root = new FilterParser().parse(filterString);
        ToStringTreeVisitor toStringTreeVisitor = new ToStringTreeVisitor();
        TRAVERSER.traverse(root, IN_OPERATOR_TRANSFORMER, toStringTreeVisitor);
        assertEquals(expected, toStringTreeVisitor.toString());
    }

}