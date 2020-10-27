package org.greenplum.pxf.plugins.hdfs.filter;

import org.greenplum.pxf.api.filter.FilterParser;
import org.greenplum.pxf.api.filter.Node;
import org.greenplum.pxf.api.filter.ToStringTreeVisitor;
import org.greenplum.pxf.api.filter.TreeTraverser;
import org.greenplum.pxf.api.filter.TreeVisitor;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class BPCharOperatorTransformerTest {

    private static final TreeTraverser TRAVERSER = new TreeTraverser();
    private static final TreeVisitor BPCHAR_TRANSFORMER = new BPCharOperatorTransformer();

    @Test
    public void testOperatorWithoutBPChar() throws Exception {
        helper("_1_ >= 2016-01-03", "a1c25s10d2016-01-03o4");
    }

    @Test
    public void testBPCharWithoutSpace() throws Exception {
        helper("_12_ = EUR", "a12c1042s3dEURo5");
    }

    @Test
    public void testBPCharWithSpace() throws Exception {
        helper("(_12_ = EUR  OR _12_ = EUR)", "a12c1042s4dEUR o5");
    }

    @Test
    public void testBPCharNotEqualsWithoutSpace() throws Exception {
        helper("_12_ <> USD", "a12c1042s3dUSDo6");
    }

    @Test
    public void testBPCharNotEqualsWithSpace() throws Exception {
        helper("(_12_ <> USD  AND _12_ <> USD)", "a12c1042s4dUSD o6");
    }

    private void helper(String expected,
                        String filterString) throws Exception {
        Node root = new FilterParser().parse(filterString);
        ToStringTreeVisitor toStringTreeVisitor = new ToStringTreeVisitor();
        TRAVERSER.traverse(root, BPCHAR_TRANSFORMER, toStringTreeVisitor);
        assertEquals(expected, toStringTreeVisitor.toString());
    }
}