package org.greenplum.pxf.plugins.hdfs.filter;

import org.greenplum.pxf.api.filter.FilterParser;
import org.greenplum.pxf.api.filter.Node;
import org.greenplum.pxf.api.filter.ToStringTreeVisitor;
import org.greenplum.pxf.api.filter.TreeTraverser;
import org.greenplum.pxf.api.filter.TreeVisitor;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class BPCharOperatorTransformerTest {

    private static final TreeTraverser TRAVERSER = new TreeTraverser();

    @Test
    void testOperatorWithoutBPChar() throws Exception {
        helper("_0_ >= 2016-01-03", "a0c25s10d2016-01-03o4");
    }

    @Test
    void testBPCharWithoutSpace() throws Exception {
        helper("_0_ = EUR", "a0c1042s3dEURo5");
    }

    @Test
    void testBPCharWithSpace() throws Exception {
        helper("(_0_ = EUR  OR _0_ = EUR)", "a0c1042s4dEUR o5");
    }

    @Test
    void testBPCharNotEqualsWithoutSpace() throws Exception {
        helper("_0_ <> USD", "a0c1042s3dUSDo6");
    }

    @Test
    void testBPCharNotEqualsWithSpace() throws Exception {
        helper("(_0_ <> US  AND _0_ <> US)", "a0c1042s3dUS o6");
    }

    @Test
    void testBPCharTrimAndPad() throws Exception {
        // we expect 0 = 'E ' OR 0 = 'E  ' OR 0 = 'E', one space, two spaces, no spaces
        helper("((_0_ = E  OR _0_ = E  ) OR _0_ = E)", "a0c1042s2dE o5");
    }

    @Test
    void testBPCharNotEqualsTrimAndPad() throws Exception {
        // we expect 0 <> 'U ' AND 0 <> 'E  ' OR 0 = 'E', one space, two spaces, no spaces
        helper("((_0_ <> U  AND _0_ <> U  ) AND _0_ <> U)", "a0c1042s2dU o6");
    }

    private void helper(String expected,
                        String filterString) throws Exception {

        List<ColumnDescriptor> columns = Collections.singletonList(
                new ColumnDescriptor("c1", DataType.BPCHAR.getOID(), 0, "", new Integer[]{3}));

        TreeVisitor bpCharOperatorTransformer = new BPCharOperatorTransformer(columns);
        Node root = new FilterParser().parse(filterString);
        ToStringTreeVisitor toStringTreeVisitor = new ToStringTreeVisitor();
        TRAVERSER.traverse(root, bpCharOperatorTransformer, toStringTreeVisitor);
        assertEquals(expected, toStringTreeVisitor.toString());
    }
}