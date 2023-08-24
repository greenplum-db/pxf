package org.greenplum.pxf.plugins.hdfs.filter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.greenplum.pxf.api.filter.FilterParser;
import org.greenplum.pxf.api.filter.Node;
import org.greenplum.pxf.api.filter.SupportedOperatorPruner;
import org.greenplum.pxf.api.filter.TreeTraverser;
import org.greenplum.pxf.api.filter.TreeVisitor;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.greenplum.pxf.plugins.hdfs.orc.ORCVectorizedAccessor.SUPPORTED_OPERATORS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class SearchArgumentBuilderTest {

    private static final TreeVisitor PRUNER = new SupportedOperatorPruner(SUPPORTED_OPERATORS);
    private static final TreeTraverser TRAVERSER = new TreeTraverser();
    private List<ColumnDescriptor> columnDescriptors;

    @BeforeEach
    public void setup() {

        columnDescriptors = new ArrayList<>();
        columnDescriptors.add(new ColumnDescriptor("id", DataType.INTEGER.getOID(), 0, "int4", null));
        columnDescriptors.add(new ColumnDescriptor("cdate", DataType.DATE.getOID(), 1, "date", null));
        columnDescriptors.add(new ColumnDescriptor("amt", DataType.FLOAT8.getOID(), 2, "float8", null));
        columnDescriptors.add(new ColumnDescriptor("grade", DataType.TEXT.getOID(), 3, "text", null));
        columnDescriptors.add(new ColumnDescriptor("b", DataType.BOOLEAN.getOID(), 4, "bool", null));
        columnDescriptors.add(new ColumnDescriptor("col-char", DataType.BPCHAR.getOID(), 5, "char", null));
        columnDescriptors.add(new ColumnDescriptor("col-varchar", DataType.VARCHAR.getOID(), 6, "varchar", null));
        columnDescriptors.add(new ColumnDescriptor("col-numeric", DataType.NUMERIC.getOID(), 7, "numeric", null));
    }

    @Test
    public void testIsNotNull() throws Exception {
        // NOT (_1_ IS NULL)
        String filterString = "a1o8l2"; // ORCA transforms is not null to NOT ( a IS NULL )
        SearchArgument.Builder filterBuilder = helper(filterString, columnDescriptors);

        assertNotNull(filterBuilder);
        assertEquals("leaf-0 = (IS_NULL cdate), expr = (not leaf-0)", filterBuilder.build().toString());
    }

    @Test
    public void testIdFilter() throws Exception {
        // id = 1
        String filterString = "a0c20s1d1o5";

        SearchArgument.Builder filterBuilder = helper(filterString, columnDescriptors);
        assertNotNull(filterBuilder);
        // single filters are wrapped in and
        assertEquals("leaf-0 = (EQUALS id 1), expr = leaf-0", filterBuilder.build().toString());
    }

    @Test
    public void testDateAndAmtFilter() throws Exception {
        // cdate > '2008-02-01' and cdate < '2008-12-01' and amt > 1200
        String filterString = "a1c25s10d2008-02-01o2a1c25s10d2008-12-01o1l0a2c20s4d1200o2l0";

        SearchArgument.Builder filterBuilder = helper(filterString, columnDescriptors);
        assertNotNull(filterBuilder);
        assertEquals("leaf-0 = (LESS_THAN_EQUALS cdate 2008-02-01), leaf-1 = (LESS_THAN cdate 2008-12-01), leaf-2 = (LESS_THAN_EQUALS amt 1200), expr = (and (not leaf-0) leaf-1 (not leaf-2))", filterBuilder.build().toString());
    }

    @Test
    public void testDateWithOrAndAmtFilter() throws Exception {
        // cdate > '2008-02-01' OR (cdate < '2008-12-01' AND amt > 1200)
        String filterString = "a1c1082s10d2008-02-01o2a1c1082s10d2008-12-01o1a0c23s4d1200o2l0l1";

        SearchArgument.Builder filterBuilder = helper(filterString, columnDescriptors);
        assertNotNull(filterBuilder);
        assertEquals("leaf-0 = (LESS_THAN_EQUALS cdate 2008-02-01), leaf-1 = (LESS_THAN cdate 2008-12-01), leaf-2 = (LESS_THAN_EQUALS id 1200), expr = (and (or (not leaf-0) leaf-1) (or (not leaf-0) (not leaf-2)))", filterBuilder.build().toString());
    }

    @Test
    public void testDateOrAmtFilter() throws Exception {
        // cdate > '2008-02-01' or amt > 1200
        String filterString = "a1c25s10d2008-02-01o2a2c20s4d1200o2l1";

        SearchArgument.Builder filterBuilder = helper(filterString, columnDescriptors);
        assertNotNull(filterBuilder);
        assertEquals("leaf-0 = (LESS_THAN_EQUALS cdate 2008-02-01), leaf-1 = (LESS_THAN_EQUALS amt 1200), expr = (or (not leaf-0) (not leaf-1))", filterBuilder.build().toString());
    }

    @Test
    public void testIsNotNullOperator() throws Exception {
        // a3 IS NOT NULL
        String filterString = "a3o9";

        SearchArgument.Builder filterBuilder = helper(filterString, columnDescriptors);
        assertNotNull(filterBuilder);
        assertEquals("leaf-0 = (IS_NULL grade), expr = (not leaf-0)", filterBuilder.build().toString());
    }

    @Test
    public void testInOperatorWithSingleItem() {
        // grade IN 'bad'
        String filterString = "a3c25s3dbado10";

        Exception e = assertThrows(IllegalArgumentException.class,
                () -> helper(filterString, columnDescriptors));
        assertEquals("filterValue should be instance of List for IN operation", e.getMessage());
    }

    @Test
    public void testInOperator() throws Exception {
        // id IN (194 , 82756)
        String filterString = "a0m1016s3d194s5d82756o10";

        SearchArgument.Builder filterBuilder = helper(filterString, columnDescriptors);
        assertNotNull(filterBuilder);
        assertEquals("leaf-0 = (IN id 194 82756), expr = leaf-0", filterBuilder.build().toString());
    }

    @Test
    public void testNotBoolean() throws Exception {
        // NOT a4
        String filterString = "a4c16s4dtrueo0l2";

        SearchArgument.Builder filterBuilder = helper(filterString, columnDescriptors);
        assertNotNull(filterBuilder);
        assertEquals("leaf-0 = (EQUALS b true), expr = (not leaf-0)", filterBuilder.build().toString());
    }

    @Test
    public void testBoolean() throws Exception {
        // a4
        String filterString = "a4c16s4dtrueo0";

        SearchArgument.Builder filterBuilder = helper(filterString, columnDescriptors);
        assertNotNull(filterBuilder);
        assertEquals("leaf-0 = (EQUALS b true), expr = leaf-0", filterBuilder.build().toString());
    }

    @Test
    public void testNotInteger() throws Exception {
        // NOT a0 = 5
        String filterString = "a0c23s1d5o6";

        SearchArgument.Builder filterBuilder = helper(filterString, columnDescriptors);
        assertNotNull(filterBuilder);
        assertEquals("leaf-0 = (EQUALS id 5), expr = (not leaf-0)", filterBuilder.build().toString());
    }

    @Test
    public void testCharFilter() throws Exception {
        // col-char = ABC
        String filterString = "a5c1042s3dABCo5";

        SearchArgument.Builder filterBuilder = helper(filterString, columnDescriptors);
        assertNotNull(filterBuilder);
        assertEquals("leaf-0 = (EQUALS col-char ABC), expr = leaf-0", filterBuilder.build().toString());
    }
    @Test
    public void testCharFilterWithPadding() throws Exception {
        // col-char = ABC
        String filterString = "a5c1042s4dABC o5";

        SearchArgument.Builder filterBuilder = helper(filterString, columnDescriptors);
        assertNotNull(filterBuilder);
        assertEquals("leaf-0 = (EQUALS col-char ABC ), expr = leaf-0", filterBuilder.build().toString());
    }

    @Test
    public void testCharFilterWithPaddingWithTransformer() throws Exception {
        // col-char = ABC
        String filterString = "a5c1042s4dABC o5";

        SearchArgument.Builder filterBuilder = helperWithTransformer(filterString, columnDescriptors);
        assertNotNull(filterBuilder);
        assertEquals("leaf-0 = (EQUALS col-char ABC ), leaf-1 = (EQUALS col-char ABC), expr = (or leaf-0 leaf-1)", filterBuilder.build().toString());
    }

    @Test
    public void testVarcharFilter() throws Exception {
        // col-varchar = ABC
        String filterString = "a6c1043s3dABCo5";

        SearchArgument.Builder filterBuilder = helper(filterString, columnDescriptors);
        assertNotNull(filterBuilder);
        assertEquals("leaf-0 = (EQUALS col-varchar ABC), expr = leaf-0", filterBuilder.build().toString());
    }

    @Test
    public void testNumericFilter() throws Exception {
        // col-numeric = 123456789.01234567890123456789012345678 <== go beyond double precision to max of 38
        String filterString = "a7c1700s39d123456789.01234567890123456789012345678o5";

        SearchArgument.Builder filterBuilder = helper(filterString, columnDescriptors);
        assertNotNull(filterBuilder);
        // make sure we are not losing precision as before
        assertEquals("leaf-0 = (EQUALS col-numeric 123456789.01234567890123456789012345678), expr = leaf-0", filterBuilder.build().toString());
    }

    @Test
    public void testNumericFilterScaleOverflow() throws Exception {
        String filterString = "a7c1700s60d12345678901234567890123456789.012345678901234567890123456789o5";

        Exception e = assertThrows(IllegalStateException.class,
                () -> helper(filterString, columnDescriptors));
        assertEquals("failed to parse number data 12345678901234567890123456789.012345678901234567890123456789 for type NUMERIC", e.getMessage());
    }

    @Test
    public void testNumericFilterIntegerPartOverflow() {
        String filterString = "a7c1700s80d1234567890123456789012345678901234567890123456789.012345678901234567890123456789o5";

        Exception e = assertThrows(IllegalStateException.class,
                () -> helper(filterString, columnDescriptors));
        assertEquals("failed to parse number data 1234567890123456789012345678901234567890123456789.012345678901234567890123456789 for type NUMERIC", e.getMessage());
    }


    private SearchArgument.Builder helper(String filterString, List<ColumnDescriptor> columnDescriptors) throws Exception {
        SearchArgumentBuilder treeVisitor =
                new SearchArgumentBuilder(columnDescriptors, new Configuration());
        // Parse the filter string into a expression tree Node
        Node root = new FilterParser().parse(filterString);
        TRAVERSER.traverse(root, PRUNER, treeVisitor);
        return treeVisitor.getFilterBuilder();
    }

    private SearchArgument.Builder helperWithTransformer(String filterString, List<ColumnDescriptor> columnDescriptors) throws Exception {
        SearchArgumentBuilder treeVisitor =
                new SearchArgumentBuilder(columnDescriptors, new Configuration());
        // Parse the filter string into a expression tree Node
        Node root = new FilterParser().parse(filterString);
        TRAVERSER.traverse(root, PRUNER, new BPCharOperatorTransformer(columnDescriptors), treeVisitor);
        return treeVisitor.getFilterBuilder();
    }

}
