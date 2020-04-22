package org.greenplum.pxf.api.filter;

import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

class ColumnPredicateBuilderTest {

    private List<ColumnDescriptor> columnDescriptors;
    private TreeTraverser treeTraverser;

    @BeforeEach
    public void setup() {
        columnDescriptors = new ArrayList<>();
        columnDescriptors.add(new ColumnDescriptor("id", DataType.INTEGER.getOID(), 0, "int4", null));
        columnDescriptors.add(new ColumnDescriptor("cdate", DataType.DATE.getOID(), 1, "date", null));
        columnDescriptors.add(new ColumnDescriptor("amt", DataType.FLOAT8.getOID(), 2, "float8", null));
        columnDescriptors.add(new ColumnDescriptor("grade", DataType.TEXT.getOID(), 3, "text", null));
        columnDescriptors.add(new ColumnDescriptor("b", DataType.BOOLEAN.getOID(), 4, "bool", null));

        treeTraverser = new TreeTraverser();
    }

    @Test
    public void testGetColumnDescriptors() {
        ColumnPredicateBuilder columnPredicateBuilder = new ColumnPredicateBuilder(columnDescriptors);
        assertSame(columnDescriptors, columnPredicateBuilder.getColumnDescriptors());
    }

    @Test
    public void testIdFilter() throws Exception {
        // id = 1
        String filterString = "a0c20s1d1o5";

        String query = helper(filterString, columnDescriptors);
        assertEquals("id = 1", query);
    }

    @Test
    public void testDateAndAmtFilter() throws Exception {
        // cdate > '2008-02-01' and cdate < '2008-12-01' and amt > 1200
        String filterString = "a1c25s10d2008-02-01o2a1c25s10d2008-12-01o1l0a2c20s4d1200o2l0";

        String query = helper(filterString, columnDescriptors);
        assertEquals("((cdate > 2008-02-01 AND cdate < 2008-12-01) AND amt > 1200)", query);
    }

    @Test
    public void testDateWithOrAndAmtFilter() throws Exception {
        // cdate > '2008-02-01' OR (cdate < '2008-12-01' AND amt > 1200)
        String filterString = "a1c1082s10d2008-02-01o2a1c1082s10d2008-12-01o1a0c23s4d1200o2l0l1";

        String query = helper(filterString, columnDescriptors);
        assertEquals("(cdate > 2008-02-01 OR (cdate < 2008-12-01 AND id > 1200))", query);
    }

    @Test
    public void testDateOrAmtFilter() throws Exception {
        // cdate > '2008-02-01' or amt > 1200
        String filterString = "a1c25s10d2008-02-01o2a2c20s4d1200o2l1";

        String query = helper(filterString, columnDescriptors);
        assertEquals("(cdate > 2008-02-01 OR amt > 1200)", query);
    }

    @Test
    public void testIsNotNullOperator() throws Exception {
        // a3 IS NOT NULL
        String filterString = "a3o9";

        String query = helper(filterString, columnDescriptors);
        assertEquals("grade IS NOT NULL", query);
    }

    @Test
    public void testInOperatorWithSingleItem() throws Exception {
        // grade IN 'bad'
        String filterString = "a3c25s3dbado10";

        String query = helper(filterString, columnDescriptors);
        assertEquals("grade IN bad", query);
    }

    @Test
    public void testInOperator() throws Exception {
        // id IN (194 , 82756)
        String filterString = "a0m1016s3d194s5d82756o10";

        String query = helper(filterString, columnDescriptors);
        assertEquals("id IN (194,82756)", query);
    }

    @Test
    public void testIdFilterWithQuotes() throws Exception {
        // id = 1
        String filterString = "a0c20s1d1o5";

        String query = helper(filterString, columnDescriptors, "\"");
        assertEquals("\"id\" = 1", query);
    }

    @Test
    public void testIdFilterWithBackticks() throws Exception {
        // id = 1
        String filterString = "a0c20s1d1o5";

        String query = helper(filterString, columnDescriptors, "`");
        assertEquals("`id` = 1", query);
    }

    @Test
    public void testNotBoolean() throws Exception {
        // NOT a4
        String filterString = "a4c16s4dtrueo0l2";

        String query = helper(filterString, columnDescriptors);
        assertEquals("NOT (b)", query);
    }

    private String helper(String filterString, List<ColumnDescriptor> columnDescriptors) throws Exception {
        return helper(filterString, columnDescriptors, null);
    }

    private String helper(String filterString, List<ColumnDescriptor> columnDescriptors, String quoteString) throws Exception {
        Node root = new FilterParser().parse(filterString);
        ColumnPredicateBuilder columnPredicateBuilder;
        if (quoteString != null) {
            columnPredicateBuilder = new ColumnPredicateBuilder(quoteString, columnDescriptors);
        } else {
            columnPredicateBuilder = new ColumnPredicateBuilder(columnDescriptors);
        }
        treeTraverser.traverse(root, columnPredicateBuilder);

        return columnPredicateBuilder.toString();
    }
}
