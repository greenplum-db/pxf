package org.greenplum.pxf.plugins.hbase;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.NullComparator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.greenplum.pxf.api.filter.FilterParser;
import org.greenplum.pxf.api.filter.Node;
import org.greenplum.pxf.api.filter.SupportedOperatorPruner;
import org.greenplum.pxf.api.filter.TreeTraverser;
import org.greenplum.pxf.api.filter.TreeVisitor;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.plugins.hbase.utilities.HBaseColumnDescriptor;
import org.greenplum.pxf.plugins.hbase.utilities.HBaseIntegerComparator;
import org.greenplum.pxf.plugins.hbase.utilities.HBaseTupleDescription;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.greenplum.pxf.plugins.hbase.HBaseAccessor.SUPPORTED_OPERATORS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HBaseFilterBuilderTest {

    private static TreeVisitor treePruner = new SupportedOperatorPruner(SUPPORTED_OPERATORS);
    private static TreeTraverser treeTraverser = new TreeTraverser();

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private HBaseTupleDescription tupleDescription;
    private byte[][] families = new byte[][]{
            new byte[]{},
            new byte[]{},
            new byte[]{},
            new byte[]{},
    };
    private byte[][] qualifiers = new byte[][]{
            new byte[]{},
            new byte[]{},
            new byte[]{},
            new byte[]{},
    };
    private int[] columnCodes = {
            DataType.INTEGER.getOID(),
            DataType.TEXT.getOID(),
            DataType.REAL.getOID(),
            DataType.TEXT.getOID(),
    };

    @Before
    public void setup() {
        tupleDescription = mock(HBaseTupleDescription.class);
        for (int i = 0; i < families.length; i++) {
            HBaseColumnDescriptor column = mock(HBaseColumnDescriptor.class);
            when(tupleDescription.getColumn(i)).thenReturn(column);
            when(column.columnFamilyBytes()).thenReturn(families[i]);
            when(column.qualifierBytes()).thenReturn(qualifiers[i]);
            when(column.columnTypeCode()).thenReturn(columnCodes[i]);
        }
    }

    @Test
    public void parseNotExpressionIgnored() throws Exception {
        assertNull(helper("a4c16s4dtrueo0l2", null));
    }

    @Test
    public void parseNotOpCodeInConstant() throws Exception {
        thrown.expect(NullPointerException.class);

        String filter = "a1c25s2dl2o1a1c20s1d2o2l0";
        // Testing that we get past the parsing stage
        // Very crude but it avoids instantiating all the necessary dependencies
        assertNull(helper(filter, null));
    }

    @Test
    public void parseIsNullExpression() throws Exception {
        Filter filter = helper("a1o8", tupleDescription);
        assertTrue(filter instanceof SingleColumnValueFilter);

        SingleColumnValueFilter result = (SingleColumnValueFilter) filter;
        assertNotNull(result);
        assertSame(families[1], result.getFamily());
        assertSame(qualifiers[1], result.getQualifier());
        assertEquals(CompareFilter.CompareOp.EQUAL, result.getOperator());
        assertTrue(result.getComparator() instanceof NullComparator);
    }

    @Test
    public void parseIsNotNullExpression() throws Exception {
        Filter filter = helper("a1o9", tupleDescription);
        assertTrue(filter instanceof SingleColumnValueFilter);

        SingleColumnValueFilter result = (SingleColumnValueFilter) filter;
        assertNotNull(result);
        assertSame(families[1], result.getFamily());
        assertSame(qualifiers[1], result.getQualifier());
        assertEquals(CompareFilter.CompareOp.NOT_EQUAL, result.getOperator());
        assertTrue(result.getComparator() instanceof NullComparator);
    }

    @Test
    public void testSimpleColumnOperator() throws Exception {
        // id > 5
        Filter filter = helper("a0c20s1d5o2", tupleDescription);

        assertNotNull(filter);
        assertTrue(filter instanceof SingleColumnValueFilter);
        SingleColumnValueFilter scvFilter = (SingleColumnValueFilter) filter;
        assertSame(families[0], scvFilter.getFamily());
        assertSame(qualifiers[0], scvFilter.getQualifier());
        assertEquals(CompareFilter.CompareOp.GREATER, scvFilter.getOperator());
        assertTrue(scvFilter.getComparator() instanceof HBaseIntegerComparator);
        assertEquals(0, scvFilter.getComparator().compareTo("5".getBytes()));
    }

    @Test
    public void testInOperator() throws Exception {
        // IN 'bad'
        Filter filter = helper("a3c25s3dbado10", null);
        assertNull(filter);
    }

    @Test
    public void testOrOperator() throws Exception {
        // a1 > '2008-02-01' or a2 > 1200
        Filter filter = helper("a1c25s10d2008-02-01o2a2c20s4d1200o2l1", tupleDescription);
        assertNotNull(filter);
        assertTrue(filter instanceof FilterList);
        FilterList filterList = (FilterList) filter;
        assertEquals(FilterList.Operator.MUST_PASS_ONE, filterList.getOperator());

        assertNotNull(filterList.getFilters());
        assertEquals(2, filterList.getFilters().size());

        Filter left = filterList.getFilters().get(0);
        Filter right = filterList.getFilters().get(1);
        assertTrue(left instanceof SingleColumnValueFilter);
        assertTrue(right instanceof SingleColumnValueFilter);

        SingleColumnValueFilter scvFilterLeft = (SingleColumnValueFilter) left;
        SingleColumnValueFilter scvFilterRight = (SingleColumnValueFilter) right;

        assertEquals(families[1], scvFilterLeft.getFamily());
        assertEquals(qualifiers[1], scvFilterLeft.getQualifier());
        assertEquals(CompareFilter.CompareOp.GREATER, scvFilterLeft.getOperator());
        assertEquals(0, scvFilterLeft.getComparator().compareTo("2008-02-01".getBytes()));

        assertEquals(families[2], scvFilterRight.getFamily());
        assertEquals(qualifiers[2], scvFilterRight.getQualifier());
        assertEquals(CompareFilter.CompareOp.GREATER, scvFilterRight.getOperator());
        assertEquals(0, scvFilterRight.getComparator().compareTo("1200".getBytes()));
    }

    @Test
    public void testFilterWithIncompatiblePredicate() throws Exception {
        // ((_1_ LIKE row1 AND _2_ < 999) AND _1_ = seq)
        Filter filter = helper("a1c25s4drow1o7a2c23s3d999o1l0a1c25s3dseqo5l0", tupleDescription);
        assertNotNull(filter);
        assertTrue(filter instanceof FilterList);
        FilterList filterList = (FilterList) filter;
        assertEquals(FilterList.Operator.MUST_PASS_ALL, filterList.getOperator());

        // LIKE is not supported so it gets dropped
        assertNotNull(filterList.getFilters());
        assertEquals(2, filterList.getFilters().size());

        Filter left = filterList.getFilters().get(0);
        Filter right = filterList.getFilters().get(1);
        assertTrue(left instanceof SingleColumnValueFilter);
        assertTrue(right instanceof SingleColumnValueFilter);

        SingleColumnValueFilter scvFilterLeft = (SingleColumnValueFilter) left;
        SingleColumnValueFilter scvFilterRight = (SingleColumnValueFilter) right;

        assertEquals(families[2], scvFilterLeft.getFamily());
        assertEquals(qualifiers[2], scvFilterLeft.getQualifier());
        assertEquals(CompareFilter.CompareOp.LESS, scvFilterLeft.getOperator());
        assertEquals(0, scvFilterLeft.getComparator().compareTo("999".getBytes()));

        assertEquals(families[1], scvFilterRight.getFamily());
        assertEquals(qualifiers[1], scvFilterRight.getQualifier());
        assertEquals(CompareFilter.CompareOp.EQUAL, scvFilterRight.getOperator());
        assertEquals(0, scvFilterRight.getComparator().compareTo("seq".getBytes()));
    }

    private Filter helper(String filterString, HBaseTupleDescription desc) throws Exception {
        HBaseFilterBuilder hBaseFilterBuilder = new HBaseFilterBuilder(desc);
        Node root = new FilterParser().parse(filterString.getBytes());
        root = treePruner.visit(root);
        treeTraverser.postOrderTraversal(root, hBaseFilterBuilder);

        return hBaseFilterBuilder.buildFilter();
    }
}