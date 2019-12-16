package org.greenplum.pxf.plugins.hive;

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


import com.google.common.collect.Lists;
import org.greenplum.pxf.api.filter.ColumnIndexOperand;
import org.greenplum.pxf.api.filter.FilterParser;
import org.greenplum.pxf.api.filter.Node;
import org.greenplum.pxf.api.filter.Operand;
import org.greenplum.pxf.api.filter.Operator;
import org.greenplum.pxf.api.filter.OperatorNode;
import org.greenplum.pxf.api.filter.TreeTraverser;
import org.greenplum.pxf.api.filter.TreeVisitor;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.greenplum.pxf.plugins.hive.HiveDataFragmenter.SUPPORTED_OPERATORS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class HivePartitionPrunerTest {

    private HivePartitionFilterBuilder visitor;
    private TreeVisitor treePruner;
    private FilterParser parser;
    private TreeTraverser treeTraverser;

    @Before
    public void setup() {
        treeTraverser = new TreeTraverser();
        parser = new FilterParser();
        visitor = new HivePartitionFilterBuilder(getColumnDescriptors());
        treePruner = new HivePartitionPruner(SUPPORTED_OPERATORS,
                false, getPartitionKeyTypes(), getColumnDescriptors());
    }

    @Test
    public void parseFilterWithThreeOperations() throws Exception {
        // (_1_ = first AND _2_ > 2)
        Node root = parser.parse("a1c25s5dfirsto5a2c20s1d2o2l0".getBytes());

        assertOperatorEquals(Operator.AND, root);
        assertNotNull(root.getChildren());
        assertEquals(2, root.getChildren().size());
        assertOperatorEquals(Operator.EQUALS, 1, "first", root.getChildren().get(0));
        assertOperatorEquals(Operator.GREATER_THAN, 2, "2", root.getChildren().get(1));
    }

    @Test
    public void testQueryWithNotOperator() throws Exception {
        treePruner = new HivePartitionPruner(SUPPORTED_OPERATORS,
                true, getPartitionKeyTypes(), getColumnDescriptors());

        // (NOT (_3_ = 4) OR NOT (_2_ = s_9))
        helper(null, "a3c23s1d4o5l2a2c25s3ds_9o5l2l1", treePruner, visitor);

        // (_1_ >= 9 AND (NOT (_3_ = 4) OR NOT (_2_ = s_9)))
        helper("stringColumn >= \"9\"", "a1c701s1d9o4a3c23s1d4o5l2a2c25s3ds_9o5l2l1l0", treePruner, visitor);
    }

    @Test
    public void parseFilterWithLogicalOperation() throws Exception {
        // (_1_ = first AND _2_ > 2)
        Node root = parser.parse("a1c25s5dfirsto5a2c20s1d2o2l0".getBytes());

        assertOperatorEquals(Operator.AND, root);
        assertNotNull(root.getChildren());
        assertEquals(2, root.getChildren().size());
    }

    @Test
    public void parseNestedExpressionWithLogicalOperation() throws Exception {
        // ((_1_ = first AND _2_ > 2) OR _1_ < 1)
        Node root = parser.parse("a1c25s5dfirsto5a2c20s1d2o2l0a1c20s1d1o1l1".getBytes());

        assertOperatorEquals(Operator.OR, root);
        assertNotNull(root.getChildren());
        assertEquals(2, root.getChildren().size());
        assertOperatorEquals(Operator.AND, root.getChildren().get(0));
        assertOperatorEquals(Operator.LESS_THAN, root.getChildren().get(1));
    }

    @Test
    public void parseIsNullExpression() throws Exception {
        // _1_ IS NULL
        Node root = parser.parse("a1o8".getBytes());
        assertOperatorEquals(Operator.IS_NULL, 1, root);
    }

    @Test
    public void parseUnsupportedIsNullExpression() throws Exception {
        // _1_ IS NULL
        Node root = parser.parse("a1o8".getBytes());
        root = treePruner.visit(root);
        assertNull(root);
    }

    @Test
    public void parseIsNotNullExpression() throws Exception {
        // _1_ IS NOT NULL
        Node root = parser.parse("a1o9".getBytes());
        assertOperatorEquals(Operator.IS_NOT_NULL, 1, root);
    }

    @Test
    public void parseUnsupportedIsNotNullExpression() throws Exception {
        // _1_ IS NOT NULL
        Node root = parser.parse("a1o9".getBytes());
        root = treePruner.visit(root);
        assertNull(root);
    }

    @Test
    public void testBuildFilterWithCompatibleAndIncompatiblePredicates() throws Exception {
        treePruner = new HivePartitionPruner(SUPPORTED_OPERATORS,
                true, getPartitionKeyTypes(), getColumnDescriptors());

        // ((_1_ LIKE row1 AND _2_ < 999) AND _1_ = seq)
        helper("stringColumn = \"seq\"",
                "a1c25s4drow1o7a2c23s3d999o1l0a1c25s3dseqo5l0",
                treePruner, visitor);
    }

    @Test
    public void testBuildSingleFilter() throws Exception {
        ColumnDescriptor columnDescriptor =
                new ColumnDescriptor("textColumn", 25, 3, "text", null, true);
        Map<String, String> partitionKeyTypes = new HashMap<>();
        partitionKeyTypes.put("textColumn", "string");

        List<ColumnDescriptor> columnDescriptors = Lists.newArrayList(null, null, null, columnDescriptor);
        visitor = new HivePartitionFilterBuilder(columnDescriptors);
        treePruner = new HivePartitionPruner(SUPPORTED_OPERATORS,
                false, partitionKeyTypes, columnDescriptors);

        // _3_ <> 2016-01-03
        helper("textColumn <> \"2016-01-03\"", "a3c25s10d2016-01-03o6", treePruner, visitor);
        // _3_ = 2016-01-03
        helper("textColumn = \"2016-01-03\"", "a3c25s10d2016-01-03o5", treePruner, visitor);
        // _3_ >= 2016-01-03
        helper("textColumn >= \"2016-01-03\"", "a3c25s10d2016-01-03o4", treePruner, visitor);
        // _3_ <= 2016-01-03
        helper("textColumn <= \"2016-01-03\"", "a3c25s10d2016-01-03o3", treePruner, visitor);
        // _3_ > 2016-01-03
        helper("textColumn > \"2016-01-03\"", "a3c25s10d2016-01-03o2", treePruner, visitor);
        // _3_ < 2016-01-03
        helper("textColumn < \"2016-01-03\"", "a3c25s10d2016-01-03o1", treePruner, visitor);
        // _3_ LIKE 2016-01-0%
        helper(null, "a3c25s10d2016-01-0%o7", treePruner, visitor);
    }

    @Test
    public void testIntegralPushDownTrue() throws Exception {

        treePruner = new HivePartitionPruner(SUPPORTED_OPERATORS,
                true, getPartitionKeyTypes(), getColumnDescriptors());

        // _0_ >= 2016-01-03
        helper(null, "a0c1082s10d2016-01-03o4", treePruner, visitor);
        // _0_ = 2016-01-03
        helper(null, "a0c1082s10d2016-01-03o5", treePruner, visitor);
        // _0_ <> 2016-01-03
        helper(null, "a0c1082s10d2016-01-03o6", treePruner, visitor);

        // _1_ >= 2016-01-03
        helper("stringColumn >= \"2016-01-03\"", "a1c25s10d2016-01-03o4", treePruner, visitor);
        // _1_ = 2016-01-03
        helper("stringColumn = \"2016-01-03\"", "a1c25s10d2016-01-03o5", treePruner, visitor);
        // _1_ <> 2016-01-03
        helper("stringColumn <> \"2016-01-03\"", "a1c25s10d2016-01-03o6", treePruner, visitor);

        // Don't support > for integral types: _2_ >= 126
        helper(null, "a2c23s3d126o4", treePruner, visitor);
        // Support = for integral types: _2_ = 126
        helper("intColumn = \"126\"", "a2c23s3d126o5", treePruner, visitor);
        // Support <> for integral types: _2_ <> 126
        helper("intColumn <> \"126\"", "a2c23s3d126o6", treePruner, visitor);

        // _3_ >= 126
        helper(null, "a3c20s3d126o4", treePruner, visitor);
        // _3_ = 126
        helper("bigIntColumn = \"126\"", "a3c20s3d126o5", treePruner, visitor);
        // _3_ <> 126
        helper("bigIntColumn <> \"126\"", "a3c20s3d126o6", treePruner, visitor);

        // _4_ >= 126
        helper(null, "a4c21s3d126o4", treePruner, visitor);
        // _4_ = 126
        helper("smallIntColumn = \"126\"", "a4c21s3d126o5", treePruner, visitor);
        // _4_ <> 126
        helper("smallIntColumn <> \"126\"", "a4c21s3d126o6", treePruner, visitor);
    }

    @Test
    public void testIntegralPushDownFalse() throws Exception {
        treePruner = new HivePartitionPruner(SUPPORTED_OPERATORS,
                false, getPartitionKeyTypes(), getColumnDescriptors());

        // _0_ >= 2016-01-03
        helper(null, "a0c1082s10d2016-01-03o4", treePruner, visitor);
        // _0_ = 2016-01-03
        helper(null, "a0c1082s10d2016-01-03o5", treePruner, visitor);
        // _0_ <> 2016-01-03
        helper(null, "a0c1082s10d2016-01-03o6", treePruner, visitor);

        // _1_ >= 2016-01-03
        helper("stringColumn >= \"2016-01-03\"", "a1c25s10d2016-01-03o4", treePruner, visitor);
        // _1_ = 2016-01-03
        helper("stringColumn = \"2016-01-03\"", "a1c25s10d2016-01-03o5", treePruner, visitor);
        // _1_ <> 2016-01-03
        helper("stringColumn <> \"2016-01-03\"", "a1c25s10d2016-01-03o6", treePruner, visitor);

        // Integral types not supported

        // _2_ >= 126
        helper(null, "a2c23s3d126o4", treePruner, visitor);
        // _2_ = 126
        helper(null, "a2c23s3d126o5", treePruner, visitor);
        // _2_ <> 126
        helper(null, "a2c23s3d126o6", treePruner, visitor);

        // _3_ >= 126
        helper(null, "a3c20s3d126o4", treePruner, visitor);
        // _3_ = 126
        helper(null, "a3c20s3d126o5", treePruner, visitor);
        // _3_ <> 126
        helper(null, "a3c20s3d126o6", treePruner, visitor);

        // _4_ >= 126
        helper(null, "a4c21s3d126o4", treePruner, visitor);
        // _4_ = 126
        helper(null, "a4c21s3d126o5", treePruner, visitor);
        // _4_ <> 126
        helper(null, "a4c21s3d126o6", treePruner, visitor);
    }

    @Test
    public void buildFilterStringWithLogicalOps() throws Exception {

        Map<String, String> partitionKeyTypes = new HashMap<>();
        partitionKeyTypes.put("stringColumn", "string");
        partitionKeyTypes.put("intColumn", "int");

        treePruner = new HivePartitionPruner(SUPPORTED_OPERATORS,
                true, partitionKeyTypes, getColumnDescriptors());

        // Terminology:
        // P is a predicate based on partition column
        // NP is a predicate based on either a non-partition column or an unsupported operator.

        // P1 AND P2 -> P1 AND P2: (_1_ = foobar AND _2_ <> 999)
        helper("(stringColumn = \"foobar\" AND intColumn <> \"999\")", "a1c25s6dfoobaro5a2c23s3d999o6l0", treePruner, visitor);
        // P1 OR P2 -> P1 OR P2: (_1_ = foobar OR _2_ <> 999)
        helper("(stringColumn = \"foobar\" OR intColumn <> \"999\")", "a1c25s6dfoobaro5a2c23s3d999o6l1", treePruner, visitor);
        // P1 AND NP1 -> P1: (_1_ = foobar AND _3_ <> 999)
        helper("stringColumn = \"foobar\"", "a1c25s6dfoobaro5a3c20s3d999o6l0", treePruner, visitor);
        // P1 OR NP1 -> null: (_1_ = foobar OR _3_ <> 999)
        helper(null, "a1c25s6dfoobaro5a3c20s3d999o6l1", treePruner, visitor);
        // NP1 OR NP2 -> null: (_3_ <> 999 OR _2_ >= 999)
        helper(null, "a3c20s3d999o6a2c20s3d999o4l1", treePruner, visitor);
        // (P1 AND P2) OR NP1 -> null: ((_1_ = foobar AND _2_ <> 999) OR _2_ >= 999)
        helper(null, "a1c25s6dfoobaro5a2c23s3d999o6l0a2c20s3d999o4l1", treePruner, visitor);
        // (P1 AND P2) AND (P3 OR NP1) -> P1 AND P2: ((_1_ = foobar AND _2_ <> 999) AND (_1_ = foobar OR _3_ <> 999))
        helper("(stringColumn = \"foobar\" AND intColumn <> \"999\")", "a1c25s6dfoobaro5a2c23s3d999o6l0a1c25s6dfoobaro5a3c20s3d999o6l1l0", treePruner, visitor);
    }

    private List<ColumnDescriptor> getColumnDescriptors() {
        ColumnDescriptor dateColumnDescriptor =
                new ColumnDescriptor("dateColumn", 1082, 0, "date", null, true);
        ColumnDescriptor stringColumnDescriptor =
                new ColumnDescriptor("stringColumn", 25, 1, "string", null, true);
        ColumnDescriptor intColumnDescriptor =
                new ColumnDescriptor("intColumn", 23, 2, "int", null, true);
        ColumnDescriptor bigIntColumnDescriptor =
                new ColumnDescriptor("bigIntColumn", 20, 3, "bigint", null, true);
        ColumnDescriptor smallIntColumnDescriptor =
                new ColumnDescriptor("smallIntColumn", 21, 4, "smallint", null, true);
        List<ColumnDescriptor> columnDescriptors = new ArrayList<>();

        columnDescriptors.add(dateColumnDescriptor);
        columnDescriptors.add(stringColumnDescriptor);
        columnDescriptors.add(intColumnDescriptor);
        columnDescriptors.add(bigIntColumnDescriptor);
        columnDescriptors.add(smallIntColumnDescriptor);

        return columnDescriptors;
    }

    private Map<String, String> getPartitionKeyTypes() {
        Map<String, String> partitionKeyTypes = new HashMap<>();
        partitionKeyTypes.put("dateColumn", "date");
        partitionKeyTypes.put("stringColumn", "string");
        partitionKeyTypes.put("intColumn", "int");
        partitionKeyTypes.put("bigIntColumn", "bigint");
        partitionKeyTypes.put("smallIntColumn", "smallint");

        return partitionKeyTypes;
    }

    private void helper(String expected, String filterString, TreeVisitor pruner, HivePartitionFilterBuilder treeVisitor) throws Exception {
        Node root = parser.parse(filterString.getBytes());
        root = pruner.visit(root);
        treeTraverser.inOrderTraversal(root, treeVisitor);
        assertEquals(expected, treeVisitor.toString());
        treeVisitor.reset();
    }

    private void assertOperatorEquals(Operator operator, Node node) {
        assertNotNull(node);
        assertTrue(node instanceof OperatorNode);
        assertEquals(operator, ((OperatorNode) node).getOperator());
    }

    /**
     * for non-logical operators
     */
    private void assertOperatorEquals(Operator operator, int columnIndex, String expectedValue, Node node) {
        assertNotNull(node);
        assertTrue(node instanceof OperatorNode);
        OperatorNode operatorNode = (OperatorNode) node;
        assertFalse(operator.isLogical());
        assertEquals(operator, operatorNode.getOperator());
        assertNotNull(node.getChildren());
        assertEquals(2, node.getChildren().size());
        assertEquals(ColumnIndexOperand.class, node.getChildren().get(0).getClass());
        assertEquals(columnIndex, ((ColumnIndexOperand) node.getChildren().get(0)).index());
        assertTrue(node.getChildren().get(1) instanceof Operand);
        assertEquals(expectedValue, node.getChildren().get(1).toString());
    }

    /**
     * for non-logical operators with a single node child
     */
    private void assertOperatorEquals(Operator operator, int columnIndex, Node node) {
        assertNotNull(node);
        assertTrue(node instanceof OperatorNode);
        OperatorNode operatorNode = (OperatorNode) node;
        assertFalse(operator.isLogical());
        assertEquals(operator, operatorNode.getOperator());
        assertNotNull(node.getChildren());
        assertEquals(1, node.getChildren().size());
        assertEquals(ColumnIndexOperand.class, node.getChildren().get(0).getClass());
        assertEquals(columnIndex, ((ColumnIndexOperand) node.getChildren().get(0)).index());
    }
}
