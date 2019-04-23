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
import org.greenplum.pxf.api.FilterParser.LogicalOperation;
import org.greenplum.pxf.api.LogicalFilter;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.junit.Test;

import org.greenplum.pxf.api.BasicFilter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.greenplum.pxf.api.FilterParser.Operation;
import static org.greenplum.pxf.api.FilterParser.Operation.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class HiveFilterBuilderTest {
    @Test
    public void parseFilterWithThreeOperations() throws Exception {
        HiveFilterBuilder builder = new HiveFilterBuilder();
        String[] consts = new String[]{"first", "2"};
        Operation[] ops = new Operation[]{HDOP_EQ, HDOP_GT};
        int[] idx = new int[]{1, 2};

        LogicalFilter filterList = (LogicalFilter) builder.getFilterObject("a1c25s5dfirsto5a2c20s1d2o2l0");
        assertEquals(LogicalOperation.HDOP_AND, filterList.getOperator());
        BasicFilter leftOperand = (BasicFilter) filterList.getFilterList().get(0);
        assertEquals(consts[0], leftOperand.getConstant().constant());
        assertEquals(idx[0], leftOperand.getColumn().index());
        assertEquals(ops[0], leftOperand.getOperation());
    }

    @Test
    public void testQueryWithNotOperator() throws Exception {
        Map<String, String> partitionKeyTypes = new HashMap<>();
        partitionKeyTypes.put("s2", "string");
        partitionKeyTypes.put("n1", "int");

        HiveFilterBuilder builder = new HiveFilterBuilder();
        builder.setColumnDescriptors(getColumnDescriptors());
        builder.setCanPushdownIntegral(true);
        builder.setPartitionKeys(getPartitionKeyTypes());

        assertNull(builder.buildFilterStringForHive("a3c23s1d4o5l2a2c25s3ds_9o5l2l1"));
        assertEquals("(stringColumn >= \"9.0\")", builder.buildFilterStringForHive("a1c701s1d9o4a3c23s1d4o5l2a2c25s3ds_9o5l2l1l0"));
    }

    @Test
    public void parseNullFilter() throws Exception {
        HiveFilterBuilder builder = new HiveFilterBuilder();
        LogicalFilter filterList = (LogicalFilter) builder.getFilterObject(null);
        assertNull(builder.getFilterObject(null));
    }

    @Test
    public void parseFilterWithLogicalOperation() throws Exception {
        HiveFilterBuilder builder = new HiveFilterBuilder();
        LogicalFilter filter = (LogicalFilter) builder.getFilterObject("a1c25s5dfirsto5a2c20s1d2o2l0");
        assertEquals(LogicalOperation.HDOP_AND, filter.getOperator());
        assertEquals(2, filter.getFilterList().size());
    }

    @Test
    public void parseNestedExpressionWithLogicalOperation() throws Exception {
        HiveFilterBuilder builder = new HiveFilterBuilder();
        LogicalFilter filter = (LogicalFilter) builder.getFilterObject("a1c25s5dfirsto5a2c20s1d2o2l0a1c20s1d1o1l1");
        assertEquals(LogicalOperation.HDOP_OR, filter.getOperator());
        assertEquals(LogicalOperation.HDOP_AND, ((LogicalFilter) filter.getFilterList().get(0)).getOperator());
        assertEquals(HDOP_LT, ((BasicFilter) filter.getFilterList().get(1)).getOperation());
    }

    @Test
    public void parseISNULLExpression() throws Exception {
        HiveFilterBuilder builder = new HiveFilterBuilder();
        BasicFilter filter = (BasicFilter) builder.getFilterObject("a1o8");
        assertEquals(Operation.HDOP_IS_NULL, filter.getOperation());
        assertEquals(1, filter.getColumn().index());
        assertNull(filter.getConstant());
    }

    @Test
    public void parseISNOTNULLExpression() throws Exception {
        HiveFilterBuilder builder = new HiveFilterBuilder();
        BasicFilter filter = (BasicFilter) builder.getFilterObject("a1o9");
        assertEquals(Operation.HDOP_IS_NOT_NULL, filter.getOperation());
        assertEquals(1, filter.getColumn().index());
        assertNull(filter.getConstant());
    }

    @Test
    public void testBuildFilterWithCompatibleAndIncompatiblePredicates() throws Exception {

        Map<String, String> partitionKeyTypes = new HashMap<>();
        partitionKeyTypes.put("stringColumn", "string");

        HiveFilterBuilder builder = new HiveFilterBuilder();
        builder.setColumnDescriptors(getColumnDescriptors());
        builder.setCanPushdownIntegral(true);
        builder.setPartitionKeys(getPartitionKeyTypes());

        assertEquals("(stringColumn = \"seq\")", builder.buildFilterStringForHive("a1c25s4drow1o7a2c23s3d999o1l0a1c25s3dseqo5l0"));
    }

    @Test
    public void testBuildSingleFilter() throws Exception {
        ColumnDescriptor columnDescriptor =
                new ColumnDescriptor("textColumn", 25, 3, "text", null, true);
        Map<String, String> partitionKeyTypes = new HashMap<>();
        partitionKeyTypes.put("textColumn", "string");

        HiveFilterBuilder builder = new HiveFilterBuilder();
        builder.setColumnDescriptors(Lists.newArrayList(null, null, null, columnDescriptor));
        builder.setCanPushdownIntegral(false);
        builder.setPartitionKeys(partitionKeyTypes);

        assertEquals("textColumn <> \"2016-01-03\"", builder.buildFilterStringForHive("a3c25s10d2016-01-03o6"));
        assertEquals("textColumn = \"2016-01-03\"", builder.buildFilterStringForHive("a3c25s10d2016-01-03o5"));
        assertEquals("textColumn >= \"2016-01-03\"", builder.buildFilterStringForHive("a3c25s10d2016-01-03o4"));
        assertEquals("textColumn <= \"2016-01-03\"", builder.buildFilterStringForHive("a3c25s10d2016-01-03o3"));
        assertEquals("textColumn > \"2016-01-03\"", builder.buildFilterStringForHive("a3c25s10d2016-01-03o2"));
        assertEquals("textColumn < \"2016-01-03\"", builder.buildFilterStringForHive("a3c25s10d2016-01-03o1"));
        assertNull(builder.buildFilterStringForHive("a3c25s10d2016-01-0%o7"));
    }

    @Test
    public void testIntegralPushdownTrue() throws Exception {

        HiveFilterBuilder builder = new HiveFilterBuilder();
        builder.setColumnDescriptors(getColumnDescriptors());
        builder.setCanPushdownIntegral(true);
        builder.setPartitionKeys(getPartitionKeyTypes());

        assertNull(builder.buildFilterStringForHive("a0c1082s10d2016-01-03o4"));
        assertNull(builder.buildFilterStringForHive("a0c1082s10d2016-01-03o5"));
        assertNull(builder.buildFilterStringForHive("a0c1082s10d2016-01-03o6"));

        assertEquals("stringColumn >= \"2016-01-03\"", builder.buildFilterStringForHive("a1c25s10d2016-01-03o4"));
        assertEquals("stringColumn = \"2016-01-03\"", builder.buildFilterStringForHive("a1c25s10d2016-01-03o5"));
        assertEquals("stringColumn <> \"2016-01-03\"", builder.buildFilterStringForHive("a1c25s10d2016-01-03o6"));

        // Don't support > for integral types
        assertNull(builder.buildFilterStringForHive("a2c23s3d126o4"));
        // Support = for integral types
        assertEquals("intColumn = \"126\"", builder.buildFilterStringForHive("a2c23s3d126o5"));
        // Support <> for integral types
        assertEquals("intColumn <> \"126\"", builder.buildFilterStringForHive("a2c23s3d126o6"));

        assertNull(builder.buildFilterStringForHive("a3c20s3d126o4"));
        assertEquals("bigIntColumn = \"126\"", builder.buildFilterStringForHive("a3c20s3d126o5"));
        assertEquals("bigIntColumn <> \"126\"", builder.buildFilterStringForHive("a3c20s3d126o6"));

        assertNull(builder.buildFilterStringForHive("a4c21s3d126o4"));
        assertEquals("smallIntColumn = \"126\"", builder.buildFilterStringForHive("a4c21s3d126o5"));
        assertEquals("smallIntColumn <> \"126\"", builder.buildFilterStringForHive("a4c21s3d126o6"));
    }

    @Test
    public void testIntegralPushdownFalse() throws Exception {

        HiveFilterBuilder builder = new HiveFilterBuilder();
        builder.setColumnDescriptors(getColumnDescriptors());
        builder.setCanPushdownIntegral(false);
        builder.setPartitionKeys(getPartitionKeyTypes());

        assertNull(builder.buildFilterStringForHive("a0c1082s10d2016-01-03o4"));
        assertNull(builder.buildFilterStringForHive("a0c1082s10d2016-01-03o5"));
        assertNull(builder.buildFilterStringForHive("a0c1082s10d2016-01-03o6"));

        assertEquals("stringColumn >= \"2016-01-03\"", builder.buildFilterStringForHive("a1c25s10d2016-01-03o4"));
        assertEquals("stringColumn = \"2016-01-03\"", builder.buildFilterStringForHive("a1c25s10d2016-01-03o5"));
        assertEquals("stringColumn <> \"2016-01-03\"", builder.buildFilterStringForHive("a1c25s10d2016-01-03o6"));

        // Integral types not supported
        assertNull(builder.buildFilterStringForHive("a2c23s3d126o4"));
        assertNull(builder.buildFilterStringForHive("a2c23s3d126o5"));
        assertNull(builder.buildFilterStringForHive("a2c23s3d126o6"));

        assertNull(builder.buildFilterStringForHive("a3c20s3d126o4"));
        assertNull(builder.buildFilterStringForHive("a3c20s3d126o5"));
        assertNull(builder.buildFilterStringForHive("a3c20s3d126o6"));

        assertNull(builder.buildFilterStringForHive("a4c21s3d126o4"));
        assertNull(builder.buildFilterStringForHive("a4c21s3d126o5"));
        assertNull(builder.buildFilterStringForHive("a4c21s3d126o6"));
    }

    @Test
    public void buildFilterStringForHiveWithLogicalOps() throws Exception {

        Map<String, String> partitionKeyTypes = new HashMap<>();
        partitionKeyTypes.put("stringColumn", "string");
        partitionKeyTypes.put("intColumn", "int");

        HiveFilterBuilder builder = new HiveFilterBuilder();
        builder.setColumnDescriptors(getColumnDescriptors());
        builder.setCanPushdownIntegral(true);
        builder.setPartitionKeys(partitionKeyTypes);

        // Terminology:
        // P is a predicate based on partition column
        // NP is a predicate based on either a non-partition column or an unsupported operator.

        // P1 AND P2 -> P1 AND P2
        assertEquals("(stringColumn = \"foobar\" AND intColumn <> \"999\")", builder.buildFilterStringForHive("a1c25s6dfoobaro5a2c23s3d999o6l0"));
        // P1 OR P2 -> P1 OR P2
        assertEquals("(stringColumn = \"foobar\" OR intColumn <> \"999\")", builder.buildFilterStringForHive("a1c25s6dfoobaro5a2c23s3d999o6l1"));
        // P1 AND NP1 -> P1
        assertEquals("(stringColumn = \"foobar\")", builder.buildFilterStringForHive("a1c25s6dfoobaro5a3c20s3d999o6l0"));
        // P1 OR NP1 -> null
        assertNull(builder.buildFilterStringForHive("a1c25s6dfoobaro5a3c20s3d999o6l1"));
        // NP1 OR NP2 -> null
        assertNull(builder.buildFilterStringForHive("a3c20s3d999o6a2c20s3d999o4l1"));
        // (P1 AND P2) OR NP1 -> null
        assertNull(builder.buildFilterStringForHive("a1c25s6dfoobaro5a2c23s3d999o6l0a2c20s3d999o4l1"));
        // (P1 AND P2) AND (P3 OR NP1) -> P1 AND P2
        assertEquals("((stringColumn = \"foobar\" AND intColumn <> \"999\"))", builder.buildFilterStringForHive("a1c25s6dfoobaro5a2c23s3d999o6l0a1c25s6dfoobaro5a3c20s3d999o6l1l0"));
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
}
