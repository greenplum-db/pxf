package org.greenplum.pxf.api.filter;

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


import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.greenplum.pxf.api.filter.Operator.*;
import static org.junit.jupiter.api.Assertions.*;


public class FilterParserTest {

    private FilterParser filterParser;
    private String filter, exception;

    @BeforeEach
    public void setUp() {
        filterParser = new FilterParser();
    }

    @Test
    public void testNullFilterString() {
        Exception ex = assertThrows(
            FilterParser.FilterStringSyntaxException.class,
            () -> filterParser.parse(null));
        assertEquals("filter parsing ended with no result (null filter string)", ex.getMessage());
    }

    @Test
    public void testLongIndexFails() throws Exception {
        filter = "a2147483647o8";
        filterParser.parse(filter); // succeeds because it's a valid int32

        filter = "a2147483648o8";

        Exception ex = assertThrows(
            FilterParser.FilterStringSyntaxException.class,
            () -> filterParser.parse(filter));
        assertEquals("value 2147483648 larger than intmax ending at 11 (filter string: 'a2147483648o8')", ex.getMessage());
    }

    @Test
    public void parseNegativeEmpty() {
        filter = "";
        runParseNegative("empty string", filter, "filter parsing ended with no result");
    }

    @Test
    public void parseNegativeNotOperand() {
        filter = "g is not an operand";
        int index = 0;
        char op = filter.charAt(index);

        runParseNegative("illegal operand g", filter,
            "unknown opcode " + op + "(" + (int) op + ") at " + index);
    }

    @Test
    public void parseNegativeBadNumber() {

        filter = "a";
        int index = 1;
        exception = "numeric argument expected at " + index;

        runParseNegative("numeric operand with no number", filter, exception);

        filter = "aa";
        exception = "numeric argument expected at " + index;

        runParseNegative("numeric operand with non-number value", filter, exception);

        filter = "a12345678901234567890123456789";
        exception = "invalid numeric argument 12345678901234567890123456789";

        runParseNegative("numeric operand with too big number", filter, exception);

        filter = "a-12345678901234567890";
        exception = "invalid numeric argument -12345678901234567890";

        runParseNegative("numeric operand with too big negative number", filter, exception);

        filter = "a12345678901223456";
        exception = "value 12345678901223456 larger than intmax ending at " + filter.length();

        runParseNegative("numeric operand with long value", filter, exception);

        filter = "a-12345678901223456";
        exception = "value -12345678901223456 larger than intmax ending at " + filter.length();

        runParseNegative("numeric operand with negative long value", filter, exception);
    }

    @Test
    public void parseNegativeBadConst() {
        filter = "cs";
        int index = 1;
        exception = "datatype OID should follow at " + index;
        runParseNegative("const operand with no datatype", filter, exception);

        filter = "c5";
        exception = "invalid DataType OID at " + index;
        runParseNegative("const operand with invalid datatype oid", filter, exception);

        filter = "c20x";
        index = 3;
        exception = "data length delimiter 's' expected at " + index;
        runParseNegative("const operand with missing 's' delimiter", filter, exception);

        filter = "c20sd";
        index = 4;
        exception = "numeric argument expected at " + index;
        runParseNegative("const operand with missing numeric data length", filter, exception);

        filter = "c20s1500";
        index = 8;
        exception = "data size larger than filter string starting at " + index;
        runParseNegative("const operand with missing 'd' delimiter", filter, exception);

        filter = "c20s1x";
        index = 5;
        exception = "data delimiter 'd' expected at " + index;
        runParseNegative("const operand with missing 'd' delimiter", filter, exception);

        filter = "c20s5d";
        index = 5;
        exception = "data size larger than filter string starting at " + index;
        runParseNegative("const operand with missing data", filter, exception);

        filter = "c20s3ds9r";
        exception = "filter parsing failed, missing operators?";
        runParseNegative("const operand with an invalid value", filter, exception);

        filter = "m1122";
        index = 4;
        exception = "invalid DataType OID at " + index;
        runParseNegative("const operand with an invalid datatype", filter, exception);

        filter = "m20";
        exception = "expected non-scalar datatype, but got datatype with oid = 20";
        runParseNegative("const operand with an scalar datatype instead of list", filter, exception);

        filter = "m1007s1d1s1d2s2d3";
        exception = "filter string is shorter than expected";
        runParseNegative("const operand with list datatype, and \"d\" tag has less data than indicated in \"s\" tag", filter, exception);

        filter = "m1007s1d1s1d2s2d123";
        index = 18;
        exception = "unknown opcode 3(51) at " + index;
        runParseNegative("const operand with list datatype, and \"d\" tag has more data than indicated in \"s\" tag", filter, exception);
    }

    @Test
    public void parseNegativeBadOperation() {
        filter = "o";
        int index = 1;
        exception = "numeric argument expected at " + index;
        runParseNegative("operation with no value", filter, exception);

        filter = "ohno";
        exception = "numeric argument expected at " + index;
        runParseNegative("operation with no number", filter, exception);

        filter = "o100";
        index = 4;
        exception = "unknown op ending at " + index;
        runParseNegative("operation with out of bounds number", filter, exception);
    }

    @Test
    public void parseNegativeNoOperator() {

        filter = "a1234567890";
        runParseNegative("filter with only column", filter, "filter parsing failed, missing operators?");

        filter = "c20s1d1";
        runParseNegative("filter with only numeric const", filter, "filter parsing failed, missing operators?");
    }

    @Test
    public void parseEmptyString() {
        filter = "c25s0d";
        exception = "filter parsing failed, missing operators?";
        runParseNegative("const operand with empty string", filter, exception);
    }

    @Test
    public void parseDecimalValues() {
        filter = "c700s3d9.0";
        exception = "filter parsing failed, missing operators?";
        runParseNegative("const operand with decimal value", filter, exception);

        filter = "c701s7d10.0001";
        exception = "filter parsing failed, missing operators?";
        runParseNegative("const operand with decimal value", filter, exception);
    }

    @Test
    public void parseNegativeValues() {
        filter = "c700s3d-90";
        exception = "filter parsing failed, missing operators?";
        runParseNegative("const operand with decimal value", filter, exception);

        filter = "c701s8d-10.0001";
        exception = "filter parsing failed, missing operators?";
        runParseNegative("const operand with decimal value", filter, exception);
    }

    @Test
    public void parseNegativeTwoParams() {

        filter = "c20s1d1c20s1d1";
        exception = "Stack not empty, missing operators?";
        runParseNegative("filter with two consts in a row", filter, exception);

        filter = "c20s1d1a1";
        exception = "Stack not empty, missing operators?";
        runParseNegative("filter with const and attribute", filter, exception);

        filter = "a1c700s1d1";
        exception = "Stack not empty, missing operators?";
        runParseNegative("filter with attribute and const", filter, exception);
    }

    @Test
    public void parseNegativeOperationFirst() {

        filter = "o1a3";
        int index = 2;
        Operator operator = LESS_THAN;
        exception = "missing operands for op " + operator + " at " + index;
        runParseNegative("filter with operation first", filter, exception);

        filter = "a2o1";
        index = 4;
        exception = "missing operands for op " + operator + " at " + index;
        runParseNegative("filter with only attribute before operation", filter, exception);
    }

    @Test
    public void parseColumnOnLeft() throws Exception {

        filter = "a1c20s1d1o1";
        Operator op = LESS_THAN;
        runParseOneOperation(filter, op);

        filter = "a1c20s1d1o2";
        op = GREATER_THAN;
        runParseOneOperation(filter, op);

        filter = "a1c20s1d1o3";
        op = LESS_THAN_OR_EQUAL;
        runParseOneOperation(filter, op);

        filter = "a1c20s1d1o4";
        op = GREATER_THAN_OR_EQUAL;
        runParseOneOperation(filter, op);

        filter = "a1c20s1d1o5";
        op = EQUALS;
        runParseOneOperation(filter, op);

        filter = "a1c20s1d1o6";
        op = NOT_EQUALS;
        runParseOneOperation(filter, op);

        filter = "a1c20s1d1o7";
        op = LIKE;
        runParseOneOperation(filter, op);

        filter = "a1o8";
        op = IS_NULL;
        runParseOneUnaryOperation(filter, op);

        filter = "a1o9";
        op = Operator.IS_NOT_NULL;
        runParseOneUnaryOperation(filter, op);

        filter = "a1m1005s1d1s1d2s1d3o10";
        op = Operator.IN;
        runParseOneOperation(filter, op);
    }

    @Test
    public void parseColumnOnRight() throws Exception {

        filter = "c20s1d1a1o1";
        Operator op = GREATER_THAN;
        runParseOneOperation(filter, op);

        filter = "c20s1d1a1o2";
        op = LESS_THAN;
        runParseOneOperation(filter, op);

        filter = "c20s1d1a1o3";
        op = GREATER_THAN_OR_EQUAL;
        runParseOneOperation(filter, op);

        filter = "c20s1d1a1o4";
        op = LESS_THAN_OR_EQUAL;
        runParseOneOperation(filter, op);

        filter = "c20s1d1a1o5";
        op = EQUALS;
        runParseOneOperation(filter, op);

        filter = "c20s1d1a1o6";
        op = NOT_EQUALS;
        runParseOneOperation(filter, op);

        filter = "c20s1d1a1o7";
        op = LIKE;
        runParseOneOperation(filter, op);
    }

    @Test
    public void parseFilterWith2Operations() throws Exception {
        filter = "a1c25s5dfirsto5a2c20s1d1o2l0";

        Node result = filterParser.parse(filter);
        assertNotNull(result);
        assertOperatorEquals(AND, result);
        assertEquals(2, result.childCount());
        assertOperatorEquals(EQUALS, result.getLeft());
        assertOperatorEquals(GREATER_THAN, result.getRight());
    }

    @Test
    public void parseLogicalAndOperator() throws Exception {
        filter = "a1c20s1d0o5a2c20s1d3o2l0";

        Node result = filterParser.parse(filter);
        assertNotNull(result);
        assertOperatorEquals(AND, result);
        assertEquals(2, result.childCount());
        assertOperatorEquals(EQUALS, result.getLeft());
        assertOperatorEquals(GREATER_THAN, result.getRight());
    }

    @Test
    public void parseLogicalOrOperator() throws Exception {
        // (_1_ = 0 OR _2_ > 3)
        filter = "a1c20s1d0o5a2c20s1d3o2l1";

        Node result = filterParser.parse(filter);
        assertNotNull(result);
        assertOperatorEquals(OR, result);
        assertEquals(2, result.childCount());
        assertOperatorEquals(EQUALS, result.getLeft());
        assertOperatorEquals(GREATER_THAN, result.getRight());

        assertTrue(result.getLeft().getLeft() instanceof ColumnIndexOperandNode);
        assertEquals(1, ((ColumnIndexOperandNode) result.getLeft().getLeft()).index());
        assertTrue(result.getLeft().getRight() instanceof ScalarOperandNode);
        assertEquals("0", ((ScalarOperandNode) result.getLeft().getRight()).getValue());

        assertTrue(result.getRight().getLeft() instanceof ColumnIndexOperandNode);
        assertEquals(2, ((ColumnIndexOperandNode) result.getRight().getLeft()).index());
        assertTrue(result.getRight().getRight() instanceof ScalarOperandNode);
        assertEquals("3", ((ScalarOperandNode) result.getRight().getRight()).getValue());
    }

    @Test
    public void parseLogicalNotOperator() throws Exception {
        // NOT (_1_ = 0)
        filter = "a1c20s1d0o5l2";

        Node result = filterParser.parse(filter);
        assertNotNull(result);
        assertOperatorEquals(Operator.NOT, result);
        assertEquals(1, result.childCount());
        assertOperatorEquals(Operator.EQUALS, result.getLeft());
    }

    @Test
    public void parseLogicalUnknownCodeError() {
        filter = "l7";
        Exception ex = assertThrows(
            FilterParser.FilterStringSyntaxException.class,
            () -> filterParser.parse(filter));
        assertEquals("unknown op ending at 2 (filter string: 'l7')", ex.getMessage());
    }

    @Test
    public void parseLogicalOperatorNotExpression() throws Exception {
        filter = "a1c25s5dfirsto5a2c20s1d2o2l0l2";

        Node result = filterParser.parse(filter);
        assertOperatorEquals(NOT, result);
        assertEquals(1, result.childCount());
        assertOperatorEquals(AND, result.getLeft());
        assertEquals(2, result.getLeft().childCount());
        assertOperatorEquals(EQUALS, result.getLeft().getLeft());
        assertOperatorEquals(GREATER_THAN, result.getLeft().getRight());
    }

    /*
     * Helper functions
     */
    private void runParseNegative(String description, String filter, String exception) {
        Exception ex = assertThrows(
            FilterParser.FilterStringSyntaxException.class,
            () -> filterParser.parse(filter),
            description);
        assertEquals(exception + filterStringMsg(filter), ex.getMessage());
    }

    private void runParseOneOperation(String filter, Operator op) throws Exception {
        Node result = filterParser.parse(filter);
        assertOperatorEquals(op, result);
    }

    private void runParseOneUnaryOperation(String filter, Operator op) throws Exception {
        Node result = filterParser.parse(filter);
        assertOperatorEquals(op, result);
    }

    private String filterStringMsg(String filter) {
        return " (filter string: '" + filter + "')";
    }

    private void assertOperatorEquals(Operator operator, Node node) {
        assertTrue(node instanceof OperatorNode);
        assertEquals(operator, ((OperatorNode) node).getOperator());
    }
}
