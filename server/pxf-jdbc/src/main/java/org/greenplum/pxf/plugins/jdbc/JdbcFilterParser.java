package org.greenplum.pxf.plugins.jdbc;

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

import org.greenplum.pxf.api.BasicFilter;
import org.greenplum.pxf.api.FilterParser;
import org.greenplum.pxf.api.LogicalFilter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.text.ParseException;

/**
 * A filter parser. Converts filterString into List<BasicFilter>.
 *
 * This class extends {@link FilterParser.FilterBuilder} and implements its
 * public methods. These should not be used, though.
 */
public class JdbcFilterParser implements FilterParser.FilterBuilder {
    /**
     * Parse filter string and return List<BasicFilter>
     *
     * @param filterString
     *
     * @return List of 'BasicFilter' objects
     *
     * @throws UnsupportedOperationException if filter string contains filter that is not supported by PXF-JDBC
     * @throws ParseException if filter string is invalid
     */
    public static List<BasicFilter> parseFilters(String filterString) throws UnsupportedOperationException, ParseException {
        return convertBasicFilterList(getFilterObject(filterString), null);
    }

    @Override
    public Object build(FilterParser.LogicalOperation op, Object leftOperand, Object rightOperand) {
        return handleLogicalOperation(op, leftOperand, rightOperand);
    }

    @Override
    public Object build(FilterParser.LogicalOperation op, Object filter) {
        return handleLogicalOperation(op, filter);
    }

    @Override
    public Object build(FilterParser.Operation opId, Object leftOperand,
                        Object rightOperand) throws Exception {
        // Assume column is on the left
        return handleSimpleOperations(
            opId,
            (FilterParser.ColumnIndex) leftOperand,
            (FilterParser.Constant) rightOperand
        );
    }

    @Override
    public Object build(FilterParser.Operation operation, Object operand) throws UnsupportedOperationException {
        if (
            operation == FilterParser.Operation.HDOP_IS_NULL ||
            operation == FilterParser.Operation.HDOP_IS_NOT_NULL
        ) {
            // Use null for the constant value of null comparison
            return handleSimpleOperations(operation, (FilterParser.ColumnIndex) operand, null);
        }
        else {
            throw new UnsupportedOperationException("Unsupported unary operation '" + operation + "'");
        }
    }

    /*
     * Handles simple column-operator-constant expressions.
     * Creates a special filter in the case the column is the row key column
     */
    private BasicFilter handleSimpleOperations(FilterParser.Operation opId,
                                               FilterParser.ColumnIndex column,
                                               FilterParser.Constant constant) {
        return new BasicFilter(opId, column, constant);
    }

    private Object handleLogicalOperation(FilterParser.LogicalOperation operator, Object leftOperand, Object rightOperand) {
        List<Object> result = new LinkedList<>();

        result.add(leftOperand);
        result.add(rightOperand);
        return new LogicalFilter(operator, result);
    }

    private Object handleLogicalOperation(FilterParser.LogicalOperation operator, Object filter) {
        return new LogicalFilter(operator, Arrays.asList(filter));
    }

    /**
     * Translates a filterString into a {@link BasicFilter} or a
     * list of such filters.
     *
     * @param filterString the string representation of the filter
     * @return a {@link BasicFilter} or a {@link List} of {@link BasicFilter}.
     * @throws ParseException if parsing the filter failed or filter is not a basic filter or list of basic filters
     */
    private static Object getFilterObject(String filterString) throws ParseException {
        try {
            FilterParser parser = new FilterParser(new JdbcFilterParser());
            Object result = parser.parse(filterString.getBytes(FilterParser.DEFAULT_CHARSET));

            if (
                !(result instanceof LogicalFilter) &&
                !(result instanceof BasicFilter) &&
                !(result instanceof List)
            ) {
                throw new Exception("'" + filterString + "' could not be resolved to a filter");
            }
            return result;
        }
        catch (Exception e) {
            throw new ParseException(e.getMessage(), 0);
        }
    }

    /**
     * Convert filter object into a list of {@link BasicFilter}
     *
     * @param filter Filter object
     * @param returnList A list of {@link BasicFilter} to append filters to. Must be null if the function is not called recursively
     *
     * @return list of filters
     */
    private static List<BasicFilter> convertBasicFilterList(Object filter, List<BasicFilter> returnList) throws UnsupportedOperationException {
        if (returnList == null) {
            returnList = new ArrayList<>();
        }

        if (filter instanceof BasicFilter) {
            returnList.add((BasicFilter) filter);
            return returnList;
        }

        LogicalFilter lfilter = (LogicalFilter) filter;
        if (lfilter.getOperator() != FilterParser.LogicalOperation.HDOP_AND) {
            throw new UnsupportedOperationException("Logical operation '" + lfilter.getOperator() + "' is not supported");
        }
        for (Object f : lfilter.getFilterList()) {
            returnList = convertBasicFilterList(f, returnList);
        }

        return returnList;
    }
}
