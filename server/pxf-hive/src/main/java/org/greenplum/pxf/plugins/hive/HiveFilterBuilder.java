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


import org.apache.hadoop.hive.serde.serdeConstants;
import org.greenplum.pxf.api.BasicFilter;
import org.greenplum.pxf.api.FilterParser;
import org.greenplum.pxf.api.LogicalFilter;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Uses the filter parser code to build a filter object, either simple - a
 * single {@link BasicFilter} object or a
 * compound - a {@link java.util.List} of
 * {@link BasicFilter} objects.
 * {@link HiveAccessor} will use the filter for
 * partition filtering.
 */
public class HiveFilterBuilder implements FilterParser.FilterBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(HiveFilterBuilder.class);

    private List<ColumnDescriptor> columnDescriptors;
    private boolean canPushdownIntegral;
    private Map<String, String> partitionKeys;

    private static final String HIVE_API_EQ = " = ";
    private static final String HIVE_API_LT = " < ";
    private static final String HIVE_API_GT = " > ";
    private static final String HIVE_API_LTE = " <= ";
    private static final String HIVE_API_GTE = " >= ";
    private static final String HIVE_API_NE = " <> ";
    private static final String HIVE_API_DQUOTE = "\"";

    /**
     * Translates a filterString into a {@link BasicFilter} or a
     * list of such filters.
     *
     * @param filterString the string representation of the filter
     * @return a single {@link BasicFilter}
     * object or a {@link java.util.List} of
     * {@link BasicFilter} objects.
     * @throws Exception if parsing the filter failed or filter is not a basic
     *                   filter or list of basic filters
     */
    public Object getFilterObject(String filterString) throws Exception {
        if (filterString == null)
            return null;

        FilterParser parser = new FilterParser(this);
        Object result = parser.parse(filterString.getBytes(FilterParser.DEFAULT_CHARSET));

        if (!(result instanceof LogicalFilter) && !(result instanceof BasicFilter)
                && !(result instanceof List)) {
            throw new Exception("String " + filterString
                    + " resolved to no filter");
        }

        return result;
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
    @SuppressWarnings("unchecked")
    public Object build(FilterParser.Operation opId, Object leftOperand,
                        Object rightOperand) throws Exception {
        // Assume column is on the left
        return handleSimpleOperations(opId,
                (FilterParser.ColumnIndex) leftOperand,
                (FilterParser.Constant) rightOperand);
    }

    @Override
    public Object build(FilterParser.Operation operation, Object operand) throws Exception {
        if (operation == FilterParser.Operation.HDOP_IS_NULL || operation == FilterParser.Operation.HDOP_IS_NOT_NULL) {
            // use null for the constant value of null comparison
            return handleSimpleOperations(operation, (FilterParser.ColumnIndex) operand, null);
        } else {
            throw new Exception("Unsupported unary operation " + operation);
        }
    }

    /*
     * Build filter string for HiveMetaStoreClient.listPartitionsByFilter API
     * method.
     *
     * The filter string parameter for
     * HiveMetaStoreClient.listPartitionsByFilter will be created from the
     * incoming getFragments filter string parameter. It will be in a format of:
     * [PARTITON1 NAME] = \"[PARTITON1 VALUE]\" AND [PARTITON2 NAME] =
     * \"[PARTITON2 VALUE]\" ... Filtering can be done only on string partition
     * keys. Integral partition keys are supported only if its enabled in Hive and PXF.
     *
     * For Example for query: SELECT * FROM TABLE1 WHERE part1 = 'AAAA' AND
     * part2 = '1111' For HIVE HiveMetaStoreClient.listPartitionsByFilter, the
     * incoming GPDB filter string will be mapped into :
     * "part1 = \"AAAA\" and part2 = \"1111\""
     *
     * Say P is a conforming predicate based on partition column and supported comparison operator
     * NP is a non conforming predicate based on either a non-partition column or an unsupported operator.
     * The following rule will be used during filter processing
     * P <op> P -> P <op> P (op can be any logical operator)
     * P AND NP -> P
     * P OR NP -> null
     * NP <op> NP -> null
     */
    public String buildFilterStringForHive(String filterInput) throws Exception {

        String filterString;

        LOG.debug("Filter string input: {}", filterInput);

        Object filter = new HiveFilterBuilder().getFilterObject(filterInput);

        if (filter instanceof LogicalFilter) {
            filterString = buildCompoundFilter((LogicalFilter) filter);
        } else {
            filterString = buildSingleFilter(filter, null);
        }

        return filterString;
    }

    private String buildCompoundFilter(LogicalFilter filter) {
        String logicalOperator;
        switch (filter.getOperator()) {
            case HDOP_AND:
                logicalOperator = " AND ";
                break;
            case HDOP_OR:
                logicalOperator = " OR ";
                break;
            default:
                // NOT not supported
                return null;
        }

        StringBuilder filterString = new StringBuilder();
        String serializedFilter;
        for (Object f : filter.getFilterList()) {
            if (f instanceof LogicalFilter) {
                serializedFilter = buildCompoundFilter((LogicalFilter) f);
            } else {
                serializedFilter = buildSingleFilter(f, filter.getOperator());
            }
            if (serializedFilter != null) {
                // We only append the operator if there is something on the filterString
                if (filterString.length() > 0) {
                    filterString.append(logicalOperator);
                }
                filterString.append(serializedFilter);
            } else if (filter.getOperator() == FilterParser.LogicalOperation.HDOP_OR) {
                // Case when one of the predicates is non-compliant and with OR operator
                // P OR NP -> null
                return null;
            }
        }

        return (filterString.length() > 0) ?
                String.format("(%s)", filterString.toString()) : null;
    }

    private boolean isFilterCompatible(String filterColumnName, FilterParser.Operation operation, FilterParser.LogicalOperation logicalOperation) {

        String colType = partitionKeys.get(filterColumnName);
        boolean isPartitionColumn = colType != null;

        boolean isIntegralSupported =
                canPushdownIntegral &&
                        (operation == FilterParser.Operation.HDOP_EQ || operation == FilterParser.Operation.HDOP_NE);

        boolean canPushDown = isPartitionColumn && (
                colType.equalsIgnoreCase(serdeConstants.STRING_TYPE_NAME) ||
                        isIntegralSupported && serdeConstants.IntegralTypes.contains(colType)
        );

        if (!canPushDown) {
            if (logicalOperation != null && logicalOperation == FilterParser.LogicalOperation.HDOP_OR) {
                return false;
            } else { // AND and NOT logical operators
                LOG.trace("Filter is on a non-partition column or on a partition column that is not supported for pushdown, ignore this filter for column: {}", filterColumnName);
                return false;
            }
        }
        return true;
    }

    /*
     * Build filter string for a single filter and append to the filters string.
     * Filter string shell be added if filter name match hive partition name
     * Single filter will be in a format of: [PARTITON NAME] = \"[PARTITON
     * VALUE]\"
     */
    private String buildSingleFilter(Object filter, FilterParser.LogicalOperation logicalOperation) {

        // Let's look first at the filter
        BasicFilter bFilter = (BasicFilter) filter;

        // Extract column name and value
        int filterColumnIndex = bFilter.getColumn().index();
        // Avoids NullPointerException in case of operations like HDOP_IS_NULL,
        // HDOP_IS_NOT_NULL where no constant value is passed as part of query
        String filterValue = bFilter.getConstant() != null ? bFilter.getConstant().constant().toString() : "";
        ColumnDescriptor filterColumn = columnDescriptors.get(filterColumnIndex);
        String filterColumnName = filterColumn.columnName();
        FilterParser.Operation operation = ((BasicFilter) filter).getOperation();

        // if filter is determined to be not being able to be pushed down, but not violating logical correctness,
        // we just skip it and return null
        if (!isFilterCompatible(filterColumnName, operation, logicalOperation)) {
            return null;
        }

        StringBuilder result = new StringBuilder(filterColumnName);
        switch (operation) {
            case HDOP_EQ:
                result.append(HIVE_API_EQ);
                break;
            case HDOP_LT:
                result.append(HIVE_API_LT);
                break;
            case HDOP_GT:
                result.append(HIVE_API_GT);
                break;
            case HDOP_LE:
                result.append(HIVE_API_LTE);
                break;
            case HDOP_GE:
                result.append(HIVE_API_GTE);
                break;
            case HDOP_NE:
                result.append(HIVE_API_NE);
                break;
            default:
                // Set filter string to blank in case of unimplemented operations
                result.setLength(0);
                return null;
        }

        result.append(HIVE_API_DQUOTE);
        result.append(filterValue);
        result.append(HIVE_API_DQUOTE);

        return result.toString();
    }

    /*
     * Handles simple column-operator-constant expressions Creates a special
     * filter in the case the column is the row key column
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

    public void setColumnDescriptors(List<ColumnDescriptor> columnDescriptors) {
        this.columnDescriptors = columnDescriptors;
    }

    public void setCanPushdownIntegral(boolean canPushdownIntegral) {
        this.canPushdownIntegral = canPushdownIntegral;
    }

    public void setPartitionKeys(Map<String, String> partitionKeys) {
        this.partitionKeys = partitionKeys;
    }
}
