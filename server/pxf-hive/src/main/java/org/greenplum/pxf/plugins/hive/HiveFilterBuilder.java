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
import org.greenplum.pxf.api.BaseFilterBuilder;
import org.greenplum.pxf.api.BasicFilter;
import org.greenplum.pxf.api.FilterParser;
import org.greenplum.pxf.api.io.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.EnumSet;
import java.util.Map;

/**
 * Uses the filter parser code to build a filter object, either simple - a
 * single {@link BasicFilter} object or a
 * compound - a {@link java.util.List} of
 * {@link BasicFilter} objects.
 * {@link HiveAccessor} will use the filter for
 * partition filtering.
 * <p>
 * Build filter string for HiveMetaStoreClient.listPartitionsByFilter API
 * method.
 * <p>
 * The filter string parameter for
 * HiveMetaStoreClient.listPartitionsByFilter will be created from the
 * incoming getFragments filter string parameter. It will be in a format of:
 * [PARTITON1 NAME] = \"[PARTITON1 VALUE]\" AND [PARTITON2 NAME] =
 * \"[PARTITON2 VALUE]\" ... Filtering can be done only on string partition
 * keys. Integral partition keys are supported only if its enabled in Hive and PXF.
 * <p>
 * For Example for query: SELECT * FROM TABLE1 WHERE part1 = 'AAAA' AND
 * part2 = '1111' For HIVE HiveMetaStoreClient.listPartitionsByFilter, the
 * incoming GPDB filter string will be mapped into :
 * "part1 = \"AAAA\" and part2 = \"1111\""
 * <p>
 * Say P is a conforming predicate based on partition column and supported comparison operator
 * NP is a non conforming predicate based on either a non-partition column or an unsupported operator.
 * The following rule will be used during filter processing
 * P <op> P -> P <op> P (op can be any logical operator)
 * P AND NP -> P
 * P OR NP -> null
 * NP <op> NP -> null
 */
public class HiveFilterBuilder extends BaseFilterBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(HiveFilterBuilder.class);
    private static final String HIVE_API_D_QUOTE = "\"";

    private static final EnumSet<FilterParser.Operator> SUPPORTED_OPERATORS =
            EnumSet.of(
                    FilterParser.Operator.EQUALS,
                    FilterParser.Operator.LESS_THAN,
                    FilterParser.Operator.GREATER_THAN,
                    FilterParser.Operator.LESS_THAN_OR_EQUAL,
                    FilterParser.Operator.GREATER_THAN_OR_EQUAL,
                    FilterParser.Operator.NOT_EQUALS
            );
    private static final EnumSet<FilterParser.Operator> SUPPORTED_LOGICAL_OPERATORS =
            EnumSet.of(
                    FilterParser.Operator.AND,
                    FilterParser.Operator.OR
            );

    private boolean canPushDownIntegral;
    private Map<String, String> partitionKeys;

    HiveFilterBuilder() {
        super(SUPPORTED_OPERATORS, SUPPORTED_LOGICAL_OPERATORS);
    }

    public void setCanPushDownIntegral(boolean canPushDownIntegral) {
        this.canPushDownIntegral = canPushDownIntegral;
    }

    public void setPartitionKeys(Map<String, String> partitionKeys) {
        this.partitionKeys = partitionKeys;
    }

    /**
     * Case when one of the predicates is non-compliant
     * and with OR operator P OR NP -> null
     *
     * @param logicalOperator the operator
     * @return true if it is compliant, false otherwise
     */
    @Override
    protected boolean canRightOperandBeOmitted(FilterParser.Operator logicalOperator) {
        return logicalOperator == FilterParser.Operator.AND;
    }

    /**
     * Filter string shall be added if filter name matches hive partition name
     * Single filter will be in a format of: [PARTITON NAME] = \"[PARTITON VALUE]\"
     *
     * @param filterColumnName the name of the column
     * @param operator         the operation
     * @param logicalOperator  the logical operator
     * @return true if filter is compatible, false otherwise
     */
    @Override
    protected boolean shouldIncludeFilter(String filterColumnName, FilterParser.Operator operator, FilterParser.Operator logicalOperator) {

        String colType = partitionKeys.get(filterColumnName);
        boolean isPartitionColumn = colType != null;

        boolean isIntegralSupported =
                canPushDownIntegral &&
                        (operator == FilterParser.Operator.EQUALS || operator == FilterParser.Operator.NOT_EQUALS);

        boolean canPushDown = isPartitionColumn && (
                colType.equalsIgnoreCase(serdeConstants.STRING_TYPE_NAME) ||
                        isIntegralSupported && serdeConstants.IntegralTypes.contains(colType)
        );

        if (!canPushDown) {
            if (logicalOperator == FilterParser.Operator.OR) {
                return false;
            } else { // AND and NOT logical operators
                LOG.trace("Filter is on a non-partition column or on a partition column that is not supported for pushdown, ignore this filter for column: {}", filterColumnName);
                return false;
            }
        }
        return true;
    }

    @Override
    protected String serializeValue(Object val, DataType type) {
        return String.format("%s%s%s", HIVE_API_D_QUOTE, val, HIVE_API_D_QUOTE);
    }
}
