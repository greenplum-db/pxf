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

import org.greenplum.pxf.api.filter.CollectionOperand;
import org.greenplum.pxf.api.filter.ColumnIndexOperand;
import org.greenplum.pxf.api.filter.Operand;
import org.greenplum.pxf.api.filter.ToStringTreeVisitor;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;

import java.util.List;
import java.util.stream.Collectors;

/**
 * A tree visitor to generate a filter string for partition filtering.
 * <p>
 * Build filter string for HiveMetaStoreClient.listPartitionsByFilter API
 * method.
 * <p>
 * The filter string parameter for HiveMetaStoreClient.listPartitionsByFilter
 * will be created from the incoming getFragments filter string parameter.
 * It will be in a format of:
 * [PARTITON1 NAME] = \"[PARTITON1 VALUE]\" AND [PARTITON2 NAME] =
 * \"[PARTITON2 VALUE]\" ... Filtering can be done only on string partition
 * keys. Integral partition keys are supported only if its enabled both in
 * Hive and PXF.
 * <p>
 * For Example for query: SELECT * FROM TABLE1 WHERE part1 = 'AAAA' AND
 * part2 = '1111' For HIVE HiveMetaStoreClient.listPartitionsByFilter, the
 * incoming GPDB filter string will be mapped into :
 * "part1 = \"AAAA\" and part2 = \"1111\""
 * <p>
 * Say P is a conforming predicate based on partition column and supported
 * comparison operator NP is a non conforming predicate based on either a
 * non-partition column or an unsupported operator.
 * <p>
 * The following rule will be used during filter processing
 * P <op> P -> P <op> P (op can be any logical operator)
 * P AND NP -> P
 * P OR NP -> null
 * NP <op> NP -> null
 */
public class HivePartitionFilterBuilder extends ToStringTreeVisitor {

    private static final String HIVE_API_D_QUOTE = "\"";

    private final List<ColumnDescriptor> columnDescriptors;

    public HivePartitionFilterBuilder(List<ColumnDescriptor> tupleDescription) {
        this.columnDescriptors = tupleDescription;
    }

    @Override
    public String toString() {
        return sb.length() > 0 ? sb.toString() : null;
    }

    @Override
    protected String getNodeValue(Operand operand) {
        if (operand instanceof ColumnIndexOperand) {
            ColumnIndexOperand columnIndexOperand = (ColumnIndexOperand) operand;
            ColumnDescriptor columnDescriptor = columnDescriptors.get(columnIndexOperand.index());
            return columnDescriptor.columnName();
        } else if (operand instanceof CollectionOperand) {
            CollectionOperand collectionOperand = (CollectionOperand) operand;
            String listValue = collectionOperand.getData().stream()
                    .map(s -> serializeValue(null, s))
                    .collect(Collectors.joining(","));
            return String.format("(%s)", listValue);
        } else {
            String value = super.getNodeValue(operand);
            return serializeValue(null, value);
        }
    }

    protected String serializeValue(DataType type, String value) {
        return String.format("%s%s%s", HIVE_API_D_QUOTE, value, HIVE_API_D_QUOTE);
    }

    /**
     * For testing purposes only
     */
    void reset() {
        sb.setLength(0);
    }
}
