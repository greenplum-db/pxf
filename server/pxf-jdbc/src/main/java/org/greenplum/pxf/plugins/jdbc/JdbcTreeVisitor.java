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

import org.apache.commons.lang.StringUtils;
import org.greenplum.pxf.api.filter.CollectionOperand;
import org.greenplum.pxf.api.filter.ColumnIndexOperand;
import org.greenplum.pxf.api.filter.Operand;
import org.greenplum.pxf.api.filter.ToStringTreeVisitor;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.greenplum.pxf.plugins.jdbc.utils.DbProduct;

import java.util.List;
import java.util.stream.Collectors;

/**
 * A filter parser. Converts filterString into List<BasicFilter>.
 * <p>
 * This class extends {@link \FilterBuilder} and implements its
 * public methods. These should not be used, though.
 */
public class JdbcTreeVisitor extends ToStringTreeVisitor {

    private final boolean hasPartition;
    private final DbProduct dbProduct;
    private final String quoteString;
    private final List<ColumnDescriptor> columnDescriptors;

    /**
     * Stores the index of the last processed column
     */
    protected int lastIndex;

    public JdbcTreeVisitor(List<ColumnDescriptor> tupleDescription) {
        this(null, null, false, tupleDescription);
    }

    public JdbcTreeVisitor(DbProduct dbProduct, String quoteString, boolean hasPartition, List<ColumnDescriptor> tupleDescription) {
        this.dbProduct = dbProduct;
        this.quoteString = quoteString;
        this.hasPartition = hasPartition;
        this.columnDescriptors = tupleDescription;
    }

    @Override
    public String toString() {
        if (sb.length() > 0) {
            if (hasPartition) {
                sb.insert(0, " WHERE (").append(")");
            } else {
                sb.insert(0, " WHERE ");
            }
        }
        return sb.toString();
    }

    @Override
    protected String getNodeValue(Operand operand) {
        if (operand instanceof ColumnIndexOperand) {
            ColumnIndexOperand columnIndexOperand = (ColumnIndexOperand) operand;

            /* We need the column index (column is guaranteed to be on the left,
             * so it always comes first. The column index is needed to get the
             * column type. The column type information is required to determine
             * how the value will be processed
             */
            lastIndex = columnIndexOperand.index();
            ColumnDescriptor columnDescriptor = columnDescriptors.get(lastIndex);
            return String.format("%s%s%s", quoteString, columnDescriptor.columnName(), quoteString);
        }

        // Obtain the datatype of the column
        ColumnDescriptor columnDescriptor = columnDescriptors.get(lastIndex);
        DataType type = DataType.get(columnDescriptor.columnTypeCode());

        if (operand instanceof CollectionOperand) {
            CollectionOperand collectionOperand = (CollectionOperand) operand;
            String listValue = collectionOperand.getData().stream()
                    .map(s -> serializeValue(type, s))
                    .collect(Collectors.joining(","));
            return String.format("(%s)", listValue);
        } else {
            String value = super.getNodeValue(operand);
            return serializeValue(type, value);
        }
    }

    protected String serializeValue(DataType type, String value) {
        switch (type) {
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT8:
            case REAL:
            case BOOLEAN:
                return value;
            case TEXT:
                return String.format("'%s'",
                        StringUtils.replace(value, "'", "''"));
            case DATE:
                // Date field has different format in different databases
                return dbProduct.wrapDate(value);
            case TIMESTAMP:
                // Timestamp field has different format in different databases
                return dbProduct.wrapTimestamp(value);
            default:
                throw new UnsupportedOperationException(String.format(
                        "Unsupported column type for filtering '%s' ", type.getOID()));
        }
    }

    protected List<ColumnDescriptor> getColumnDescriptors() {
        return columnDescriptors;
    }
}
