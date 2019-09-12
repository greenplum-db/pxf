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
import org.greenplum.pxf.api.*;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.greenplum.pxf.plugins.jdbc.utils.DbProduct;

import java.util.EnumSet;
import java.util.List;

/**
 * A filter parser. Converts filterString into List<BasicFilter>.
 * <p>
 * This class extends {@link \FilterBuilder} and implements its
 * public methods. These should not be used, though.
 */
public class JdbcFilterParser extends BaseFilterBuilder {

    private static final EnumSet<FilterParser.Operation> SUPPORTED_OPERATIONS =
            EnumSet.of(
                    FilterParser.Operation.HDOP_LT,
                    FilterParser.Operation.HDOP_GT,
                    FilterParser.Operation.HDOP_LE,
                    FilterParser.Operation.HDOP_GE,
                    FilterParser.Operation.HDOP_EQ,
                    FilterParser.Operation.HDOP_LIKE,
                    FilterParser.Operation.HDOP_NE,
                    // TODO: In is not supported?
                    // FilterParser.Operation.HDOP_IN,
                    FilterParser.Operation.HDOP_IS_NULL,
                    FilterParser.Operation.HDOP_IS_NOT_NULL
            );
    private static final EnumSet<FilterParser.LogicalOperation> SUPPORTED_OPERATORS =
            EnumSet.of(
                    FilterParser.LogicalOperation.HDOP_AND,
                    FilterParser.LogicalOperation.HDOP_NOT,
                    FilterParser.LogicalOperation.HDOP_OR
            );

    private final boolean hasPartition;
    private final DbProduct dbProduct;
    private final String quoteString;

    public JdbcFilterParser(DbProduct dbProduct, String quoteString, boolean hasPartition, List<ColumnDescriptor> tupleDescription) {
        super(SUPPORTED_OPERATIONS, SUPPORTED_OPERATORS);
        this.dbProduct = dbProduct;
        this.quoteString = quoteString;
        this.hasPartition = hasPartition;
        setColumnDescriptors(tupleDescription);
    }

    @Override
    public String buildFilterString(String filterInput) throws Exception {
        String filterString = super.buildFilterString(filterInput);
        if (StringUtils.isNotBlank(filterString)) {
            String clause = " WHERE %s";
            if (hasPartition) {
                clause = " WHERE (%s)";
            }
            return String.format(clause, filterString);
        } else {
            return "";
        }
    }

    @Override
    protected String getColumnName(ColumnDescriptor column) {
        return String.format("%s%s%s", quoteString, column.columnName(), quoteString);
    }

    @Override
    protected boolean isCompliantWithOperator(FilterParser.LogicalOperation operator) {
        return true;
    }

    @Override
    protected boolean isFilterCompatible(String filterColumnName, FilterParser.Operation operation, FilterParser.LogicalOperation logicalOperation) {
        return true;
    }

    @Override
    protected String serializeValue(Object val, DataType type) {
        switch (type) {
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT8:
            case REAL:
            case BOOLEAN:
                return val.toString();
            case TEXT:
                return String.format("'%s'",
                        StringUtils.replace(val.toString(), "'", "''"));
            case DATE:
                // Date field has different format in different databases
                return dbProduct.wrapDate(val);
            case TIMESTAMP:
                // Timestamp field has different format in different databases
                return dbProduct.wrapTimestamp(val);
            default:
                throw new UnsupportedOperationException(String.format(
                        "Unsupported column type for filtering '%s' ", type.getOID()));
        }
    }
}
