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
import org.greenplum.pxf.api.filter.ColumnPredicateBuilder;
import org.greenplum.pxf.api.filter.Node;
import org.greenplum.pxf.api.filter.ToStringTreeVisitor;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.greenplum.pxf.plugins.jdbc.utils.DbProduct;

import java.util.List;


/**
 * Converts an expression {@link Node} into a valid predicate for the
 * target {@link DbProduct}.
 * This class extends {@link ToStringTreeVisitor} and overrides the required
 * methods.
 */
public class JdbcPredicateBuilder extends ColumnPredicateBuilder {

    private final boolean hasPartition;
    private final DbProduct dbProduct;

    public JdbcPredicateBuilder(DbProduct dbProduct,
                                List<ColumnDescriptor> tupleDescription) {
        this(dbProduct, "", false, tupleDescription);
    }

    public JdbcPredicateBuilder(DbProduct dbProduct,
                                String quoteString,
                                boolean hasPartition,
                                List<ColumnDescriptor> tupleDescription) {
        super(quoteString, tupleDescription);
        this.dbProduct = dbProduct;
        this.hasPartition = hasPartition;
    }

    @Override
    public String toString() {
        if (sb.length() == 0) return "";

        if (hasPartition) {
            sb.insert(0, " WHERE (").append(")");
        } else {
            sb.insert(0, " WHERE ");
        }
        return sb.toString();
    }

    @Override
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
}
