package org.greenplum.pxf.plugins.jdbc.partitioning;

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

import org.greenplum.pxf.plugins.jdbc.utils.DbProduct;

/**
 * A special type of partition: contains IS NULL or IS NOT NULL constraint.
 *
 * As it cannot be constructed (requested) by user, it has no type() method.
 *
 * Currently, only {@link NullPartition#NullPartition(String)} is used to construct this class.
 * In other words, IS NOT NULL is never used (but is supported).
 */
class NullPartition extends BasePartition implements JdbcFragmentMetadata {
    private static final long serialVersionUID = 0L;

    private final boolean isNull;

    /**
     * Construct a NullPartition with the given column and constraint
     * @param column
     * @param isNull true if IS NULL must be used
     */
    public NullPartition(String column, boolean isNull) {
        super(column);
        this.isNull = isNull;
    }

    /**
     * Construct a NullPartition with the given column and IS NULL constraint
     * @param column
     */
    public NullPartition(String column) {
        this(column, true);
    }

    @Override
    public String toSqlConstraint(String quoteString, DbProduct dbProduct) {
        if (quoteString == null) {
            throw new RuntimeException("Quote string cannot be null");
        }

        StringBuilder sb = new StringBuilder();

        String columnQuoted = quoteString + column + quoteString;

        sb.append(
            columnQuoted
        ).append(
            isNull ? " IS NULL" : " IS NOT NULL"
        );

        return sb.toString();
    }

    /**
     * Getter
     */
    public boolean isNull() {
        return isNull;
    }
}
