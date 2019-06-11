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
import org.greenplum.pxf.plugins.jdbc.partitioning.PartitionType;

import java.io.Serializable;

public class NullPartition implements JdbcFragmentMetadata, Serializable {
    private static final long serialVersionUID = 1L;

    private final String column;
    private final boolean isNull;

    /**
     * Construct a NullPartition with given column and constraint type
     * @param column
     * @param isNull true if IS NULL must be used
     */
    public NullPartition(String column, boolean isNull) {
        assert column != null;

        this.column = column;
        this.isNull = isNull;
    }

    /**
     * Construct a NullPartition with given column and IS NULL constraint
     * @param column
     */
    public NullPartition(String column) {
        this(column, true);
    }

    @Override
    public String getColumn() {
        return column;
    }

    @Override
    public PartitionType getType() {
        return PartitionType.NULL;
    }

    @Override
    public String toSqlConstraint(String quoteString, DbProduct dbProduct) {
        assert quoteString != null;

        StringBuilder sb = new StringBuilder();

        String columnQuoted = quoteString + column + quoteString;

        sb.append(
            columnQuoted
        ).append(
            isNull ? " IS NULL" : " IS NOT NULL"
        );

        return sb.toString();
    }

    public boolean isNull() {
        return isNull;
    }
}
